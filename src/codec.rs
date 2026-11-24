//! Sentry Wire Codec: parses and re-serializes Sentry `Envelope` payloads
//! and legacy `/store/` JSON bodies, classifying each `Item` into an
//! `Item Category` and extracting `environment` metadata.
//!
//! Parsing never rejects a payload: unrecognized or corrupted bodies are
//! reported as [`Payload::Unrecognized`] so ingestion can fail open and
//! forward the raw bytes.

use serde_json::Value;

/// The rate-limiting classification of an `Item`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ItemCategory {
    Event,
    Transaction,
    Other,
}

impl ItemCategory {
    /// The wire name used in `X-Sentry-Rate-Limits` entries.
    pub fn as_str(self) -> &'static str {
        match self {
            ItemCategory::Event => "event",
            ItemCategory::Transaction => "transaction",
            ItemCategory::Other => "other",
        }
    }
}

/// Per-item metadata extracted by the codec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ItemSummary {
    pub category: ItemCategory,
    /// Item `environment` extracted from JSON item bodies, `"unknown"` when
    /// absent.
    pub environment: String,
}

const UNKNOWN_ENVIRONMENT: &str = "unknown";

/// What the codec recognized in an ingestion payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Payload {
    /// A multi-line Sentry `Envelope`.
    Envelope(Envelope),
    /// A legacy `/store/` JSON body treated as a single `event` `Item`.
    StoreEvent { environment: String },
    /// Anything the codec cannot understand; ingestion must fail open and
    /// forward the raw bytes.
    Unrecognized,
}

impl Payload {
    /// Parses an `Envelope` body (`\n`-separated header/item lines).
    pub fn parse_envelope(bytes: &[u8]) -> Payload {
        match Envelope::parse(bytes) {
            Ok(envelope) => Payload::Envelope(envelope),
            Err(EnvelopeError) => Payload::Unrecognized,
        }
    }

    /// Parses a legacy `/store/` JSON body as a single `event` `Item`.
    pub fn parse_store_event(bytes: &[u8]) -> Payload {
        match parse_json(bytes) {
            Some(Value::Object(_)) => Payload::StoreEvent {
                environment: environment_of_json(bytes)
                    .unwrap_or_else(|| UNKNOWN_ENVIRONMENT.into()),
            },
            _ => Payload::Unrecognized,
        }
    }
}

/// Why an `Envelope` could not be parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EnvelopeError;

/// A parsed Sentry `Envelope`: raw header line plus parsed `Item`s.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    header: Vec<u8>,
    items: Vec<EnvelopeItem>,
}

/// One parsed `Item`: its raw header line and raw body bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvelopeItem {
    header: Vec<u8>,
    body: Vec<u8>,
}

impl Envelope {
    fn parse(bytes: &[u8]) -> Result<Self, EnvelopeError> {
        let (header, mut rest) = split_line(bytes).ok_or(EnvelopeError)?;
        ensure_json_object(header)?;
        let mut items = Vec::new();
        while !rest.is_empty() {
            let (item_header, item_rest) = split_line(rest).ok_or(EnvelopeError)?;
            let header = parse_json(item_header).ok_or(EnvelopeError)?;
            let length = optional_length(&header)?;
            let (body, remainder) = match length {
                Some(length) => {
                    let (body, remainder) = item_rest
                        .split_at_checked(length)
                        .map(|(body, remainder)| (body.to_vec(), remainder))
                        .ok_or(EnvelopeError)?;
                    // A length-delimited `Item` body is followed by an
                    // optional newline before the next `Item` header.
                    let remainder = remainder.strip_prefix(b"\n").unwrap_or(remainder);
                    (body, remainder)
                }
                None => (item_rest.to_vec(), &[][..]),
            };
            rest = remainder;
            items.push(EnvelopeItem {
                header: item_header.to_vec(),
                body,
            });
        }
        Ok(Self {
            header: header.to_vec(),
            items,
        })
    }

    /// The envelope header's `event_id`, if present.
    pub fn event_id(&self) -> Option<String> {
        parse_json(&self.header)
            .and_then(|header| header.get("event_id").cloned())
            .and_then(|value| value.as_str().map(str::to_string))
    }

    /// Per-item category and environment summaries.
    pub fn summaries(&self) -> Vec<ItemSummary> {
        self.items
            .iter()
            .map(|item| ItemSummary {
                category: item.category(),
                environment: environment_of_json(&item.body)
                    .unwrap_or_else(|| UNKNOWN_ENVIRONMENT.into()),
            })
            .collect()
    }

    /// The parsed `Item`s with their raw header and body bytes.
    pub fn items(&self) -> &[EnvelopeItem] {
        &self.items
    }

    /// Returns a copy of this envelope keeping only the items whose mask
    /// entry is `true`. Returns `None` when no items would remain.
    ///
    /// # Panics
    ///
    /// Panics when `keep` does not have one entry per item.
    pub fn filter_items(&self, keep: &[bool]) -> Option<Self> {
        assert_eq!(keep.len(), self.items.len(), "filter mask length mismatch");
        let items = self
            .items
            .iter()
            .zip(keep)
            .filter(|(_, keep)| **keep)
            .map(|(item, _)| EnvelopeItem {
                header: item.header.clone(),
                body: item.body.clone(),
            })
            .collect::<Vec<_>>();
        (!items.is_empty()).then(|| Self {
            header: self.header.clone(),
            items,
        })
    }

    /// Re-serializes the `Envelope` into a wire-format byte string.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.header.clone();
        bytes.push(b'\n');
        for item in &self.items {
            bytes.extend_from_slice(&item.header);
            bytes.push(b'\n');
            bytes.extend_from_slice(&item.body);
            bytes.push(b'\n');
        }
        bytes
    }

    /// Merges compatible envelopes into one combined `Envelope`.
    ///
    /// Items keep their arrival order. A merge of several envelopes drops
    /// the per-envelope headers (an `event_id` header would be ambiguous
    /// across merged events); a merge of a single envelope preserves it
    /// unchanged. Items without an explicit `length` get one injected so
    /// that a lengthless item no longer swallows the following items.
    ///
    /// # Panics
    ///
    /// Panics when `envelopes` is empty.
    pub fn merge(envelopes: &[Self]) -> Self {
        assert!(
            !envelopes.is_empty(),
            "merging requires at least one envelope"
        );
        if envelopes.len() == 1 {
            return envelopes[0].clone();
        }
        let items = envelopes
            .iter()
            .flat_map(Self::items)
            .map(EnvelopeItem::with_explicit_length)
            .collect();
        Self {
            header: b"{}".to_vec(),
            items,
        }
    }
}

impl EnvelopeItem {
    fn category(&self) -> ItemCategory {
        let item_type = parse_json(&self.header).and_then(|header| header.get("type").cloned());
        match item_type.as_ref().and_then(Value::as_str) {
            Some("event") => ItemCategory::Event,
            Some("transaction") => ItemCategory::Transaction,
            _ => ItemCategory::Other,
        }
    }

    /// Returns a copy whose header carries an explicit `length` matching
    /// the body, so the item cannot swallow what follows it.
    fn with_explicit_length(&self) -> Self {
        let Some(mut header) = parse_json(&self.header) else {
            return self.clone();
        };
        if header.get("length").is_none() {
            header["length"] = Value::from(self.body.len() as u64);
        }
        Self {
            header: serde_json::to_vec(&header).unwrap_or_else(|_| self.header.clone()),
            body: self.body.clone(),
        }
    }
}

/// Splits the leading line (up to and excluding `\n`) from the rest.
fn split_line(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    let newline = bytes.iter().position(|&byte| byte == b'\n')?;
    let (line, rest) = bytes.split_at(newline);
    Some((line, &rest[1..]))
}

/// Parses bytes as a JSON value, returning `None` on any parse failure.
fn parse_json(bytes: &[u8]) -> Option<Value> {
    serde_json::from_slice(bytes).ok()
}

/// `true` when the bytes parse as a JSON object (map).
fn ensure_json_object(bytes: &[u8]) -> Result<(), EnvelopeError> {
    match parse_json(bytes) {
        Some(Value::Object(_)) => Ok(()),
        _ => Err(EnvelopeError),
    }
}

/// Reads the optional non-negative integer `length` field from an item
/// header, failing on non-integer or negative values.
fn optional_length(header: &Value) -> Result<Option<usize>, EnvelopeError> {
    match header.get("length") {
        None => Ok(None),
        Some(Value::Number(length)) => length
            .as_u64()
            .map(|length| Some(length as usize))
            .ok_or(EnvelopeError),
        Some(_) => Err(EnvelopeError),
    }
}

/// Extracts the `environment` field from a JSON item body.
fn environment_of_json(body: &[u8]) -> Option<String> {
    let value = parse_json(body)?;
    value
        .get("environment")
        .and_then(Value::as_str)
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds an envelope wire payload from header/item lines, filling the
    /// `length` field of `{LEN}` placeholders with the real body size.
    fn envelope(header: &str, items: &[(&str, &[u8])]) -> Vec<u8> {
        let mut payload = format!("{header}\n").into_bytes();
        for (index, (item_header, body)) in items.iter().enumerate() {
            let item_header = item_header.replace("{LEN}", &body.len().to_string());
            payload.extend_from_slice(item_header.as_bytes());
            payload.push(b'\n');
            payload.extend_from_slice(body);
            if index + 1 < items.len() {
                payload.push(b'\n');
            }
        }
        payload
    }

    fn multi_item_envelope() -> Vec<u8> {
        envelope(
            "{\"event_id\":\"84df5a5f-51d8-409a-9416-a1adb4b0e3d0\",\"sent_at\":\"2024-01-01T00:00:00Z\"}",
            &[
                (
                    "{\"type\":\"event\",\"length\":{LEN}}",
                    br#"{"environment":"production","message":"boom","timestamp":"2024-01-01T00:00:00Z"}"#,
                ),
                (
                    "{\"type\":\"transaction\"}",
                    br#"{"environment":"staging","trace_id":"abc"}"#,
                ),
            ],
        )
    }

    #[test]
    fn parses_multi_item_envelope_with_categories_and_environment() {
        let payload = Payload::parse_envelope(&multi_item_envelope());
        let Payload::Envelope(envelope) = payload else {
            panic!("expected an envelope payload");
        };
        assert_eq!(envelope.items().len(), 2);
        assert_eq!(
            envelope.event_id().as_deref(),
            Some("84df5a5f-51d8-409a-9416-a1adb4b0e3d0")
        );

        let items = envelope.summaries();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].category, ItemCategory::Event);
        assert_eq!(items[0].environment, "production");
        assert_eq!(items[1].category, ItemCategory::Transaction);
        assert_eq!(items[1].environment, "staging");
    }

    #[test]
    fn attachment_without_length_consumes_the_rest() {
        let payload = b"{\"event_id\":\"id-1\"}\n{\"type\":\"attachment\",\"length\":5}\n\x00\x01\x02\x03\x04\n{\"type\":\"event\",\"length\":2}\n{}";

        let envelope = Payload::parse_envelope(payload);
        let Payload::Envelope(envelope) = envelope else {
            panic!("expected an envelope payload");
        };

        assert_eq!(envelope.items().len(), 2);
        let items = envelope.summaries();
        assert_eq!(items[0].category, ItemCategory::Other);
        assert_eq!(items[0].environment, "unknown");
        assert_eq!(items[1].environment, "unknown");
    }

    #[test]
    fn unrecognized_bodies_fail_open() {
        for body in [
            &b"not json at all"[..],
            b"{}\nnot json",
            b"[]",
            b"\"string\"",
            b"{}",
        ] {
            assert_eq!(Payload::parse_envelope(body), Payload::Unrecognized);
        }
    }

    #[test]
    fn envelope_round_trips_through_serialize() {
        let payload = Payload::parse_envelope(&multi_item_envelope());
        let Payload::Envelope(envelope) = payload else {
            panic!("expected an envelope payload");
        };

        let serialized = envelope.to_bytes();
        let reparsed = Payload::parse_envelope(&serialized);
        assert_eq!(
            Payload::Envelope(Envelope::parse(&serialized).expect("reparse")),
            reparsed
        );
    }

    #[test]
    fn filter_items_rebuilds_an_envelope_with_only_kept_items() {
        let payload = Payload::parse_envelope(&multi_item_envelope());
        let Payload::Envelope(envelope) = payload else {
            panic!("expected an envelope payload");
        };

        // Drop the first (event) item, keep the second (attachment).
        let filtered = envelope.filter_items(&[false, true]).expect("kept items");

        assert_eq!(filtered.items().len(), 1);
        let serialized = filtered.to_bytes();
        let reparsed = Payload::parse_envelope(&serialized);
        let Payload::Envelope(reparsed) = reparsed else {
            panic!("filtered envelope must reparse");
        };
        assert_eq!(reparsed.items().len(), 1);
        assert_eq!(reparsed.summaries()[0].category, ItemCategory::Transaction);
        // Filtering out every item yields None.
        assert!(envelope.filter_items(&[false, false]).is_none());
    }

    /// Two envelopes sharing one `(project_id, credentials)` merge key,
    /// with mixed item types and one binary attachment body.
    fn envelope_with(id: &str, item_type: &str, body: &[u8]) -> Vec<u8> {
        let header = format!("{{\"event_id\":\"{id}\"}}");
        let item_header = format!("{{\"type\":\"{item_type}\",\"length\":{{LEN}}}}");
        envelope(&header, &[(&item_header, body)])
    }

    #[test]
    fn merge_combines_items_of_multiple_envelopes() {
        let first = Payload::parse_envelope(&envelope_with(
            "id-1",
            "event",
            br#"{"environment":"production","message":"boom"}"#,
        ));
        let second = Payload::parse_envelope(&envelope_with(
            "id-2",
            "transaction",
            br#"{"environment":"staging","trace_id":"abc"}"#,
        ));
        let (Payload::Envelope(first), Payload::Envelope(second)) = (first, second) else {
            panic!("expected envelope payloads");
        };

        let merged = Envelope::merge(&[first, second]);

        // The merged envelope drops per-event headers (event_id would be
        // ambiguous) and keeps every item in arrival order.
        assert_eq!(merged.event_id(), None);
        assert_eq!(merged.items().len(), 2);
        let summaries = merged.summaries();
        assert_eq!(summaries[0].category, ItemCategory::Event);
        assert_eq!(summaries[1].category, ItemCategory::Transaction);

        let serialized = merged.to_bytes();
        let reparsed = Payload::parse_envelope(&serialized);
        let Payload::Envelope(reparsed) = reparsed else {
            panic!("merged envelope must reparse");
        };
        assert_eq!(reparsed.items().len(), 2);
        assert_eq!(reparsed.summaries()[0].category, ItemCategory::Event);
        assert_eq!(reparsed.summaries()[1].category, ItemCategory::Transaction);
    }

    #[test]
    fn merge_preserves_binary_attachments_and_item_order() {
        let attachment_body: &[u8] = &[0x00, b'\n', 0xff, b'\r', 0x42];
        let first = Payload::parse_envelope(&envelope_with("id-1", "event", b"{}"));
        let second = Payload::parse_envelope(&envelope_with("id-2", "attachment", attachment_body));
        let (Payload::Envelope(first), Payload::Envelope(second)) = (first, second) else {
            panic!("expected envelope payloads");
        };

        let merged = Envelope::merge(&[first, second]);
        let serialized = merged.to_bytes();
        let Payload::Envelope(reparsed) = Payload::parse_envelope(&serialized) else {
            panic!("merged envelope must reparse");
        };

        assert_eq!(reparsed.items().len(), 2);
        assert_eq!(reparsed.summaries()[1].category, ItemCategory::Other);
        // Binary bytes survive the merge byte-for-byte.
        let items = reparsed.items();
        assert_eq!(items[1].body, attachment_body);
    }

    #[test]
    fn merge_injects_length_into_lengthless_items() {
        // A lengthless item consumes the rest of its envelope, so it can only
        // be last within its source envelope; merging must inject an explicit
        // `length` so it stops before the following envelope's items.
        let first = b"{\"event_id\":\"id-1\"}\n{\"type\":\"attachment\"}\nbinary-attachment\n";
        let second = b"{\"event_id\":\"id-2\"}\n{\"type\":\"event\",\"length\":2}\n{}\n";
        let first = Payload::parse_envelope(first);
        let second = Payload::parse_envelope(second);
        let (Payload::Envelope(first), Payload::Envelope(second)) = (first, second) else {
            panic!("expected envelope payloads");
        };
        assert_eq!(
            first.items().len(),
            1,
            "fixture must be one lengthless item"
        );
        // The codec treats the trailing separator as part of a lengthless
        // item's body; the merge must preserve that body byte-for-byte.
        let lengthless_body = first.items()[0].body.clone();

        let merged = Envelope::merge(&[first, second]);
        assert_eq!(merged.items().len(), 2);

        let serialized = merged.to_bytes();
        let Payload::Envelope(reparsed) = Payload::parse_envelope(&serialized) else {
            panic!("merged envelope must reparse");
        };
        assert_eq!(reparsed.items().len(), 2);
        assert_eq!(reparsed.items()[0].body, lengthless_body);
        assert_eq!(reparsed.items()[1].body, b"{}");
    }

    #[test]
    fn merge_of_a_single_envelope_preserves_it_unchanged() {
        let payload = Payload::parse_envelope(&multi_item_envelope());
        let Payload::Envelope(envelope) = payload else {
            panic!("expected an envelope payload");
        };

        let merged = Envelope::merge(std::slice::from_ref(&envelope));

        assert_eq!(merged, envelope);
        assert_eq!(
            merged.event_id().as_deref(),
            Some("84df5a5f-51d8-409a-9416-a1adb4b0e3d0")
        );
    }

    #[test]
    fn store_json_is_a_single_event_item() {
        let payload =
            Payload::parse_store_event(br#"{"event_id":"42","environment":"production"}"#);
        assert_eq!(
            payload,
            Payload::StoreEvent {
                environment: "production".into()
            }
        );
    }

    #[test]
    fn store_json_without_environment_is_still_an_event() {
        assert_eq!(
            Payload::parse_store_event(br#"{"event_id":42}"#),
            Payload::StoreEvent {
                environment: "unknown".into()
            }
        );
    }

    #[test]
    fn non_json_store_bodies_fail_open() {
        assert_eq!(
            Payload::parse_store_event(b"\x00\x01\x02"),
            Payload::Unrecognized
        );
        assert_eq!(
            Payload::parse_store_event(b"[1,2,3]"),
            Payload::Unrecognized
        );
    }
}
