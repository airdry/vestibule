//! Decompression of `Ingestion Route` bodies with streaming size limits.
//!
//! The proxy keeps the raw (still compressed) bytes in the `Ingestion
//! Queue` and forwards them with their original `Content-Encoding`;
//! decompression here exists only so the Sentry Wire Codec can inspect
//! payloads.

use std::io::Read;

use flate2::read::{GzDecoder, ZlibDecoder};

/// The decompression outcome for a request body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecompressedBody {
    /// The body was compressed and has been decompressed.
    Bytes(Vec<u8>),
    /// The `Content-Encoding` is not a supported compression; the raw body
    /// must be used as-is (fail open).
    Passthrough,
}

/// Why decompression could not produce a usable body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DecompressError {
    /// The decompressed body would exceed `ingest.max_decompressed_body_bytes`.
    #[error("decompressed body exceeds the configured limit")]
    TooLarge,
    /// The compressed stream is malformed; the raw bytes must fail open.
    #[error("corrupted compressed body")]
    Corrupt,
}

/// Decompresses `body` according to the `Content-Encoding`, streaming the
/// output and aborting as soon as it exceeds `max_bytes`.
///
/// Unsupported or absent encodings pass through untouched; a corrupt
/// compressed stream is reported as [`DecompressError::Corrupt`] so the
/// caller can fail open with the raw bytes.
///
/// # Errors
///
/// Returns [`DecompressError::TooLarge`] when the decompressed size
/// exceeds `max_bytes` and [`DecompressError::Corrupt`] on a broken
/// compressed stream.
pub fn decompress(
    encoding: &str,
    body: &[u8],
    max_bytes: u64,
) -> Result<DecompressedBody, DecompressError> {
    match normalize_encoding(encoding).as_deref() {
        Some("gzip") => decompress_with(Codec::Gzip, body, max_bytes),
        Some("deflate") => decompress_with(Codec::Deflate, body, max_bytes),
        Some("br") => decompress_with(Codec::Brotli, body, max_bytes),
        _ => Ok(DecompressedBody::Passthrough),
    }
}

/// The supported compression codecs.
enum Codec {
    Gzip,
    Deflate,
    Brotli,
}

impl Codec {
    fn decoder<'a>(&self, body: &'a [u8]) -> Box<dyn Read + 'a> {
        match self {
            Codec::Gzip => Box::new(GzDecoder::new(body)),
            Codec::Deflate => Box::new(ZlibDecoder::new(body)),
            Codec::Brotli => Box::new(brotli::Decompressor::new(body, 4096)),
        }
    }
}

/// Reads the whole decoder output while enforcing the size limit.
fn decompress_with(
    codec: Codec,
    body: &[u8],
    max_bytes: u64,
) -> Result<DecompressedBody, DecompressError> {
    let mut decoder = codec.decoder(body);
    let mut output = Vec::new();
    let mut chunk = [0; 4096];
    let mut produced: u64 = 0;
    loop {
        let read = decoder
            .read(&mut chunk)
            .map_err(|_| DecompressError::Corrupt)?;
        if read == 0 {
            return Ok(DecompressedBody::Bytes(output));
        }
        produced += read as u64;
        if produced > max_bytes {
            return Err(DecompressError::TooLarge);
        }
        output.extend_from_slice(&chunk[..read]);
    }
}

/// Normalizes the `Content-Encoding` header value.
fn normalize_encoding(raw: &str) -> Option<String> {
    let encoding = raw.trim().to_ascii_lowercase();
    match encoding.as_str() {
        "" | "identity" => None,
        _ => Some(encoding),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::Compression;
    use flate2::write::{GzEncoder, ZlibEncoder};

    fn gzip(data: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        std::io::Write::write_all(&mut encoder, data).unwrap();
        encoder.finish().unwrap()
    }

    fn deflate(data: &[u8]) -> Vec<u8> {
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        std::io::Write::write_all(&mut encoder, data).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn decompresses_gzip_deflate_and_brotli_payloads() {
        let body = b"{}";
        for (encoding, compressed) in [
            ("gzip", gzip(body)),
            ("deflate", deflate(body)),
            ("br", {
                let mut compressed = Vec::new();
                let mut encoder = brotli::CompressorWriter::new(&mut compressed, 4096, 5, 20);
                std::io::Write::write_all(&mut encoder, body).unwrap();
                drop(encoder);
                compressed
            }),
        ] {
            match decompress(encoding, &compressed, 1_024).expect("decompression succeeds") {
                DecompressedBody::Bytes(bytes) => assert_eq!(bytes, body, "for {encoding}"),
                DecompressedBody::Passthrough => panic!("{encoding} must be decompressed"),
            }
        }
    }

    #[test]
    fn unsupported_or_identity_encodings_pass_through() {
        for encoding in ["", "identity", "br, gzip", "unsupported"] {
            assert_eq!(
                decompress(encoding, b"raw", 1_024).expect("passthrough"),
                DecompressedBody::Passthrough
            );
        }
    }

    #[test]
    fn zip_bomb_is_cut_off_at_the_decompressed_limit() {
        // 1 MB of zeros compresses to a few dozen bytes.
        let compressed = gzip(&[0; 1024 * 1024]);

        let outcome = decompress("gzip", &compressed, 1024);

        assert_eq!(outcome, Err(DecompressError::TooLarge));
    }

    #[test]
    fn corrupt_compressed_bodies_fail_open_as_corrupt() {
        let compressed = gzip(b"{}");
        assert_eq!(
            decompress("gzip", &compressed[..compressed.len() - 1], 1_024),
            Err(DecompressError::Corrupt)
        );
        assert_eq!(
            decompress("gzip", b"garbage", 1_024),
            Err(DecompressError::Corrupt)
        );
    }
}
