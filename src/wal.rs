//! Segmented disk WAL backing the `Ingestion Queue`.
//!
//! Append-only numbered segment files store fixed frames (length prefix +
//! CRC32 checksum + enqueue timestamp + route metadata + auth credentials +
//! payload bytes). The consumer checkpoint (segment + byte offset) is
//! updated atomically after confirmed `Upstream` delivery.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::config::{FsyncMode, QueueSettings};

/// Segment file prefix; segment ids are zero-padded in the file name.
const SEGMENT_PREFIX: &str = "wal-";
const SEGMENT_SUFFIX: &str = ".log";
/// Persisted consumer checkpoint (segment id + byte offset).
const CHECKPOINT_FILE: &str = "checkpoint";
/// Temp file used for atomic checkpoint replacement.
const CHECKPOINT_TMP_FILE: &str = "checkpoint.tmp";

/// A source of wall-clock time, injectable for deterministic TTL tests.
pub trait Clock: Send + Sync {
    /// Current time as milliseconds since the Unix epoch.
    fn now_unix_ms(&self) -> u64;
}

/// The system wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// A record appended to the `Ingestion Queue`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    /// When the record was accepted, in Unix epoch milliseconds.
    pub enqueued_at_unix_ms: u64,
    /// Route metadata, e.g. `/api/42/envelope/`.
    pub route: String,
    /// Auth credentials (headers and query) forwarded with the payload.
    pub auth: String,
    /// The raw payload bytes.
    pub body: Vec<u8>,
}

/// The point in the WAL up to which all records are fully handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Position {
    pub segment_id: u64,
    pub offset: u64,
}

/// A record read from the queue together with its commit position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRecord {
    pub record: WalRecord,
    /// Commit point at the end of this record's frame.
    pub position: Position,
}

/// Errors surfaced by the `Ingestion Queue`.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("wal io error in {context}: {source}")]
    Io {
        context: String,
        source: std::io::Error,
    },
}

/// Reads exactly 8 little-endian bytes as a `u64`.
fn le_u64(bytes: &[u8]) -> Result<u64, FrameError> {
    bytes
        .try_into()
        .map(u64::from_le_bytes)
        .map_err(|_| FrameError::Corrupt)
}

/// Reads exactly 4 little-endian bytes as a `u32`.
fn le_u32(bytes: &[u8]) -> Result<u32, FrameError> {
    bytes
        .try_into()
        .map(u32::from_le_bytes)
        .map_err(|_| FrameError::Corrupt)
}

/// Reads exactly 2 little-endian bytes as a `u16`.
fn le_u16(bytes: &[u8]) -> Result<u16, FrameError> {
    bytes
        .try_into()
        .map(u16::from_le_bytes)
        .map_err(|_| FrameError::Corrupt)
}

impl WalError {
    fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }
}

/// The durable `Ingestion Queue` stored in `queue.dir`.
pub struct Wal {
    dir: PathBuf,
    max_bytes: u64,
    segment_size_bytes: u64,
    max_age: Duration,
    fsync_mode: FsyncMode,
    clock: Arc<dyn Clock>,
    active_segment_id: u64,
    active_file: File,
    active_size: u64,
    total_size: u64,
    /// Consumer position: everything before it is committed.
    read_pos: Position,
}

impl Wal {
    /// Opens (or creates) the WAL in `settings.dir`, recovering from any
    /// incomplete or corrupt trailing frames.
    ///
    /// # Errors
    ///
    /// Returns a [`WalError`] when the directory or segment files cannot be
    /// read or written.
    pub fn open(
        dir: &Path,
        settings: &QueueSettings,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, WalError> {
        fs::create_dir_all(dir).map_err(|source| WalError::io("create queue dir", source))?;

        let segment_ids = list_segment_ids(dir)?;
        let active_segment_id = segment_ids.last().copied().unwrap_or(1);
        let active_file = open_segment_file(dir, active_segment_id)?;
        let active_size = active_file
            .metadata()
            .map_err(|source| WalError::io("read active segment metadata", source))?
            .len();

        let checkpoint = read_checkpoint(dir)?;
        let (read_pos, truncated) = recover(dir, checkpoint, &segment_ids)?;

        let wal = Self {
            dir: dir.to_path_buf(),
            max_bytes: settings.max_bytes,
            segment_size_bytes: settings.segment_size_bytes,
            max_age: settings.max_age,
            fsync_mode: settings.fsync_mode,
            clock,
            active_segment_id,
            active_file,
            active_size: if truncated == Some(active_segment_id) {
                file_size(&segment_path(dir, active_segment_id))?
            } else {
                active_size
            },
            total_size: total_segment_bytes(dir)?,
            read_pos,
        };
        wal.persist_checkpoint()?;
        Ok(wal)
    }

    /// Durably appends a record to the active segment.
    ///
    /// # Errors
    ///
    /// Returns a [`WalError`] when the record cannot be written.
    pub fn append(&mut self, route: &str, auth: &str, body: &[u8]) -> Result<(), WalError> {
        let frame = encode_frame(self.clock.now_unix_ms(), route, auth, body);

        if self.active_size > 0 && self.active_size + frame.len() as u64 > self.segment_size_bytes {
            self.rotate_segment()?;
        }

        self.active_file
            .write_all(&frame)
            .map_err(|source| WalError::io("append record", source))?;
        if self.fsync_mode == FsyncMode::EveryWrite {
            self.sync()?;
        }

        self.active_size += frame.len() as u64;
        self.total_size += frame.len() as u64;
        self.evict_overflowing_segments()?;
        Ok(())
    }

    /// Returns the oldest uncommitted record, skipping (and committing past)
    /// records older than the `max_age` TTL.
    ///
    /// `None` means the queue is drained. Records are returned again until
    /// committed, giving at-least-once delivery semantics.
    ///
    /// # Errors
    ///
    /// Returns a [`WalError`] when segment files cannot be read.
    pub fn next_record(&mut self) -> Result<Option<PendingRecord>, WalError> {
        loop {
            let segment_id = match existing_segment_at_or_after(&self.dir, self.read_pos.segment_id)
            {
                Some(segment_id) => segment_id,
                None => return Ok(None),
            };
            let offset = if segment_id == self.read_pos.segment_id {
                self.read_pos.offset
            } else {
                0
            };

            let mut file = open_segment_file(&self.dir, segment_id)?;
            match read_frame_at(&mut file, offset) {
                Ok(Some(frame)) => {
                    let frame_end = offset + frame.len as u64;
                    if self
                        .clock
                        .now_unix_ms()
                        .saturating_sub(frame.record.enqueued_at_unix_ms)
                        > self.max_age.as_millis() as u64
                    {
                        self.commit(Position {
                            segment_id,
                            offset: frame_end,
                        })?;
                        continue;
                    }
                    return Ok(Some(PendingRecord {
                        record: frame.record,
                        position: Position {
                            segment_id,
                            offset: frame_end,
                        },
                    }));
                }
                Ok(None) => {
                    // Segment drained: continue with the next one, if any.
                    if segment_id == self.active_segment_id {
                        return Ok(None);
                    }
                    self.read_pos = Position {
                        segment_id: segment_id + 1,
                        offset: 0,
                    };
                }
                Err(FrameError::Incomplete | FrameError::Corrupt) => {
                    return Ok(None);
                }
            }
        }
    }

    /// Atomically advances the consumer checkpoint to `position`.
    ///
    /// # Errors
    ///
    /// Returns a [`WalError`] when the checkpoint cannot be persisted.
    pub fn commit(&mut self, position: Position) -> Result<(), WalError> {
        if position > self.read_pos {
            self.read_pos = position;
            self.persist_checkpoint()?;
        }
        Ok(())
    }

    /// Flushes buffered writes to disk.
    ///
    /// # Errors
    ///
    /// Returns a [`WalError`] when fsync fails.
    pub fn sync(&mut self) -> Result<(), WalError> {
        self.active_file
            .sync_data()
            .map_err(|source| WalError::io("fsync segment", source))
    }

    fn rotate_segment(&mut self) -> Result<(), WalError> {
        self.active_segment_id += 1;
        self.active_file = open_segment_file(&self.dir, self.active_segment_id)?;
        self.active_size = 0;
        Ok(())
    }

    /// Enforces `max_bytes` with `drop_oldest`: deletes the oldest whole
    /// closed segment(s) and advances the consumer cursor cleanly.
    fn evict_overflowing_segments(&mut self) -> Result<(), WalError> {
        while self.total_size > self.max_bytes {
            let Some(oldest) = list_segment_ids(&self.dir)?
                .into_iter()
                .find(|id| *id < self.active_segment_id)
            else {
                break;
            };

            let size = file_size(&segment_path(&self.dir, oldest))?;
            fs::remove_file(segment_path(&self.dir, oldest))
                .map_err(|source| WalError::io("evict overflowed segment", source))?;
            self.total_size -= size;

            if self.read_pos.segment_id <= oldest {
                self.read_pos = Position {
                    segment_id: self.active_segment_id,
                    offset: 0,
                };
                self.persist_checkpoint()?;
            }
        }
        Ok(())
    }

    fn persist_checkpoint(&self) -> Result<(), WalError> {
        let tmp_path = self.dir.join(CHECKPOINT_TMP_FILE);
        let mut tmp = File::create(&tmp_path)
            .map_err(|source| WalError::io("create checkpoint temp file", source))?;
        tmp.write_all(&self.read_pos.segment_id.to_le_bytes())
            .and_then(|()| tmp.write_all(&self.read_pos.offset.to_le_bytes()))
            .and_then(|()| tmp.sync_data())
            .map_err(|source| WalError::io("write checkpoint", source))?;
        drop(tmp);
        fs::rename(&tmp_path, self.dir.join(CHECKPOINT_FILE))
            .map_err(|source| WalError::io("replace checkpoint", source))
    }
}

fn segment_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(format!("{SEGMENT_PREFIX}{segment_id:020}{SEGMENT_SUFFIX}"))
}

fn open_segment_file(dir: &Path, segment_id: u64) -> Result<File, WalError> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(segment_path(dir, segment_id))
        .map_err(|source| WalError::io("open segment", source))
}

fn file_size(path: &Path) -> Result<u64, WalError> {
    path.metadata()
        .map(|metadata| metadata.len())
        .map_err(|source| WalError::io("read file size", source))
}

fn list_segment_ids(dir: &Path) -> Result<Vec<u64>, WalError> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(dir).map_err(|source| WalError::io("list queue dir", source))? {
        let entry = entry.map_err(|source| WalError::io("read queue dir entry", source))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(id) = parse_segment_id(name) {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

fn parse_segment_id(name: &str) -> Option<u64> {
    name.strip_prefix(SEGMENT_PREFIX)?
        .strip_suffix(SEGMENT_SUFFIX)?
        .parse()
        .ok()
}

/// The smallest existing segment id at least `segment_id`.
fn existing_segment_at_or_after(dir: &Path, segment_id: u64) -> Option<u64> {
    list_segment_ids(dir)
        .ok()?
        .into_iter()
        .find(|id| *id >= segment_id)
}

fn total_segment_bytes(dir: &Path) -> Result<u64, WalError> {
    list_segment_ids(dir)?
        .iter()
        .map(|id| file_size(&segment_path(dir, *id)))
        .sum()
}

fn read_checkpoint(dir: &Path) -> Result<Option<Position>, WalError> {
    let bytes = match fs::read(dir.join(CHECKPOINT_FILE)) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(WalError::io("read checkpoint", source)),
    };
    if bytes.len() != 16 {
        return Err(WalError::io(
            "read checkpoint",
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("expected 16 checkpoint bytes, got {}", bytes.len()),
            ),
        ));
    }
    Ok(Some(Position {
        segment_id: le_u64(&bytes[..8]).map_err(|_| {
            WalError::io(
                "parse checkpoint",
                std::io::Error::new(std::io::ErrorKind::InvalidData, "checkpoint too short"),
            )
        })?,
        offset: le_u64(&bytes[8..]).map_err(|_| {
            WalError::io(
                "parse checkpoint",
                std::io::Error::new(std::io::ErrorKind::InvalidData, "checkpoint too short"),
            )
        })?,
    }))
}

/// Scans segments from the checkpoint, truncating incomplete or corrupt
/// trailing frames, and returns the resumed consumer position plus the id
/// of the segment that was truncated (if any).
fn recover(
    dir: &Path,
    checkpoint: Option<Position>,
    segment_ids: &[u64],
) -> Result<(Position, Option<u64>), WalError> {
    let Some(from) = checkpoint else {
        let start = Position {
            segment_id: segment_ids.first().copied().unwrap_or(1),
            offset: 0,
        };
        return Ok((start, None));
    };

    let mut position = from;
    let mut truncated = None;

    for &segment_id in segment_ids.iter().filter(|id| **id >= from.segment_id) {
        let path = segment_path(dir, segment_id);
        let start_offset = if segment_id == from.segment_id {
            from.offset
        } else {
            0
        };
        let size = file_size(&path)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| WalError::io("open segment for recovery", source))?;
        let mut offset = start_offset;
        while offset < size {
            match read_frame_at(&mut file, offset) {
                Ok(Some(frame)) => offset += frame.len as u64,
                Ok(None) => break,
                Err(FrameError::Incomplete | FrameError::Corrupt) => {
                    file.set_len(offset)
                        .map_err(|source| WalError::io("truncate corrupt tail", source))?;
                    truncated = Some(segment_id);
                    break;
                }
            }
        }
        if segment_id == from.segment_id && offset < start_offset {
            position = Position { segment_id, offset };
        }
    }

    Ok((position, truncated))
}

/// A frame with its encoded length and decoded record.
struct Frame {
    record: WalRecord,
    len: usize,
}

#[derive(Debug)]
enum FrameError {
    /// A partial frame was found (crash mid-write).
    Incomplete,
    /// A complete-looking frame failed checksum or decoding.
    Corrupt,
}

/// Reads the frame starting at `offset`; `Ok(None)` means clean EOF.
fn read_frame_at(file: &mut File, offset: u64) -> Result<Option<Frame>, FrameError> {
    if file
        .seek(SeekFrom::Start(offset))
        .map_err(|_| FrameError::Corrupt)?
        != offset
    {
        return Ok(None);
    }

    let mut header = [0_u8; 8];
    match read_exact(file, &mut header) {
        ReadOutcome::Eof => return Ok(None),
        ReadOutcome::Partial => return Err(FrameError::Incomplete),
        ReadOutcome::Full => {}
    }
    let data_len = le_u32(&header[..4])? as usize;
    let stored_crc = le_u32(&header[4..])?;

    let mut data = vec![0_u8; data_len];
    match read_exact(file, &mut data) {
        ReadOutcome::Eof | ReadOutcome::Partial => return Err(FrameError::Incomplete),
        ReadOutcome::Full => {}
    }

    if crc32fast::hash(&data) != stored_crc {
        return Err(FrameError::Corrupt);
    }

    decode_frame(&data).map(|record| {
        Some(Frame {
            record,
            len: data_len + 8,
        })
    })
}

enum ReadOutcome {
    Full,
    Partial,
    Eof,
}

fn read_exact(file: &mut File, buffer: &mut [u8]) -> ReadOutcome {
    let mut filled = 0;
    while filled < buffer.len() {
        match file.read(&mut buffer[filled..]) {
            Ok(0) => {
                return if filled == 0 {
                    ReadOutcome::Eof
                } else {
                    ReadOutcome::Partial
                };
            }
            Ok(bytes) => filled += bytes,
            Err(source) if source.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => return ReadOutcome::Partial,
        }
    }
    ReadOutcome::Full
}

fn decode_frame(data: &[u8]) -> Result<WalRecord, FrameError> {
    let mut cursor: usize = 0;
    let take = |cursor: &mut usize, size: usize| -> Result<&[u8], FrameError> {
        let end = cursor.checked_add(size).ok_or(FrameError::Corrupt)?;
        let slice = data.get(*cursor..end).ok_or(FrameError::Corrupt)?;
        *cursor = end;
        Ok(slice)
    };

    let enqueued_at_unix_ms = le_u64(take(&mut cursor, 8)?)?;
    let route_len = usize::from(le_u16(take(&mut cursor, 2)?)?);
    let route = String::from_utf8_lossy(take(&mut cursor, route_len)?).into_owned();
    let auth_len = usize::from(le_u16(take(&mut cursor, 2)?)?);
    let auth = String::from_utf8_lossy(take(&mut cursor, auth_len)?).into_owned();
    let body_len = le_u32(take(&mut cursor, 4)?)? as usize;
    let body = take(&mut cursor, body_len)?.to_vec();

    if cursor != data.len() {
        return Err(FrameError::Corrupt);
    }

    Ok(WalRecord {
        enqueued_at_unix_ms,
        route,
        auth,
        body,
    })
}

fn encode_frame(enqueued_at_unix_ms: u64, route: &str, auth: &str, body: &[u8]) -> Vec<u8> {
    let mut data = Vec::with_capacity(16 + route.len() + auth.len() + body.len());
    data.extend_from_slice(&enqueued_at_unix_ms.to_le_bytes());
    data.extend_from_slice(&(route.len() as u16).to_le_bytes());
    data.extend_from_slice(route.as_bytes());
    data.extend_from_slice(&(auth.len() as u16).to_le_bytes());
    data.extend_from_slice(auth.as_bytes());
    data.extend_from_slice(&(body.len() as u32).to_le_bytes());
    data.extend_from_slice(body);

    let mut frame = Vec::with_capacity(8 + data.len());
    frame.extend_from_slice(&(data.len() as u32).to_le_bytes());
    frame.extend_from_slice(&crc32fast::hash(&data).to_le_bytes());
    frame.extend_from_slice(&data);
    frame
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::config::QueueSettings;

    /// A controllable clock for deterministic TTL tests.
    struct MockClock {
        now_unix_ms: Mutex<u64>,
    }

    impl MockClock {
        fn new(start_unix_ms: u64) -> Arc<Self> {
            Arc::new(Self {
                now_unix_ms: Mutex::new(start_unix_ms),
            })
        }

        fn advance_ms(&self, millis: u64) {
            *self.now_unix_ms.lock().expect("lock clock") += millis;
        }
    }

    impl Clock for MockClock {
        fn now_unix_ms(&self) -> u64 {
            *self.now_unix_ms.lock().expect("lock clock")
        }
    }

    fn test_settings(dir: &Path) -> QueueSettings {
        QueueSettings {
            dir: dir.to_path_buf(),
            max_bytes: 1024 * 1024,
            segment_size_bytes: 1024 * 1024,
            max_age: std::time::Duration::from_secs(3600),
            overflow_policy: crate::config::OverflowPolicy::DropOldest,
            fsync_mode: crate::config::FsyncMode::EveryWrite,
            fsync_interval: std::time::Duration::from_millis(200),
        }
    }

    fn tempdir() -> PathBuf {
        tempfile::TempDir::new().expect("create tempdir").keep()
    }

    fn record_body(index: usize) -> Vec<u8> {
        format!("body-{index}").into_bytes()
    }

    fn append_record(wal: &mut Wal, index: usize) {
        wal.append(
            &format!("/api/{index}/envelope/"),
            &format!("auth-{index}"),
            &record_body(index),
        )
        .expect("append record");
    }

    fn segment_files(dir: &Path) -> Vec<PathBuf> {
        let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
            .expect("read wal dir")
            .map(|entry| entry.expect("dir entry").path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
            .collect();
        files.sort();
        files
    }

    #[test]
    fn appended_records_round_trip_after_reopen() {
        let dir = tempdir();
        let clock = MockClock::new(1_000);

        let mut wal = Wal::open(&dir, &test_settings(&dir), clock).expect("open wal");
        append_record(&mut wal, 1);
        append_record(&mut wal, 2);
        drop(wal);

        let mut wal =
            Wal::open(&dir, &test_settings(&dir), MockClock::new(1_000)).expect("reopen wal");
        let first = wal
            .next_record()
            .expect("read first")
            .expect("first record");
        assert_eq!(first.record.route, "/api/1/envelope/");
        assert_eq!(first.record.auth, "auth-1");
        assert_eq!(first.record.body, record_body(1));
        wal.commit(first.position).expect("commit first");

        let second = wal
            .next_record()
            .expect("read second")
            .expect("second record");
        assert_eq!(second.record.body, record_body(2));
        wal.commit(second.position).expect("commit second");

        assert!(wal.next_record().expect("read drained").is_none());
    }

    #[test]
    fn rotates_segments_at_segment_size() {
        let dir = tempdir();
        let mut settings = test_settings(&dir);
        settings.segment_size_bytes = 128;

        let mut wal = Wal::open(&dir, &settings, MockClock::new(1_000)).expect("open wal");
        for index in 1..=10 {
            append_record(&mut wal, index);
        }

        assert!(
            segment_files(&dir).len() > 1,
            "expected several segments, got {:?}",
            segment_files(&dir)
        );

        drop(wal);
        let mut wal = Wal::open(&dir, &settings, MockClock::new(1_000)).expect("reopen wal");
        for index in 1..=10 {
            let pending = wal.next_record().expect("read").expect("pending record");
            assert_eq!(pending.record.body, record_body(index));
            wal.commit(pending.position).expect("commit");
        }
    }

    #[test]
    fn recovers_from_crash_truncated_tail() {
        let dir = tempdir();
        let mut wal =
            Wal::open(&dir, &test_settings(&dir), MockClock::new(1_000)).expect("open wal");
        append_record(&mut wal, 1);
        append_record(&mut wal, 2);
        drop(wal);

        // Simulate a crash mid-write: chop some bytes off the last segment.
        let segment = &segment_files(&dir)[0];
        let full_len = std::fs::metadata(segment).expect("segment metadata").len();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(segment)
            .expect("open segment");
        file.set_len(full_len - 5).expect("truncate segment");
        drop(file);

        let mut wal =
            Wal::open(&dir, &test_settings(&dir), MockClock::new(1_000)).expect("reopen wal");
        let pending = wal
            .next_record()
            .expect("read")
            .expect("first record survives");
        assert_eq!(pending.record.body, record_body(1));
        wal.commit(pending.position).expect("commit");
        assert!(wal.next_record().expect("read truncated").is_none());

        // New appends work after recovery.
        append_record(&mut wal, 3);
        let pending = wal
            .next_record()
            .expect("read")
            .expect("record after recovery");
        assert_eq!(pending.record.body, record_body(3));
    }

    #[test]
    fn recovers_from_crc_corrupted_tail_frame() {
        let dir = tempdir();
        let mut wal =
            Wal::open(&dir, &test_settings(&dir), MockClock::new(1_000)).expect("open wal");
        append_record(&mut wal, 1);
        append_record(&mut wal, 2);
        drop(wal);

        // Corrupt the payload of the last frame (flip a byte near the end).
        let segment = &segment_files(&dir)[0];
        let full_len = std::fs::metadata(segment).expect("segment metadata").len() as usize;
        let mut bytes = std::fs::read(segment).expect("read segment");
        bytes[full_len - 1] ^= 0xff;
        std::fs::write(segment, bytes).expect("write corrupted segment");

        let mut wal =
            Wal::open(&dir, &test_settings(&dir), MockClock::new(1_000)).expect("reopen wal");
        let pending = wal
            .next_record()
            .expect("read")
            .expect("first record survives");
        assert_eq!(pending.record.body, record_body(1));
        wal.commit(pending.position).expect("commit");
        assert!(wal.next_record().expect("read corrupted").is_none());
    }

    #[test]
    fn drop_oldest_evicts_closed_segments_and_advances_cursor() {
        let dir = tempdir();
        let mut settings = test_settings(&dir);
        settings.segment_size_bytes = 200;
        settings.max_bytes = 300;

        let mut wal = Wal::open(&dir, &settings, MockClock::new(1_000)).expect("open wal");
        for index in 1..=8 {
            append_record(&mut wal, index);
        }

        let total: u64 = segment_files(&dir)
            .iter()
            .map(|path| std::fs::metadata(path).expect("metadata").len())
            .sum();
        assert!(
            total <= settings.max_bytes + settings.segment_size_bytes,
            "wal size {total} exceeds the drop_oldest budget"
        );

        // The consumer cursor must start after the evicted oldest segment:
        // the freshest records remain readable in order, and eviction did
        // happen (some of the oldest records are gone).
        let all_bodies: Vec<Vec<u8>> = (1..=8).map(record_body).collect();
        let mut seen = Vec::new();
        while let Some(pending) = wal.next_record().expect("read") {
            seen.push(pending.record.body);
            wal.commit(pending.position).expect("commit");
        }
        assert!(!seen.is_empty(), "at least the newest records must survive");
        assert!(
            seen.len() < all_bodies.len(),
            "expected drop_oldest to evict some records, all {seen:?} survived"
        );
        assert_eq!(
            seen,
            all_bodies[all_bodies.len() - seen.len()..],
            "surviving records must be the freshest ones in order"
        );
    }

    #[test]
    fn skips_records_older_than_max_age_ttl() {
        let dir = tempdir();
        let mut settings = test_settings(&dir);
        settings.max_age = std::time::Duration::from_secs(60);

        let clock = MockClock::new(1_000);
        let mut wal = Wal::open(&dir, &settings, Arc::clone(&clock) as _).expect("open wal");
        append_record(&mut wal, 1);

        // Fresh record is returned.
        let pending = wal.next_record().expect("read").expect("fresh record");
        assert_eq!(pending.record.body, record_body(1));
        wal.commit(pending.position).expect("commit");

        // Expire the queue, then append a fresh record after the expiry.
        clock.advance_ms(61_000);
        append_record(&mut wal, 2);
        let pending = wal
            .next_record()
            .expect("read")
            .expect("record after expiry");
        assert_eq!(pending.record.body, record_body(2));
        wal.commit(pending.position).expect("commit");

        // Now let every remaining record go stale: nothing is delivered.
        append_record(&mut wal, 3);
        clock.advance_ms(61_000);
        assert!(wal.next_record().expect("read").is_none());

        // And the queue keeps accepting fresh records after the stale skip.
        append_record(&mut wal, 4);
        let pending = wal
            .next_record()
            .expect("read")
            .expect("fresh record after skip");
        assert_eq!(pending.record.body, record_body(4));
    }

    #[test]
    fn persists_checkpoint_across_reopen() {
        let dir = tempdir();
        let mut wal =
            Wal::open(&dir, &test_settings(&dir), MockClock::new(1_000)).expect("open wal");
        append_record(&mut wal, 1);
        append_record(&mut wal, 2);

        let pending = wal.next_record().expect("read").expect("first record");
        wal.commit(pending.position).expect("commit");
        drop(wal);

        let mut wal =
            Wal::open(&dir, &test_settings(&dir), MockClock::new(1_000)).expect("reopen wal");
        let pending = wal.next_record().expect("read").expect("second record");
        assert_eq!(pending.record.body, record_body(2));
        wal.commit(pending.position).expect("commit");
        assert!(wal.next_record().expect("read").is_none());
    }

    #[test]
    fn returns_uncommitted_record_again_for_at_least_once_delivery() {
        let dir = tempdir();
        let mut wal =
            Wal::open(&dir, &test_settings(&dir), MockClock::new(1_000)).expect("open wal");
        append_record(&mut wal, 1);

        let first = wal.next_record().expect("read").expect("first read");
        let second = wal
            .next_record()
            .expect("read")
            .expect("uncommitted replay");
        assert_eq!(first, second);
    }
}
