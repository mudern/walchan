use std::{
    fs::{self, OpenOptions},
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    path::Path,
};

use thiserror::Error;

/// File magic for the WAL container header and record prelude (`b"WALC"` in LE).
pub const MAGIC: u32 = 0x5741_4C43;
pub const VERSION: u16 = 1;

/// Fixed header size (kept within one filesystem block).
pub const HEADER_SIZE: u64 = 4096;

/// Slot layout:
/// generation u64 | committed_off u64 | crc32 u32 | pad u32
pub const SLOT_SIZE: usize = 24;
pub const SLOT0_OFF: u64 = 0x0C;
pub const SLOT1_OFF: u64 = SLOT0_OFF + SLOT_SIZE as u64;

/// WAL-layer errors.
#[derive(Debug, Error)]
pub enum WalError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("codec error")]
    Codec(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("wal corrupted at offset {offset}: {reason}")]
    Corrupt { offset: u64, reason: String },

    #[error("invalid header: {0}")]
    BadHeader(String),
}

#[inline]
pub fn fsync_data(f: &std::fs::File) -> std::io::Result<()> {
    #[cfg(feature = "fsync")]
    {
        f.sync_data()
    }
    #[cfg(not(feature = "fsync"))]
    {
        let _ = f;
        Ok(())
    }
}

/// Ensure parent directory exists.
pub fn ensure_parent_dir(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.exists()
    {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

/// Append one record and return its byte length.
///
/// Record format (LE):
///
/// ```text
/// [magic u32][version u16][flags u16=0]
/// [len u32][payload bytes][crc32 u32]
/// ```
pub fn write_record(w: &mut std::fs::File, payload: &[u8]) -> std::io::Result<u64> {
    w.seek(SeekFrom::End(0))?;
    let start = w.stream_position()?;

    let mut header = [0u8; 12];
    header[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    header[4..6].copy_from_slice(&VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&0u16.to_le_bytes());
    header[8..12].copy_from_slice(&(payload.len() as u32).to_le_bytes());

    let crc = crc32fast::hash(payload);

    w.write_all(&header)?;
    w.write_all(payload)?;
    w.write_all(&crc.to_le_bytes())?;

    Ok(w.stream_position()? - start)
}

/// Strict record read: any inconsistency is an error.
pub fn read_record_strict(r: &mut std::fs::File, offset: u64) -> Result<(Vec<u8>, u64), WalError> {
    r.seek(SeekFrom::Start(offset))?;

    let mut buf4 = [0u8; 4];
    let mut buf2 = [0u8; 2];

    r.read_exact(&mut buf4)?;
    let magic = u32::from_le_bytes(buf4);
    if magic != MAGIC {
        return Err(WalError::Corrupt {
            offset,
            reason: format!("bad magic: {magic:#x}"),
        });
    }

    r.read_exact(&mut buf2)?;
    let ver = u16::from_le_bytes(buf2);
    if ver != VERSION {
        return Err(WalError::Corrupt {
            offset,
            reason: format!("unsupported version: {ver}"),
        });
    }

    r.read_exact(&mut buf2)?; // flags

    r.read_exact(&mut buf4)?;
    let len = u32::from_le_bytes(buf4) as usize;

    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload)?;

    r.read_exact(&mut buf4)?;
    let crc = u32::from_le_bytes(buf4);
    if crc != crc32fast::hash(&payload) {
        return Err(WalError::Corrupt {
            offset,
            reason: "crc mismatch".into(),
        });
    }

    Ok((payload, r.stream_position()?))
}

/// Lenient scan read used for recovery.
pub fn read_record_lenient(
    r: &mut std::fs::File,
    offset: u64,
    file_len: u64,
) -> Result<Option<u64>, WalError> {
    if offset >= file_len {
        return Ok(None);
    }

    match read_record_strict(r, offset) {
        Ok((_, next)) => Ok(Some(next)),
        Err(WalError::Io(e)) if e.kind() == ErrorKind::UnexpectedEof => Err(WalError::Io(e)),
        Err(e) => Err(e),
    }
}

fn slot_crc(generation: u64, committed_off: u64) -> u32 {
    let mut b = [0u8; 16];
    b[0..8].copy_from_slice(&generation.to_le_bytes());
    b[8..16].copy_from_slice(&committed_off.to_le_bytes());
    crc32fast::hash(&b)
}

fn read_slot(buf: &[u8]) -> (u64, u64, u32) {
    (
        u64::from_le_bytes(buf[0..8].try_into().unwrap()),
        u64::from_le_bytes(buf[8..16].try_into().unwrap()),
        u32::from_le_bytes(buf[16..20].try_into().unwrap()),
    )
}

fn write_slot(buf: &mut [u8], generation: u64, committed_off: u64) {
    let crc = slot_crc(generation, committed_off);
    buf[0..8].copy_from_slice(&generation.to_le_bytes());
    buf[8..16].copy_from_slice(&committed_off.to_le_bytes());
    buf[16..20].copy_from_slice(&crc.to_le_bytes());
    buf[20..24].copy_from_slice(&0u32.to_le_bytes());
}

/// Initialize WAL header if file is empty.
pub fn init_file_if_needed(f: &mut std::fs::File) -> Result<(), WalError> {
    if f.metadata()?.len() != 0 {
        return Ok(());
    }

    let mut header = vec![0u8; HEADER_SIZE as usize];
    header[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    header[4..6].copy_from_slice(&VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&(HEADER_SIZE as u16).to_le_bytes());

    let committed = HEADER_SIZE;
    write_slot(&mut header[SLOT0_OFF as usize..][..SLOT_SIZE], 1, committed);
    write_slot(&mut header[SLOT1_OFF as usize..][..SLOT_SIZE], 0, committed);

    f.seek(SeekFrom::Start(0))?;
    f.write_all(&header)?;
    f.flush()?;
    fsync_data(f)?;
    Ok(())
}

/// Read newest committed offset from header.
pub fn read_committed_from_header(f: &mut std::fs::File) -> Result<(u64, u64), WalError> {
    let mut header = vec![0u8; HEADER_SIZE as usize];
    f.seek(SeekFrom::Start(0))?;
    f.read_exact(&mut header)?;

    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    if magic != MAGIC {
        return Err(WalError::BadHeader(format!("bad magic: {magic:#x}")));
    }
    let ver = u16::from_le_bytes(header[4..6].try_into().unwrap());
    if ver != VERSION {
        return Err(WalError::BadHeader(format!("unsupported version: {ver}")));
    }
    let hs = u16::from_le_bytes(header[6..8].try_into().unwrap()) as u64;
    if hs != HEADER_SIZE {
        return Err(WalError::BadHeader(format!("unexpected header_size: {hs}")));
    }

    let (g0, o0, c0) = read_slot(&header[SLOT0_OFF as usize..][..SLOT_SIZE]);
    let (g1, o1, c1) = read_slot(&header[SLOT1_OFF as usize..][..SLOT_SIZE]);

    let v0 = c0 == slot_crc(g0, o0);
    let v1 = c1 == slot_crc(g1, o1);

    let sanitize = |o: u64| o.max(HEADER_SIZE);

    Ok(match (v0, v1) {
        (true, true) => {
            if g0 >= g1 {
                (sanitize(o0), g0)
            } else {
                (sanitize(o1), g1)
            }
        }
        (true, false) => (sanitize(o0), g0),
        (false, true) => (sanitize(o1), g1),
        _ => (HEADER_SIZE, 0),
    })
}

/// Commit offset to header using two-slot scheme.
pub fn write_committed_to_header(
    f: &mut std::fs::File,
    committed: u64,
    prev_generation: u64,
) -> Result<u64, WalError> {
    let generation = prev_generation.wrapping_add(1).max(1);
    let slot_off = if generation.is_multiple_of(2) {
        SLOT1_OFF
    } else {
        SLOT0_OFF
    };

    let mut slot = [0u8; SLOT_SIZE];
    write_slot(&mut slot, generation, committed.max(HEADER_SIZE));

    f.seek(SeekFrom::Start(slot_off))?;
    f.write_all(&slot)?;
    f.flush()?;
    fsync_data(f)?;
    Ok(generation)
}

/// Scan tail and truncate partial or corrupted records.
pub fn scan_and_recover_tail(
    f: &mut std::fs::File,
    start_off: u64,
    truncate: bool,
) -> Result<u64, WalError> {
    let file_len = f.metadata()?.len().max(HEADER_SIZE);
    let mut cur = start_off.max(HEADER_SIZE);
    if cur > file_len {
        cur = HEADER_SIZE;
    }
    let mut last_good = cur;

    loop {
        match read_record_lenient(f, cur, file_len) {
            Ok(Some(next)) => {
                last_good = next;
                cur = next;
            }
            Ok(None) => return Ok(file_len),
            Err(WalError::Io(e)) if e.kind() == ErrorKind::UnexpectedEof => {
                f.set_len(last_good)?;
                f.flush()?;
                fsync_data(f)?;
                return Ok(last_good);
            }
            Err(WalError::Corrupt { .. }) if truncate => {
                f.set_len(last_good)?;
                f.flush()?;
                fsync_data(f)?;
                return Ok(last_good);
            }
            Err(e) => return Err(e),
        }
    }
}

/// Count records after a committed offset.
pub fn count_records_from(path: &Path, start_off: u64, file_len: u64) -> Result<usize, WalError> {
    if file_len <= start_off {
        return Ok(0);
    }

    let mut f = OpenOptions::new().read(true).open(path)?;
    let mut cur = start_off.max(HEADER_SIZE);
    let mut cnt = 0usize;

    loop {
        match read_record_lenient(&mut f, cur, file_len) {
            Ok(Some(next)) => {
                cnt = cnt.saturating_add(1);
                cur = next;
            }
            Ok(None) => return Ok(cnt),
            Err(e) => return Err(e),
        }
    }
}
