use bytes::BufMut;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

const WAL_FILE_PREFIX: &str = "wal-";
const WAL_FILE_EXT: &str = ".log";
const MAX_SEGMENT_SIZE: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

impl From<&Op> for u8 {
    fn from(val: &Op) -> Self {
        match val {
            Op::Put(_, _) => 0,
            Op::Delete(_) => 1,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
#[non_exhaustive]
enum WalVersion {
    V1 = 1,
}

impl From<WalVersion> for u8 {
    fn from(value: WalVersion) -> Self {
        match value {
            WalVersion::V1 => 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct WalEntry {
    version: WalVersion,
    crc: u32,
    op: Op,
}

#[derive(Debug, Clone)]
pub(crate) struct WalWriter {
    current_segment: Arc<usize>,
    file: Arc<Mutex<BufWriter<File>>>,
    max_segment_size: usize,
    wal_dir: PathBuf,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum WalError {
    #[error("Failed to initalize WAL")]
    InitFailed,
    #[error("Unable to write WAL entry")]
    WriteFailure,
}

impl WalWriter {
    pub(crate) fn open(path: &PathBuf) -> Result<Self, WalError> {
        let segment_no = 1;
        let segment_path =
            PathBuf::from(path).join(format!("{WAL_FILE_PREFIX}{:0>5}{WAL_FILE_EXT}", segment_no));

        if let Ok(wal_file) = OpenOptions::new()
            .read(true)
            .append(true)
            .create_new(true)
            .open(&segment_path)
        {
            Ok(Self {
                current_segment: Arc::new(segment_no),
                file: Arc::new(Mutex::new(BufWriter::new(wal_file))),
                max_segment_size: MAX_SEGMENT_SIZE,
                wal_dir: path.to_path_buf(),
            })
        } else {
            eprintln!(
                "Attempted to create WAL file that already exists in path: {:?}",
                &segment_path
            );
            Err(WalError::InitFailed)
        }
    }

    pub(crate) fn write(&self, op: &Op) -> Result<usize, WalError> {
        let mut hasher = crc32fast::Hasher::new();
        match op {
            Op::Put(k, v) => {
                hasher.update(&[op.into()]);
                hasher.update(k);
                hasher.update(v);
            }
            Op::Delete(k) => {
                hasher.update(&[op.into()]);
                hasher.update(k);
            }
        };

        let buf = encode(WalVersion::V1, hasher.finalize(), op);

        let mut file = self.file.lock().unwrap();
        match file.write(&buf) {
            Ok(total) => Ok(total),
            Err(_) => Err(WalError::WriteFailure),
        }
    }

    pub(crate) fn flush(&self) -> std::io::Result<()> {
        let mut guard = self.file.lock().unwrap();
        guard.flush()?;
        Ok(())
    }
}

fn encode(version: WalVersion, crc: u32, op: &Op) -> Vec<u8> {
    let mut buf: Vec<u8> = vec![];

    buf.put_u8(version.into());
    buf.put_u32_le(crc);
    buf.put_u8(op.into());

    match op {
        Op::Put(k, v) => {
            let key_len = k.len();
            buf.put_u16_le(key_len as u16);
            buf.put(k as &[u8]);

            let val_len = v.len();
            buf.put_u16_le(val_len as u16);
            buf.put(v as &[u8]);
        }
        Op::Delete(k) => {
            let key_len = k.len();
            buf.put_u16_le(key_len as u16);
            buf.put(k as &[u8]);
        }
    }

    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;
    use bytes::Bytes;
    use pretty_assertions::assert_eq;
    use temp_dir::TempDir;

    #[test]
    fn wal_writer_open_creates_wal_file() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path();

        let wal_writer = WalWriter::open(&path.to_path_buf());
        assert!(wal_writer.is_ok());

        let expected_path = PathBuf::from(path).join("wal-00001.log");
        let result = std::fs::exists(wal_writer.unwrap().wal_dir);
        assert!(result.is_ok());
        assert!(result.unwrap());

        let wal_exists = std::fs::exists(expected_path);
        assert!(wal_exists.is_ok());
        assert!(wal_exists.unwrap());
    }

    #[test]
    fn wal_writer_writes_op_log() -> Result<(), Box<dyn std::error::Error>> {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path();

        let writer = WalWriter::open(&path.to_path_buf())?;
        let wal_path = PathBuf::from(path).join("wal-00001.log");

        let key = Bytes::from("100");
        let value = Bytes::from("1234");
        let op_one = Op::Put(key.to_vec(), value.to_vec());
        let op_two = Op::Delete(key.to_vec());

        writer.write(&op_one)?;
        writer.write(&op_two)?;
        writer.flush()?;

        let contents = std::fs::read(wal_path).unwrap();
        assert!(!contents.is_empty());

        let mut buf: &[u8] = contents.as_ref();
        assert_eq!(u8::from(WalVersion::V1), buf.get_u8());

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&[(&op_one).into()]);
        hasher.update(&key);
        hasher.update(&value);
        let expected_crc = hasher.finalize();
        assert_eq!(expected_crc, buf.get_u32_le());

        assert_eq!(u8::from(&op_one), buf.get_u8());

        let key_len = buf.get_u16_le() as usize;
        assert_eq!(key, &buf[..key_len]);
        buf.advance(key_len);

        let value_len = buf.get_u16_le() as usize;
        assert_eq!(value, &buf[..value_len]);
        buf.advance(value_len);

        assert_eq!(u8::from(WalVersion::V1), buf.get_u8());

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&[(&op_two).into()]);
        hasher.update(&key);
        let expected_crc = hasher.finalize();
        assert_eq!(expected_crc, buf.get_u32_le());

        assert_eq!(u8::from(&op_two), buf.get_u8());

        let key_len = buf.get_u16_le() as usize;
        assert_eq!(key, &buf[..key_len]);
        buf.advance(key_len);

        assert!(buf.is_empty());
        Ok(())
    }
}
