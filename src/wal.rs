use rkyv::{Archive, Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

const WAL_FILE_PREFIX: &str = "wal-";
const WAL_FILE_EXT: &str = ".log";
const MAX_SEGMENT_SIZE: usize = 1024 * 1024 * 8;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Archive)]
#[rkyv(compare(PartialEq), derive(Debug))]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Archive)]
#[rkyv(compare(PartialEq), derive(Debug))]
enum WalVersion {
    V1 = 1,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Archive)]
#[rkyv(compare(PartialEq), derive(Debug))]
struct WalEntry {
    version: WalVersion,
    crc: u32,
    op: Op,
}

#[derive(Debug, Clone)]
struct WalWriter {
    current_segment: Arc<usize>,
    file: Arc<Mutex<BufWriter<File>>>,
    max_segment_size: usize,
    wal_dir: PathBuf,
}

#[derive(thiserror::Error, Debug)]
enum WalError {
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
            .open(segment_path)
        {
            Ok(Self {
                current_segment: Arc::new(segment_no),
                file: Arc::new(Mutex::new(BufWriter::new(wal_file))),
                max_segment_size: MAX_SEGMENT_SIZE,
                wal_dir: path.to_path_buf(),
            })
        } else {
            Err(WalError::InitFailed)
        }
    }

    pub(crate) fn write(&self, op: &Op) -> Result<usize, WalError> {
        let mut hasher = crc32fast::Hasher::new();
        match op {
            Op::Put(k, v) => {
                hasher.update(k);
                hasher.update(v);
            }
            Op::Delete(k) => {
                hasher.update(k);
            }
        };

        let entry = WalEntry {
            version: WalVersion::V1,
            crc: hasher.finalize(),
            op: op.clone(),
        };

        let buf = match rkyv::to_bytes::<rkyv::rancor::Error>(&entry) {
            Ok(b) => b,
            Err(_) => return Err(WalError::WriteFailure),
        };

        let mut file = self.file.lock().unwrap();
        match file.write(&buf) {
            Ok(total) => Ok(total),
            Err(_) => Err(WalError::WriteFailure),
        }
    }

    pub(crate) fn flush(&self) -> Result<(), WalError> {
        match self.file.lock() {
            Ok(mut file) => {
                let _ = file.flush();
                Ok(())
            }
            Err(_) => Err(WalError::WriteFailure),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use pretty_assertions::assert_eq;
    use temp_dir::TempDir;

    #[test]
    fn wal_writer_open_creates_wal_file() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path();

        let wal_writer = WalWriter::open(&PathBuf::from(path));
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

        let writer = WalWriter::open(&PathBuf::from(path))?;
        let wal_path = PathBuf::from(path).join("wal-00001.log");

        let key = Bytes::from("100");
        let value = Bytes::from("1234");
        let op = Op::Put(key.to_vec(), value.to_vec());

        writer.write(&op)?;
        writer.flush()?;

        let contents = std::fs::read(wal_path).unwrap();
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&key);
        hasher.update(&value);
        let expected_crc = hasher.finalize();
        let expected_wal_entry = WalEntry {
            version: WalVersion::V1,
            crc: expected_crc,
            op: op.clone(),
        };

        assert!(!contents.is_empty());

        let archived = rkyv::access::<ArchivedWalEntry, rkyv::rancor::Error>(&contents).unwrap();
        let deserialized_contents =
            rkyv::deserialize::<WalEntry, rkyv::rancor::Error>(archived).unwrap();

        assert_eq!(deserialized_contents, expected_wal_entry);
        Ok(())
    }
}
