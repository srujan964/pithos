use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use bytes::Buf;
use serde::{Deserialize, Serialize};

use crate::compaction::CompactionTask;

pub(crate) const MANIFEST_FILE: &str = "MANIFEST.json";

#[derive(Clone, Debug)]
pub(crate) struct SSTables {
    id: usize,
    path: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ManifestError {
    #[error("Failed to write record to manifest file")]
    RecordWriteFailed(#[from] std::io::Error),
    #[error("Unable to open manifest file at path: {0}")]
    RecoveryFailure(PathBuf),
    #[error("Unable to serialize/deserialize manifest record")]
    InvalidRecord(#[from] serde_json::Error),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    CompactionResult(CompactionTask, Vec<usize>),
}

#[derive(Clone, Debug)]
pub(crate) struct Manifest {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Manifest {
    pub(crate) fn create(dir: &Path) -> std::io::Result<Self> {
        let path = dir.join(MANIFEST_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub(crate) fn add_record(&self, record: ManifestRecord) -> Result<(), ManifestError> {
        let mut file = self.file.lock().unwrap();
        let buf = serde_json::to_vec(&record)?;
        let len: u64 = buf.len() as u64;
        file.write_all(&len.to_le_bytes())?;
        file.write_all(&buf)?;
        file.flush()?;
        Ok(())
    }

    pub(crate) fn recover(path: &Path) -> Result<(Self, Vec<ManifestRecord>), ManifestError> {
        let filepath = path.join(MANIFEST_FILE);
        let mut file = match OpenOptions::new().read(true).append(true).open(&filepath) {
            Ok(file) => file,
            Err(_) => return Err(ManifestError::RecoveryFailure(filepath)),
        };

        let mut records = Vec::new();

        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf = buf.as_slice();

        while buf.has_remaining() {
            let len = buf.get_u64_le() as usize;
            let record_buf = &buf[..len];
            buf.advance(len);
            let record = serde_json::from_slice::<ManifestRecord>(record_buf)?;
            records.push(record);
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(BufWriter::new(file))),
            },
            records,
        ))
    }

    pub(crate) fn file_exists(dir: &Path) -> bool {
        let filepath = dir.join(MANIFEST_FILE);
        std::fs::exists(filepath).unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use temp_dir::TempDir;

    use crate::{
        compaction::{CompactionTask, level::Task},
        manifest::{Manifest, ManifestRecord},
    };

    #[test]
    fn add_record_entry_to_manifest() -> Result<(), Box<dyn std::error::Error>> {
        let tempdir = TempDir::new().unwrap();
        let dir_path = tempdir.path();
        let manifest = Manifest::create(dir_path).unwrap();

        let memtable_record = ManifestRecord::NewMemtable(1);
        let flush_record = ManifestRecord::Flush(1);

        manifest.add_record(memtable_record.clone())?;
        manifest.add_record(flush_record.clone())?;

        let task = Task {
            input_level: Some(1),
            next_level: 2,
            files_to_compact: vec![1],
            overlapping_ssts: vec![2, 3],
        };
        let compaction_task = CompactionTask::Level(task);
        let compaction_record = ManifestRecord::CompactionResult(compaction_task, vec![2, 3, 4]);
        manifest.add_record(compaction_record.clone())?;

        let (_, records) = Manifest::recover(dir_path)?;
        let expected_records = vec![memtable_record, flush_record, compaction_record];

        assert_eq!(expected_records.len(), records.len());

        Ok(())
    }
}
