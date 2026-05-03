use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, SystemTime},
};

use bytes::Bytes;

use crate::{
    block,
    compaction::level::LeveledCompactionOptions,
    core::{CoreStorageInner, OrchestrationError, State},
    iterator::{StorageIter, merge_iterator::MultiMergeIterator},
    memtable::Buffer,
    sst::{self, SSTable, iterator::ConcatenatingIterator},
    types::Value,
};

pub(crate) mod level;

#[derive(Clone, Debug)]
pub enum CompactionOptions {
    Leveled(LeveledCompactionOptions),
}

impl Default for CompactionOptions {
    fn default() -> Self {
        CompactionOptions::Leveled(LeveledCompactionOptions {
            level_size_multiplier: 2,
            max_levels: 3,
            l0_num_files_threshold: 2,
            base_level_size_mb: 128,
            max_output_size_mb: 64,
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) enum CompactionTask {
    Level(level::Task),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CompactionError {
    #[error("L0 threshold needs to be > 1")]
    InvalidL0Threshold,
}

#[derive(Clone, Debug)]
pub(crate) struct Level {
    files: Vec<FileMetadata>,
    size: usize,
    max_size: usize,
}

impl Level {
    pub(crate) fn new(files: &[FileMetadata], max_size: usize) -> Self {
        let total_size = files.iter().map(|file| file.size).sum::<u64>() as usize;

        Self {
            files: files.to_vec(),
            size: total_size,
            max_size,
        }
    }

    pub(crate) fn size(&self) -> usize {
        self.size
    }

    pub(crate) fn max_size(&self) -> usize {
        self.max_size
    }

    pub(crate) fn files(&self) -> &[FileMetadata] {
        &self.files
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FileMetadata {
    id: usize,
    smallest: Vec<u8>,
    largest: Vec<u8>,
    size: u64,
    created_at: SystemTime,
}

impl FileMetadata {
    pub(crate) fn from_sstable(sstable: &Arc<SSTable>) -> Self {
        Self {
            id: sstable.id(),
            smallest: sstable.first_key().to_vec(),
            largest: sstable.last_key().to_vec(),
            size: sstable.table_size() as u64,
            created_at: sstable
                .created_at()
                .expect("SSTable should have a created_at value associated to it"),
        }
    }

    pub(crate) fn smallest_key(&self) -> &[u8] {
        self.smallest.as_ref()
    }

    pub(crate) fn largest_key(&self) -> &[u8] {
        self.largest.as_ref()
    }

    pub(crate) fn size(&self) -> usize {
        self.size as usize
    }

    pub(crate) fn id(&self) -> usize {
        self.id
    }
}

impl<B: Buffer + Clone + Sync + Send + 'static> CoreStorageInner<B> {
    pub(crate) fn generate_compaction_task(&self, state: &State<B>) -> Option<CompactionTask> {
        match &self.options.compaction_opts {
            CompactionOptions::Leveled(opts) => {
                let task = level::pick_compaction(state, &opts)?;
                Some(CompactionTask::Level(task))
            }
        }
    }

    fn compact_and_build_sst(
        &self,
        iter: impl StorageIter<KeyVal = (Bytes, Value)>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SSTable>>, OrchestrationError> {
        let mut new_ssts: Vec<Arc<SSTable>> = Vec::new();

        let sst_id = self.next_memtable_id();
        let path = self.get_sst_path(sst_id);
        let sst = match sst::build_sstable(
            sst_id,
            iter,
            block::BLOCK_SIZE,
            path,
            compact_to_bottom_level,
        ) {
            Ok(sst) => Arc::new(sst),
            Err(e) => {
                eprintln!("Error buildling SST: {:?}", e);
                return Err(OrchestrationError::CompactionFailure);
            }
        };

        new_ssts.push(sst);
        Ok(new_ssts)
    }

    pub(crate) fn compact(
        &self,
        task: &CompactionTask,
    ) -> Result<Vec<Arc<SSTable>>, OrchestrationError> {
        let CompactionTask::Level(task) = task;
        let state = self.state.load_full();
        let CompactionOptions::Leveled(opts) = &self.options.compaction_opts;

        let upper_ssts: Vec<_> = task
            .files_to_compact
            .iter()
            .filter_map(|id| state.sstables.get(id).cloned())
            .collect();

        let lower_ssts: Vec<_> = task
            .overlapping_ssts
            .iter()
            .filter_map(|id| state.sstables.get(id).cloned())
            .collect();

        let compact_to_bottom = task.next_level == opts.max_levels;

        let upper_iter = ConcatenatingIterator::new_and_seek_to_first(upper_ssts)
            .map_err(|_| OrchestrationError::CompactionFailure)?;

        if lower_ssts.is_empty() {
            return self.compact_and_build_sst(upper_iter, compact_to_bottom);
        }

        let lower_iter = ConcatenatingIterator::new_and_seek_to_first(lower_ssts)
            .map_err(|_| OrchestrationError::CompactionFailure)?;

        let iter = MultiMergeIterator::new(upper_iter, lower_iter);
        self.compact_and_build_sst(iter, compact_to_bottom)
    }

    pub(crate) fn apply_compaction_result(
        &self,
        state: &State<B>,
        task: &CompactionTask,
        output: &[usize],
    ) -> (State<B>, Vec<usize>) {
        let CompactionTask::Level(task) = task;
        let mut state = state.clone();
        let mut files_to_remove: Vec<usize> = vec![];

        let mut upper_ids: HashSet<usize> = task.files_to_compact.iter().copied().collect();
        let mut lower_ids: HashSet<usize> = task.overlapping_ssts.iter().copied().collect();

        match task.input_level {
            None => {
                state.level_zero = state
                    .level_zero
                    .iter()
                    .filter_map(|id| {
                        if upper_ids.remove(id) {
                            None
                        } else {
                            Some(*id)
                        }
                    })
                    .collect();
            }
            Some(lvl) => {
                state.levels[lvl - 1].1 = state.levels[lvl - 1]
                    .1
                    .iter()
                    .filter_map(|id| {
                        if upper_ids.remove(id) {
                            None
                        } else {
                            Some(*id)
                        }
                    })
                    .collect();
            }
        }

        files_to_remove.extend(&task.files_to_compact);
        files_to_remove.extend(&task.overlapping_ssts);

        let mut new_lower: Vec<usize> = state.levels[task.next_level - 1]
            .1
            .iter()
            .filter_map(|id| {
                if lower_ids.remove(id) {
                    None
                } else {
                    Some(*id)
                }
            })
            .collect();

        new_lower.extend(output);
        new_lower.sort_by(|a, b| {
            state
                .sstables
                .get(a)
                .unwrap()
                .first_key()
                .cmp(state.sstables.get(b).unwrap().first_key())
        });

        state.levels[task.next_level - 1].1 = new_lower;
        (state, files_to_remove)
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>, OrchestrationError> {
        let CompactionOptions::Leveled(_) = self.options.compaction_opts;
        let this = self.clone();

        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_compact() {
                        eprintln!("Compaction failed! Original error: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });

        Ok(Some(handle))
    }
}
