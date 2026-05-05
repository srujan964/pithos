use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use crate::{
    compaction::level::{self, LeveledCompactionOptions},
    core::State,
    memtable::Buffer,
};

#[derive(Clone, Debug)]
pub struct FIFOCompactionOptions {
    pub hot_ttl: Duration,
    pub max_hot_sst_count: usize,
    pub small_file_threshold_mb: usize,
    pub max_merge_output_mb: usize,
    pub cold: LeveledCompactionOptions,
    pub cold_max_total_size_mb: usize,
    pub cold_ttl: Option<Duration>,
}

///
/// An overview of the variants:
///
/// Promote: move expired hot SSTs into the cold, leveled partition. Does not build new SSTs.
/// HotMerge: merge multiple small SSTs into one larger file.
/// ColdCompact: apply classic leveled compaction within the cold parition.
/// ColdEvict: Delete files in the cold parition that are older than the TTL, or exceed size cap.
///
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) enum TieredTask {
    Promote { files: Vec<usize> },
    HotMerge { files: Vec<usize> },
    ColdCompact(level::Task),
    ColdEvict { files: Vec<usize> },
}

pub(crate) fn pick_compaction<B: Buffer + Clone + Sync + Send + 'static>(
    state: &State<B>,
    opts: &FIFOCompactionOptions,
) -> Option<TieredTask> {
    let now = SystemTime::now();

    // Highest priority is to promote expired files to cold tiers.
    // Read the files in level_zero from the back as the vec is ordered newest to oldest.
    let expired: Vec<usize> = state
        .level_zero
        .iter()
        .rev()
        .take_while(|&&id| {
            state
                .sstables
                .get(&id)
                .and_then(|sst| sst.created_at().ok())
                .map(|t| now.duration_since(t).unwrap_or_default() >= opts.hot_ttl)
                .unwrap_or(false)
        })
        .copied()
        .collect();

    if !expired.is_empty() {
        return Some(TieredTask::Promote { files: expired });
    }

    // Next up is to evict files in cold storage that have expired i.e older than the provided TTL.
    // They're present in the deepest levels.
    if let Some(cold_ttl) = opts.cold_ttl {
        let cold_expired: Vec<usize> = state
            .levels
            .iter()
            .rev()
            .flat_map(|(_, ids)| ids)
            .filter(|&&id| {
                state
                    .sstables
                    .get(&id)
                    .and_then(|sst| sst.created_at().ok())
                    .map(|t| now.duration_since(t).unwrap_or_default() >= cold_ttl)
                    .unwrap_or(false)
            })
            .copied()
            .collect();

        if !cold_expired.is_empty() {
            return Some(TieredTask::ColdEvict {
                files: cold_expired,
            });
        }
    }

    // Evict the oldest files in cold storage if we hit the soft cap on size instead.
    // Again look at the deepest levels first.
    let cold_total: usize = state
        .levels
        .iter()
        .flat_map(|(_, ids)| ids)
        .filter_map(|id| state.sstables.get(id))
        .map(|sst| sst.table_size())
        .sum();

    let total_size_cold_tier = opts.cold_max_total_size_mb * 1024 * 1024;
    if cold_total > total_size_cold_tier {
        let excess = cold_total.saturating_sub(total_size_cold_tier);
        let mut to_evict = Vec::new();
        let mut reclaimed = 0usize;

        'outer: for (_, ids) in state.levels.iter().rev() {
            for &id in ids {
                if reclaimed >= excess {
                    break 'outer;
                }
                if let Some(sst) = state.sstables.get(&id) {
                    reclaimed += sst.table_size();
                    to_evict.push(id);
                }
            }
        }

        if !to_evict.is_empty() {
            return Some(TieredTask::ColdEvict { files: to_evict });
        }
    }

    // Merge a run of files in hot storage, avoids having to read multiple files for newer data.
    // Merge as long as the combined size is within threshold.
    // Search from the oldest end (back of level_zero) for a contiguous run of small SSTs.
    if state.level_zero.len() > opts.max_hot_sst_count {
        let threshold = opts.small_file_threshold_mb * 1024 * 1024;
        let max_output = opts.max_merge_output_mb * 1024 * 1024;
        let mut run = Vec::new();
        let mut combined = 0usize;

        for &id in state.level_zero.iter().rev() {
            if let Some(sst) = state.sstables.get(&id) {
                let size = sst.table_size();
                if size > threshold || combined + size > max_output {
                    break;
                }
                run.push(id);
                combined += size;
            }
        }

        if run.len() >= 2 {
            return Some(TieredTask::HotMerge { files: run });
        }
    }

    // Apply leveled compaction on cold storage.
    // A minor difference compared to Leveled strategy is that it does not consider L0 as
    // files in L0 have to undergo hot-to-cold promotion as implemented above instead of by
    // number of files.
    let cold_view = State {
        level_zero: vec![],
        ..state.clone()
    };
    level::pick_compaction(&cold_view, &opts.cold).map(TieredTask::ColdCompact)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap, collections::VecDeque, fs::OpenOptions, ops::Bound, path::Path,
        sync::Arc, time::Duration,
    };

    use bytes::Bytes;
    use temp_dir::TempDir;

    use crate::{
        block::MetaBlock,
        compaction::{
            fifo::{FIFOCompactionOptions, TieredTask, pick_compaction},
            level::LeveledCompactionOptions,
        },
        core::State,
        filter::{Filter, bloom::Bloom},
        index::Index,
        memtable::{Buffer, MemError, MemtableIterator, TableOptions},
        sst::SSTable,
    };

    #[derive(Clone, Debug)]
    struct TestMemtable;

    impl Buffer for TestMemtable {
        fn create(_: usize, _: Option<TableOptions>) -> Self {
            TestMemtable
        }
        fn id(&self) -> usize {
            0
        }
        fn size(&self) -> usize {
            0
        }
        fn put(&self, _: &Bytes, _: &Bytes) -> Result<usize, MemError> {
            unimplemented!()
        }
        fn delete(&self, _: &Bytes) -> Result<(), MemError> {
            unimplemented!()
        }
        fn get(&self, _: &Bytes) -> Option<Bytes> {
            None
        }
        fn scan(&self, _: Bound<Bytes>, _: Bound<Bytes>) -> MemtableIterator {
            unimplemented!()
        }
        fn flush(&self, _: std::path::PathBuf) -> Result<SSTable, MemError> {
            unimplemented!()
        }
    }

    fn create_sst(
        id: usize,
        dir: &Path,
        first_key: &[u8],
        last_key: &[u8],
        size: usize,
    ) -> SSTable {
        let full_path = dir.join(format!("SST-{id}"));
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&full_path)
            .unwrap();
        SSTable {
            id,
            index: Index {
                start: 0,
                blocks: vec![],
                size: 0,
            },
            meta: MetaBlock {
                filter: Bloom::build(&[]),
                first_key: first_key.to_vec(),
                last_key: last_key.to_vec(),
                num_blks: 0,
                count_per_blk: vec![],
                block_offsets: vec![],
            },
            file: Arc::new(file),
            path: full_path,
            size,
        }
    }

    fn default_opts() -> FIFOCompactionOptions {
        FIFOCompactionOptions {
            hot_ttl: Duration::from_secs(3600),
            max_hot_sst_count: 4,
            small_file_threshold_mb: 1,
            max_merge_output_mb: 4,
            cold: LeveledCompactionOptions {
                max_output_size_mb: 64,
                level_size_multiplier: 2,
                max_levels: 3,
                l0_num_files_threshold: 2,
                base_level_size_mb: 128,
            },
            cold_max_total_size_mb: 512,
            cold_ttl: None,
        }
    }

    fn make_state(
        level_zero: Vec<usize>,
        levels: Vec<(usize, Vec<usize>)>,
        sstables: HashMap<usize, Arc<SSTable>>,
    ) -> State<TestMemtable> {
        State {
            memtable: Arc::new(TestMemtable),
            frozen: VecDeque::new(),
            level_zero,
            levels,
            sstables,
        }
    }

    // Immediately mark every SST as expired with `hot_ttl` = 0.
    #[test]
    fn promotes_all_expired_hot_ssts() {
        let dir = TempDir::new().unwrap();

        let sst1 = create_sst(1, dir.path(), b"a_001", b"a_100", 512);
        let sst2 = create_sst(2, dir.path(), b"b_001", b"b_100", 512);

        let mut tables = HashMap::new();
        tables.insert(1, Arc::new(sst1));
        tables.insert(2, Arc::new(sst2));

        // sst2 is newer, sst1 is older
        let state = make_state(
            vec![2, 1],
            vec![(0, vec![]), (1, vec![]), (2, vec![])],
            tables,
        );

        let opts = FIFOCompactionOptions {
            hot_ttl: Duration::ZERO,
            ..default_opts()
        };

        let task = pick_compaction(&state, &opts);
        assert!(matches!(task, Some(TieredTask::Promote { .. })));

        let TieredTask::Promote { files } = task.unwrap() else {
            panic!()
        };
        assert_eq!(files, vec![1, 2]);
    }

    #[test]
    fn returns_none_when_nothing_to_do() {
        let dir = TempDir::new().unwrap();

        let sst1 = create_sst(1, dir.path(), b"a_001", b"a_100", 512);

        let mut tables = HashMap::new();
        tables.insert(1, Arc::new(sst1));

        // One hot SST, well within count limit; cold is empty; large TTL.
        let state = make_state(vec![1], vec![(0, vec![]), (1, vec![]), (2, vec![])], tables);

        let task = pick_compaction(&state, &default_opts());
        assert!(task.is_none());
    }

    #[test]
    fn evicts_cold_ssts_when_over_size_cap() {
        let dir = TempDir::new().unwrap();

        // Two cold SSTs, each 1 MB => total 2 MB; cap is 1 MB.
        let cold1 = create_sst(1, dir.path(), b"a_001", b"a_100", 1024 * 1024);
        let cold2 = create_sst(2, dir.path(), b"b_001", b"b_100", 1024 * 1024);

        let mut tables = HashMap::new();
        tables.insert(1, Arc::new(cold1));
        tables.insert(2, Arc::new(cold2));

        // Cold SSTs in the deepest level.
        let state = make_state(
            vec![],
            vec![(0, vec![]), (1, vec![]), (2, vec![2, 1])],
            tables,
        );

        let opts = FIFOCompactionOptions {
            cold_max_total_size_mb: 1,
            ..default_opts()
        };

        let task = pick_compaction(&state, &opts);
        assert!(matches!(task, Some(TieredTask::ColdEvict { .. })));

        let TieredTask::ColdEvict { files } = task.unwrap() else {
            panic!()
        };
        // Excess = 2 MB - 1 MB = 1 MB; first cold SST (1 MB) covers it.
        assert!(!files.is_empty());
    }

    #[test]
    fn merges_small_hot_files_when_count_exceeds_limit() {
        let dir = TempDir::new().unwrap();

        // Five hot SSTs, each 256 KB; 1 MB small-file threshold
        // max_hot_sst_count is set to 4, so 5 files should trigger the merge
        let kb256 = 256 * 1024;
        let mut tables = HashMap::new();
        for id in 1..=5 {
            let key = format!("{id:05}").into_bytes();
            let sst = create_sst(id, dir.path(), &key, &key, kb256);
            tables.insert(id, Arc::new(sst));
        }

        let state = make_state(
            vec![5, 4, 3, 2, 1],
            vec![(0, vec![]), (1, vec![]), (2, vec![])],
            tables,
        );

        let task = pick_compaction(&state, &default_opts());
        assert!(matches!(task, Some(TieredTask::HotMerge { .. })));

        let TieredTask::HotMerge { files } = task.unwrap() else {
            panic!()
        };
        assert_eq!(files.len(), 5);
        assert_eq!(files, vec![1, 2, 3, 4, 5]);
    }
}
