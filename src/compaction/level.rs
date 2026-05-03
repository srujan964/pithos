use crate::{
    compaction::{FileMetadata, Level},
    core::State,
    memtable::Buffer,
};

#[derive(Clone, Debug)]
pub struct LeveledCompactionOptions {
    pub max_output_size_mb: usize,
    pub level_size_multiplier: usize,
    pub max_levels: usize,
    pub l0_num_files_threshold: usize,
    pub base_level_size_mb: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct Task {
    pub(crate) input_level: Option<usize>,
    pub(crate) next_level: usize,
    pub(crate) files_to_compact: Vec<usize>,
    pub(crate) overlapping_ssts: Vec<usize>,
}

fn overlapping_range(tables: &[FileMetadata]) -> (&[u8], &[u8]) {
    (
        tables.iter().map(|meta| meta.smallest_key()).min().unwrap(),
        tables.iter().map(|meta| meta.largest_key()).max().unwrap(),
    )
}

fn overlapping_ssts(level_ssts: &[FileMetadata], range: (&[u8], &[u8])) -> Vec<usize> {
    level_ssts
        .iter()
        .filter(|meta| !(meta.largest_key() < range.0 || meta.smallest_key() > range.1))
        .map(|meta| meta.id())
        .collect()
}

fn compact_l0<B: Buffer + Clone + Sync + Send + 'static>(
    state: &State<B>,
    higher_levels: &[Level],
    target_level: usize,
) -> Option<Task> {
    let sst_ids = state.level_zero.clone();
    let l0_stable_meta: Vec<FileMetadata> = state
        .level_zero
        .iter()
        .filter_map(|id| state.sstables.get(id).map(FileMetadata::from_sstable))
        .collect();

    let range = overlapping_range(&l0_stable_meta);
    let target_ssts = overlapping_ssts(higher_levels[target_level].files(), range);

    // TODO: Trivial move if `target_ssts` is empty.

    Some(Task {
        input_level: None,
        next_level: 1,
        files_to_compact: sst_ids,
        overlapping_ssts: target_ssts,
    })
}

fn compact_to_lower_level(target_level: usize, levels: &[Level]) -> Option<Task> {
    let level = &levels[target_level];
    if let Some(oldest_sst) = level.files().first() {
        let sst_to_compact = vec![oldest_sst.clone()];
        let next_level = &levels[target_level + 1];
        let range = overlapping_range(&sst_to_compact);
        let next_level_ssts = overlapping_ssts(next_level.files(), range);

        return Some(Task {
            input_level: Some(target_level + 1), // 1-indexed: state.levels[target_level]
            next_level: target_level + 2,        // 1-indexed: state.levels[target_level + 1]
            files_to_compact: vec![oldest_sst.id],
            overlapping_ssts: next_level_ssts,
        });
    }

    None
}

// Compute target sizes of L1+ files.
fn compute_level_sizes<B: Buffer + Clone + Sync + Send + 'static>(
    state: &State<B>,
    opts: &LeveledCompactionOptions,
) -> (Vec<Level>, usize) {
    let base_level_size_bytes = opts.base_level_size_mb * 1024 * 1024;

    let mut levels = Vec::with_capacity(opts.max_levels);
    let max_level_ssts: Vec<FileMetadata> = state.levels[opts.max_levels - 1]
        .1
        .iter()
        .filter_map(|sst_id| state.sstables.get(sst_id).map(FileMetadata::from_sstable))
        .collect();

    let max_level_size: usize = max_level_ssts.iter().map(|meta| meta.size()).sum();
    let max_level = Level::new(&max_level_ssts, max_level_size.max(base_level_size_bytes));
    let mut target_level: usize = opts.max_levels;

    levels.insert(0, max_level);

    for i in (0..(opts.max_levels - 1)).rev() {
        let next_level = levels
            .last()
            .expect("Highest-numbered level data should be available at this point");
        let level_ssts_meta: Vec<FileMetadata> = state.levels[i]
            .1
            .iter()
            .filter_map(|sst_id| state.sstables.get(sst_id).map(FileMetadata::from_sstable))
            .collect();

        let max_size = next_level.size() / opts.level_size_multiplier;
        target_level = i;
        levels.insert(0, Level::new(&level_ssts_meta, max_size));
    }

    (levels, target_level)
}

pub(crate) fn pick_compaction<B: Buffer + Clone + Sync + Send + 'static>(
    state: &State<B>,
    opts: &LeveledCompactionOptions,
) -> Option<Task> {
    let (levels, target_level) = compute_level_sizes(state, opts);

    // If L0 breaches threshold, compact all of its files into a higher-numbered level.
    if state.level_zero.len() >= opts.l0_num_files_threshold {
        return compact_l0(state, &levels, target_level);
    }

    // Otherwise, possibly compact to a lower (higher-numbered) level.
    let mut priorities = Vec::with_capacity(levels.len());
    for (id, level) in levels.iter().enumerate() {
        let pr: f64 = level.size() as f64 / level.max_size() as f64;
        if pr > 1.0 {
            priorities.push((pr, id))
        }
    }

    priorities.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());

    if let Some((_, target_level)) = priorities.first() {
        return compact_to_lower_level(*target_level, &levels);
    }

    None
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, VecDeque},
        fmt,
        fs::OpenOptions,
        ops::Bound,
        path::{Path, PathBuf},
        sync::Arc,
    };

    use bytes::Bytes;
    use temp_dir::TempDir;

    use crate::{
        block::MetaBlock,
        compaction::level::{LeveledCompactionOptions, pick_compaction},
        core::State,
        filter::{Filter, bloom::Bloom},
        index::Index,
        memtable::{Buffer, MemError, MemtableIterator, TableOptions},
        sst::SSTable,
    };

    #[derive(Clone, Debug)]
    struct TestMemtable {
        size: usize,
    }

    impl Buffer for TestMemtable {
        fn create(_id: usize, _options: Option<TableOptions>) -> Self {
            TestMemtable { size: 0 }
        }

        fn id(&self) -> usize {
            1
        }

        fn size(&self) -> usize {
            self.size
        }

        fn put(&self, key: &Bytes, value: &Bytes) -> Result<usize, MemError> {
            todo!()
        }

        fn delete(&self, key: &Bytes) -> Result<(), MemError> {
            todo!()
        }

        fn get(&self, key: &Bytes) -> Option<Bytes> {
            todo!()
        }

        fn scan(&self, _start: Bound<Bytes>, _end: Bound<Bytes>) -> MemtableIterator {
            todo!()
        }

        fn flush(&self, path: PathBuf) -> Result<SSTable, MemError> {
            todo!()
        }
    }

    fn create_sst(
        id: usize,
        path: &Path,
        first_key: &[u8],
        last_key: &[u8],
        size: usize,
    ) -> SSTable {
        let index = Index {
            start: 0,
            blocks: vec![],
            size: 0,
        };

        let metablock = MetaBlock {
            filter: Bloom::build(&[]),
            first_key: first_key.to_vec(),
            last_key: last_key.to_vec(),
            num_blks: 0,
            count_per_blk: vec![],
            block_offsets: vec![],
        };
        let full_path = path.join(format!("SST-{}", id));
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&full_path)
            .unwrap();

        SSTable {
            id,
            index,
            meta: metablock,
            file: Arc::new(file),
            path: full_path,
            size,
        }
    }

    #[test]
    fn pick_compaction_returns_l0_compaction_task() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path();
        let memtable = TestMemtable::create(1, None);

        let file_1 = create_sst(1, path, b"key_1".as_ref(), b"key_5".as_ref(), 1024);
        let file_2 = create_sst(2, path, b"key_6".as_ref(), b"key_11".as_ref(), 1024);
        let file_3 = create_sst(3, path, b"key_13".as_ref(), b"key_21".as_ref(), 1024 * 4);
        let file_4 = create_sst(4, path, b"key_26".as_ref(), b"key_52".as_ref(), 1024 * 4);
        let file_5 = create_sst(
            5,
            path,
            b"key_34".as_ref(),
            b"key_255".as_ref(),
            1024 * 1024,
        );
        let file_6 = create_sst(
            6,
            path,
            b"key_256".as_ref(),
            b"key_511".as_ref(),
            1024 * 1024,
        );

        let mut tables_map = HashMap::new();
        tables_map.insert(1, Arc::new(file_1));
        tables_map.insert(2, Arc::new(file_2));
        tables_map.insert(3, Arc::new(file_3));
        tables_map.insert(4, Arc::new(file_4));
        tables_map.insert(5, Arc::new(file_5));
        tables_map.insert(6, Arc::new(file_6));

        let state = State {
            memtable: Arc::new(memtable),
            frozen: VecDeque::new(),
            level_zero: vec![6, 5],
            levels: vec![(0, vec![3, 4]), (1, vec![1, 2]), (2, vec![]), (3, vec![])],
            sstables: tables_map,
        };

        let opts = LeveledCompactionOptions {
            max_output_size_mb: 128 * 1024,
            level_size_multiplier: 2,
            max_levels: 4,
            l0_num_files_threshold: 2,
            base_level_size_mb: 1024 * 1024 * 2,
        };

        let maybe_task = pick_compaction(&state, &opts);
        assert!(maybe_task.is_some());

        let task = maybe_task.unwrap();
        assert_eq!(task.input_level, None);
        assert_eq!(task.next_level, 1);
        assert_eq!(task.files_to_compact, vec![6, 5]);
        assert_eq!(task.overlapping_ssts, vec![4]);
    }
}
