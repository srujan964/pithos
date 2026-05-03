use std::sync::Arc;

use bytes::Bytes;

use crate::{
    block::{BlockInfo, iterator::BlockIterator},
    iterator::StorageIter,
    sst::SSTable,
    types::Pair,
};

pub(crate) struct SSTableIterator {
    table: Arc<SSTable>,
    cur_block_iter: Option<BlockIterator>,
    block_idx: usize,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum SSTableIterError {
    #[error("Failed to create iterator for this sstable")]
    InitFailed,
    #[error("These sstables cannot be concatenated in this order")]
    IncorrectOrdering,
}

impl SSTableIterator {
    pub(crate) fn new(table: Arc<SSTable>) -> Self {
        Self {
            table,
            block_idx: 0,
            cur_block_iter: None,
        }
    }

    pub(crate) fn new_and_seek_to_key(
        table: Arc<SSTable>,
        key: &Bytes,
    ) -> Result<Self, SSTableIterError> {
        let block_info: BlockInfo = match table.find_block_for_seek(key) {
            Ok(block_info) => block_info,
            Err(_) => {
                return Err(SSTableIterError::InitFailed);
            }
        };

        let block_iter: BlockIterator = match table.block_iter_by_offset(block_info.clone()) {
            Ok(iter) => iter,
            Err(_) => {
                eprintln!("Failed to create block iter: {:?}", &block_info);
                return Err(SSTableIterError::InitFailed);
            }
        };

        Ok(Self {
            table,
            block_idx: block_info.idx(),
            cur_block_iter: Some(block_iter),
        })
    }
}

impl StorageIter for SSTableIterator {
    type KeyVal = Pair;

    fn next(&mut self) -> Option<Self::KeyVal> {
        std::iter::Iterator::next(self)
    }
}

impl Iterator for SSTableIterator {
    type Item = Pair;

    fn next(&mut self) -> Option<Self::Item> {
        if self.block_idx >= self.table.meta.block_offsets.len() {
            return None;
        }

        if self.cur_block_iter.is_none() {
            self.cur_block_iter = self.table.block_iter(self.block_idx).ok();

            if self.cur_block_iter.is_none() {
                return None;
            }
        }

        // Current block iterator can possibly be exhausted, so capture it first.
        let item = self.cur_block_iter.as_mut().and_then(StorageIter::next);

        // Return the current block iterator's next item or load the next block's iterator
        item.or_else(|| {
            // Already ensured we have atleast one more block left so we're safe to increment the
            // index.

            self.block_idx += 1;
            self.cur_block_iter = self.table.block_iter(self.block_idx).ok();

            self.cur_block_iter.as_mut().and_then(StorageIter::next)
        })
    }
}

pub(crate) struct ConcatenatingIterator {
    current: Option<SSTableIterator>,
    sst_idx: usize,
    sstables: Vec<Arc<SSTable>>,
}

impl ConcatenatingIterator {
    // Ensure that SSTs being concatenated are ordered correctly
    fn validate(sstables: &[Arc<SSTable>]) -> bool {
        if sstables.is_empty() {
            return false;
        }

        for table in sstables {
            if table.first_key() > table.last_key() {
                return false;
            }
        }

        if sstables.len() >= 2 {
            for i in 0..(sstables.len() - 1) {
                if sstables[i].last_key() > sstables[i + 1].first_key() {
                    return false;
                }
            }
        }

        true
    }

    pub(crate) fn new_and_seek_to_first(
        sstables: Vec<Arc<SSTable>>,
    ) -> Result<Self, SSTableIterError> {
        if !Self::validate(&sstables) {
            return Err(SSTableIterError::IncorrectOrdering);
        }

        Ok(Self {
            current: Some(SSTableIterator::new(sstables[0].clone())),
            sst_idx: 0,
            sstables,
        })
    }

    pub(crate) fn new_and_seek_to_key(
        sstables: Vec<Arc<SSTable>>,
        key: &Bytes,
    ) -> Result<Self, SSTableIterError> {
        if !Self::validate(&sstables) {
            return Err(SSTableIterError::IncorrectOrdering);
        }

        let idx = sstables
            .partition_point(|sst| sst.first_key() <= key)
            .saturating_sub(1);

        if idx >= sstables.len() {
            return Ok(Self {
                current: None,
                sst_idx: sstables.len() - 1,
                sstables,
            });
        }

        Ok(Self {
            current: Some(SSTableIterator::new_and_seek_to_key(
                sstables[idx].clone(),
                key,
            )?),
            sst_idx: idx,
            sstables,
        })
    }
}

impl StorageIter for ConcatenatingIterator {
    type KeyVal = Pair;

    fn next(&mut self) -> Option<Self::KeyVal> {
        std::iter::Iterator::next(self)
    }
}

impl Iterator for ConcatenatingIterator {
    type Item = Pair;

    fn next(&mut self) -> Option<Self::Item> {
        if self.sst_idx >= self.sstables.len() {
            return None;
        }

        let item = self.current.as_mut().and_then(StorageIter::next);

        item.or_else(|| {
            self.sst_idx += 1;
            self.current = Some(SSTableIterator::new(self.sstables[self.sst_idx].clone()));

            self.current.as_mut().and_then(StorageIter::next)
        })
    }
}
