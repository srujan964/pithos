use std::sync::Arc;

use bytes::Bytes;

use crate::{
    iterator::{StorageIter, block::BlockIterator},
    sst::{BlockInfo, SSTable},
    types::Pair,
};

pub(crate) struct SSTIterator {
    table: Arc<SSTable>,
    cur_block_iter: Option<BlockIterator>,
    block_idx: usize,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum SSTIterError {
    #[error("Failed to create iterator for this sstable")]
    InitFailed,
}

impl SSTIterator {
    pub(crate) fn new(table: Arc<SSTable>) -> Self {
        Self {
            table,
            block_idx: 0,
            cur_block_iter: None,
        }
    }

    pub(crate) fn new_and_seek_to_key(
        table: Arc<SSTable>,
        key: Bytes,
    ) -> Result<Self, SSTIterError> {
        let block_info: BlockInfo = match table.find_block_by_key(&key) {
            Ok(block_info) => block_info,
            Err(_) => return Err(SSTIterError::InitFailed),
        };

        let block_iter = match table.block_iter_by_offset(block_info) {
            Ok(iter) => iter,
            Err(_) => return Err(SSTIterError::InitFailed),
        };

        Ok(Self {
            table,
            block_idx: 0,
            cur_block_iter: Some(block_iter),
        })
    }
}

impl StorageIter for SSTIterator {
    type KeyVal = Pair;

    fn next(&mut self) -> Option<Self::KeyVal> {
        std::iter::Iterator::next(self)
    }
}

impl Iterator for SSTIterator {
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
