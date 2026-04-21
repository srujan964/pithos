use std::sync::Arc;

use bytes::Bytes;

use crate::{iterator::StorageIter, memtable::Value, sst::block::Block};

pub(crate) struct BlockIterator {
    block: Arc<Block>,
    first_key: Vec<u8>,
    previous: Option<(Bytes, Value)>,
    idx: usize,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum BlockIterError {
    #[error("Underlying block has no data")]
    EmptyBlock,
}

impl BlockIterator {
    pub(crate) fn new(block: Arc<Block>) -> Result<Self, BlockIterError> {
        let block = block.clone();
        if block.data.is_empty() || block.offsets.is_empty() {
            return Err(BlockIterError::EmptyBlock);
        }

        Ok(Self {
            first_key: block.first_key(),
            block,
            previous: None,
            idx: 0,
        })
    }
}

impl StorageIter for BlockIterator {
    type KeyVal = (Bytes, Value);

    fn next(&mut self) -> Option<Self::KeyVal> {
        Iterator::next(self)
    }
}

impl Iterator for BlockIterator {
    type Item = (Bytes, Value);

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx + 1 >= self.block.offsets.len() {
            eprintln!("here for some reason");
            return None;
        }

        let offset = self.block.offsets[self.idx];

        let pair = self
            .block
            .decode_pair(&offset)
            .map(|(k, v)| {
                if v.is_empty() {
                    (Bytes::copy_from_slice(k), Value::Tombstone)
                } else {
                    (
                        Bytes::copy_from_slice(k),
                        Value::Plain(Bytes::copy_from_slice(v)),
                    )
                }
            })
            .ok();
        self.previous = pair.clone();
        self.idx += 1;

        pair
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use pretty_assertions::assert_eq;

    use crate::{
        iterator::block::BlockIterator,
        memtable::Value,
        sst::block::{Block, BlockBuilder},
    };

    fn build_block() -> Vec<Block> {
        let mut builder = BlockBuilder::init(1, 64);
        builder.encode_pair(b"key_1", b"value_1");
        builder.encode_pair(b"key_2", b"value_2");
        builder.encode_pair(b"key_3", b"value_3");
        builder.build()
    }

    #[test]
    fn block_iterator_not_created_for_an_empty_block() {
        let block = Block {
            id: 1,
            data: vec![],
            offsets: vec![],
            num_entries: 0,
        };
        let iter = BlockIterator::new(Arc::new(block));

        assert!(iter.is_err());
    }

    #[test]
    fn block_iterator_returns_first_elements_of_block() {
        let mut blocks = build_block();
        let block = blocks.pop().unwrap();

        let mut iter = BlockIterator::new(Arc::new(block)).unwrap();

        let item: Option<(Bytes, Value)> = iter.next();
        let pair = item.unwrap();

        assert_eq!(pair.0, Bytes::from("key_1"));
        assert_eq!(pair.1, Value::Plain(Bytes::from("value_1")));
    }

    #[test]
    fn block_iterator_goes_through_block_contents_until_exhausted() {
        let mut blocks = build_block();
        let block = blocks.pop().unwrap();

        let iter = BlockIterator::new(Arc::new(block)).unwrap();

        for (idx, item) in iter.into_iter().enumerate() {
            let expected_key = format!("key_{}", idx + 1);
            let expected_value = format!("value_{}", idx + 1);
            assert_eq!(item.0, Bytes::from(expected_key));
            assert_eq!(item.1, Value::Plain(Bytes::from(expected_value)))
        }
    }
}
