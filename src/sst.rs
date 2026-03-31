use crate::{index::Index, sst::block::Block};
use bytes::BufMut;

pub(crate) const U16_SIZE: usize = std::mem::size_of::<u16>();
pub(crate) const U64_SIZE: usize = std::mem::size_of::<u64>();

pub(crate) const BLOCK_SIZE: usize = 4 * 1024;
pub(crate) const MAX_TABLE_SIZE: usize = 64 * 1024 * 1024;
pub(crate) const FOOTER_SIZE: usize = 4 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct SSTable {
    pub(crate) blocks: Vec<block::Block>,
    pub(crate) index: Index,
    pub(crate) first_key: Vec<u8>,
    pub(crate) last_key: Vec<u8>,
    pub(crate) max_size: usize,
    pub(crate) footer: TableFooter,
}

#[derive(Debug, Clone)]
pub(crate) struct TableFooter {
    index_offset: u64,
    identifier: Vec<u8>,
    size: usize,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum SSTableError {
    #[error("Failed to initalize SSTable")]
    InitFailed,
}

impl SSTable {
    pub(crate) fn new(
        blocks: &[Block],
        index: &Index,
        max_size: usize,
    ) -> Result<Self, SSTableError> {
        match (index.first_key(), index.last_key()) {
            (Some(first), Some(last)) => {
                let magic = vec![0x48, 0x6f, 0x70, 0x65];
                let footer = TableFooter {
                    index_offset: index.start,
                    identifier: magic.clone(),
                    size: U64_SIZE + magic.len(),
                };

                Ok(Self {
                    blocks: blocks.to_vec(),
                    index: index.clone(),
                    first_key: first.to_vec(),
                    last_key: last.to_vec(),
                    max_size,
                    footer,
                })
            }
            (_, _) => Err(SSTableError::InitFailed),
        }
    }
}

pub(crate) mod block {
    use super::BufMut;
    use super::U16_SIZE;
    use crate::types::Pair;
    use bytes::Buf;
    use std::vec;

    #[derive(Debug, Clone)]
    pub(crate) struct Block {
        pub(crate) id: usize,
        pub(crate) data: Vec<u8>,
        pub(crate) offsets: Vec<u16>,
    }

    impl Block {
        pub(crate) fn size(&self) -> usize {
            self.data.len()
        }

        pub(crate) fn decode_pair(&self, offset: &u16) -> Result<Pair, DecodingError> {
            let offset = *offset as usize;
            if offset >= self.data.len() {
                return Err(DecodingError::InaccessibleOffset);
            }

            let (pair, _size) = Self::decode(&self.data, offset);
            Ok(pair)
        }

        pub(crate) fn decode_all(&self) -> Result<Vec<Pair>, DecodingError> {
            let mut start = 0;
            let mut pairs = vec![];

            while start < self.data.len() {
                let (pair, size) = Self::decode(&self.data, start);
                pairs.push(pair);
                start += size;
            }
            Ok(pairs)
        }

        fn decode(buf: &[u8], offset: usize) -> (Pair, usize) {
            let mut buf = &buf[offset..];

            let key_len = buf.get_u16_le() as usize;
            let key = &buf[..key_len];
            buf.advance(key_len);

            let val_len = buf.get_u16_le() as usize;
            let val = &buf[..val_len];
            buf.advance(val_len);

            let size = key.len() + val.len() + 2 * U16_SIZE;

            (Pair::new(key, val), size)
        }
    }

    #[derive(Debug, Clone)]
    pub(crate) struct BlockBuilder {
        num: usize,
        blocks: Vec<Block>,
        cur_block_offset: u64,
        cur_block_data: Vec<u8>,
        offsets: Vec<u16>,
        max_block_size: usize,
    }

    #[derive(thiserror::Error, Debug, PartialEq)]
    pub(crate) enum DecodingError {
        #[error("Invalid offset for this block")]
        InaccessibleOffset,
    }

    impl BlockBuilder {
        pub(crate) fn init(block_num: usize, block_size: usize) -> Self {
            BlockBuilder {
                num: block_num,
                blocks: vec![],
                cur_block_offset: 0,
                cur_block_data: vec![],
                offsets: vec![],
                max_block_size: block_size,
            }
        }

        pub(crate) fn latest_block_offset(&self) -> u64 {
            self.cur_block_offset
        }

        pub(crate) fn latest_key_offset(&self) -> Option<u16> {
            self.offsets.last().copied()
        }

        pub(crate) fn encode_pair(&mut self, key: &[u8], value: &[u8]) -> usize {
            let key_len = key.len();
            let value_len = value.len();
            let encoded_size = key_len + value_len + (U16_SIZE * 2);

            if self.cur_block_data.len() + encoded_size >= self.max_block_size {
                let block = Block {
                    id: self.num,
                    data: self.cur_block_data.clone(),
                    offsets: self.offsets.clone(),
                };
                self.cur_block_offset += block.size() as u64;
                self.blocks.push(block);
                self.num += 1;
                self.clear();
            }
            self.offsets.push(self.cur_block_data.len() as u16);

            self.cur_block_data.put_u16_le(key_len as u16);
            self.cur_block_data.put(key);
            self.cur_block_data.put_u16_le(value_len as u16);
            self.cur_block_data.put(value);

            encoded_size
        }

        pub(crate) fn build(&mut self) -> Vec<Block> {
            if !self.cur_block_data.is_empty() {
                let block = Block {
                    id: self.num,
                    data: self.cur_block_data.clone(),
                    offsets: self.offsets.clone(),
                };
                self.blocks.push(block);
            }

            self.blocks.clone()
        }

        fn clear(&mut self) {
            self.cur_block_data.clear();
            self.offsets.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::sst::block::{BlockBuilder, DecodingError};
    use bytes::BufMut;
    use pretty_assertions::assert_eq;

    #[test]
    fn block_builder_encodes_pair() {
        let mut builder = BlockBuilder::init(1, 4 * 1024);
        let k1 = "key_1".as_bytes();
        let v1 = "value_1".as_bytes();
        let k2 = "key_2".as_bytes();
        let v2 = "value_2".as_bytes();
        // Two 2 byte length values + 5 bytes of key_1 + 7 bytes of value_1 = 16
        let expected_offsets: Vec<u16> = vec![0, 16];

        builder.encode_pair(k1, v1);
        builder.encode_pair(k2, v2);
        let blocks = builder.build();

        let mut expected_data = Vec::new();
        expected_data.put_u16_le(k1.len() as u16);
        expected_data.put(k1);
        expected_data.put_u16_le(v1.len() as u16);
        expected_data.put(v1);
        expected_data.put_u16_le(k2.len() as u16);
        expected_data.put(k2);
        expected_data.put_u16_le(v2.len() as u16);
        expected_data.put(v2);

        assert!(!blocks.is_empty());
        let block = blocks.first().unwrap();

        assert!(!block.data.is_empty());
        assert_eq!(block.data, expected_data);
        assert_eq!(block.offsets, expected_offsets);
    }

    #[test]
    fn block_decode_returns_key_value_pair() {
        let mut builder = BlockBuilder::init(1, 4 * 1024);
        let k1 = "key_1".as_bytes();
        let v1 = "value_1".as_bytes();
        let k2 = "key_2".as_bytes();
        let v2 = "value_2".as_bytes();

        builder.encode_pair(k1, v1);
        builder.encode_pair(k2, v2);
        let blocks = builder.build();
        let block = blocks.first().unwrap();

        let offset = block.offsets.first().unwrap();
        let first_result = block.decode_pair(offset).unwrap();
        let offset = block.offsets.last().unwrap();
        let second_result = block.decode_pair(offset).unwrap();

        assert_eq!(first_result.key(), k1);
        assert_eq!(first_result.value(), v1);

        assert_eq!(second_result.key(), k2);
        assert_eq!(second_result.value(), v2);
    }

    #[test]
    fn block_decode_returns_err_for_invalid_offset() {
        let mut builder = BlockBuilder::init(1, 4 * 1024);
        let k1 = "key_1".as_bytes();
        let v1 = "value_1".as_bytes();
        let k2 = "key_2".as_bytes();
        let v2 = "value_2".as_bytes();

        builder.encode_pair(k1, v1);
        builder.encode_pair(k2, v2);
        let blocks = builder.build();
        let block = blocks.first().unwrap();

        let offset = u16::MAX;
        let actual = block.decode_pair(&offset);

        assert!(actual.is_err());
        let err = actual.unwrap_err();
        assert_eq!(err, DecodingError::InaccessibleOffset);
    }

    #[test]
    fn block_decode_all_decodes_entire_block() {
        let mut builder = BlockBuilder::init(1, 4 * 1024);
        let k1 = "key_1".as_bytes();
        let v1 = "value_1".as_bytes();
        let k2 = "key_2".as_bytes();
        let v2 = "value_2".as_bytes();

        builder.encode_pair(k1, v1);
        builder.encode_pair(k2, v2);
        let blocks = builder.build();
        let block = blocks.first().unwrap();

        let pairs = block.decode_all().unwrap();
        let mut pairs_iter = pairs.iter();

        let pair_one = pairs_iter.next().unwrap();
        assert_eq!(pair_one.key(), k1);
        assert_eq!(pair_one.value(), v1);

        let pair_two = pairs_iter.next().unwrap();
        assert_eq!(pair_two.key(), k2);
        assert_eq!(pair_two.value(), v2);
    }
}
