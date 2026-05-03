use crate::filter::Filter;
use crate::filter::bloom::Bloom;
use crate::sst::U16_SIZE;
use crate::sst::U32_SIZE;
use crate::sst::U64_SIZE;

use bytes::Buf;
use bytes::BufMut;
use std::vec;

pub(crate) mod iterator;

#[derive(Debug, Clone)]
pub(crate) struct Block {
    pub(crate) id: usize,
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
    pub(crate) num_entries: u16,
}

type RawPair<'a> = (&'a [u8], &'a [u8]);

pub(crate) const BLOCK_SIZE: usize = 4 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct BlockInfo {
    block_idx: usize,
    block_offset: u64,
    block_size: u64,
}

impl BlockInfo {
    pub(crate) fn new(idx: usize, offset: u64, size: u64) -> Self {
        Self {
            block_idx: idx,
            block_offset: offset,
            block_size: size,
        }
    }

    pub(crate) fn idx(&self) -> usize {
        self.block_idx
    }

    pub(crate) fn offset(&self) -> u64 {
        self.block_offset
    }

    pub(crate) fn size(&self) -> u64 {
        self.block_size
    }
}

impl Block {
    pub(crate) fn size(&self) -> usize {
        self.data_length() + self.offsets_data_size()
    }

    pub(crate) fn data_length(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn offsets_data_size(&self) -> usize {
        self.offsets.len() * U16_SIZE
    }

    pub(crate) fn first_key(&self) -> Vec<u8> {
        let buf = &self.data[..].to_vec();
        let mut buf = &buf[..];
        let key_len = buf.get_u16_le() as usize;
        let key = buf[..key_len].to_vec();
        buf.advance(key_len);

        key
    }

    pub(crate) fn decode_at(&'_ self, offset: &u16) -> Result<RawPair<'_>, DecodingError> {
        let offset = *offset as usize;
        if offset >= self.data.len() {
            return Err(DecodingError::InaccessibleOffset);
        }

        let pair = Self::decode_pair(&self.data, offset);
        Ok(pair)
    }

    pub(crate) fn decode_full(
        buf: &mut [u8],
        block_idx: usize,
        offset_data_start: usize,
        num_entries: usize,
    ) -> Self {
        let data = buf[..offset_data_start].to_vec();

        let mut offsets: Vec<u16> = Vec::with_capacity(num_entries);
        let mut buf = &buf[offset_data_start..];
        for _ in 0..num_entries {
            offsets.push(buf.get_u16_le());
        }

        Self {
            id: block_idx,
            data,
            offsets,
            num_entries: num_entries as u16,
        }
    }

    pub(crate) fn decode_pair(buf: &'_ [u8], offset: usize) -> RawPair<'_> {
        let mut buf = &buf[offset..];

        let key_len = buf.get_u16_le() as usize;
        let key = &buf[..key_len];
        buf.advance(key_len);

        let val_len = buf.get_u16_le() as usize;
        let val = &buf[..val_len];
        buf.advance(val_len);

        (key, val)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MetaBlock {
    pub(super) filter: Bloom,
    pub(super) first_key: Vec<u8>,
    pub(super) last_key: Vec<u8>,
    pub(super) num_blks: u16,
    pub(super) count_per_blk: Vec<u16>,
    pub(super) block_offsets: Vec<u64>,
}

impl MetaBlock {
    pub(super) fn encode(&self, buf: &mut Vec<u8>) -> bool {
        self.filter.encode(buf);
        let key_len = self.first_key.len() as u16;
        buf.put_u16_le(key_len);
        buf.put(self.first_key.as_ref());

        let key_len = self.last_key.len() as u16;
        buf.put_u16_le(key_len);
        buf.put(self.last_key.as_ref());

        buf.put_u16_le(self.num_blks);

        let len = self.count_per_blk.len() as u32;
        buf.put_u32_le(len);
        for count in self.count_per_blk.iter() {
            buf.put_u16_le(*count);
        }

        let len = self.block_offsets.len() as u32;
        buf.put_u32_le(len);
        for offset in self.block_offsets.iter() {
            buf.put_u64_le(*offset);
        }

        true
    }

    pub(super) fn decode(mut buf: &[u8]) -> Self {
        let filter = Bloom::decode(buf);
        buf.advance(filter.size());

        let key_len = buf.get_u16_le() as usize;
        let first_key = buf[..key_len].to_vec();
        buf.advance(key_len);

        let key_len = buf.get_u16_le() as usize;
        let last_key = buf[..key_len].to_vec();
        buf.advance(key_len);

        let num_blocks = buf.get_u16_le();

        let len = buf.get_u32_le();
        let mut count_per_block = Vec::with_capacity(len as usize);
        for _ in 0..len {
            count_per_block.push(buf.get_u16_le());
        }

        let len = buf.get_u32_le();
        let mut block_offset = Vec::with_capacity(len as usize);
        for _ in 0..len {
            block_offset.push(buf.get_u64_le());
        }

        Self {
            filter,
            first_key,
            last_key,
            num_blks: num_blocks,
            count_per_blk: count_per_block,
            block_offsets: block_offset,
        }
    }

    pub(super) fn size(&self) -> usize {
        let sizes = [
            self.filter.size(),
            (self.first_key.len() + U16_SIZE),
            (self.last_key.len() + U16_SIZE),
            U16_SIZE,
            ((self.count_per_blk.len() * U16_SIZE) + U32_SIZE),
            ((self.block_offsets.len() * U64_SIZE) + U32_SIZE),
        ];
        sizes.iter().sum()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BlockBuilder {
    num: usize,
    blocks: Vec<Block>,
    cur_block_offset: u64,
    cur_block_data: Vec<u8>,
    offsets: Vec<u16>,
    key_hashes: Vec<u64>,
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
            key_hashes: vec![],
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
                num_entries: self.offsets.len() as u16,
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

        self.key_hashes.push(seahash::hash(key));

        encoded_size
    }

    pub(crate) fn build(&mut self) -> Vec<Block> {
        if !self.cur_block_data.is_empty() {
            let block = Block {
                id: self.num,
                data: self.cur_block_data.clone(),
                offsets: self.offsets.clone(),
                num_entries: self.offsets.len() as u16,
            };
            self.blocks.push(block);
        }

        self.blocks.clone()
    }

    pub(crate) fn key_hashes(&self) -> &[u64] {
        &self.key_hashes
    }

    fn clear(&mut self) {
        self.cur_block_data.clear();
        self.offsets.clear();
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;

    use crate::block::{BlockBuilder, DecodingError};

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

        assert_eq!(block.first_key(), "key_1".as_bytes());
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
        let first_result = block.decode_at(offset).unwrap();
        let offset = block.offsets.last().unwrap();
        let second_result = block.decode_at(offset).unwrap();

        assert_eq!(first_result.0, k1);
        assert_eq!(first_result.1, v1);

        assert_eq!(second_result.0, k2);
        assert_eq!(second_result.1, v2);
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
        let actual = block.decode_at(&offset);

        assert!(actual.is_err());
        let err = actual.unwrap_err();
        assert_eq!(err, DecodingError::InaccessibleOffset);
    }
}
