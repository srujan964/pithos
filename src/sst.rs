use bytes::BufMut;

pub(crate) const U16_SIZE: usize = std::mem::size_of::<u16>();

#[derive(Debug, Clone)]
pub(crate) struct SSTable {
    blocks: Vec<block::Block>,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    size: usize,
}

impl SSTable {
    pub(crate) fn new(id: usize, size: usize) -> Self {
        Self {
            blocks: vec![],
            first_key: vec![],
            last_key: vec![],
            size,
        }
    }
}

mod block {
    use super::BufMut;
    use super::U16_SIZE;
    use crate::types::Pair;
    use bytes::Buf;
    use std::vec;

    #[derive(Debug, Clone)]
    pub(crate) struct Block {
        pub(crate) size: usize,
        pub(crate) data: Vec<u8>,
        pub(crate) offsets: Vec<u16>,
    }

    impl Block {
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
        block: Vec<u8>,
        offsets: Vec<u16>,
        max_block_size: usize,
    }

    #[derive(thiserror::Error, Debug, PartialEq)]
    pub(crate) enum EncodingError {
        #[error("Max block size limit hit")]
        BlockSizeLimitReached,
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
                block: vec![],
                offsets: vec![],
                max_block_size: block_size,
            }
        }

        pub(crate) fn encode_pair(
            &mut self,
            key: &[u8],
            value: &[u8],
        ) -> Result<usize, EncodingError> {
            let key_len = key.len();
            let value_len = value.len();
            let encoded_size = key_len + value_len + (U16_SIZE * 2);

            if self.block.len() + encoded_size >= self.max_block_size {
                return Err(EncodingError::BlockSizeLimitReached);
            }
            self.offsets.push(self.block.len() as u16);

            self.block.put_u16_le(key_len as u16);
            self.block.put(key);
            self.block.put_u16_le(value_len as u16);
            self.block.put(value);

            Ok(encoded_size)
        }

        pub(crate) fn build(&mut self) -> Block {
            Block {
                size: self.max_block_size,
                data: self.block.clone(),
                offsets: self.offsets.clone(),
            }
        }
    }
}

mod index {}

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

        builder.encode_pair(k1, v1).unwrap();
        builder.encode_pair(k2, v2).unwrap();
        let block = builder.build();

        let mut expected_data = Vec::new();
        expected_data.put_u16_le(k1.len() as u16);
        expected_data.put(k1);
        expected_data.put_u16_le(v1.len() as u16);
        expected_data.put(v1);
        expected_data.put_u16_le(k2.len() as u16);
        expected_data.put(k2);
        expected_data.put_u16_le(v2.len() as u16);
        expected_data.put(v2);

        assert!(!block.data.is_empty());
        assert_eq!(block.data, expected_data);
        assert_eq!(block.offsets, expected_offsets);
    }

    #[test]
    fn block_builder_returns_err_on_encoding_past_size_limit() {
        let mut builder = BlockBuilder::init(1, 100);
        let k1 = "key_1".as_bytes();
        let v1 = "value_1".as_bytes();
        let k2 = "this-is-an-extremely-long-key".as_bytes();
        let v2 = "and-this-is-an-extremely-long-value".as_bytes();
        let k3 = "key_3".as_bytes();
        let v3 = "value_3".as_bytes();

        builder.encode_pair(k1, v1).unwrap();
        builder.encode_pair(k2, v2).unwrap();
        let result = builder.encode_pair(k3, v3);
        assert!(result.is_err());

        let block = builder.build();
        assert!(!block.data.is_empty());
    }

    #[test]
    fn block_decode_returns_key_value_pair() {
        let mut builder = BlockBuilder::init(1, 4 * 1024);
        let k1 = "key_1".as_bytes();
        let v1 = "value_1".as_bytes();
        let k2 = "key_2".as_bytes();
        let v2 = "value_2".as_bytes();

        builder.encode_pair(k1, v1).unwrap();
        builder.encode_pair(k2, v2).unwrap();
        let block = builder.build();

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

        builder.encode_pair(k1, v1).unwrap();
        builder.encode_pair(k2, v2).unwrap();
        let block = builder.build();

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

        builder.encode_pair(k1, v1).unwrap();
        builder.encode_pair(k2, v2).unwrap();
        let block = builder.build();

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
