use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use crate::{
    filter::{Filter, bloom::Bloom},
    index::{Index, IndexBuilder},
    memtable::Value,
    sst::block::{Block, BlockBuilder, MetaBlock},
};
use bytes::{BufMut, Bytes};
use crossbeam_skiplist::map::Iter;

pub(crate) const U16_SIZE: usize = std::mem::size_of::<u16>();
pub(crate) const U32_SIZE: usize = std::mem::size_of::<u32>();
pub(crate) const U64_SIZE: usize = std::mem::size_of::<u64>();

pub(crate) const BLOCK_SIZE: usize = 4 * 1024;
pub(crate) const MAX_TABLE_SIZE: usize = 64 * 1024 * 1024;
pub(crate) const FOOTER_SIZE: usize = 24;

///
/// The on-disk format of an SSTable is as follows:
///
/// +----------------+----------------+-------+----------------+---------------+----------+
/// |  Data block 1  |  Data block 2  |  ...  |  Data block N  |  Index Block  |  Footer  |
/// +----------------+----------------+-------+----------------+---------------+----------+
///
#[derive(Clone, Debug)]
pub(crate) struct SSTableData {
    pub(crate) blocks: Vec<block::Block>,
    pub(crate) index: Index,
    pub(crate) meta: MetaBlock,
    pub(crate) footer: TableFooter,
}

#[derive(Clone, Debug)]
pub(crate) struct SSTable {
    id: usize,
    index: Index,
    filter: Bloom,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    path: PathBuf,
    size: usize,
}

///
/// +-------------+----------------+-----------------+-------------+
/// |     id      |  index_offset  |   meta offset   |    magic    |
/// |  (4 bytes)  |    (8 bytes)   |    (8 bytes)    |  (4 bytes)  |
/// +-------------+----------------+-----------------+-------------+
///
#[derive(Debug, Clone)]
pub(crate) struct TableFooter {
    sst_id: usize,
    index_offset: u64,
    meta_offset: u64,
    magic: Vec<u8>,
    size: usize,
}

impl TableFooter {
    fn encode(&self, mut buf: &mut [u8]) -> bool {
        let id = self.sst_id as u32;
        buf.put_u32_le(id);
        buf.put_u64_le(self.index_offset);
        buf.put_u64_le(self.meta_offset);
        buf.put_slice(&self.magic);
        true
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum SSTableError {
    #[error("Failed to initialize SSTable")]
    InitFailed,
    #[error("Unable to flush memtable to sst")]
    WriteFailed(#[from] std::io::Error),
}

struct WriteableSSTable {
    file: File,
}

impl WriteableSSTable {
    pub(crate) fn create(path: &PathBuf) -> Result<Self, SSTableError> {
        match OpenOptions::new().write(true).create_new(true).open(path) {
            Ok(file) => Ok(Self { file }),
            Err(e) => {
                eprintln!(
                    "Error creating sst file {}: Original error: {}",
                    path.display(),
                    e
                );
                Err(SSTableError::InitFailed)
            }
        }
    }

    pub(crate) fn write(&mut self, data: &SSTableData) -> Result<usize, std::io::Error> {
        assert_eq!(data.footer.size, FOOTER_SIZE);

        let mut size = 0;
        for data_block in data.blocks.iter() {
            let _ = self.file.write(&data_block.data)?;
        }

        let meta_size = data.meta.size();
        let mut meta_buf = vec![0; meta_size];
        data.meta.encode(&mut meta_buf);
        size += self.file.write(&meta_buf)?;

        let index_size = data.index.size();
        let mut index_buf = vec![0; index_size];
        data.index.encode(&mut index_buf);
        size += self.file.write(&index_buf)?;

        let mut footer_buf = vec![0; data.footer.size];
        data.footer.encode(&mut footer_buf);
        size += self.file.write(&footer_buf)?;
        self.file.sync_all()?;

        Ok(size)
    }
}

impl SSTableData {
    pub(crate) fn init(
        id: usize,
        block_builder: &mut BlockBuilder,
        index_builder: &mut IndexBuilder,
        data_dir: PathBuf,
    ) -> Result<SSTable, SSTableError> {
        let blocks: Vec<Block> = block_builder.build();
        let index_start_offset = blocks.iter().map(|b| b.size()).sum::<usize>() as u64;
        let index = index_builder.build(index_start_offset);

        let meta_offset = index_start_offset + index.size() as u64;
        let filter = Bloom::build(block_builder.key_hashes());
        let footer_size = U32_SIZE * 2 + U64_SIZE * 2;

        let data = match (index.first_key(), index.last_key()) {
            (Some(first), Some(last)) => {
                let magic = vec![0x48, 0x6f, 0x70, 0x65];
                let footer = TableFooter {
                    sst_id: id,
                    index_offset: index_start_offset,
                    meta_offset,
                    magic: magic.clone(),
                    size: footer_size,
                };

                let meta = MetaBlock {
                    filter: filter.clone(),
                    first_key: first.to_vec(),
                    last_key: last.to_vec(),
                };

                Ok(Self {
                    blocks: blocks.to_vec(),
                    index: index.clone(),
                    meta,
                    footer,
                })
            }
            (_, _) => Err(SSTableError::InitFailed),
        };

        let data = data?;
        let path = Path::new(&data_dir).join(format!("sst-{id}"));
        let mut writer = WriteableSSTable::create(&path)?;

        let bytes_written = writer.write(&data)?;
        Ok(SSTable {
            id,
            index,
            filter,
            first_key: data.meta.first_key,
            last_key: data.meta.last_key,
            path,
            size: bytes_written,
        })
    }
}

pub(crate) fn build_sstable(
    memtable_id: usize,
    memtable_iter: Iter<'_, Bytes, Value>,
    block_size: usize,
    data_dir: PathBuf,
) -> Result<SSTable, SSTableError> {
    let mut block_builder = BlockBuilder::init(1, block_size);
    let mut index_builder = IndexBuilder::builder();
    memtable_iter.for_each(|entry| {
        let key = entry.key().to_vec();
        let value = match entry.value() {
            Value::Plain(v) => v.to_vec(),
            Value::Tombstone => vec![],
        };

        block_builder.encode_pair(&key, &value);
        let block_offset = block_builder.latest_block_offset();
        if let Some(key_offset) = block_builder.latest_key_offset() {
            index_builder.add_entry(&key, block_offset, key_offset);
        }
    });

    SSTableData::init(
        memtable_id,
        &mut block_builder,
        &mut index_builder,
        data_dir,
    )
}

pub(crate) mod block {
    use super::BufMut;
    use super::U16_SIZE;
    use crate::filter::Filter;
    use crate::filter::bloom::Bloom;
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
    pub(crate) struct MetaBlock {
        pub(super) filter: Bloom,
        pub(super) first_key: Vec<u8>,
        pub(super) last_key: Vec<u8>,
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
            true
        }

        pub(super) fn size(&self) -> usize {
            self.filter.size() + self.first_key.len() + self.last_key.len() + U16_SIZE * 2
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
            self.key_hashes.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{
        memtable::{Buffer, Memtable},
        sst::{
            block::{BlockBuilder, DecodingError},
            build_sstable,
        },
    };
    use bytes::{BufMut, Bytes};
    use pretty_assertions::assert_eq;
    use temp_dir::TempDir;

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

    fn test_memtable(path: PathBuf) -> Memtable {
        let options = crate::memtable::TableOptions {
            data_dir: path,
            max_size: 256,
        };
        Memtable::create(1, Some(options))
    }
    #[test]
    fn build_sstable_from_memtable_contents() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path();
        let table = test_memtable(path.to_path_buf());
        let k1 = Bytes::from("1");
        let v1 = Bytes::from("123");

        let k2 = Bytes::from("2");
        let v2 = Bytes::from("456");

        let k3 = Bytes::from("3");
        let v3 = Bytes::from("789");

        let k4 = Bytes::from("4");
        let v4 = Bytes::from("0000");

        let _ = table.put(&k1, &v1);
        let _ = table.put(&k2, &v2);
        let _ = table.put(&k3, &v3);
        let _ = table.put(&k4, &v4);

        let datadir = tempdir.path();
        let result = build_sstable(1, table.store.iter(), 32, datadir.to_path_buf());
        assert!(result.is_ok());

        let sst = result.unwrap();

        assert!(sst.size > 0);
    }

    #[test]
    fn memtable_freeze_creates_sst() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path();
        let table = test_memtable(path.to_path_buf());
        let k1 = Bytes::from("1");
        let v1 = Bytes::from("123");

        let k2 = Bytes::from("2");
        let v2 = Bytes::from("456");

        let k3 = Bytes::from("3");
        let v3 = Bytes::from("789");

        let _ = table.put(&k1, &v1);
        let _ = table.put(&k2, &v2);
        let _ = table.put(&k3, &v3);

        let result = table.flush();
        assert!(result.is_ok());

        let sst = result.unwrap();
        assert!(sst.size > 0);
    }
}
