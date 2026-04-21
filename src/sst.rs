use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    filter::{Filter, bloom::Bloom},
    index::{Index, IndexBuilder},
    sst::block::{Block, BlockBuilder, MetaBlock},
    types::{Pair, Value},
};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::map::Iter;

pub(crate) const U16_SIZE: usize = std::mem::size_of::<u16>();
pub(crate) const U32_SIZE: usize = std::mem::size_of::<u32>();
pub(crate) const U64_SIZE: usize = std::mem::size_of::<u64>();

pub(crate) const BLOCK_SIZE: usize = 4 * 1024;
pub(crate) const MAX_TABLE_SIZE: usize = 64 * 1024 * 1024;
pub(crate) const FOOTER_SIZE: usize = 24;

pub(crate) const SST_FILE_PREFIX: &str = "sst";

///
/// The on-disk format of an SSTable is as follows:
///
/// +----------------+----------------+-------+----------------+--------+--------+----------+
/// |  Data block 1  |  Data block 2  |  ...  |  Data block N  |  Index |  Meta  |  Footer  |
/// +----------------+----------------+-------+----------------+--------+--------+----------+
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
    meta: MetaBlock,
    file: Arc<File>,
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
    fn update_index_offset(&mut self, new: u64) {
        self.index_offset = new;
    }

    fn update_meta_offset(&mut self, new: u64) {
        self.meta_offset = new;
    }

    fn encode(&self, buf: &mut Vec<u8>) -> bool {
        let id = self.sst_id as u32;
        buf.put_u32_le(id);
        buf.put_u64_le(self.index_offset);
        buf.put_u64_le(self.meta_offset);
        buf.put_slice(&self.magic);
        true
    }

    fn decode(mut buf: &[u8]) -> Self {
        let id = buf.get_u32_le();
        let index_offset = buf.get_u64_le();
        let meta_offset = buf.get_u64_le();
        let magic_len = U32_SIZE;
        let magic = buf[..magic_len].to_vec();

        Self {
            sst_id: id as usize,
            index_offset,
            meta_offset,
            magic,
            size: FOOTER_SIZE,
        }
    }

    fn size(&self) -> usize {
        self.size
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum SSTableError {
    #[error("Failed to initialize SSTable")]
    InitFailed,
    #[error("Unable to flush memtable to SSTable")]
    WriteFailed(#[from] std::io::Error),
    #[error("Unable to recover SSTable from file")]
    FailedToOpen,
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

    pub(crate) fn write(&mut self, data: &mut SSTableData) -> Result<usize, std::io::Error> {
        assert_eq!(data.footer.size, FOOTER_SIZE);

        let mut size = 0;
        for data_block in data.blocks.iter() {
            size += self.file.write(&data_block.data)?;

            let offsets_len = U16_SIZE * data_block.offsets.len();
            let mut offset_buf = vec![0; offsets_len];
            for offset in data_block.offsets.iter() {
                offset_buf.put_u16_le(*offset);
            }
            size += self.file.write(&offset_buf)?;
        }

        data.footer.update_index_offset(size as u64);
        let index_size = data.index.size();
        let mut index_buf = Vec::with_capacity(index_size);
        data.index.encode(&mut index_buf);
        size += self.file.write(&index_buf)?;

        data.footer.update_meta_offset(size as u64);
        let meta_size = data.meta.size();
        let mut meta_buf = Vec::with_capacity(meta_size);
        data.meta.encode(&mut meta_buf);
        size += self.file.write(&meta_buf)?;

        let mut footer_buf = Vec::with_capacity(data.footer.size);
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
        let filter = Bloom::build(block_builder.key_hashes());

        let data = match (index.first_key(), index.last_key()) {
            (Some(first), Some(last)) => {
                let mut block_offsets: Vec<u64> = vec![0];
                for block in blocks.iter().skip(1) {
                    block_offsets.push(block.size() as u64);
                }

                let meta = MetaBlock {
                    filter: filter.clone(),
                    first_key: first.to_vec(),
                    last_key: last.to_vec(),
                    num_blks: blocks.len() as u16,
                    count_per_blk: blocks.iter().map(|blk| blk.num_entries).collect(),
                    block_offsets: block_offsets,
                };

                let magic = vec![0x48, 0x6f, 0x70, 0x65];
                let footer = TableFooter {
                    sst_id: id,
                    index_offset: 0,
                    meta_offset: 0,
                    magic: magic.clone(),
                    size: U32_SIZE * 2 + U64_SIZE * 2,
                };

                Ok(Self {
                    blocks,
                    index: index.clone(),
                    meta,
                    footer,
                })
            }
            (_, _) => Err(SSTableError::InitFailed),
        };

        let mut data = data?;
        let path = Path::new(&data_dir).join(format!("{}-{}", SST_FILE_PREFIX, id));
        let mut writer = WriteableSSTable::create(&path)?;

        let bytes_written = writer.write(&mut data)?;
        let read_only_file = OpenOptions::new().read(true).open(&path)?;

        Ok(SSTable {
            id,
            index,
            meta: data.meta,
            file: Arc::new(read_only_file),
            path,
            size: bytes_written,
        })
    }
}

impl SSTable {
    pub(crate) fn open(id: usize, path: &Path, file: Arc<File>) -> Result<Self, SSTableError> {
        let size: u64 = match file.metadata() {
            Ok(metadata) => metadata.len(),
            Err(_e) => {
                return Err(SSTableError::FailedToOpen);
            }
        };

        let footer_offset = size - FOOTER_SIZE as u64;
        let raw_footer_data = Self::read_at(&file, FOOTER_SIZE, footer_offset)?;
        let footer = TableFooter::decode(&raw_footer_data);

        let meta_size = footer_offset - footer.meta_offset;
        let raw_meta_data = Self::read_at(&file, meta_size as usize, footer.meta_offset)?;
        let meta = MetaBlock::decode(&raw_meta_data);

        let index_size = footer.meta_offset - footer.index_offset;
        let raw_index_data = Self::read_at(&file, index_size as usize, footer.index_offset)?;
        let index = Index::decode(&raw_index_data, index_size as usize, footer.index_offset);

        Ok(Self {
            id,
            index,
            meta,
            file,
            path: path.to_path_buf(),
            size: size as usize,
        })
    }

    pub(crate) fn probe(&self, key: &Bytes) -> bool {
        self.meta.filter.may_contain(key)
    }

    fn read_at(file: &File, len: usize, offset: u64) -> Result<Vec<u8>, std::io::Error> {
        let mut buf = Vec::with_capacity(len);
        buf.resize(len, 0);
        file.read_exact_at(&mut buf, offset)?;
        Ok(buf)
    }

    pub(crate) fn fetch(&self, key: &Bytes) -> Result<Bytes, SSTableError> {
        if let Some(found) = self.index.find_idx_entry_by_key(key) {
            let (idx_entry, block_size) = found;
            let block_offset = idx_entry.block_offset();
            let key_offset = idx_entry.key_offset();

            let raw_block = Self::read_at(&self.file, block_size as usize, block_offset)?;
            let pair = Block::decode_pair(&raw_block, key_offset as usize);
            assert_eq!(key, pair.0);

            Ok(Bytes::copy_from_slice(pair.1))
        } else {
            Err(SSTableError::InitFailed)
        }
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

type RawPair<'a> = (&'a [u8], &'a [u8]);

pub(crate) mod block {
    use super::BufMut;
    use super::U16_SIZE;
    use crate::filter::Filter;
    use crate::filter::bloom::Bloom;
    use crate::sst::RawPair;
    use crate::sst::U32_SIZE;
    use crate::sst::U64_SIZE;
    use bytes::Buf;
    use std::vec;

    #[derive(Debug, Clone)]
    pub(crate) struct Block {
        pub(crate) id: usize,
        pub(crate) data: Vec<u8>,
        pub(crate) offsets: Vec<u16>,
        pub(crate) num_entries: u16,
    }

    impl Block {
        pub(crate) fn size(&self) -> usize {
            self.data.len()
        }

        pub(crate) fn first_key(&self) -> Vec<u8> {
            let mut buf = &self.data[..];
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
            self.key_hashes.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, path::PathBuf, sync::Arc};

    use crate::{
        memtable::{Buffer, Memtable},
        sst::{
            SST_FILE_PREFIX, SSTable,
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

        let expected_path = path.join(format!("{}-{}", SST_FILE_PREFIX, table.id.clone()));
        let file = OpenOptions::new().read(true).open(&expected_path);

        assert!(file.is_ok());
        let file = file.unwrap();

        let sst = SSTable::open(table.id, &expected_path, Arc::new(file));
        assert!(sst.is_ok());
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
        assert!(sst.probe(&k1));
    }
}
