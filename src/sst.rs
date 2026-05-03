use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use crate::{
    block::{
        Block, BlockBuilder, BlockInfo, MetaBlock, iterator::BlockIterError,
        iterator::BlockIterator,
    },
    filter::{Filter, bloom::Bloom},
    index::{Index, IndexBuilder},
    iterator::StorageIter,
    types::Value,
};
use bytes::{Buf, BufMut, Bytes};

pub(crate) const U16_SIZE: usize = std::mem::size_of::<u16>();
pub(crate) const U32_SIZE: usize = std::mem::size_of::<u32>();
pub(crate) const U64_SIZE: usize = std::mem::size_of::<u64>();

pub(crate) const MAX_TABLE_SIZE: usize = 64 * 1024 * 1024;
pub(crate) const FOOTER_SIZE: usize = 24;

pub(crate) const SST_FILE_PREFIX: &str = "sst";

pub(crate) mod iterator;

#[derive(Clone, Debug)]
pub(crate) struct SSTableData {
    pub(crate) blocks: Vec<Block>,
    pub(crate) index: Index,
    pub(crate) meta: MetaBlock,
    pub(crate) footer: TableFooter,
}

///
/// The on-disk format of an SSTable is as follows:
///
/// +----------------+----------------+-------+----------------+--------+--------+----------+
/// |  Data block 1  |  Data block 2  |  ...  |  Data block N  |  Index |  Meta  |  Footer  |
/// +----------------+----------------+-------+----------------+--------+--------+----------+
///
#[derive(Clone, Debug)]
pub(crate) struct SSTable {
    pub(crate) id: usize,
    pub(crate) index: Index,
    pub(crate) meta: MetaBlock,
    pub(crate) file: Arc<File>,
    pub(crate) path: PathBuf,
    pub(crate) size: usize,
}

///
/// Footer including SST information including beginning offsets of index and meta blocks.
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
    #[error("Failed to create SST file")]
    InitFailed,
    #[error("Unable to flush memtable to SSTable")]
    WriteFailed(#[from] std::io::Error),
    #[error("Unable to recover SST from file")]
    FailedToOpen,
    #[error("Cannot create an iterator for given block")]
    InvalidBlock,
    #[error("Block is empty")]
    EmptyBlock(#[from] BlockIterError),
    #[error("Key does not exist in this block")]
    NonexistentKey,
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

            let offsets_len = data_block.offsets_data_size();
            let mut offset_buf = Vec::with_capacity(offsets_len);
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
        path: PathBuf,
    ) -> Result<SSTable, SSTableError> {
        let blocks: Vec<Block> = block_builder.build();
        let index_start_offset = blocks.iter().map(|b| b.data_length()).sum::<usize>() as u64;
        let index = index_builder.build(index_start_offset);
        let filter = Bloom::build(block_builder.key_hashes());

        let data = match (index.first_key(), index.last_key()) {
            (Some(first), Some(last)) => {
                let mut block_offsets: Vec<u64> = vec![];
                block_offsets.push(0);
                for block in blocks.iter() {
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
        let mut writer = WriteableSSTable::create(&path)?;

        let bytes_written = writer.write(&mut data)?;
        let read_only_file = OpenOptions::new().read(true).open(&path)?;

        eprintln!("{} bytes written to file {:?}", bytes_written, &path);
        let sst = SSTable::open(id, &path, Arc::new(read_only_file))?;

        Ok(sst)
    }
}

impl SSTable {
    pub(crate) fn id(&self) -> usize {
        self.id
    }

    pub(crate) fn table_size(&self) -> usize {
        self.size
    }

    pub(crate) fn first_key(&self) -> &[u8] {
        &self.meta.first_key
    }

    pub(crate) fn last_key(&self) -> &[u8] {
        &self.meta.last_key
    }

    pub(crate) fn created_at(&self) -> io::Result<SystemTime> {
        let metadata = self.file.metadata()?;
        metadata.created()
    }

    pub(crate) fn open(id: usize, path: &Path, file: Arc<File>) -> Result<Self, SSTableError> {
        let size: u64 = match file.metadata() {
            Ok(metadata) => metadata.len(),
            Err(_e) => {
                return Err(SSTableError::FailedToOpen);
            }
        };

        eprintln!("SST File {} - size: {}", path.display(), size);
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
        let mut buf = vec![0; len];
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

            if pair.1.is_empty() {
                return Err(SSTableError::NonexistentKey);
            }

            Ok(Bytes::copy_from_slice(pair.1))
        } else {
            Err(SSTableError::InitFailed)
        }
    }

    pub(crate) fn contains(&self, key: &Bytes) -> bool {
        &self.meta.first_key <= key && key <= &self.meta.last_key
    }

    pub(crate) fn precedes_range(&self, key: &Bytes) -> bool {
        key < &self.meta.first_key
    }

    pub(crate) fn find_block_by_key(&self, key: &Bytes) -> Result<BlockInfo, SSTableError> {
        if let Some(entry) = self.index.find_idx_entry_by_key(key) {
            let (idx_entry, block_size) = entry;
            let block_offset = idx_entry.block_offset();

            let block_idx = match self
                .meta
                .block_offsets
                .iter()
                .find(|&&offset| offset == block_offset)
            {
                Some(idx) => *idx as usize,
                None => {
                    eprintln!("Unable to find block idx from offset");
                    return Err(SSTableError::InvalidBlock);
                }
            };

            Ok(BlockInfo::new(block_idx, block_offset, block_size))
        } else {
            eprintln!("Key {:?} doesn't exist in table: {}", key, self.id);
            Err(SSTableError::NonexistentKey)
        }
    }

    pub(crate) fn find_block_for_seek(&self, key: &[u8]) -> Result<BlockInfo, SSTableError> {
        let (idx_entry, block_size) = self.index.seek(key).ok_or(SSTableError::NonexistentKey)?;

        let block_offset = idx_entry.block_offset();
        let block_idx = self
            .meta
            .block_offsets
            .iter()
            .position(|&offset| offset == block_offset)
            .ok_or(SSTableError::InvalidBlock)?;

        Ok(BlockInfo::new(block_idx, block_offset, block_size))
    }

    pub(crate) fn block_iter(&self, block_idx: usize) -> Result<BlockIterator, SSTableError> {
        if block_idx >= self.meta.num_blks as usize {
            return Err(SSTableError::InvalidBlock);
        }

        let block_start: u64 = match self.meta.block_offsets.get(block_idx) {
            Some(offset) => *offset,
            None => return Err(SSTableError::InvalidBlock),
        };

        let next_offset: u64 = if block_idx == self.meta.block_offsets.len() {
            self.index.start
        } else {
            *self.meta.block_offsets.get(block_idx + 1).unwrap()
        };
        let block_size = (next_offset - block_start) as usize;

        let num_entries = *self.meta.count_per_blk.get(block_idx).unwrap();
        let offset_data_size: usize = U16_SIZE * (num_entries as usize);
        let offset_data_start = next_offset - offset_data_size as u64;
        let block_info = BlockInfo::new(block_idx, block_start, block_size as u64);

        let block =
            self.load_block(block_info, offset_data_start as usize, num_entries as usize)?;

        match BlockIterator::new(Arc::new(block)) {
            Ok(iter) => Ok(iter),
            Err(_) => Err(SSTableError::InvalidBlock),
        }
    }

    pub(crate) fn block_iter_by_offset(
        &self,
        block_info: BlockInfo,
    ) -> Result<BlockIterator, SSTableError> {
        let num_entries = *self.meta.count_per_blk.get(block_info.idx()).unwrap();
        let offset_data_size: usize = U16_SIZE * num_entries as usize;
        let offset_data_start: usize = (block_info.size() as usize) - offset_data_size;

        let block = self.load_block(block_info, offset_data_start, num_entries as usize)?;

        match BlockIterator::new(Arc::new(block)) {
            Ok(iter) => Ok(iter),
            Err(_) => Err(SSTableError::InvalidBlock),
        }
    }

    fn load_block(
        &self,
        block_info: BlockInfo,
        offset_data_start: usize,
        num_entries: usize,
    ) -> Result<Block, SSTableError> {
        let mut raw_block =
            Self::read_at(&self.file, block_info.size() as usize, block_info.offset())?;
        Ok(Block::decode_full(
            &mut raw_block,
            block_info.idx(),
            offset_data_start,
            num_entries,
        ))
    }
}

pub(crate) fn build_sstable(
    memtable_id: usize,
    iter: impl StorageIter<KeyVal = (Bytes, Value)>,
    block_size: usize,
    path: PathBuf,
    skip_empty_values: bool,
) -> Result<SSTable, SSTableError> {
    let mut block_builder = BlockBuilder::init(1, block_size);
    let mut index_builder = IndexBuilder::builder();

    iter.for_each(|pair| {
        let key = pair.0.to_vec();
        let value = match pair.1 {
            Value::Plain(v) => v.to_vec(),
            Value::Tombstone => vec![],
        };

        if value.is_empty() && skip_empty_values {
            return;
        }

        block_builder.encode_pair(&key, &value);
        let block_offset = block_builder.latest_block_offset();
        if let Some(key_offset) = block_builder.latest_key_offset() {
            index_builder.add_entry(&key, block_offset, key_offset);
        }
    });

    SSTableData::init(memtable_id, &mut block_builder, &mut index_builder, path)
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, path::PathBuf, sync::Arc};

    use crate::{
        memtable::{Buffer, Memtable, MemtableIterator},
        sst::{SSTable, build_sstable},
    };
    use bytes::Bytes;
    use temp_dir::TempDir;

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

        let sst_path = path.join("sst-file.sst");
        let result = build_sstable(
            1,
            MemtableIterator::from_map(table.store.clone()),
            32,
            sst_path,
            false,
        );
        assert!(result.is_ok());

        let sst = result.unwrap();

        assert!(sst.size > 0);

        let expected_path = path.join("sst-file.sst");
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

        let result = table.flush(path.join(format!("sst-file.sst")));
        assert!(result.is_ok());

        let unknown_key = Bytes::from("4");

        let sst = result.unwrap();
        assert!(sst.size > 0);
        assert!(sst.probe(&k1));
        assert!(sst.contains(&k2));
        assert!(!sst.contains(&unknown_key));
    }
}
