use bytes::BufMut;

use crate::sst;

#[derive(Debug, Clone)]
pub(crate) struct IndexEntry {
    key: Vec<u8>,
    block_offset: u64,
    key_offset: u16,
}

///
/// +---------------------+-------+--------------------------+--------------------+
/// |  key_len (2 bytes)  |  key  |  block_offset (8 bytes)  |  offset (2 bytes)  |
/// +---------------------+-------+--------------------------+--------------------+
///
#[derive(Debug, Clone)]
pub(crate) struct Index {
    pub(crate) start: u64,
    pub(crate) blocks: Vec<IndexEntry>,
    size: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexBuilder {
    entries: Vec<IndexEntry>,
}

impl IndexEntry {
    pub(crate) fn offsets(&self) -> (u64, u16) {
        (self.block_offset, self.key_offset)
    }

    pub(crate) fn size(&self) -> usize {
        sst::U16_SIZE + self.key.len() + sst::U64_SIZE + sst::U16_SIZE
    }
}

impl IndexBuilder {
    pub(crate) fn builder() -> Self {
        IndexBuilder { entries: vec![] }
    }

    pub(crate) fn add_entry(&mut self, key: &[u8], block_offset: u64, key_offset: u16) {
        self.entries.push(IndexEntry {
            key: key.to_vec(),
            block_offset,
            key_offset,
        })
    }

    pub(crate) fn build(&mut self, start: u64) -> Index {
        Index {
            start,
            blocks: self.entries.clone(),
            size: self.entries.iter().map(|b| b.size()).sum(),
        }
    }
}

impl Index {
    pub(crate) fn find_block_offset(&self, key: &[u8]) -> Option<u64> {
        self.blocks
            .binary_search_by_key(&key.to_vec(), |entry| entry.key.clone())
            .map(|idx: usize| self.blocks[idx].block_offset)
            .ok()
    }

    pub(crate) fn find_block_by_key(&self, key: &[u8]) -> Option<&IndexEntry> {
        self.blocks
            .binary_search_by_key(&key.to_vec(), |entry| entry.key.clone())
            .map(|idx: usize| &self.blocks[idx])
            .ok()
    }

    pub(crate) fn encode(&self, mut buf: &mut [u8]) -> bool {
        for entry in self.blocks.iter() {
            let key_len = entry.key.len() as u16;
            buf.put_u16_le(key_len);
            buf.put(entry.key.as_ref());
            buf.put_u64_le(entry.block_offset);
            buf.put_u16_le(entry.key_offset)
        }
        true
    }

    pub(crate) fn first_key(&self) -> Option<&[u8]> {
        self.blocks.first().map(|entry| entry.key.as_ref())
    }

    pub(crate) fn last_key(&self) -> Option<&[u8]> {
        self.blocks.last().map(|entry| entry.key.as_ref())
    }

    pub(crate) fn size(&self) -> usize {
        self.size
    }
}

#[cfg(test)]
mod tests {
    use bytes::Buf;

    use crate::index::Index;
    use crate::index::IndexEntry;

    #[test]
    fn find_index_block_offset_by_key() {
        let blocks: Vec<IndexEntry> = (1..=10)
            .map(|i| {
                let key = format!("{i}000");
                IndexEntry {
                    key: key.into(),
                    block_offset: i * 1000_u64,
                    key_offset: 100u16,
                }
            })
            .collect();
        let index = Index {
            start: 0,
            blocks,
            size: 1024,
        };

        let offset = index.find_block_offset("7000".as_bytes());

        assert!(offset.is_some());
        assert_eq!(offset.unwrap(), 7000u64)
    }

    #[test]
    fn find_index_block_returns_none_for_nonexistent_key() {
        let blocks: Vec<IndexEntry> = (1..=5)
            .map(|i| {
                let key = "000".to_string();
                IndexEntry {
                    key: key.into(),
                    block_offset: i * 1000_u64,
                    key_offset: 100u16,
                }
            })
            .collect();
        let index = Index {
            start: 0,
            blocks,
            size: 1024,
        };

        let offset = index.find_block_offset("7000".as_bytes());

        assert!(offset.is_none());
    }

    #[test]
    fn encode_index_writes_contents_to_buffer() {
        let blocks: Vec<IndexEntry> = (1..=5)
            .map(|i| {
                let key = "000".to_string();
                IndexEntry {
                    key: key.into(),
                    block_offset: i * 1000_u64,
                    key_offset: 100u16,
                }
            })
            .collect();
        let index = Index {
            start: 0,
            blocks: blocks.clone(),
            size: 1024,
        };

        let size = index.size();
        let mut buf = vec![0; size];

        let ok = index.encode(&mut buf);

        assert!(ok);
        assert!(!buf.is_empty());

        let mut buf: &[u8] = &buf;

        for i in 0..blocks.len() {
            let key_len = buf.get_u16_le() as usize;
            let key = buf[..key_len].to_vec();
            buf.advance(key_len as usize);

            let block_offset = buf.get_u64_le();
            let key_offset = buf.get_u16_le();

            assert_eq!(key, blocks[i].key);
            assert_eq!(block_offset, blocks[i].block_offset);
            assert_eq!(key_offset, blocks[i].key_offset);
        }
    }
}
