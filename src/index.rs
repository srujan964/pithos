use crate::sst;

#[derive(Debug, Clone)]
pub(crate) struct IndexEntry {
    key: Vec<u8>,
    block_offset: u64,
    offset: u16,
}

#[derive(Debug, Clone)]
pub(crate) struct Index {
    pub(crate) start: u64,
    pub(crate) blocks: Vec<IndexEntry>,
    pub(crate) size: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexBuilder {
    entries: Vec<IndexEntry>,
}

impl IndexEntry {
    pub(crate) fn offsets(&self) -> (u64, u16) {
        (self.block_offset, self.offset)
    }

    pub(crate) fn size(&self) -> usize {
        self.key.len() + sst::U64_SIZE + sst::U16_SIZE
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
            offset: key_offset,
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
                    offset: 100u16,
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
                    offset: 100u16,
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
}
