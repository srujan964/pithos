pub(crate) mod bloom;

pub(crate) trait Filter {
    /// Test an item for membership within a Filter.
    fn may_contain(&self, item: &[u8]) -> bool;

    /// Build a Filter instance from key hashes.
    fn build(hashes: &[u64]) -> Self;

    /// Serialize contents to a buffer.
    fn decode(buf: &[u8]) -> Self;

    /// Deserialize into a Filter instance from a buffer.
    fn encode(&self, buf: &mut [u8]);
}

/// An immutable bit vector.
pub(crate) trait BitSet {
    /// Check whether the bit at given position is set or not.
    fn is_set(&self, idx: usize) -> bool;

    /// Length of the vector in bits.
    fn len(&self) -> usize;
}

/// A mutable bit vector.
pub(crate) trait BitSetMut {
    /// Set a bit in the vector.
    fn set(&mut self, idx: usize);
}
