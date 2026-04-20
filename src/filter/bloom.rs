use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{
    filter::{BitSet, BitSetMut, Filter},
    sst::U16_SIZE,
};

/// Ideally, we only suffer an unnecessary disk-access 1 in every 100 keys.
const FALSE_POS_RATE: f64 = 0.01;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Bloom {
    filter: Bytes,
    k: u8,
}

impl Filter for Bloom {
    fn may_contain(&self, item: &[u8]) -> bool {
        let hash: u64 = seahash::hash(item);
        for i in 0..self.k {
            let pos = Bloom::ith_hash(hash, i as usize, BitSet::len(&self.filter) as u64);
            if !self.filter.is_set(pos as usize) {
                return false;
            }
        }
        true
    }

    fn build(hashes: &[u64]) -> Self {
        let Params {
            m,
            n: _,
            k,
            bits_per_key: _,
        } = Bloom::estimate_parameters(hashes.len(), FALSE_POS_RATE);
        let k = k.clamp(1, 30);

        // Convert required number of bits to bytes, rounding up to the next byte.
        let nbytes = m.div_ceil(8) as usize;
        let nbits = nbytes * 8;

        let mut filter = BytesMut::with_capacity(nbytes);
        filter.resize(nbytes, 0);
        for hash in hashes {
            let h = *hash;
            for i in 0..k {
                let pos = Bloom::ith_hash(h, i as usize, nbits as u64);
                filter.set(pos as usize);
            }
        }

        Self {
            filter: filter.freeze(),
            k: k as u8,
        }
    }

    fn decode(buf: &[u8]) -> Self {
        let mut buf = buf;
        let len = buf.get_u16_le() as usize;
        let filter = &buf[..len];
        buf.advance(len);

        let bits = &filter[..len - 1];
        let k = filter[len - 1];

        Self {
            filter: Bytes::copy_from_slice(bits),
            k,
        }
    }

    // The bloom filter is encoded as follows:
    //
    // +-------------------------------------------------+
    // |  len (16 bits)  |  filter bytes  |  k (8 bits)  |
    // +-------------------------------------------------+
    //
    fn encode(&self, buf: &mut Vec<u8>) {
        let filter_len: u16 = (self.filter.len() + 1) as u16;
        buf.put_u16_le(filter_len);
        buf.extend(&self.filter);
        buf.put_u8(self.k);
    }
}

#[derive(Debug)]
struct Params {
    m: u32,
    n: usize,
    k: u32,
    bits_per_key: usize,
}

impl Bloom {
    /// Estimate the number of hash functions needed for a given set size `n` and false-positive
    /// rate `fpr`.
    fn estimate_parameters(n: usize, fpr: f64) -> Params {
        // refer - https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions
        let m: f64 = -(n as f64) * fpr.ln() / std::f64::consts::LN_2.powi(2);
        let bits_per_key: f64 = (m / n as f64).ceil();
        let k = bits_per_key * std::f64::consts::LN_2;

        Params {
            m: m as u32,
            n,
            k: k as u32,
            bits_per_key: bits_per_key as usize,
        }
    }

    /// Rather than having `k` different hash functions, use the original 64-bit hash
    /// and instead compute the hash from the general form:
    ///
    /// ```g_i(x) = h1(x) + h2(x) * i % p```
    ///
    /// where h1(x) and h2(x) are independent hash functions with the range {0,1,...,p-1}.
    fn ith_hash(hash: u64, i: usize, m: u64) -> u64 {
        let h1 = hash & 0xFFFF_FFFF;
        let mut h2 = hash >> 32;
        h2 |= 1;
        h1.wrapping_add(h2.wrapping_mul(i as u64)) % m
    }

    pub(crate) fn size(&self) -> usize {
        self.filter.len() + U16_SIZE + 8
    }
}

/// Convert a bit index to a position within a byte group.
fn to_byte_index(bit_idx: usize) -> (usize, usize) {
    (bit_idx / 8, bit_idx % 8)
}

// Considering a slice of bytes as a bitset.
impl<T: AsRef<[u8]>> BitSet for T {
    fn is_set(&self, idx: usize) -> bool {
        let (byte_group, pos) = to_byte_index(idx);
        (self.as_ref()[byte_group] >> pos) & 1 != 0
    }

    fn len(&self) -> usize {
        self.as_ref().len() * 8
    }
}

// Considering a mutable slice of bytes as a bitset.
impl<T: AsMut<[u8]>> BitSetMut for T {
    fn set(&mut self, idx: usize) {
        let (byte_group, pos) = to_byte_index(idx);
        let mask = 1u8 << pos;
        self.as_mut()[byte_group] |= mask;
    }
}

#[cfg(test)]
mod tests {
    use crate::filter::{BitSet, BitSetMut, Filter, bloom::Bloom};

    #[test]
    fn bitset_set_sets_bit_at_position_to_one() {
        // Bitset contents: 00000000
        let mut bitset: Vec<u8> = vec![0; 2];

        bitset.set(0);

        assert_eq!(bitset[0], 1);
    }

    #[test]
    fn bitset_is_set_returns_true_if_bit_at_position_is_one() {
        // Bitset contents: 10101010
        let bitset: Vec<u8> = vec![0xA; 2];

        assert!(!bitset.is_set(2));
        assert!(bitset.is_set(3));
    }

    #[test]
    fn filter_contains_item_that_was_added_to_it_prior() {
        let keys = ["This", "is", "a", "test."];
        let hashes: Vec<u64> = keys.iter().map(|k| seahash::hash(k.as_bytes())).collect();

        let filter = Bloom::build(&hashes);

        assert!(filter.may_contain("is".as_bytes()))
    }

    #[test]
    fn filter_contains_all_inserted_keys() {
        let keys = [
            "Lorem ipsum dolor sit amet consectetur adipiscing elit",
            "Consectetur adipiscing elit quisque faucibus ex sapien vitae",
            "Ex sapien vitae pellentesque sem placerat in id",
            "Placerat in id cursus mi pretium tellus duis",
            "Pretium tellus duis convallis tempus leo eu aenean",
        ];
        let hashes: Vec<u64> = keys.iter().map(|k| seahash::hash(k.as_bytes())).collect();

        let filter = Bloom::build(&hashes);

        for k in keys {
            assert!(filter.may_contain(k.as_bytes()), "Expected key {}", k);
        }
    }

    #[test]
    fn filter_encode_to_bytes() {
        let keys = ["This", "is", "a", "test."];
        let hashes: Vec<u64> = keys.iter().map(|k| seahash::hash(k.as_bytes())).collect();
        let filter = Bloom::build(&hashes);

        let mut buf: Vec<u8> = vec![];
        filter.encode(&mut buf);

        let result = Bloom::decode(&buf);

        assert_eq!(result, filter);
    }
}
