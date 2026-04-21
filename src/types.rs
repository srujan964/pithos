#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Pair {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Pair {
    pub(crate) fn new(key: &[u8], value: &[u8]) -> Self {
        Pair {
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    pub(crate) fn key(&self) -> Vec<u8> {
        self.key.clone()
    }

    pub(crate) fn value(&self) -> Vec<u8> {
        self.value.clone()
    }
}
