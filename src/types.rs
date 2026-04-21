use bytes::Bytes;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Value {
    Plain(Bytes),
    Tombstone,
}

pub(crate) type Pair = (Bytes, Value);
