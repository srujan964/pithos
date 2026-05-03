use bytes::Bytes;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Value {
    Plain(Bytes),
    Tombstone,
}

pub(crate) type Pair = (Bytes, Value);

impl Value {
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Value::Plain(bytes) => bytes.is_empty(),
            Value::Tombstone => true,
        }
    }
}
