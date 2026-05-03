pub mod types {
    use bytes::Buf;

    pub trait Type {
        fn to_bytes(&self) -> Vec<u8>;

        fn from_bytes(data: &[u8]) -> Self;
    }

    #[derive(thiserror::Error, Debug)]
    pub enum ParseError {
        #[error("Cannot parse bytes to Value type")]
        InvalidBytes,
        #[error("String bytes contains non-UTF8 characters")]
        NonUtf8Bytes(#[from] std::string::FromUtf8Error),
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum Value {
        String(String),
        Bytes(Vec<u8>),
        Float64(f64),
        I64(i64),
        U64(u64),
        Boolean(bool),
    }

    impl Value {
        pub fn kind(&self) -> &'static str {
            match self {
                Self::String(_) => "String",
                Self::Bytes(_) => "bytes",
                Self::Float64(_) => "f64",
                Self::I64(_) => "i64",
                Self::U64(_) => "u64",
                Self::Boolean(_) => "bool",
            }
        }

        pub(crate) fn as_bytes(&self) -> Vec<u8> {
            let mut buf = vec![];
            match self {
                Self::String(contents) => {
                    buf.push(1);
                    buf.extend_from_slice(contents.as_bytes());
                }
                Self::Bytes(contents) => {
                    buf.push(2);
                    buf.extend_from_slice(contents.as_slice());
                }
                Self::Float64(value) => {
                    buf.push(3);
                    buf.extend_from_slice(&value.to_le_bytes());
                }
                Self::I64(value) => {
                    buf.push(4);
                    buf.extend_from_slice(&value.to_le_bytes());
                }
                Self::U64(value) => {
                    buf.push(5);
                    buf.extend_from_slice(&value.to_le_bytes());
                }
                Self::Boolean(value) => {
                    buf.push(6);
                    buf.push(u8::from(*value));
                }
            };

            buf
        }

        pub(crate) fn from_bytes(bytes: Vec<u8>) -> Result<Self, ParseError> {
            let mut buf: &[u8] = bytes.as_ref();
            let identifier: u8 = buf.get_u8();

            match identifier {
                1 => {
                    let string = String::from_utf8(buf.to_vec())?;
                    Ok(Self::String(string))
                }
                2 => Ok(Self::Bytes(buf.to_vec())),
                3 => Ok(Self::Float64(buf.get_f64_le())),
                4 => Ok(Self::I64(buf.get_i64_le())),
                5 => Ok(Self::U64(buf.get_u64_le())),
                6 => {
                    let b = buf.get_u8() != 0;
                    Ok(Self::Boolean(b))
                }
                _ => Err(ParseError::InvalidBytes),
            }
        }
    }
}

pub mod storage {
    use std::sync::Arc;

    use crate::{
        api::types::Value,
        compaction::CompactionOptions,
        core::{CoreOptions, CoreStorage, CoreStorageInner},
        iterator::CombinedIterator,
        memtable::Memtable,
    };

    use bytes::Bytes;

    const NUM_MEMTABLE_LIMIT: usize = 3;

    #[derive(Clone, Debug)]
    pub struct StorageOptions {
        data_dir: String,
        max_memtable_size: usize,
        memtable_limit: usize,
    }

    impl StorageOptions {
        pub fn new(data_dir: String, max_memtable_size: usize) -> StorageOptions {
            StorageOptions {
                data_dir,
                max_memtable_size,
                memtable_limit: NUM_MEMTABLE_LIMIT,
            }
        }
    }

    #[derive(Clone, Debug, thiserror::Error)]
    pub enum StorageError {
        #[error("Unable to insert value")]
        InsertFailed,
        #[error("Unable to delete key")]
        DeleteFailed,
        #[error("Unable to create iterator")]
        IterationFailed,
        #[error("There was an issue closing the storage")]
        UnableToClose,
    }

    #[derive(Debug)]
    pub struct Storage {
        storage: Arc<CoreStorage<Memtable>>,
    }

    type IteratorInner = CombinedIterator;

    pub struct Iterator {
        inner: IteratorInner,
    }

    impl std::iter::Iterator for crate::api::storage::Iterator {
        type Item = (Vec<u8>, Vec<u8>);

        fn next(&mut self) -> Option<Self::Item> {
            match self.inner.next() {
                Some((k, v)) => match v {
                    crate::types::Value::Plain(v) => Some((k.to_vec(), v.to_vec())),
                    crate::types::Value::Tombstone => Some((k.to_vec(), vec![])),
                },
                None => None,
            }
        }
    }

    impl Storage {
        pub fn open(options: StorageOptions) -> Result<Storage, StorageError> {
            if let Ok(storage) = CoreStorage::open(Some(CoreOptions {
                data_dir: options.data_dir.into(),
                max_memtable_size: options.max_memtable_size,
                memtable_limit: options.memtable_limit,
                compaction_opts: CompactionOptions::default(),
            })) {
                Ok(Storage { storage })
            } else {
                Err(StorageError::IterationFailed)
            }
        }

        pub fn get(&self, key: Vec<u8>) -> Option<Value> {
            match self.storage.get(Bytes::from(key)) {
                Ok(bytes) => Value::from_bytes(bytes.to_vec()).ok(),
                Err(_) => None,
            }
        }

        pub fn put(&self, key: Vec<u8>, value: Value) -> Result<(), StorageError> {
            let value = value.as_bytes();
            self.storage
                .put(Bytes::from(key), Bytes::from(value))
                .map_err(|_| StorageError::InsertFailed)
        }

        pub fn delete(&self, key: Vec<u8>) -> Result<(), StorageError> {
            self.storage
                .delete(Bytes::from(key))
                .map_err(|_| StorageError::DeleteFailed)
        }

        pub fn scan(
            &self,
            start: Vec<u8>,
            end: Vec<u8>,
        ) -> Result<crate::api::storage::Iterator, StorageError> {
            match self.storage.scan(Bytes::from(start), Bytes::from(end)) {
                Ok(iter) => Ok(crate::api::storage::Iterator { inner: iter }),
                Err(_) => Err(StorageError::IterationFailed),
            }
        }

        pub fn close(&self) -> Result<(), StorageError> {
            match self.storage.close() {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Error closing storage: {:?}", e);
                    return Err(StorageError::UnableToClose);
                }
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::api::types::Value;
    use pretty_assertions::assert_eq;

    #[test]
    fn value_bytes_from_string() {
        let raw_value = "Lorem ipsum dolor sit amet.";
        let string_val = String::from(raw_value);
        let value = Value::String(string_val.clone());

        let mut expected_bytes = vec![1];
        expected_bytes.extend_from_slice(string_val.as_bytes());
        assert_eq!(value.as_bytes(), expected_bytes);
        assert_eq!(Value::from_bytes(expected_bytes).unwrap(), value);
    }

    #[test]
    fn value_bytes_from_byte_vec() {
        let bytes = vec![1u8, 2u8, 3u8, 4u8, 5u8];
        let value = Value::Bytes(bytes.clone());

        let expected_bytes = vec![2u8, 1u8, 2u8, 3u8, 4u8, 5u8];
        assert_eq!(value.as_bytes(), expected_bytes);
        assert_eq!(Value::from_bytes(expected_bytes).unwrap(), value);
    }

    #[test]
    fn value_bytes_from_float() {
        let float = f64::MAX;
        let value = Value::Float64(float);

        let mut expected_bytes = vec![3];
        expected_bytes.extend_from_slice(&float.to_le_bytes());
        assert_eq!(value.as_bytes(), expected_bytes);
        assert_eq!(Value::from_bytes(expected_bytes).unwrap(), value);
    }

    #[test]
    fn value_bytes_from_signed_int() {
        let int = i64::MAX;
        let value = Value::I64(int);

        let mut expected_bytes = vec![4];
        expected_bytes.extend_from_slice(&int.to_le_bytes());
        assert_eq!(value.as_bytes(), expected_bytes);
        assert_eq!(Value::from_bytes(expected_bytes).unwrap(), value);
    }

    #[test]
    fn value_bytes_from_unsigned_int() {
        let int = u64::MAX;
        let value = Value::U64(int);

        let mut expected_bytes = vec![5];
        expected_bytes.extend_from_slice(&int.to_le_bytes());
        assert_eq!(value.as_bytes(), expected_bytes);
        assert_eq!(Value::from_bytes(expected_bytes).unwrap(), value);
    }

    #[test]
    fn value_bytes_from_bool() {
        let value = Value::Boolean(true);

        let expected_bytes = vec![6, 1];
        assert_eq!(value.as_bytes(), expected_bytes);
        let result = Value::from_bytes(expected_bytes);

        assert_eq!(result.unwrap(), value);
    }
}
