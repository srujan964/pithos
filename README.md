# Pithos

A key-value database storage engine balancing reads and writes,
attempting to improve range scan performance.


## Usage

```rust
    use pithos::api::{
        storage::{Storage, StorageOptions},
        types::Value,
    };

    let opts = StorageOptions::new(
        "/tmp/data".to_string(),
        64 * 1024 * 1024,
        CompactionOptions::default(),
    );

    let storage = Storage::open(opts)?;

    storage.put("Key".into(), Value::String("Value".to_string()))?;

    let value: Value = storage.get("Key".into()).unwrap();

    let pairs: Vec<_> = storage
        .scan("starting_key".into(), "ending_key".into())?
        .collect();
```

## Examples

An minimal example CLI is available in `examples/`. Run it using:
```
  mkdir /tmp/data
  cargo run --examples cli /tmp/data
```
