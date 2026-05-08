use std::io::{self, BufRead, Write};
use std::process;
use std::time::Duration;

use pithos::api::{
    storage::{Storage, StorageOptions},
    types::Value,
};
use pithos::{CompactionOptions, FIFOCompactionOptions, LeveledCompactionOptions};

const MAX_MEMTABLE_SIZE: usize = 4 * 1024 * 1024;

const USAGE: &str = "\
Commands:
  get <key>
  put <key> <type> <value>
  delete <key>
  scan <start> <end>
  exit

Types for put: str | bytes | i64 | u64 | f64 | bool";

fn cold_opts(base_level_size_mb: usize) -> LeveledCompactionOptions {
    LeveledCompactionOptions {
        max_output_size_mb: 64,
        level_size_multiplier: 2,
        max_levels: 3,
        l0_num_files_threshold: 2,
        base_level_size_mb,
    }
}

fn compaction_opts(
    scheme: &str,
    hot_ttl_secs: u64,
    cold_base_level_mb: usize,
) -> CompactionOptions {
    match scheme {
        "tiered" => CompactionOptions::Tiered(FIFOCompactionOptions {
            hot_ttl: Duration::from_secs(hot_ttl_secs),
            max_hot_sst_count: 8,
            small_file_threshold_mb: 4,
            max_merge_output_mb: 16,
            cold: cold_opts(cold_base_level_mb),
            cold_max_total_size_mb: 512,
            cold_ttl: None,
        }),
        "leveled" | _ => CompactionOptions::default(),
    }
}

fn open(
    data_dir: &str,
    scheme: &str,
    hot_ttl_secs: u64,
    memtable_bytes: usize,
    cold_base_level_mb: usize,
) -> Storage {
    let opts = StorageOptions::new(
        data_dir.to_string(),
        memtable_bytes,
        compaction_opts(scheme, hot_ttl_secs, cold_base_level_mb),
    );
    Storage::open(opts).unwrap_or_else(|e| {
        eprintln!("error: failed to open storage at '{}': {}", data_dir, e);
        process::exit(1);
    })
}

fn parse_value(tag: &str, raw: &str) -> Result<Value, String> {
    match tag {
        "str" => Ok(Value::String(raw.to_string())),
        "bytes" => Ok(Value::Bytes(raw.as_bytes().to_vec())),
        "i64" => raw
            .parse::<i64>()
            .map(Value::I64)
            .map_err(|e| format!("invalid i64: {e}")),
        "u64" => raw
            .parse::<u64>()
            .map(Value::U64)
            .map_err(|e| format!("invalid u64: {e}")),
        "f64" => raw
            .parse::<f64>()
            .map(Value::Float64)
            .map_err(|e| format!("invalid f64: {e}")),
        "bool" => match raw {
            "true" => Ok(Value::Boolean(true)),
            "false" => Ok(Value::Boolean(false)),
            _ => Err(format!(
                "invalid bool: expected 'true' or 'false', got '{raw}'"
            )),
        },
        _ => Err(format!(
            "unknown type '{tag}': expected str | bytes | i64 | u64 | f64 | bool"
        )),
    }
}

fn display_value(v: &Value) -> String {
    match v {
        Value::String(s) => format!("str: {s}"),
        Value::Bytes(b) => format!("bytes: {}", String::from_utf8_lossy(b)),
        Value::I64(n) => format!("i64: {n}"),
        Value::U64(n) => format!("u64: {n}"),
        Value::Float64(f) => format!("f64: {f}"),
        Value::Boolean(b) => format!("bool: {b}"),
    }
}

fn handle(storage: &Storage, args: &[&str]) {
    match args {
        ["get", key] => match storage.get(key.as_bytes().to_vec()) {
            Some(value) => println!("{}", display_value(&value)),
            None => println!("not found"),
        },
        ["put", key, tag, raw] => match parse_value(tag, raw) {
            Ok(value) => {
                if let Err(e) = storage.put(key.as_bytes().to_vec(), value) {
                    eprintln!("error: put failed: {e}");
                }
            }
            Err(e) => eprintln!("error: {e}"),
        },
        ["delete", key] => {
            if let Err(e) = storage.delete(key.as_bytes().to_vec()) {
                eprintln!("error: delete failed: {e}");
            }
        }
        ["scan", start, end] => {
            match storage.scan(start.as_bytes().to_vec(), end.as_bytes().to_vec()) {
                Ok(iter) => {
                    for (k, v) in iter {
                        let key = String::from_utf8_lossy(&k);
                        let value = display_value(&v);
                        println!("{key}\t{value}");
                    }
                }
                Err(e) => eprintln!("error: scan failed: {e}"),
            }
        }
        ["help"] => println!("{USAGE}"),
        _ => eprintln!("error: unrecognised command\n\n{USAGE}"),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!(
            "Usage: cli <data-dir> [leveled|tiered] [hot_ttl_secs] [memtable_bytes] [cold_base_level_mb]"
        );
        process::exit(1);
    }

    let data_dir = &args[1];
    let scheme = args.get(2).map(String::as_str).unwrap_or("leveled");
    let hot_ttl_secs: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(60);
    let memtable_bytes: usize = args
        .get(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(MAX_MEMTABLE_SIZE);
    let cold_base_level_mb: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(128);
    let storage = open(
        data_dir,
        scheme,
        hot_ttl_secs,
        memtable_bytes,
        cold_base_level_mb,
    );
    println!("pithos ({data_dir}, {scheme}) — type 'help' for commands, 'exit' to quit");

    let stdin = io::stdin();
    let mut stdout = io::stdout();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };

        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line == "exit" {
            break;
        }

        let parts: Vec<&str> = line.splitn(5, ' ').collect();
        handle(&storage, &parts);

        print!("> ");
        let _ = stdout.flush();
    }

    storage.close().unwrap_or_else(|e| {
        eprintln!("error: failed to close storage: {e}");
        process::exit(1);
    });
}
