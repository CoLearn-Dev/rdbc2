# Rust DataBase Connectivity (RDBC)

This is a Rust implementation of the Java DataBase Connectivity (JDBC) API, a continuation and reimplementation of
the [rdbc](https://github.com/tokio-rs/rdbc) project.

## Goals

- Provide a common API for accessing relational databases in Rust.

## Usage

### Installation

Add the following to your `Cargo.toml`:

```toml
[dependencies]
rdbc2 = "0.2"
```

### Example

```
use rdbc2;

let mut database = rdbc2::dbc::Database::new(<database_url>)?;

let result = database.execute_query(<query_string>)?;
let serialized_result = database.execute_query_and_serialize(<query_string>)?; // Serializes the result into a JSON string
let serialized_result_raw = database.execute_query_and_serialize_raw( < query_string>)?; // Serializes the result into an u8 array

// Or with parameters
let result = database.execute_query_with_params( < query_string>, < params>)?;
let serialized_result = database.execute_query_and_serialize_with_params(<query_string>, <params>)?;
```

## Supported Databases

- [x] MySQL
- [x] SQLite
- [ ] PostgreSQL


