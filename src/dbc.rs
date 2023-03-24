mod mysql;
mod sqlite;
mod postgres;

use std::sync::Arc;
use serde::{Serialize, Deserialize};
use serde_json;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub trait Connection {
    fn execute(&mut self, query: &str) -> Result<QueryResult, Error>;
}

pub struct Database {
    pub(crate) url: String,
    pub(crate) connection: Box<dyn Connection>,
}

impl Database {
    pub fn new(url: &str) -> Result<Self, Error> {
        let connection = match url {
            url if url.starts_with("mysql://") => mysql::MySQLConnection::get_connection(url)?,
            url if url.starts_with("sqlite://") => sqlite::SQLiteConnection::get_connection(url)?,
            _ => return Err("Unsupported dbc type".into()),
        };

        Ok(Database {
            url: url.to_string(),
            connection,
        })
    }

    pub fn execute_query(&mut self, query: &str) -> Result<QueryResult, Error> {
        self.connection.execute(query)
    }

    pub fn execute_query_to_json(&mut self, query: &str) -> Result<String, Error> {
        let result = self.execute_query(query)?;
        Ok(serde_json::to_string(&result)?)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Value {
    NULL,
    Bytes(Vec<u8>),
    String(String),
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f32),
    Double(f64),
    /// year, month, day, hour, minutes, seconds, micro seconds
    Date(u16, u8, u8, u8, u8, u8, u32),
    /// is negative, days, hours, minutes, seconds, micro seconds
    Time(bool, u32, u8, u8, u8, u32),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Column {
    pub name: String,
    pub column_type: ColumnType,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Row {
    values: Vec<Value>,
    columns: Arc<[Column]>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryResult {
    pub rows: Vec<Row>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ColumnType {
    NULL,
    DECIMAL,
    INT,
    TINY,
    SHORT,
    LONG,
    LONGLONG,
    FLOAT,
    BIT,
    DOUBLE,
    STRING,
    TIMESTAMP,
    DATE,
    TIME,
    YEAR,
    DATETIME,
    JSON,
    ENUM,
    SET,
    BLOB,
    GEOMETRY,
}
