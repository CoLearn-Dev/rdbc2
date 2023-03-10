mod mysql;

use std::sync::Arc;
use serde::{Serialize, Deserialize};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub trait Connection {
    fn execute(&mut self, query: &str) -> Result<QueryResult, Error>;
}

pub struct Database {
    pub(crate) url: String,
    pub(crate) connection: Box<dyn Connection>,
}

impl Database {
    pub fn new(&self, url: &str) -> Self {
        let connection = self.get_connection(url).unwrap();
        Database {
            url: url.to_string(),
            connection,
        }
    }

    pub(crate) fn execute_query(&mut self, query: &str) -> Result<QueryResult, Error> {
        self.connection.execute(query)
    }

    fn get_connection(&self, url: &str) -> Result<Box<dyn Connection>, Error> {
        if url.starts_with("mysql://") {
            Ok(self._get_mysql_connection(url)?)
        } else {
            Err("Unsupported dbc type".into())
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Value {
    NULL,
    Bytes(Vec<u8>),
    Int(i64),
    UInt(u64),
    Float(f32),
    Double(f64),
    /// year, month, day, hour, minutes, seconds, micro seconds
    Date(u16, u8, u8, u8, u8, u8, u32),
    /// is negative, days, hours, minutes, seconds, micro seconds
    Time(bool, u32, u8, u8, u8, u32),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Column {
    pub name: String,
    pub column_type: ColumnType,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Row {
    values: Vec<Option<Value>>,
    columns: Arc<[Column]>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryResult {
    pub rows: Vec<Row>,
}

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
