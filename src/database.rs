mod mysql;

use serde_json::Map;
use serde::ser::{Serialize, SerializeSeq, SerializeMap, Serializer};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub trait Database {
    fn connect(&self) -> Result<(), Error>;
}

pub trait Connection {
    fn new(query: &str) -> Result<Box<dyn Connection>, Error>;

    fn execute(&self, query: &str) -> Result<QueryResult, Error>;
}

pub fn get_connection(url: &str) -> Result<Box<dyn Connection>, Error> {
    if url.starts_with("mysql://") {
        Ok(Box::new(mysql::MySQLConnection::new(url)?))
    } else {
        Err("Unsupported database type".into())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DataType {
    Bool,
    Byte,
    Char,
    Short,
    Integer,
    Float,
    Double,
    Decimal,
    Date,
    Time,
    Datetime,
    Utf8,
    Binary,
}

pub struct Cell {
    pub data_type: DataType,
    pub value: String,
}

pub struct Col {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub data: Vec<Cell>,
}

pub struct Row {
    pub data: Vec<Cell>,
}

pub struct QueryResult {
    pub rows: Vec<Row>,
}

impl Serialize for QueryResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.rows.len()))?;
        for row in &self.rows {
            let mut map = seq.serialize_element::<Map<String, DataType>>()?;
            for (i, column) in row.data.iter().enumerate() {
                map.serialize_entry(&i.to_string(), column)?;
            }
            map.end()?;
        }
        seq.end()
    }
}