mod mysql;

use serde_json::Map;
use serde::ser::{Serialize, SerializeSeq, SerializeMap, Serializer};

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
        // TODO: add conditional compilation?
        if url.starts_with("mysql://") {
            Ok(self._get_mysql_connection(url)?)
        } else {
            Err("Unsupported dbc type".into())
        }
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