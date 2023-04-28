use std::sync::Arc;

use serde::{Deserialize, Serialize};

mod mysql;
mod sqlite;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub trait Connection {
    fn execute(&mut self, query: &str) -> Result<QueryResult, Error>;
}

pub struct Database {
    pub(crate) connection: Box<dyn Connection>,
}

impl Database {
    pub fn new(url: &str) -> Result<Self, Error> {
        let connection = match url {
            url if url.starts_with("mysql://") => mysql::MySQLConnection::get_connection(url)?,
            url if url.starts_with("sqlite://") => sqlite::SQLiteConnection::get_connection(url)?,
            _ => return Err("Unsupported dbc type".into()),
        };

        Ok(Database { connection })
    }

    pub fn execute_query(&mut self, query: &str) -> Result<QueryResult, Error> {
        self.connection.execute(query)
    }

    pub fn execute_query_with_params(
        &mut self,
        query: &str,
        params: &[&str],
    ) -> Result<QueryResult, Error> {
        let mut query = query.to_string();
        for param in params {
            let quoted_param = if param.strip_prefix('\'').is_some() {
                // If the parameter already has single quotes, don't add them again.
                // This avoids SQL injection vulnerabilities when the parameter contains a quote.
                param.to_string()
            } else {
                // If the parameter doesn't have single quotes, add them.
                format!("'{}'", param)
            };
            query = query.replacen('?', quoted_param.as_str(), 1);
        }
        self.execute_query(&query)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Value {
    NULL,
    #[serde(with = "base64")]
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

impl Row {
    pub fn new(values: Vec<Value>, columns: Arc<[Column]>) -> Self {
        Row { values, columns }
    }

    pub fn get_value(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    pub fn get_column(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }

    pub fn get_value_by_name(&self, name: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|column| column.name == name)
            .and_then(|index| self.values.get(index))
    }

    pub fn get_column_by_name(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|column| column.name == name)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryResult {
    pub rows: Vec<Row>,
    pub affected_row_count: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ColumnType {
    NULL,
    DECIMAL,
    // 64 bit, collective type for TINY, SHORT, LONG, LONGLONG
    INT,
    FLOAT,
    BIT,
    DOUBLE,
    STRING,
    VARCHAR,
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
    UNKNOWN,
}

mod base64 {
    use base64::Engine;
    use serde::{Deserialize, Serialize};
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        let engine = base64::engine::GeneralPurpose::new(
            &base64::alphabet::STANDARD,
            base64::engine::general_purpose::NO_PAD,
        );
        let base64 = engine.encode(v);
        String::serialize(&base64, s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let base64 = String::deserialize(d)?;
        let engine = base64::engine::GeneralPurpose::new(
            &base64::alphabet::STANDARD,
            base64::engine::general_purpose::NO_PAD,
        );
        engine
            .decode(base64.as_bytes())
            .map_err(serde::de::Error::custom)
    }
}
