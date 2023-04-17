use std::sync::Arc;

use rusqlite;
use rusqlite::types::Type;

use crate::dbc;

pub(crate) struct SQLiteConnection {
    connection: rusqlite::Connection,
}

impl SQLiteConnection {
    pub(crate) fn get_connection(url: &str) -> Result<Box<dyn dbc::Connection>, dbc::Error> {
        let connection;
        if url == "sqlite://:memory:" {
            connection = rusqlite::Connection::open_in_memory()?;
        } else {
            connection = rusqlite::Connection::open(url)?;
        }
        Ok(Box::new(SQLiteConnection { connection }) as Box<dyn dbc::Connection>)
    }
}

impl dbc::Connection for SQLiteConnection {
    fn execute(&mut self, query: &str) -> Result<dbc::QueryResult, dbc::Error> {
        let mut statement = self.connection.prepare(query)?;
        let columns = statement
            .columns()
            .iter()
            .map(|column| {
                let sqlite_type = column.decl_type().unwrap();
                dbc::Column {
                    name: column.name().to_string(),
                    column_type: sqlite_type.into(),
                }
            })
            .collect::<Vec<dbc::Column>>();
        let columns = Arc::from(columns);
        let num_columns = statement.column_count();

        if !query.starts_with("SELECT") {
            let affected_rows = statement.execute([])?;
            return Ok(dbc::QueryResult {
                rows: Vec::new(),
                affected_rows,
            });
        }

        let mut rows: Vec<dbc::Row> = Vec::new();
        let mut result = statement.query([])?;
        while let Some(row) = result.next()? {
            let mut values: Vec<dbc::Value> = Vec::new();
            for i in 0..num_columns {
                let value = row.get_ref(i).unwrap().into();
                values.push(value);
            }

            rows.push(dbc::Row {
                values,
                columns: Arc::clone(&columns),
            });
        }
        Ok(dbc::QueryResult {
            rows,
            affected_rows: 0 as usize,
        })
    }
}

impl From<rusqlite::types::ValueRef<'_>> for dbc::Value {
    fn from(value: rusqlite::types::ValueRef) -> Self {
        match value {
            rusqlite::types::ValueRef::Null => dbc::Value::NULL,
            rusqlite::types::ValueRef::Integer(i) => dbc::Value::Int(i),
            rusqlite::types::ValueRef::Real(f) => dbc::Value::Double(f),
            rusqlite::types::ValueRef::Text(s) => dbc::Value::Bytes(s.to_vec()),
            rusqlite::types::ValueRef::Blob(b) => dbc::Value::Bytes(b.to_vec()),
        }
    }
}

impl From<&str> for dbc::ColumnType {
    fn from(sqlite_type: &str) -> Self {
        match sqlite_type {
            "INTEGER" => dbc::ColumnType::INT,
            "REAL" => dbc::ColumnType::DOUBLE,
            "TEXT" => dbc::ColumnType::VARCHAR,
            "BLOB" => dbc::ColumnType::BLOB,
            _ => dbc::ColumnType::UNKNOWN, // Create an issue or PR if you need more type support
        }
    }
}
