use std::sync::Arc;

use rusqlite;

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
        Ok(Box::new(SQLiteConnection {
            connection,
        }) as Box<dyn dbc::Connection>)
    }
}

impl dbc::Connection for SQLiteConnection {
    fn execute(&mut self, query: &str) -> Result<dbc::QueryResult, dbc::Error> {
        let mut statement = self.connection.prepare(query)?;
        let columns = statement.column_names().iter().map(
            |column| {
                dbc::Column {
                    name: column.to_string(),
                    column_type: dbc::ColumnType::STRING, // TODO: get column type
                }
            }
        ).collect::<Vec<dbc::Column>>();
        let columns = Arc::from(columns);
        let num_columns = statement.column_count();

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
