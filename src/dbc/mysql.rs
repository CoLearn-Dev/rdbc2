use std::sync::Arc;

use mysql;
use mysql::prelude::Queryable;
use mysql_common::constants::ColumnType;

use crate::dbc;

pub(crate) struct MySQLConnection {
    connection: mysql::Conn,
}

impl MySQLConnection {
    pub(crate) fn get_connection(url: &str) -> Result<Box<dyn dbc::Connection>, dbc::Error> {
        Ok(Box::new(MySQLConnection {
            connection: mysql::Conn::new(url)?,
        }) as Box<dyn dbc::Connection>)
    }
}

impl dbc::Connection for MySQLConnection {
    fn execute(&mut self, query: &str) -> Result<dbc::QueryResult, dbc::Error> {
        let result = self.connection.query_iter(query)?;
        let affected_rows = result.affected_rows() as usize;
        let columns = result
            .columns()
            .as_ref()
            .iter()
            .map(|column| dbc::Column {
                name: column.name_str().to_string(),
                column_type: column.column_type().into(),
            })
            .collect::<Vec<dbc::Column>>();
        let columns = Arc::from(columns);

        let mut rows: Vec<dbc::Row> = Vec::new();
        for row in result {
            let row = row?;
            let values: Vec<dbc::Value> = row
                .unwrap_raw()
                .iter()
                .map(|value| {
                    if value.is_none() {
                        dbc::Value::NULL
                    } else {
                        value.as_ref().unwrap().into()
                    }
                })
                .collect();
            rows.push(dbc::Row {
                values,
                columns: Arc::clone(&columns),
            });
        }
        Ok(dbc::QueryResult {
            rows,
            affected_rows,
        })
    }
}

impl From<&mysql::Value> for dbc::Value {
    fn from(value: &mysql::Value) -> Self {
        match value {
            mysql::Value::NULL => dbc::Value::NULL,
            mysql::Value::Bytes(bytes) => dbc::Value::Bytes(bytes.clone()),
            mysql::Value::Int(int) => dbc::Value::Int(*int),
            mysql::Value::UInt(uint) => dbc::Value::UInt(*uint),
            mysql::Value::Float(float) => dbc::Value::Float(*float),
            mysql::Value::Double(double) => dbc::Value::Double(*double),
            mysql::Value::Date(year, month, day, hour, minute, second, microsecond) => {
                dbc::Value::Date(*year, *month, *day, *hour, *minute, *second, *microsecond)
            }
            mysql::Value::Time(negative, days, hours, minutes, seconds, microseconds) => {
                dbc::Value::Time(*negative, *days, *hours, *minutes, *seconds, *microseconds)
            }
        }
    }
}

impl From<ColumnType> for dbc::ColumnType {
    fn from(column_type: ColumnType) -> Self {
        match column_type {
            ColumnType::MYSQL_TYPE_DECIMAL => dbc::ColumnType::DECIMAL,
            ColumnType::MYSQL_TYPE_TINY => dbc::ColumnType::TINY,
            ColumnType::MYSQL_TYPE_SHORT => dbc::ColumnType::SHORT,
            ColumnType::MYSQL_TYPE_LONG => dbc::ColumnType::LONG,
            ColumnType::MYSQL_TYPE_FLOAT => dbc::ColumnType::FLOAT,
            ColumnType::MYSQL_TYPE_DOUBLE => dbc::ColumnType::DOUBLE,
            ColumnType::MYSQL_TYPE_NULL => dbc::ColumnType::NULL,
            ColumnType::MYSQL_TYPE_TIMESTAMP => dbc::ColumnType::TIMESTAMP,
            ColumnType::MYSQL_TYPE_LONGLONG => dbc::ColumnType::LONGLONG,
            ColumnType::MYSQL_TYPE_INT24 => dbc::ColumnType::INT,
            ColumnType::MYSQL_TYPE_DATE => dbc::ColumnType::DATE,
            ColumnType::MYSQL_TYPE_TIME => dbc::ColumnType::TIME,
            ColumnType::MYSQL_TYPE_DATETIME => dbc::ColumnType::TIMESTAMP,
            ColumnType::MYSQL_TYPE_YEAR => dbc::ColumnType::INT,
            ColumnType::MYSQL_TYPE_NEWDATE => dbc::ColumnType::DATE, // Internal? do we need this?
            ColumnType::MYSQL_TYPE_VARCHAR => dbc::ColumnType::STRING,
            ColumnType::MYSQL_TYPE_BIT => dbc::ColumnType::BIT,
            ColumnType::MYSQL_TYPE_TIMESTAMP2 => dbc::ColumnType::TIMESTAMP,
            ColumnType::MYSQL_TYPE_DATETIME2 => dbc::ColumnType::DATETIME,
            ColumnType::MYSQL_TYPE_TIME2 => dbc::ColumnType::TIME,
            ColumnType::MYSQL_TYPE_JSON => dbc::ColumnType::JSON,
            ColumnType::MYSQL_TYPE_NEWDECIMAL => dbc::ColumnType::DECIMAL,
            ColumnType::MYSQL_TYPE_ENUM => dbc::ColumnType::ENUM,
            ColumnType::MYSQL_TYPE_SET => dbc::ColumnType::SET,
            ColumnType::MYSQL_TYPE_TINY_BLOB => dbc::ColumnType::BLOB,
            ColumnType::MYSQL_TYPE_MEDIUM_BLOB => dbc::ColumnType::BLOB,
            ColumnType::MYSQL_TYPE_LONG_BLOB => dbc::ColumnType::BLOB,
            ColumnType::MYSQL_TYPE_BLOB => dbc::ColumnType::BLOB,
            ColumnType::MYSQL_TYPE_VAR_STRING => dbc::ColumnType::STRING,
            ColumnType::MYSQL_TYPE_STRING => dbc::ColumnType::STRING,
            ColumnType::MYSQL_TYPE_GEOMETRY => dbc::ColumnType::GEOMETRY,
            _ => {
                panic!("Unknown column type: {:?}", column_type);
            }
        }
    }
}
