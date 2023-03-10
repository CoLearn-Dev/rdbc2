use std::any::Any;
use mysql;
use mysql::prelude::Queryable;
use mysql_common::constants::ColumnType;
use crate::dbc;

pub(crate) struct MySQLConnection {
    connection: mysql::Conn,
}

impl dbc::Database {
    pub(crate) fn _get_mysql_connection(url: &str) -> Result<Box<dyn dbc::Connection>, dbc::Error> {
        Ok(Box::new(MySQLConnection{
            connection: mysql::Conn::new(url)?,
        }) as Box<dyn dbc::Connection>)
    }
}


impl dbc::Connection for MySQLConnection {
    fn execute(&mut self, query: &str) -> Result<dbc::QueryResult, dbc::Error> {
        let result = self.connection.query_iter(query)?;
        let mut rows: Vec<dbc::Row> = Vec::new();
        for row in result {
            let row = row?;
            let mut data = Vec::new();
            row.unwrap().into_iter().for_each(|column| {
                let datatype = to_dbc_type(&column.type_id());
                data.push(dbc::Cell {
                    data_type: datatype,
                    value: column.to_string(),
                });
            });
            rows.push(dbc::Row { data });
        }
        Ok(dbc::QueryResult { rows })
    }
}

// FIXME: Still not working
fn to_dbc_type(t: &mysql::TypeId) -> dbc::DataType {
    match mysql::TypeId.ColumnType(t) {
        ColumnType::MYSQL_TYPE_FLOAT => dbc::DataType::Float,
        ColumnType::MYSQL_TYPE_DOUBLE => dbc::DataType::Double,
        ColumnType::MYSQL_TYPE_TINY => dbc::DataType::Byte,
        ColumnType::MYSQL_TYPE_SHORT => dbc::DataType::Short,
        ColumnType::MYSQL_TYPE_LONG => dbc::DataType::Integer,
        ColumnType::MYSQL_TYPE_LONGLONG => dbc::DataType::Integer,
        ColumnType::MYSQL_TYPE_DECIMAL => dbc::DataType::Decimal,
        ColumnType::MYSQL_TYPE_NEWDECIMAL => dbc::DataType::Decimal,
        ColumnType::MYSQL_TYPE_STRING => dbc::DataType::Utf8,
        ColumnType::MYSQL_TYPE_VAR_STRING => dbc::DataType::Utf8,
        ColumnType::MYSQL_TYPE_VARCHAR => dbc::DataType::Utf8,
        ColumnType::MYSQL_TYPE_TINY_BLOB => dbc::DataType::Binary,
        ColumnType::MYSQL_TYPE_MEDIUM_BLOB => dbc::DataType::Binary,
        ColumnType::MYSQL_TYPE_LONG_BLOB => dbc::DataType::Binary,
        ColumnType::MYSQL_TYPE_BLOB => dbc::DataType::Binary,
        ColumnType::MYSQL_TYPE_BIT => dbc::DataType::Bool,
        ColumnType::MYSQL_TYPE_DATE => dbc::DataType::Date,
        ColumnType::MYSQL_TYPE_TIME => dbc::DataType::Time,
        ColumnType::MYSQL_TYPE_TIMESTAMP => dbc::DataType::Datetime, // TODO: Data type for timestamps in UTC?
        ColumnType::MYSQL_TYPE_DATETIME => dbc::DataType::Datetime,
        mysql_datatype => todo!("Datatype not currently supported: {:?}", mysql_datatype),
    }
}