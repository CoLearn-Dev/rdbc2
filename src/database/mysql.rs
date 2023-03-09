use crate::database;
use mysql;
use mysql::prelude::Queryable;
use mysql::serde_json::Value;
use mysql_common::constants::ColumnType;
use sqlparser::ast::Query;
use crate::database::{QueryResult, Row};
use crate::database as rdbc;

pub(crate) struct MySQLConnection {
    connection: mysql::Conn,
}

impl database::Connection for MySQLConnection {
    fn new(url: &str) -> Result<Box<dyn database::Connection>, database::Error> {
        let opts = mysql::Opts::from_url(url)?;
        let mut connection = mysql::Conn::new(opts)?;
        Ok(Box::new(MySQLConnection { connection }))
    }

    // execute a query using the connection
    fn execute(&mut self, query: &str) -> Result<QueryResult, database::Error> {

        let result = self.connection.query_iter(query)?;
        let mut rows: Vec<Row> = Vec::new();
        for row in result {
            let row = row?;
            let mut data = Vec::new();
            for column in row {
                let column = to_rdbc_type(&column.column_type(), column?);
                data.push(column);
            }
            rows.push(rdbc::Row { data });
        }
        Ok(rdbc::QueryResult { rows })
    }
}

fn to_rdbc_type(t: &ColumnType) -> rdbc::DataType {
    match t {
        ColumnType::MYSQL_TYPE_FLOAT => rdbc::DataType::Float,
        ColumnType::MYSQL_TYPE_DOUBLE => rdbc::DataType::Double,
        ColumnType::MYSQL_TYPE_TINY => rdbc::DataType::Byte,
        ColumnType::MYSQL_TYPE_SHORT => rdbc::DataType::Short,
        ColumnType::MYSQL_TYPE_LONG => rdbc::DataType::Integer,
        ColumnType::MYSQL_TYPE_LONGLONG => rdbc::DataType::Integer,
        ColumnType::MYSQL_TYPE_DECIMAL => rdbc::DataType::Decimal,
        ColumnType::MYSQL_TYPE_NEWDECIMAL => rdbc::DataType::Decimal,
        ColumnType::MYSQL_TYPE_STRING => rdbc::DataType::Utf8,
        ColumnType::MYSQL_TYPE_VAR_STRING => rdbc::DataType::Utf8,
        ColumnType::MYSQL_TYPE_VARCHAR => rdbc::DataType::Utf8,
        ColumnType::MYSQL_TYPE_TINY_BLOB => rdbc::DataType::Binary,
        ColumnType::MYSQL_TYPE_MEDIUM_BLOB => rdbc::DataType::Binary,
        ColumnType::MYSQL_TYPE_LONG_BLOB => rdbc::DataType::Binary,
        ColumnType::MYSQL_TYPE_BLOB => rdbc::DataType::Binary,
        ColumnType::MYSQL_TYPE_BIT => rdbc::DataType::Bool,
        ColumnType::MYSQL_TYPE_DATE => rdbc::DataType::Date,
        ColumnType::MYSQL_TYPE_TIME => rdbc::DataType::Time,
        ColumnType::MYSQL_TYPE_TIMESTAMP => rdbc::DataType::Datetime, // TODO: Data type for timestamps in UTC?
        ColumnType::MYSQL_TYPE_DATETIME => rdbc::DataType::Datetime,
        mysql_datatype => todo!("Datatype not currently supported: {:?}", mysql_datatype),
    };
}