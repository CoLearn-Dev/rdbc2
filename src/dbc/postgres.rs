use std::sync::Arc;

use postgres;

use crate::dbc;

pub struct PostgresConnection {
    pub(crate) connection: postgres::Client,
}

impl PostgresConnection {
    pub(crate) fn get_connection(url: &str) -> Result<Box<dyn dbc::Connection>, dbc::Error> {
        Ok(Box::new(PostgresConnection {
            connection: postgres::Client::connect(url, postgres::NoTls)?,
        }) as Box<dyn dbc::Connection>)
    }
}

impl dbc::Connection for PostgresConnection {
    fn execute(&mut self, query: &str) -> Result<dbc::QueryResult, dbc::Error> {
        let result = self.connection.query(query, &[])?;
        let columns = result
            .columns()
            .iter()
            .map(|column| dbc::Column {
                name: column.name().to_string(),
                column_type: column.type_().into(),
            })
            .collect::<Vec<dbc::Column>>();
        let columns = Arc::from(columns);

        let mut rows: Vec<dbc::Row> = Vec::new();
        for row in result {
            let values: Vec<dbc::Value> = row
                .iter()
                .map(|value| {
                    if value.is_none() {
                        dbc::Value::NULL
                    } else {
                        value.unwrap().into()
                    }
                })
                .collect();
            rows.push(dbc::Row {
                values,
                columns: Arc::clone(&columns),
            });
        }
        Ok(dbc::QueryResult { rows })
    }
}

impl From<postgres::types::Type> for dbc::ColumnType {
    fn from(value: postgres::types::Type) -> Self {
        match value {
            postgres::types::Type::BOOL => dbc::ColumnType::BOOL,
            postgres::types::Type::INT2 => dbc::ColumnType::INT2,
            postgres::types::Type::INT4 => dbc::ColumnType::INT4,
            postgres::types::Type::INT8 => dbc::ColumnType::INT8,
            postgres::types::Type::FLOAT4 => dbc::ColumnType::FLOAT4,
            postgres::types::Type::FLOAT8 => dbc::ColumnType::FLOAT8,
            postgres::types::Type::NUMERIC => dbc::ColumnType::NUMERIC,
            postgres::types::Type::TIMESTAMP => dbc::ColumnType::TIMESTAMP,
            postgres::types::Type::TIMESTAMPTZ => dbc::ColumnType::TIMESTAMPTZ,
            postgres::types::Type::DATE => dbc::ColumnType::DATE,
            postgres::types::Type::TIME => dbc::ColumnType::TIME,
            postgres::types::Type::TIMETZ => dbc::ColumnType::TIMETZ,
            postgres::types::Type::INTERVAL => dbc::ColumnType::INTERVAL,
            postgres::types::Type::TEXT => dbc::ColumnType::TEXT,
            postgres::types::Type::CHAR => dbc::ColumnType::CHAR,
            postgres::types::Type::VARCHAR => dbc::ColumnType::VARCHAR,
            postgres::types::Type::BYTEA => dbc::ColumnType::BYTEA,
            postgres::types::Type::UUID => dbc::ColumnType::UUID,
            postgres::types::Type::JSON => dbc::ColumnType::JSON,
            postgres::types::Type::JSONB => dbc::ColumnType::JSONB,
            postgres::types::Type::XML => dbc::ColumnType::XML,
            postgres::types::Type::OID => dbc::ColumnType::OID,
            postgres::types::Type::CIDR => dbc::ColumnType::CIDR,
            postgres::types::Type::INET => dbc::ColumnType::INET,
            postgres::types::Type::MACADDR => dbc::ColumnType::MACADDR,
            postgres::types::Type::BIT => dbc::ColumnType::BIT,
            postgres::types::Type::VARBIT => dbc::ColumnType::VARBIT,
            _ => dbc::ColumnType::UNKNOWN,
        }
    }
}
