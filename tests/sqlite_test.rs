use rdbc2::dbc;

mod common;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

const SQLITE_DATABASE_URL: &str = "sqlite://:memory:";

fn _prepare_sqlite_database() -> Result<dbc::Database, Error> {
    let mut database = dbc::Database::new(SQLITE_DATABASE_URL)?;

    let drop_table_query = "DROP TABLE IF EXISTS test_table";
    database.execute_query(drop_table_query)?;

    // Create a test table with two rows
    let create_table_query = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
    database.execute_query(create_table_query)?;

    Ok(database)
}

#[tokio::test]
async fn test_sqlite_simple_query() -> Result<(), Error> {
    let database = _prepare_sqlite_database()?;
    Ok(common::test_simple_query(database).await?)
}

#[tokio::test]
async fn test_sqlite_query_with_params_and_serialize() -> Result<(), Error> {
    let database = _prepare_sqlite_database()?;
    Ok(common::test_query_with_params_and_serialize(database).await?)
}
