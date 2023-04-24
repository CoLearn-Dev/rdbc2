use rdbc2::dbc;

mod common;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

fn _get_mysql_connection_url() -> String {
    if std::env::var("MYSQL_DATABASE_URL").is_ok() {
        std::env::var("MYSQL_DATABASE_URL").unwrap()
    } else {
        "mysql://localhost:3306/?user=nociza&password=password".to_owned()
    }
}

fn _prepare_mysql_database() -> Result<dbc::Database, Error> {
    let url = _get_mysql_connection_url();
    let mut database = dbc::Database::new(&url)?;
    let query = "DROP DATABASE IF EXISTS test";
    database.execute_query(query)?;
    let query = "CREATE DATABASE IF NOT EXISTS test";
    database.execute_query(query)?;
    let query = "USE test";
    database.execute_query(query)?;
    // Create a test table with two rows
    let query = "CREATE TABLE IF NOT EXISTS test_table (id INT NOT NULL AUTO_INCREMENT, name VARCHAR(255) NOT NULL, PRIMARY KEY (id))";
    database.execute_query(query)?;
    Ok(database)
}

#[tokio::test]
#[serial_test::serial]
async fn test_mysql_simple_query() -> Result<(), Error> {
    let database = _prepare_mysql_database()?;
    common::test_simple_query(database).await
}

#[tokio::test]
#[serial_test::serial]
async fn test_mysql_query_with_params() -> Result<(), Error> {
    let database = _prepare_mysql_database()?;
    common::test_query_with_params(database).await
}

#[tokio::test]
#[serial_test::serial]
async fn test_mysql_query_with_params_and_serialize() -> Result<(), Error> {
    let database = _prepare_mysql_database()?;
    common::test_query_with_params_and_serialize(database).await
}
