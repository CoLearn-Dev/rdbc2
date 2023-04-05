use mysql;

use rdbc2::dbc;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

fn _prepare_mysql_database(url: String) -> Result<(), Error> {
    let mut database = dbc::Database::new(url.as_str())?;

    let query = "DROP DATABASE IF EXISTS test";
    database.execute_query(query)?;
    let query = "CREATE DATABASE IF NOT EXISTS test";
    database.execute_query(query)?;
    let query = "USE test";
    database.execute_query(query)?;
    let query = "CREATE TABLE IF NOT EXISTS test_table (id INT NOT NULL AUTO_INCREMENT, name VARCHAR(255) NOT NULL, PRIMARY KEY (id))";
    database.execute_query(query)?;

    Ok(())
}

fn _cleanup_mysql_database(url: String) -> Result<(), Error> {
    let mut database = dbc::Database::new(url.as_str())?;
    let query = "DROP DATABASE IF EXISTS test";
    database.execute_query(query)?;

    Ok(())
}

fn _get_mysql_connection_url() -> String {
    if std::env::var("MYSQL_DATABASE_URL").is_ok() {
        std::env::var("MYSQL_DATABASE_URL").unwrap()
    } else {
        "mysql://localhost:3306/?user=nociza&password=password".to_owned()
    }
}

#[tokio::test]
async fn test_mysql_simple_query() -> Result<(), Error> {
    let url = _get_mysql_connection_url();
    _prepare_mysql_database(url.clone())?;

    let mut database = dbc::Database::new(url.as_str())?;

    // Use the test database
    let use_query = "USE test";
    database.execute_query(use_query)?;

    // Insert two rows into the test_table
    let insert_query = "INSERT INTO test_table (name) VALUES ('test1'), ('test2')";
    database.execute_query(insert_query)?;

    // Select all rows from test_table
    let select_query = "SELECT * FROM test_table";
    let result = database.execute_query(select_query)?;

    assert_eq!(result.rows.len(), 2);

    // Verify the data returned by the query
    let first_row = &result.rows[0];
    let second_row = &result.rows[1];
    assert_eq!(
        first_row.get_value_by_name("name"),
        Some(&dbc::Value::Bytes("test1".to_owned().into_bytes()))
    );
    assert_eq!(
        second_row.get_value_by_name("name"),
        Some(&dbc::Value::Bytes("test2".to_owned().into_bytes()))
    );

    _cleanup_mysql_database(url.clone())?;

    Ok(())
}

#[tokio::test]
async fn test_mysql_query_with_params() -> Result<(), Error> {
    let url = _get_mysql_connection_url();
    _prepare_mysql_database(url.clone())?;

    let mut database = dbc::Database::new(url.as_str())?;

    // Use the test database
    let use_query = "USE test";
    database.execute_query(use_query)?;

    // Insert two rows into the test_table
    let insert_query = "INSERT INTO test_table (name) VALUES ('test1'), ('test2')";
    database.execute_query(insert_query)?;

    // Select all rows from test_table
    let select_query = "SELECT * FROM test_table WHERE name = ?";
    let result = database.execute_query_with_params(select_query, &["test1"])?;

    assert_eq!(result.rows.len(), 1);

    // Verify the data returned by the query
    let first_row = &result.rows[0];
    assert_eq!(
        first_row.get_value_by_name("name"),
        Some(&dbc::Value::Bytes("test1".to_owned().into_bytes()))
    );

    _cleanup_mysql_database(url.clone())?;

    Ok(())
}

#[tokio::test]
async fn test_mysql_query_with_params_and_serialize() -> Result<(), Error> {
    let url = _get_mysql_connection_url();
    _prepare_mysql_database(url.clone())?;

    let mut database = dbc::Database::new(url.as_str())?;

    // Use the test database
    let use_query = "USE test";
    database.execute_query(use_query)?;

    // Insert two rows into the test_table
    let insert_query = "INSERT INTO test_table (name) VALUES ('test1'), ('test2')";
    database.execute_query(insert_query)?;

    // Update the test_table to set the name of the first row to "updated"
    let update_query = "UPDATE test_table SET name = ? WHERE id = ?";
    let result = database.execute_query_with_params(update_query, &["updated", "1"])?;

    // Select all rows from test_table
    let select_query = "SELECT * FROM test_table WHERE id = ?";
    let result = database.execute_query_with_params_and_serialize(select_query, &["1"])?;
    let expected_result = r#"{"rows":[{"values":[{"Bytes":[49]},{"Bytes":[117,112,100,97,116,101,100]}],"columns":[{"name":"id","column_type":"LONG"},{"name":"name","column_type":"STRING"}]}]}"#;
    assert_eq!(result, expected_result);

    // deserialize the result and verify the data
    let deserialized_result: dbc::QueryResult = serde_json::from_str(&result)?;
    let first_row = &deserialized_result.rows[0];
    assert_eq!(
        first_row.get_value_by_name("name"),
        Some(&dbc::Value::Bytes("updated".to_owned().into_bytes()))
    );

    _cleanup_mysql_database(url.clone())?;

    Ok(())
}
