use rdbc2::dbc;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

#[tokio::test]
async fn test_sqlite_simple_query() -> Result<(), Error> {
    let mut database = dbc::Database::new("sqlite://:memory:")?;

    // Create a test table with two rows
    let create_table_query = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
    database.execute_query(create_table_query)?;
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

    // Update the name of the first row to "updated"
    let update_query = "UPDATE test_table SET name = ? WHERE id = ?";
    let result = database.execute_query_with_params(update_query, &["updated", "1"])?;
    assert_eq!(result.affected_rows, 1);

    // Select the first row from test_table where the name is "updated"
    let select_query = "SELECT * FROM test_table WHERE name = ?";
    let result = database.execute_query_with_params(select_query, &["updated"])?;
    assert_eq!(result.rows.len(), 1);

    // Verify the data returned by the query
    let first_row = &result.rows[0];
    assert_eq!(
        first_row.get_value_by_name("name"),
        Some(&dbc::Value::Bytes("updated".to_owned().into_bytes()))
    );

    Ok(())
}

#[tokio::test]
async fn test_sqlite_query_with_params_and_serialize() -> Result<(), Error> {
    let mut database = dbc::Database::new("sqlite://:memory:")?;

    // Create a test table with two rows
    let create_table_query = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
    database.execute_query(create_table_query)?;
    let insert_query = "INSERT INTO test_table (name) VALUES ('test1'), ('test2')";
    database.execute_query(insert_query)?;

    // Select all rows from test_table where the name is "test1"
    let select_query = "SELECT * FROM test_table WHERE name = ?";
    let result = database.execute_query_with_params_and_serialize(select_query, &["test1"])?;
    assert_eq!(
        result,
        r#"{"rows":[{"values":[{"Int":1},{"Bytes":[116,101,115,116,49]}],"columns":[{"name":"id","column_type":"STRING"},{"name":"name","column_type":"STRING"}]}],"affected_rows":1}"#
    ); // Note: The column type is STRING because the column type is not known in the current implementation

    // Update the name of the first row to "updated"
    let update_query = "UPDATE test_table SET name = ? WHERE id = ?";
    database.execute_query_with_params_and_serialize(update_query, &["updated", "1"])?;

    // Select the first row from test_table where the id is 1
    let select_query = "SELECT * FROM test_table WHERE id = ?";
    let result = database.execute_query_with_params_and_serialize(select_query, &["1"])?;
    assert_eq!(
        result,
        r#"{"rows":[{"values":[{"Int":1},{"Bytes":[117,112,100,97,116,101,100]}],"columns":[{"name":"id","column_type":"STRING"},{"name":"name","column_type":"STRING"}]}],"affected_rows":1}"#
    );

    // Deserialize the result and verify the data
    let result: dbc::QueryResult = serde_json::from_str(&result)?;
    let first_row = &result.rows[0];
    assert_eq!(
        first_row.get_value_by_name("name"),
        Some(&dbc::Value::Bytes("updated".to_owned().into_bytes()))
    );

    Ok(())
}
