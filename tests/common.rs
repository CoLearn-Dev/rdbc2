use rdbc2::dbc;

fn _cleanup_database(mut database: dbc::Database) -> Result<(), dbc::Error> {
    let query = "DROP TABLE IF EXISTS test_table";
    database.execute_query(query)?;

    Ok(())
}

pub(crate) async fn test_simple_query(mut database: dbc::Database) -> Result<(), dbc::Error> {
    // Create a test table with two rows
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
    assert_eq!(result.affected_row_count, 1);

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

    _cleanup_database(database)?;

    Ok(())
}

pub(crate) async fn test_query_with_params(mut database: dbc::Database) -> Result<(), dbc::Error> {
    // Insert two rows into test_table
    let insert_query = "INSERT INTO test_table (name) VALUES (?)";
    let result = database.execute_query_with_params(insert_query, &["test1"])?;
    assert_eq!(result.affected_row_count, 1);
    let result = database.execute_query_with_params(insert_query, &["test2"])?;
    assert_eq!(result.affected_row_count, 1);

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
    assert_eq!(result.affected_row_count, 1);

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

    _cleanup_database(database)?;

    Ok(())
}

pub(crate) async fn test_query_with_params_and_serialize(
    mut database: dbc::Database,
) -> Result<(), dbc::Error> {
    // Insert two rows into test_table
    let insert_query = "INSERT INTO test_table (name) VALUES (?)";
    let result = database.execute_query_with_params(insert_query, &["test1"])?;
    assert_eq!(result.affected_row_count, 1);
    let result = database.execute_query_with_params(insert_query, &["test2"])?;
    assert_eq!(result.affected_row_count, 1);

    // Update the name of the first row to "updated"
    let update_query = "UPDATE test_table SET name = ? WHERE id = ?";
    let result = database.execute_query_with_params(update_query, &["updated", "1"])?;
    assert_eq!(result.affected_row_count, 1);

    // Select the first row from test_table where the name is "updated"
    let select_query = "SELECT * FROM test_table WHERE id = ?";
    let result = database.execute_query_with_params(select_query, &["1"])?;

    // Serialize the result
    let result = serde_json::to_string(&result)?;

    // Verify the data returned by the query
    let expected_result = r#"{"rows":[{"values":[{"Int":1},{"Bytes":"dXBkYXRlZA"}],"columns":[{"name":"id","column_type":"INT"},{"name":"name","column_type":"VARCHAR"}]}],"affected_row_count":0}"#;
    assert_eq!(result, expected_result);

    _cleanup_database(database)?;

    Ok(())
}
