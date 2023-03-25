use rdbc2::dbc;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

#[tokio::test]
async fn test_sqlite_simple_query() -> Result<(), Error> {
    let mut database = dbc::Database::new("sqlite://:memory:")?;
    let query = "SELECT 1";
    let result = database.execute_query(query)?;
    assert_eq!(result.rows.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_sqlite_query_with_params() -> Result<(), Error> {
    let mut database = dbc::Database::new("sqlite://:memory:")?;
    let query = "SELECT ? + ?";
    let result = database.execute_query_with_params(query, &["1", "2"])?;
    assert_eq!(result.rows.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_sqlite_query_with_params_and_serialize() -> Result<(), Error> {
    let mut database = dbc::Database::new("sqlite://:memory:")?;
    let query = "SELECT ? + ?";
    let result = database.execute_query_with_params_and_serialize(query, &["1", "2"])?;
    assert_eq!(result, r#"{"rows":[{"values":[{"Int":2}],"columns":[{"name":"1 + 1","column_type":"STRING"}]}]}"#); // currently all columns are STRING

    Ok(())
}