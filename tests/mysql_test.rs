use rdbc2::dbc;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

#[tokio::test]
async fn test_mysql_connection() -> Result<(), Error> {
    let url = "mysql://localhost:3306/?user=nociza&password=password";
    let mut database = dbc::Database::new(url)?;
    let query = "SELECT 1";
    let result = database.execute_query(query)?;
    assert_eq!(result.rows.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_mysql_connection_with_params() -> Result<(), Error> {
    let url = "mysql://localhost:3306/?user=nociza&password=password";
    let mut database = dbc::Database::new(url)?;
    let query = "SELECT 1";
    let result = database.execute_query(query).unwrap();
    assert_eq!(result.rows.len(), 1);

    Ok(())
}