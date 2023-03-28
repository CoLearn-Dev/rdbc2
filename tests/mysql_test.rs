use rdbc2::dbc;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

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
    let mut database = dbc::Database::new(url.as_str())?;
    let query = "SELECT 1";
    let result = database.execute_query(query)?;
    assert_eq!(result.rows.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_mysql_query_with_params() -> Result<(), Error> {
    let url = _get_mysql_connection_url();
    let mut database = dbc::Database::new(url.as_str())?;
    let query = "SELECT ? + ?";
    let result = database.execute_query_with_params(query, &["1", "2"])?;
    assert_eq!(result.rows.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_mysql_query_with_params_and_serialize() -> Result<(), Error> {
    let url = _get_mysql_connection_url();
    let mut database = dbc::Database::new(url.as_str())?;
    let query = "SELECT ? + ?";
    let result = database.execute_query_with_params_and_serialize(query, &["1", "2"])?;
    assert_eq!(result, r#"{"rows":[{"values":[{"Bytes":[50]}],"columns":[{"name":"1 + 1","column_type":"LONGLONG"}]}]}"#);

    Ok(())
}