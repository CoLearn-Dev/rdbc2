#[cfg(test)]
mod mysql_tests {
    use rdbc2::dbc;
    use super::*;

    #[tokio::test]
    async fn test_mysql_connection() {
        let url = "mysql://username:password@localhost/database";
        let mut database = dbc::Database::new(url);
        let query = "SELECT 1";
        let result = database.execute_query(query).unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_mysql_connection_with_params() {
        let url = "mysql://username:password@localhost/database";
        let mut database = dbc::Database::new(url);
        let query = "SELECT 1";
        let result = database.execute_query(query).unwrap();
        assert_eq!(result.rows.len(), 1);
    }
}