use super::{ColumnInfo, Database, ForeignKey, Row, TableInfo, Value};
use anyhow::Result;
use async_trait::async_trait;
use sqlx::{Column, MySqlPool, Row as SqlxRow, TypeInfo};

pub struct MysqlDb {
    pool: MySqlPool,
}

impl MysqlDb {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = MySqlPool::connect(url).await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl Database for MysqlDb {
    async fn list_tables(&self) -> Result<Vec<String>> {
        let rows = sqlx::query("SHOW TABLES").fetch_all(&self.pool).await?;
        Ok(rows.iter().map(|r| r.get::<String, _>(0)).collect())
    }

    async fn describe_table(&self, table: &str) -> Result<TableInfo> {
        // Get columns
        let col_sql = format!(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY \
             FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}' \
             ORDER BY ORDINAL_POSITION",
            table.replace('\'', "''")
        );
        let col_rows = sqlx::query(&col_sql).fetch_all(&self.pool).await?;
        let mut columns = Vec::new();
        for row in &col_rows {
            let name: String = row.try_get("COLUMN_NAME").unwrap_or_default();
            let data_type: String = row.try_get("DATA_TYPE").unwrap_or_default();
            let is_nullable: String = row.try_get("IS_NULLABLE").unwrap_or_default();
            let col_key: String = row.try_get("COLUMN_KEY").unwrap_or_default();
            columns.push(ColumnInfo {
                name,
                data_type,
                nullable: is_nullable == "YES",
                is_primary_key: col_key == "PRI",
            });
        }

        // Get foreign keys
        let fk_sql = format!(
            "SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME \
             FROM information_schema.KEY_COLUMN_USAGE \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}' \
             AND REFERENCED_TABLE_NAME IS NOT NULL",
            table.replace('\'', "''")
        );
        let fk_rows = sqlx::query(&fk_sql).fetch_all(&self.pool).await?;
        let mut foreign_keys = Vec::new();
        for row in &fk_rows {
            let from_column: String = row.try_get("COLUMN_NAME").unwrap_or_default();
            let to_table: String = row.try_get("REFERENCED_TABLE_NAME").unwrap_or_default();
            let to_column: String = row.try_get("REFERENCED_COLUMN_NAME").unwrap_or_default();
            foreign_keys.push(ForeignKey {
                from_column,
                to_table,
                to_column,
            });
        }

        Ok(TableInfo {
            name: table.to_string(),
            columns,
            foreign_keys,
        })
    }

    async fn query(&self, sql: &str) -> Result<Vec<Row>> {
        let rows = sqlx::query(sql).fetch_all(&self.pool).await?;
        let mut result = Vec::new();
        for row in &rows {
            let mut map = Row::new();
            for col in row.columns() {
                let name = col.name().to_string();
                let type_info = col.type_info();
                let val = decode_mysql_value(row, col.ordinal(), type_info.name());
                map.insert(name, val);
            }
            result.push(map);
        }
        Ok(result)
    }
}

fn decode_mysql_value(
    row: &sqlx::mysql::MySqlRow,
    idx: usize,
    type_name: &str,
) -> Value {
    use sqlx::Row as _;
    let upper = type_name.to_uppercase();
    if upper.contains("INT") || upper.contains("BIT") || upper.contains("YEAR") {
        match row.try_get::<i64, _>(idx) {
            Ok(v) => Value::Integer(v),
            Err(_) => Value::Null,
        }
    } else if upper.contains("FLOAT")
        || upper.contains("DOUBLE")
        || upper.contains("DECIMAL")
        || upper.contains("NUMERIC")
    {
        match row.try_get::<f64, _>(idx) {
            Ok(v) => Value::Float(v),
            Err(_) => Value::Null,
        }
    } else if upper.contains("BLOB") || upper.contains("BINARY") {
        match row.try_get::<Vec<u8>, _>(idx) {
            Ok(v) => Value::Bytes(v),
            Err(_) => Value::Null,
        }
    } else {
        match row.try_get::<String, _>(idx) {
            Ok(v) => Value::Text(v),
            Err(_) => Value::Null,
        }
    }
}
