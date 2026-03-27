use super::{ColumnInfo, Database, ForeignKey, Row, TableInfo, Value};
use anyhow::Result;
use async_trait::async_trait;
use sqlx::{Column, Row as SqlxRow, SqlitePool, TypeInfo};

pub struct SqliteDb {
    pool: SqlitePool,
}

impl SqliteDb {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = sqlx::pool::PoolOptions::<sqlx::Sqlite>::new()
            .acquire_timeout(std::time::Duration::from_secs(5))
            .max_connections(5)
            .connect(url)
            .await?;
        Ok(Self { pool })
    }

    pub fn from_pool(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Database for SqliteDb {
    async fn list_tables(&self) -> Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(|r| r.get::<String, _>(0)).collect())
    }

    async fn describe_table(&self, table: &str) -> Result<TableInfo> {
        // Get column info via PRAGMA
        let pragma_sql = format!("PRAGMA table_info(\"{}\")", table);
        let col_rows = sqlx::query(&pragma_sql).fetch_all(&self.pool).await?;

        let mut columns = Vec::new();
        for row in &col_rows {
            let name: String = row.get("name");
            let data_type: String = row.try_get("type").unwrap_or_default();
            let notnull: i64 = row.try_get("notnull").unwrap_or(0);
            let pk: i64 = row.try_get("pk").unwrap_or(0);
            columns.push(ColumnInfo {
                name,
                data_type: data_type.clone(),
                column_type: data_type,
                nullable: notnull == 0,
                is_primary_key: pk > 0,
            });
        }

        // Get foreign key info via PRAGMA
        let fk_sql = format!("PRAGMA foreign_key_list(\"{}\")", table);
        let fk_rows = sqlx::query(&fk_sql).fetch_all(&self.pool).await?;

        let mut foreign_keys = Vec::new();
        for row in &fk_rows {
            let from_column: String = row.try_get("from").unwrap_or_default();
            let to_table: String = row.try_get("table").unwrap_or_default();
            let to_column: String = row.try_get("to").unwrap_or_default();
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
                let val = decode_sqlite_value(row, col.ordinal(), type_info.name());
                map.insert(name, val);
            }
            result.push(map);
        }
        Ok(result)
    }
}

fn decode_sqlite_value(
    row: &sqlx::sqlite::SqliteRow,
    idx: usize,
    type_name: &str,
) -> Value {
    use sqlx::Row as _;
    match type_name.to_uppercase().as_str() {
        "INTEGER" | "INT" | "BIGINT" | "SMALLINT" | "TINYINT" | "BOOLEAN" => {
            match row.try_get::<i64, _>(idx) {
                Ok(v) => Value::Integer(v),
                Err(_) => Value::Null,
            }
        }
        "REAL" | "FLOAT" | "DOUBLE" | "NUMERIC" | "DECIMAL" => {
            match row.try_get::<f64, _>(idx) {
                Ok(v) => Value::Float(v),
                Err(_) => Value::Null,
            }
        }
        "BLOB" => match row.try_get::<Vec<u8>, _>(idx) {
            Ok(v) => Value::Bytes(v),
            Err(_) => Value::Null,
        },
        _ => match row.try_get::<String, _>(idx) {
            Ok(v) => Value::Text(v),
            Err(_) => match row.try_get::<i64, _>(idx) {
                Ok(v) => Value::Integer(v),
                Err(_) => match row.try_get::<f64, _>(idx) {
                    Ok(v) => Value::Float(v),
                    Err(_) => Value::Null,
                },
            },
        },
    }
}
