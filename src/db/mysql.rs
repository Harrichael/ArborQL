use super::{ColumnInfo, Database, ForeignKey, Row, TableInfo, Value};
use anyhow::Result;
use async_trait::async_trait;
use chrono;
use sqlx::{Column, MySqlPool, Row as SqlxRow, TypeInfo};
use std::collections::HashMap;

pub struct MysqlDb {
    pool: MySqlPool,
}

impl MysqlDb {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = MySqlPool::connect(url).await?;
        Ok(Self { pool })
    }
}

/// Try to decode a column as String; if the DB returns VARBINARY (common in
/// some MySQL collations for information_schema), fall back to UTF-8 bytes.
fn get_string(row: &sqlx::mysql::MySqlRow, col: &str) -> String {
    use sqlx::Row as _;
    if let Ok(s) = row.try_get::<String, _>(col) {
        return s;
    }
    if let Ok(b) = row.try_get::<Vec<u8>, _>(col) {
        return String::from_utf8_lossy(&b).into_owned();
    }
    String::new()
}

fn get_string_idx(row: &sqlx::mysql::MySqlRow, idx: usize) -> String {
    use sqlx::Row as _;
    if let Ok(s) = row.try_get::<String, _>(idx) {
        return s;
    }
    if let Ok(b) = row.try_get::<Vec<u8>, _>(idx) {
        return String::from_utf8_lossy(&b).into_owned();
    }
    String::new()
}

#[async_trait]
impl Database for MysqlDb {
    async fn list_tables(&self) -> Result<Vec<String>> {
        let rows = sqlx::query("SHOW TABLES").fetch_all(&self.pool).await?;
        Ok(rows.iter().map(|r| get_string_idx(r, 0)).collect())
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
            let name = get_string(row, "COLUMN_NAME");
            let data_type = get_string(row, "DATA_TYPE");
            let is_nullable = get_string(row, "IS_NULLABLE");
            let col_key = get_string(row, "COLUMN_KEY");
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
            let from_column = get_string(row, "COLUMN_NAME");
            let to_table = get_string(row, "REFERENCED_TABLE_NAME");
            let to_column = get_string(row, "REFERENCED_COLUMN_NAME");
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

    async fn describe_all_tables(&self, tables: &[String]) -> Result<Vec<TableInfo>> {
        if tables.is_empty() {
            return Ok(vec![]);
        }

        // Fetch ALL columns for all tables in one query.
        let col_rows = sqlx::query(
            "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY \
             FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() \
             ORDER BY TABLE_NAME, ORDINAL_POSITION",
        )
        .fetch_all(&self.pool)
        .await?;

        // Fetch ALL foreign keys in one query.
        let fk_rows = sqlx::query(
            "SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME \
             FROM information_schema.KEY_COLUMN_USAGE \
             WHERE TABLE_SCHEMA = DATABASE() \
             AND REFERENCED_TABLE_NAME IS NOT NULL",
        )
        .fetch_all(&self.pool)
        .await?;

        // Build maps keyed by table name.
        let mut col_map: HashMap<String, Vec<ColumnInfo>> = HashMap::new();
        for row in &col_rows {
            let tname = get_string(row, "TABLE_NAME");
            let name = get_string(row, "COLUMN_NAME");
            let data_type = get_string(row, "DATA_TYPE");
            let is_nullable = get_string(row, "IS_NULLABLE");
            let col_key = get_string(row, "COLUMN_KEY");
            col_map.entry(tname).or_default().push(ColumnInfo {
                name,
                data_type,
                nullable: is_nullable == "YES",
                is_primary_key: col_key == "PRI",
            });
        }

        let mut fk_map: HashMap<String, Vec<ForeignKey>> = HashMap::new();
        for row in &fk_rows {
            let tname = get_string(row, "TABLE_NAME");
            let from_column = get_string(row, "COLUMN_NAME");
            let to_table = get_string(row, "REFERENCED_TABLE_NAME");
            let to_column = get_string(row, "REFERENCED_COLUMN_NAME");
            fk_map.entry(tname).or_default().push(ForeignKey {
                from_column,
                to_table,
                to_column,
            });
        }

        Ok(tables
            .iter()
            .map(|t| TableInfo {
                name: t.clone(),
                columns: col_map.remove(t).unwrap_or_default(),
                foreign_keys: fk_map.remove(t).unwrap_or_default(),
            })
            .collect())
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

    // Universal NULL probe: sqlx checks for SQL NULL before type compatibility,
    // so any Option<T> returns Ok(None) when the column value is NULL.
    // We use Vec<u8> as the probe because it's accepted for nearly all MySQL types.
    if matches!(row.try_get::<Option<Vec<u8>>, _>(idx), Ok(None)) {
        return Value::Null;
    }

    // Integer families — sqlx MySQL requires the exact Rust type per variant.
    // Try from smallest to largest; sqlx returns Err on type mismatch.
    if upper.contains("INT") || upper == "YEAR" {
        if let Ok(Some(v)) = row.try_get::<Option<i8>,  _>(idx) { return Value::Integer(v as i64); }
        if let Ok(Some(v)) = row.try_get::<Option<u8>,  _>(idx) { return Value::Integer(v as i64); }
        if let Ok(Some(v)) = row.try_get::<Option<i16>, _>(idx) { return Value::Integer(v as i64); }
        if let Ok(Some(v)) = row.try_get::<Option<u16>, _>(idx) { return Value::Integer(v as i64); }
        if let Ok(Some(v)) = row.try_get::<Option<i32>, _>(idx) { return Value::Integer(v as i64); }
        if let Ok(Some(v)) = row.try_get::<Option<u32>, _>(idx) { return Value::Integer(v as i64); }
        if let Ok(Some(v)) = row.try_get::<Option<i64>, _>(idx) { return Value::Integer(v); }
        if let Ok(Some(v)) = row.try_get::<Option<u64>, _>(idx) { return Value::Integer(v as i64); }
        if let Ok(Some(v)) = row.try_get::<Option<bool>, _>(idx) { return Value::Integer(v as i64); }
        return not_implemented(type_name);
    }

    // BIT columns: sqlx MySQL decodes BIT as u64 (big-endian integer).
    if upper == "BIT" {
        if let Ok(Some(v)) = row.try_get::<Option<u64>, _>(idx) { return Value::Integer(v as i64); }
        if let Ok(Some(v)) = row.try_get::<Option<bool>, _>(idx) { return Value::Integer(v as i64); }
        if let Ok(Some(b)) = row.try_get::<Option<Vec<u8>>, _>(idx) {
            let v = b.iter().fold(0u64, |acc, &byte| (acc << 8) | (byte as u64));
            return Value::Integer(v as i64);
        }
        return not_implemented(type_name);
    }

    // BOOLEAN is a MySQL alias for TINYINT(1) but sqlx may report it separately.
    if upper == "BOOLEAN" || upper == "BOOL" {
        if let Ok(Some(v)) = row.try_get::<Option<bool>, _>(idx) { return Value::Integer(v as i64); }
        return not_implemented(type_name);
    }

    if upper.contains("FLOAT") || upper.contains("DOUBLE") {
        // FLOAT → f32, DOUBLE → f64 in sqlx MySQL; try both so either works.
        if let Ok(Some(v)) = row.try_get::<Option<f32>, _>(idx) { return Value::Float(v as f64); }
        if let Ok(Some(v)) = row.try_get::<Option<f64>, _>(idx) { return Value::Float(v); }
        return not_implemented(type_name);
    }

    // DECIMAL/NUMERIC: sqlx returns these as strings without a decimal feature.
    if upper.contains("DECIMAL") || upper.contains("NUMERIC") {
        if let Ok(Some(s)) = row.try_get::<Option<String>, _>(idx) {
            if let Ok(v) = s.parse::<f64>() { return Value::Float(v); }
            return Value::Text(s);
        }
        return not_implemented(type_name);
    }

    // Date/time types require the chrono feature in sqlx MySQL binary protocol.
    if upper == "DATE" {
        if let Ok(Some(v)) = row.try_get::<Option<chrono::NaiveDate>, _>(idx) {
            return Value::Text(v.format("%Y-%m-%d").to_string());
        }
        return not_implemented(type_name);
    }
    if upper == "DATETIME" || upper == "TIMESTAMP" {
        if let Ok(Some(v)) = row.try_get::<Option<chrono::NaiveDateTime>, _>(idx) {
            return Value::Text(v.format("%Y-%m-%d %H:%M:%S").to_string());
        }
        if let Ok(Some(v)) = row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(idx) {
            return Value::Text(v.format("%Y-%m-%d %H:%M:%S").to_string());
        }
        if let Ok(Some(s)) = row.try_get::<Option<String>, _>(idx) { return Value::Text(s); }
        return not_implemented(type_name);
    }
    if upper == "TIME" {
        if let Ok(Some(v)) = row.try_get::<Option<chrono::NaiveTime>, _>(idx) {
            return Value::Text(v.format("%H:%M:%S").to_string());
        }
        return not_implemented(type_name);
    }

    // Pure binary blobs and BINARY(n) / VARBINARY — decode as bytes → UTF-8 if possible.
    if upper.contains("BLOB") || upper.contains("BINARY") {
        if let Ok(Some(v)) = row.try_get::<Option<Vec<u8>>, _>(idx) {
            return match String::from_utf8(v.clone()) {
                Ok(s) => Value::Text(s),
                Err(_) => Value::Bytes(v),
            };
        }
        return not_implemented(type_name);
    }

    // Broad string catch-all: TEXT, VARCHAR, CHAR, ENUM, SET, JSON, etc.
    if let Ok(Some(s)) = row.try_get::<Option<String>, _>(idx) {
        return Value::Text(s);
    }

    // Last-resort raw bytes.
    if let Ok(Some(b)) = row.try_get::<Option<Vec<u8>>, _>(idx) {
        return match String::from_utf8(b.clone()) {
            Ok(s) => Value::Text(s),
            Err(_) => Value::Bytes(b),
        };
    }

    not_implemented(type_name)
}

/// Log a warning for an unsupported type (deduplicated) and return the sentinel value.
fn not_implemented(type_name: &str) -> Value {
    use std::collections::HashSet;
    use std::sync::Mutex;
    static WARNED: Mutex<Option<HashSet<String>>> = Mutex::new(None);
    let mut guard = WARNED.lock().unwrap();
    let seen = guard.get_or_insert_with(HashSet::new);
    if seen.insert(type_name.to_string()) {
        crate::log::warn(format!("MySQL type '{}' is not yet supported — showing NOT IMPLEMENTED", type_name));
    }
    Value::Text("NOT IMPLEMENTED".to_string())
}
