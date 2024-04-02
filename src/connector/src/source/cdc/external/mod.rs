// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod mock_external_table;
mod postgres;

#[cfg(not(madsim))]
mod maybe_tls_connector;

use std::collections::HashMap;
use std::fmt;

use anyhow::Context;
use futures::stream::BoxStream;
use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use mysql_async::prelude::*;
use mysql_common::params::Params;
use mysql_common::value::Value;
use risingwave_common::bail;
use risingwave_common::catalog::{Schema, OFFSET_COLUMN_NAME};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use serde_derive::{Deserialize, Serialize};

use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::mysql_row_to_owned_row;
use crate::source::cdc::external::mock_external_table::MockExternalTableReader;
use crate::source::cdc::external::postgres::{PostgresExternalTableReader, PostgresOffset};
use crate::WithPropertiesExt;

#[derive(Debug)]
pub enum CdcTableType {
    Undefined,
    MySql,
    Postgres,
    Citus,
}

impl CdcTableType {
    pub fn from_properties(with_properties: &impl WithPropertiesExt) -> Self {
        let connector = with_properties.get_connector().unwrap_or_default();
        match connector.as_str() {
            "mysql-cdc" => Self::MySql,
            "postgres-cdc" => Self::Postgres,
            "citus-cdc" => Self::Citus,
            _ => Self::Undefined,
        }
    }

    pub fn can_backfill(&self) -> bool {
        matches!(self, Self::MySql | Self::Postgres)
    }

    pub async fn create_table_reader(
        &self,
        with_properties: HashMap<String, String>,
        schema: Schema,
    ) -> ConnectorResult<ExternalTableReaderImpl> {
        match self {
            Self::MySql => Ok(ExternalTableReaderImpl::MySql(
                MySqlExternalTableReader::new(with_properties, schema).await?,
            )),
            Self::Postgres => Ok(ExternalTableReaderImpl::Postgres(
                PostgresExternalTableReader::new(with_properties, schema).await?,
            )),
            _ => bail!("invalid external table type: {:?}", *self),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SchemaTableName {
    // namespace of the table, e.g. database in mysql, schema in postgres
    pub schema_name: String,
    pub table_name: String,
}

pub const TABLE_NAME_KEY: &str = "table.name";
pub const SCHEMA_NAME_KEY: &str = "schema.name";
pub const DATABASE_NAME_KEY: &str = "database.name";

impl SchemaTableName {
    pub fn new(schema_name: String, table_name: String) -> Self {
        Self {
            schema_name,
            table_name,
        }
    }

    pub fn from_properties(properties: &HashMap<String, String>) -> Self {
        let table_type = CdcTableType::from_properties(properties);
        let table_name = properties.get(TABLE_NAME_KEY).cloned().unwrap_or_default();

        let schema_name = match table_type {
            CdcTableType::MySql => properties
                .get(DATABASE_NAME_KEY)
                .cloned()
                .unwrap_or_default(),
            CdcTableType::Postgres | CdcTableType::Citus => {
                properties.get(SCHEMA_NAME_KEY).cloned().unwrap_or_default()
            }
            _ => {
                unreachable!("invalid external table type: {:?}", table_type);
            }
        };

        Self {
            schema_name,
            table_name,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MySqlOffset {
    pub filename: String,
    pub position: u64,
}

impl MySqlOffset {
    pub fn new(filename: String, position: u64) -> Self {
        Self { filename, position }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum CdcOffset {
    MySql(MySqlOffset),
    Postgres(PostgresOffset),
}

// Example debezium offset for Postgres:
// {
//     "sourcePartition":
//     {
//         "server": "RW_CDC_1004"
//     },
//     "sourceOffset":
//     {
//         "last_snapshot_record": false,
//         "lsn": 29973552,
//         "txId": 1046,
//         "ts_usec": 1670826189008456,
//         "snapshot": true
//     }
// }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebeziumOffset {
    #[serde(rename = "sourcePartition")]
    pub source_partition: HashMap<String, String>,
    #[serde(rename = "sourceOffset")]
    pub source_offset: DebeziumSourceOffset,
    #[serde(rename = "isHeartbeat")]
    pub is_heartbeat: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DebeziumSourceOffset {
    // postgres snapshot progress
    pub last_snapshot_record: Option<bool>,
    // mysql snapshot progress
    pub snapshot: Option<bool>,

    // mysql binlog offset
    pub file: Option<String>,
    pub pos: Option<u64>,

    // postgres offset
    pub lsn: Option<u64>,
    #[serde(rename = "txId")]
    pub txid: Option<i64>,
    pub tx_usec: Option<u64>,
}

impl MySqlOffset {
    pub fn parse_debezium_offset(offset: &str) -> ConnectorResult<Self> {
        let dbz_offset: DebeziumOffset = serde_json::from_str(offset)
            .with_context(|| format!("invalid upstream offset: {}", offset))?;

        Ok(Self {
            filename: dbz_offset
                .source_offset
                .file
                .context("binlog file not found in offset")?,
            position: dbz_offset
                .source_offset
                .pos
                .context("binlog position not found in offset")?,
        })
    }
}

pub type CdcOffsetParseFunc = Box<dyn Fn(&str) -> ConnectorResult<CdcOffset> + Send>;

pub trait ExternalTableReader {
    fn get_normalized_table_name(&self, table_name: &SchemaTableName) -> String;

    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset>;

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>>;
}

#[derive(Debug)]
pub enum ExternalTableReaderImpl {
    MySql(MySqlExternalTableReader),
    Postgres(PostgresExternalTableReader),
    Mock(MockExternalTableReader),
}

#[derive(Debug)]
pub struct MySqlExternalTableReader {
    config: ExternalTableConfig,
    rw_schema: Schema,
    field_names: String,
    // use mutex to provide shared mutable access to the connection
    conn: tokio::sync::Mutex<mysql_async::Conn>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExternalTableConfig {
    #[serde(rename = "hostname")]
    pub host: String,
    pub port: String,
    pub username: String,
    pub password: String,
    #[serde(rename = "database.name")]
    pub database: String,
    #[serde(rename = "schema.name", default = "Default::default")]
    pub schema: String,
    #[serde(rename = "table.name")]
    pub table: String,
    /// `ssl.mode` specifies the SSL/TLS encryption level for secure communication with Postgres.
    /// Choices include `disable`, `prefer`, and `require`.
    /// This field is optional. `prefer` is used if not specified.
    #[serde(rename = "ssl.mode", default = "Default::default")]
    pub sslmode: SslMode,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SslMode {
    Disable,
    Prefer,
    Require,
}

impl Default for SslMode {
    fn default() -> Self {
        Self::Prefer
    }
}

impl fmt::Display for SslMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SslMode::Disable => "disable",
            SslMode::Prefer => "prefer",
            SslMode::Require => "require",
        })
    }
}

impl ExternalTableReader for MySqlExternalTableReader {
    fn get_normalized_table_name(&self, table_name: &SchemaTableName) -> String {
        // schema name is the database name in mysql
        format!("`{}`.`{}`", table_name.schema_name, table_name.table_name)
    }

    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        let mut conn = self.conn.lock().await;

        let sql = "SHOW MASTER STATUS".to_string();
        let mut rs = conn.query::<mysql_async::Row, _>(sql).await?;
        let row = rs
            .iter_mut()
            .exactly_one()
            .ok()
            .context("expect exactly one row when reading binlog offset")?;

        Ok(CdcOffset::MySql(MySqlOffset {
            filename: row.take("File").unwrap(),
            position: row.take("Position").unwrap(),
        }))
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        self.snapshot_read_inner(table_name, start_pk, primary_keys, limit)
    }
}

impl MySqlExternalTableReader {
    pub async fn new(
        with_properties: HashMap<String, String>,
        rw_schema: Schema,
    ) -> ConnectorResult<Self> {
        tracing::debug!(?rw_schema, "create mysql external table reader");

        let config = serde_json::from_value::<ExternalTableConfig>(
            serde_json::to_value(with_properties).unwrap(),
        )
        .context("failed to extract mysql connector properties")?;

        let database_url = format!(
            "mysql://{}:{}@{}:{}/{}",
            config.username, config.password, config.host, config.port, config.database
        );
        let opts = mysql_async::Opts::from_url(&database_url).map_err(mysql_async::Error::Url)?;
        let conn = mysql_async::Conn::new(opts).await?;

        let field_names = rw_schema
            .fields
            .iter()
            .filter(|f| f.name != OFFSET_COLUMN_NAME)
            .map(|f| Self::quote_column(f.name.as_str()))
            .join(",");

        Ok(Self {
            config,
            rw_schema,
            field_names,
            conn: tokio::sync::Mutex::new(conn),
        })
    }

    pub fn get_cdc_offset_parser() -> CdcOffsetParseFunc {
        Box::new(move |offset| {
            Ok(CdcOffset::MySql(MySqlOffset::parse_debezium_offset(
                offset,
            )?))
        })
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        start_pk_row: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) {
        let order_key = primary_keys
            .iter()
            .map(|col| Self::quote_column(col))
            .join(",");
        let sql = if start_pk_row.is_none() {
            format!(
                "SELECT {} FROM {} ORDER BY {} LIMIT {limit}",
                self.field_names,
                self.get_normalized_table_name(&table_name),
                order_key,
            )
        } else {
            let filter_expr = Self::filter_expression(&primary_keys);
            format!(
                "SELECT {} FROM {} WHERE {} ORDER BY {} LIMIT {limit}",
                self.field_names,
                self.get_normalized_table_name(&table_name),
                filter_expr,
                order_key,
            )
        };

        let mut conn = self.conn.lock().await;

        // Set session timezone to UTC
        conn.exec_drop("SET time_zone = \"+00:00\"", ()).await?;

        if start_pk_row.is_none() {
            let rs_stream = sql.stream::<mysql_async::Row, _>(&mut *conn).await?;
            let row_stream = rs_stream.map(|row| {
                // convert mysql row into OwnedRow
                let mut row = row?;
                Ok::<_, ConnectorError>(mysql_row_to_owned_row(&mut row, &self.rw_schema))
            });

            pin_mut!(row_stream);
            #[for_await]
            for row in row_stream {
                let row = row?;
                yield row;
            }
        } else {
            let field_map = self
                .rw_schema
                .fields
                .iter()
                .map(|f| (f.name.as_str(), f.data_type.clone()))
                .collect::<HashMap<_, _>>();

            // fill in start primary key params
            let params: Vec<_> = primary_keys
                .iter()
                .zip_eq_fast(start_pk_row.unwrap().into_iter())
                .map(|(pk, datum)| {
                    if let Some(value) = datum {
                        let ty = field_map.get(pk.as_str()).unwrap();
                        let val = match ty {
                            DataType::Boolean => Value::from(value.into_bool()),
                            DataType::Int16 => Value::from(value.into_int16()),
                            DataType::Int32 => Value::from(value.into_int32()),
                            DataType::Int64 => Value::from(value.into_int64()),
                            DataType::Float32 => Value::from(value.into_float32().into_inner()),
                            DataType::Float64 => Value::from(value.into_float64().into_inner()),
                            DataType::Varchar => Value::from(String::from(value.into_utf8())),
                            DataType::Date => Value::from(value.into_date().0),
                            DataType::Time => Value::from(value.into_time().0),
                            DataType::Timestamp => Value::from(value.into_timestamp().0),
                            _ => bail!("unsupported primary key data type: {}", ty),
                        };
                        ConnectorResult::Ok((pk.clone(), val))
                    } else {
                        bail!("primary key {} cannot be null", pk);
                    }
                })
                .try_collect::<_, _, ConnectorError>()?;

            let rs_stream = sql
                .with(Params::from(params))
                .stream::<mysql_async::Row, _>(&mut *conn)
                .await?;

            let row_stream = rs_stream.map(|row| {
                // convert mysql row into OwnedRow
                let mut row = row?;
                Ok::<_, ConnectorError>(mysql_row_to_owned_row(&mut row, &self.rw_schema))
            });

            pin_mut!(row_stream);
            #[for_await]
            for row in row_stream {
                let row = row?;
                yield row;
            }
        };
    }

    // mysql cannot leverage the given key to narrow down the range of scan,
    // we need to rewrite the comparison conditions by our own.
    // (a, b) > (x, y) => (`a` > x) OR ((`a` = x) AND (`b` > y))
    fn filter_expression(columns: &[String]) -> String {
        let mut conditions = vec![];
        // push the first condition
        conditions.push(format!(
            "({} > :{})",
            Self::quote_column(&columns[0]),
            columns[0]
        ));
        for i in 2..=columns.len() {
            // '=' condition
            let mut condition = String::new();
            for (j, col) in columns.iter().enumerate().take(i - 1) {
                if j == 0 {
                    condition.push_str(&format!("({} = :{})", Self::quote_column(col), col));
                } else {
                    condition.push_str(&format!(" AND ({} = :{})", Self::quote_column(col), col));
                }
            }
            // '>' condition
            condition.push_str(&format!(
                " AND ({} > :{})",
                Self::quote_column(&columns[i - 1]),
                columns[i - 1]
            ));
            conditions.push(format!("({})", condition));
        }
        if columns.len() > 1 {
            conditions.join(" OR ")
        } else {
            conditions.join("")
        }
    }

    fn quote_column(column: &str) -> String {
        format!("`{}`", column)
    }
}

impl ExternalTableReader for ExternalTableReaderImpl {
    fn get_normalized_table_name(&self, table_name: &SchemaTableName) -> String {
        match self {
            ExternalTableReaderImpl::MySql(mysql) => mysql.get_normalized_table_name(table_name),
            ExternalTableReaderImpl::Postgres(postgres) => {
                postgres.get_normalized_table_name(table_name)
            }
            ExternalTableReaderImpl::Mock(mock) => mock.get_normalized_table_name(table_name),
        }
    }

    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        match self {
            ExternalTableReaderImpl::MySql(mysql) => mysql.current_cdc_offset().await,
            ExternalTableReaderImpl::Postgres(postgres) => postgres.current_cdc_offset().await,
            ExternalTableReaderImpl::Mock(mock) => mock.current_cdc_offset().await,
        }
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        self.snapshot_read_inner(table_name, start_pk, primary_keys, limit)
    }
}

impl ExternalTableReaderImpl {
    pub fn get_cdc_offset_parser(&self) -> CdcOffsetParseFunc {
        match self {
            ExternalTableReaderImpl::MySql(_) => MySqlExternalTableReader::get_cdc_offset_parser(),
            ExternalTableReaderImpl::Postgres(_) => {
                PostgresExternalTableReader::get_cdc_offset_parser()
            }
            ExternalTableReaderImpl::Mock(_) => MockExternalTableReader::get_cdc_offset_parser(),
        }
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) {
        let stream = match self {
            ExternalTableReaderImpl::MySql(mysql) => {
                mysql.snapshot_read(table_name, start_pk, primary_keys, limit)
            }
            ExternalTableReaderImpl::Postgres(postgres) => {
                postgres.snapshot_read(table_name, start_pk, primary_keys, limit)
            }
            ExternalTableReaderImpl::Mock(mock) => {
                mock.snapshot_read(table_name, start_pk, primary_keys, limit)
            }
        };

        pin_mut!(stream);
        #[for_await]
        for row in stream {
            let row = row?;
            yield row;
        }
    }
}

#[cfg(test)]
mod tests {

    use futures::pin_mut;
    use futures_async_stream::for_await;
    use maplit::{convert_args, hashmap};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
    use risingwave_common::types::DataType;

    use crate::source::cdc::external::{
        CdcOffset, ExternalTableReader, MySqlExternalTableReader, MySqlOffset, SchemaTableName,
    };

    #[test]
    fn test_mysql_filter_expr() {
        let cols = vec!["id".to_string()];
        let expr = MySqlExternalTableReader::filter_expression(&cols);
        assert_eq!(expr, "(`id` > :id)");

        let cols = vec!["aa".to_string(), "bb".to_string(), "cc".to_string()];
        let expr = MySqlExternalTableReader::filter_expression(&cols);
        assert_eq!(
            expr,
            "(`aa` > :aa) OR ((`aa` = :aa) AND (`bb` > :bb)) OR ((`aa` = :aa) AND (`bb` = :bb) AND (`cc` > :cc))"
        );
    }

    #[test]
    fn test_mysql_binlog_offset() {
        let off0_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000001", "pos": 105622, "snapshot": true }, "isHeartbeat": false }"#;
        let off1_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000007", "pos": 1062363217, "snapshot": true }, "isHeartbeat": false }"#;
        let off2_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000007", "pos": 659687560, "snapshot": true }, "isHeartbeat": false }"#;
        let off3_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000008", "pos": 7665875, "snapshot": true }, "isHeartbeat": false }"#;
        let off4_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000008", "pos": 7665875, "snapshot": true }, "isHeartbeat": false }"#;

        let off0 = CdcOffset::MySql(MySqlOffset::parse_debezium_offset(off0_str).unwrap());
        let off1 = CdcOffset::MySql(MySqlOffset::parse_debezium_offset(off1_str).unwrap());
        let off2 = CdcOffset::MySql(MySqlOffset::parse_debezium_offset(off2_str).unwrap());
        let off3 = CdcOffset::MySql(MySqlOffset::parse_debezium_offset(off3_str).unwrap());
        let off4 = CdcOffset::MySql(MySqlOffset::parse_debezium_offset(off4_str).unwrap());

        assert!(off0 <= off1);
        assert!(off1 > off2);
        assert!(off2 < off3);
        assert_eq!(off3, off4);
    }

    // manual test case
    #[ignore]
    #[tokio::test]
    async fn test_mysql_table_reader() {
        let columns = vec![
            ColumnDesc::named("v1", ColumnId::new(1), DataType::Int32),
            ColumnDesc::named("v2", ColumnId::new(2), DataType::Decimal),
            ColumnDesc::named("v3", ColumnId::new(3), DataType::Varchar),
            ColumnDesc::named("v4", ColumnId::new(4), DataType::Date),
        ];
        let rw_schema = Schema {
            fields: columns.iter().map(Field::from).collect(),
        };
        let props = convert_args!(hashmap!(
                "hostname" => "localhost",
                "port" => "8306",
                "username" => "root",
                "password" => "123456",
                "database.name" => "mytest",
                "table.name" => "t1"));

        let reader = MySqlExternalTableReader::new(props, rw_schema)
            .await
            .unwrap();
        let offset = reader.current_cdc_offset().await.unwrap();
        println!("BinlogOffset: {:?}", offset);

        let off0_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000001", "pos": 105622, "snapshot": true }, "isHeartbeat": false }"#;
        let parser = MySqlExternalTableReader::get_cdc_offset_parser();
        println!("parsed offset: {:?}", parser(off0_str).unwrap());

        let table_name = SchemaTableName {
            schema_name: "mytest".to_string(),
            table_name: "t1".to_string(),
        };

        let stream = reader.snapshot_read(table_name, None, vec!["v1".to_string()], 1000);
        pin_mut!(stream);
        #[for_await]
        for row in stream {
            println!("OwnedRow: {:?}", row);
        }
    }
}
