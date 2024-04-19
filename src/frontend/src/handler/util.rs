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

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Context as _;
use bytes::{Bytes, BytesMut};
use futures::Stream;
use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::RowSetResult;
use pgwire::pg_server::BoxedError;
use pgwire::types::{Format, FormatIterator, Row};
use pin_project_lite::pin_project;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Field;
use risingwave_common::row::Row as _;
use risingwave_common::types::{write_date_time_tz, DataType, ScalarRefImpl, Timestamptz};
use risingwave_common::util::epoch::Epoch;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_sqlparser::ast::{
    BinaryOperator, CompatibleSourceSchema, ConnectorSchema, Expr, ObjectName, OrderByExpr, Query,
    Select, SelectItem, SetExpr, TableFactor, TableWithJoins, Value,
};

use crate::error::{ErrorCode, Result as RwResult};
use crate::session::cursor_manager::{KV_LOG_STORE_EPOCH, KV_LOG_STORE_SEQ_ID, KV_LOG_STORE_VNODE};
use crate::session::{current, SessionImpl};

pin_project! {
    /// Wrapper struct that converts a stream of DataChunk to a stream of RowSet based on formatting
    /// parameters.
    ///
    /// This is essentially `StreamExt::map(self, move |res| res.map(|chunk| to_pg_rows(chunk,
    /// format)))` but we need a nameable type as part of [`super::PgResponseStream`], but we cannot
    /// name the type of a closure.
    pub struct DataChunkToRowSetAdapter<VS>
    where
        VS: Stream<Item = Result<DataChunk, BoxedError>>,
    {
        #[pin]
        chunk_stream: VS,
        column_types: Vec<DataType>,
        formats: Vec<Format>,
        session_data: StaticSessionData,
    }
}

// Static session data frozen at the time of the creation of the stream
struct StaticSessionData {
    timezone: String,
}

impl<VS> DataChunkToRowSetAdapter<VS>
where
    VS: Stream<Item = Result<DataChunk, BoxedError>>,
{
    pub fn new(
        chunk_stream: VS,
        column_types: Vec<DataType>,
        formats: Vec<Format>,
        session: Arc<SessionImpl>,
    ) -> Self {
        let session_data = StaticSessionData {
            timezone: session.config().timezone(),
        };
        Self {
            chunk_stream,
            column_types,
            formats,
            session_data,
        }
    }
}

impl<VS> Stream for DataChunkToRowSetAdapter<VS>
where
    VS: Stream<Item = Result<DataChunk, BoxedError>>,
{
    type Item = RowSetResult;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.chunk_stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(chunk) => match chunk {
                Some(chunk_result) => match chunk_result {
                    Ok(chunk) => Poll::Ready(Some(
                        to_pg_rows(this.column_types, chunk, this.formats, this.session_data)
                            .map_err(|err| err.into()),
                    )),
                    Err(err) => Poll::Ready(Some(Err(err))),
                },
                None => Poll::Ready(None),
            },
        }
    }
}

/// Format scalars according to postgres convention.
fn pg_value_format(
    data_type: &DataType,
    d: ScalarRefImpl<'_>,
    format: Format,
    session_data: &StaticSessionData,
) -> RwResult<Bytes> {
    // format == false means TEXT format
    // format == true means BINARY format
    match format {
        Format::Text => {
            if *data_type == DataType::Timestamptz {
                Ok(timestamptz_to_string_with_session_data(d, session_data))
            } else {
                Ok(d.text_format(data_type).into())
            }
        }
        Format::Binary => Ok(d
            .binary_format(data_type)
            .context("failed to format binary value")?),
    }
}

fn timestamptz_to_string_with_session_data(
    d: ScalarRefImpl<'_>,
    session_data: &StaticSessionData,
) -> Bytes {
    let tz = d.into_timestamptz();
    let time_zone = Timestamptz::lookup_time_zone(&session_data.timezone).unwrap();
    let instant_local = tz.to_datetime_in_zone(time_zone);
    let mut result_string = BytesMut::new();
    write_date_time_tz(instant_local, &mut result_string).unwrap();
    result_string.into()
}

fn to_pg_rows(
    column_types: &[DataType],
    chunk: DataChunk,
    formats: &[Format],
    session_data: &StaticSessionData,
) -> RwResult<Vec<Row>> {
    assert_eq!(chunk.dimension(), column_types.len());

    chunk
        .rows()
        .map(|r| {
            let format_iter = FormatIterator::new(formats, chunk.dimension())
                .map_err(ErrorCode::InternalError)?;
            let row = r
                .iter()
                .zip_eq_fast(column_types)
                .zip_eq_fast(format_iter)
                .map(|((data, t), format)| match data {
                    Some(data) => Some(pg_value_format(t, data, format, session_data)).transpose(),
                    None => Ok(None),
                })
                .try_collect()?;
            Ok(Row::new(row))
        })
        .try_collect()
}

/// Convert from [`Field`] to [`PgFieldDescriptor`].
pub fn to_pg_field(f: &Field) -> PgFieldDescriptor {
    PgFieldDescriptor::new(
        f.name.clone(),
        f.data_type().to_oid(),
        f.data_type().type_len(),
    )
}

#[easy_ext::ext(SourceSchemaCompatExt)]
impl CompatibleSourceSchema {
    /// Convert `self` to [`ConnectorSchema`] and warn the user if the syntax is deprecated.
    pub fn into_v2_with_warning(self) -> ConnectorSchema {
        match self {
            CompatibleSourceSchema::RowFormat(inner) => {
                // TODO: should be warning
                current::notice_to_user("RisingWave will stop supporting the syntax \"ROW FORMAT\" in future versions, which will be changed to \"FORMAT ... ENCODE ...\" syntax.");
                inner.into_source_schema_v2()
            }
            CompatibleSourceSchema::V2(inner) => inner,
        }
    }
}

pub fn gen_query_from_table_name(from_name: ObjectName) -> Query {
    let table_factor = TableFactor::Table {
        name: from_name,
        alias: None,
        as_of: None,
    };
    let from = vec![TableWithJoins {
        relation: table_factor,
        joins: vec![],
    }];
    let select = Select {
        from,
        projection: vec![SelectItem::Wildcard(None)],
        ..Default::default()
    };
    let body = SetExpr::Select(Box::new(select));
    Query {
        with: None,
        body,
        order_by: vec![],
        limit: None,
        offset: None,
        fetch: None,
    }
}

pub fn gen_query_from_logstore_ge_rw_timestamp(logstore_name: &str, rw_timestamp: i64) -> Query {
    let table_factor = TableFactor::Table {
        name: ObjectName(vec![logstore_name.into()]),
        alias: None,
        as_of: None,
    };
    let from = vec![TableWithJoins {
        relation: table_factor,
        joins: vec![],
    }];
    let selection = Some(Expr::BinaryOp {
        left: Box::new(Expr::Identifier(KV_LOG_STORE_EPOCH.into())),
        op: BinaryOperator::GtEq,
        right: Box::new(Expr::Value(Value::Number(rw_timestamp.to_string()))),
    });
    let except_columns = vec![
        Expr::Identifier(KV_LOG_STORE_SEQ_ID.into()),
        Expr::Identifier(KV_LOG_STORE_VNODE.into()),
    ];
    let select = Select {
        from,
        projection: vec![SelectItem::Wildcard(Some(except_columns))],
        selection,
        ..Default::default()
    };
    let order_by = vec![OrderByExpr {
        expr: Expr::Identifier(KV_LOG_STORE_EPOCH.into()),
        asc: None,
        nulls_first: None,
    }];
    let body = SetExpr::Select(Box::new(select));
    Query {
        with: None,
        body,
        order_by,
        limit: None,
        offset: None,
        fetch: None,
    }
}

pub fn convert_unix_millis_to_logstore_i64(unix_millis: u64) -> i64 {
    let epoch = Epoch::from_unix_millis(unix_millis);
    convert_epoch_to_logstore_i64(epoch.0)
}

pub fn convert_epoch_to_logstore_i64(epoch: u64) -> i64 {
    epoch as i64 ^ (1i64 << 63)
}

pub fn convert_logstore_i64_to_unix_millis(logstore_i64: i64) -> u64 {
    let epoch = Epoch::from(logstore_i64 as u64 ^ (1u64 << 63));
    epoch.as_unix_millis()
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use postgres_types::{ToSql, Type};
    use risingwave_common::array::*;
    use risingwave_common::types::Timestamptz;

    use super::*;

    #[test]
    fn test_to_pg_field() {
        let field = Field::with_name(DataType::Int32, "v1");
        let pg_field = to_pg_field(&field);
        assert_eq!(pg_field.get_name(), "v1");
        assert_eq!(pg_field.get_type_oid(), DataType::Int32.to_oid());
    }

    #[test]
    fn test_to_pg_rows() {
        let chunk = DataChunk::from_pretty(
            "i I f    T
             1 6 6.01 aaa
             2 . .    .
             3 7 7.01 vvv
             4 . .    .  ",
        );
        let static_session = StaticSessionData {
            timezone: "UTC".into(),
        };
        let rows = to_pg_rows(
            &[
                DataType::Int32,
                DataType::Int64,
                DataType::Float32,
                DataType::Varchar,
            ],
            chunk,
            &[],
            &static_session,
        );
        let expected: Vec<Vec<Option<Bytes>>> = vec![
            vec![
                Some("1".into()),
                Some("6".into()),
                Some("6.01".into()),
                Some("aaa".into()),
            ],
            vec![Some("2".into()), None, None, None],
            vec![
                Some("3".into()),
                Some("7".into()),
                Some("7.01".into()),
                Some("vvv".into()),
            ],
            vec![Some("4".into()), None, None, None],
        ];
        let vec = rows
            .unwrap()
            .into_iter()
            .map(|r| r.values().iter().cloned().collect_vec())
            .collect_vec();

        assert_eq!(vec, expected);
    }

    #[test]
    fn test_to_pg_rows_mix_format() {
        let chunk = DataChunk::from_pretty(
            "i I f    T
             1 6 6.01 aaa
            ",
        );
        let static_session = StaticSessionData {
            timezone: "UTC".into(),
        };
        let rows = to_pg_rows(
            &[
                DataType::Int32,
                DataType::Int64,
                DataType::Float32,
                DataType::Varchar,
            ],
            chunk,
            &[Format::Binary, Format::Binary, Format::Binary, Format::Text],
            &static_session,
        );
        let mut raw_params = vec![BytesMut::new(); 3];
        1_i32.to_sql(&Type::ANY, &mut raw_params[0]).unwrap();
        6_i64.to_sql(&Type::ANY, &mut raw_params[1]).unwrap();
        6.01_f32.to_sql(&Type::ANY, &mut raw_params[2]).unwrap();
        let raw_params = raw_params
            .into_iter()
            .map(|b| b.freeze())
            .collect::<Vec<_>>();
        let expected: Vec<Vec<Option<Bytes>>> = vec![vec![
            Some(raw_params[0].clone()),
            Some(raw_params[1].clone()),
            Some(raw_params[2].clone()),
            Some("aaa".into()),
        ]];
        let vec = rows
            .unwrap()
            .into_iter()
            .map(|r| r.values().iter().cloned().collect_vec())
            .collect_vec();

        assert_eq!(vec, expected);
    }

    #[test]
    fn test_value_format() {
        use {DataType as T, ScalarRefImpl as S};
        let static_session = StaticSessionData {
            timezone: "UTC".into(),
        };

        let f = |t, d, f| pg_value_format(t, d, f, &static_session).unwrap();
        assert_eq!(&f(&T::Float32, S::Float32(1_f32.into()), Format::Text), "1");
        assert_eq!(
            &f(&T::Float32, S::Float32(f32::NAN.into()), Format::Text),
            "NaN"
        );
        assert_eq!(
            &f(&T::Float64, S::Float64(f64::NAN.into()), Format::Text),
            "NaN"
        );
        assert_eq!(
            &f(&T::Float32, S::Float32(f32::INFINITY.into()), Format::Text),
            "Infinity"
        );
        assert_eq!(
            &f(
                &T::Float32,
                S::Float32(f32::NEG_INFINITY.into()),
                Format::Text
            ),
            "-Infinity"
        );
        assert_eq!(
            &f(&T::Float64, S::Float64(f64::INFINITY.into()), Format::Text),
            "Infinity"
        );
        assert_eq!(
            &f(
                &T::Float64,
                S::Float64(f64::NEG_INFINITY.into()),
                Format::Text
            ),
            "-Infinity"
        );
        assert_eq!(&f(&T::Boolean, S::Bool(true), Format::Text), "t");
        assert_eq!(&f(&T::Boolean, S::Bool(false), Format::Text), "f");
        assert_eq!(
            &f(
                &T::Timestamptz,
                S::Timestamptz(Timestamptz::from_micros(-1)),
                Format::Text
            ),
            "1969-12-31 23:59:59.999999+00:00"
        );
    }
}
