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

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::LazyLock;

use auto_enums::auto_enum;
pub use avro::AvroParserConfig;
pub use canal::*;
use csv_parser::CsvParser;
pub use debezium::*;
use futures::{Future, TryFutureExt};
use futures_async_stream::try_stream;
pub use json_parser::*;
pub use protobuf::*;
use risingwave_common::array::{ArrayBuilderImpl, Op, StreamChunk};
use risingwave_common::bail;
use risingwave_common::catalog::{KAFKA_TIMESTAMP_COLUMN_NAME, TABLE_NAME_COLUMN_NAME};
use risingwave_common::log::LogSuppresser;
use risingwave_common::metrics::GLOBAL_ERROR_METRICS;
use risingwave_common::types::{Datum, Scalar, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::tracing::InstrumentStream;
use risingwave_pb::catalog::{
    SchemaRegistryNameStrategy as PbSchemaRegistryNameStrategy, StreamSourceInfo,
};
use risingwave_pb::plan_common::additional_column::ColumnType as AdditionalColumnType;
use thiserror_ext::AsReport;

use self::avro::AvroAccessBuilder;
use self::bytes_parser::BytesAccessBuilder;
pub use self::mysql::mysql_row_to_owned_row;
use self::plain_parser::PlainParser;
pub use self::postgres::postgres_row_to_owned_row;
use self::simd_json_parser::DebeziumJsonAccessBuilder;
pub use self::unified::json::TimestamptzHandling;
use self::unified::AccessImpl;
use self::upsert_parser::UpsertParser;
use self::util::get_kafka_topic;
use crate::connector_common::AwsAuthProps;
use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::maxwell::MaxwellParser;
use crate::parser::simd_json_parser::DebeziumMongoJsonAccessBuilder;
use crate::parser::util::{
    extract_header_inner_from_meta, extract_headers_from_meta, extreact_timestamp_from_meta,
};
use crate::schema::schema_registry::SchemaRegistryAuth;
use crate::schema::InvalidOptionError;
use crate::source::monitor::GLOBAL_SOURCE_METRICS;
use crate::source::{
    extract_source_struct, BoxSourceStream, ChunkSourceStream, SourceColumnDesc, SourceColumnType,
    SourceContext, SourceContextRef, SourceEncode, SourceFormat, SourceMessage, SourceMeta,
};

pub mod additional_columns;
mod avro;
mod bytes_parser;
mod canal;
mod common;
mod csv_parser;
mod debezium;
mod json_parser;
mod maxwell;
mod mysql;
pub mod plain_parser;
mod postgres;

mod protobuf;
pub mod scalar_adapter;
mod unified;
mod upsert_parser;
mod util;

pub use debezium::DEBEZIUM_IGNORE_KEY;
use risingwave_common::buffer::BitmapBuilder;
pub use unified::{AccessError, AccessResult};

/// A builder for building a [`StreamChunk`] from [`SourceColumnDesc`].
pub struct SourceStreamChunkBuilder {
    descs: Vec<SourceColumnDesc>,
    builders: Vec<ArrayBuilderImpl>,
    op_builder: Vec<Op>,
    vis_builder: BitmapBuilder,
}

impl SourceStreamChunkBuilder {
    pub fn with_capacity(descs: Vec<SourceColumnDesc>, cap: usize) -> Self {
        let builders = descs
            .iter()
            .map(|desc| desc.data_type.create_array_builder(cap))
            .collect();

        Self {
            descs,
            builders,
            op_builder: Vec::with_capacity(cap),
            vis_builder: BitmapBuilder::with_capacity(cap),
        }
    }

    pub fn row_writer(&mut self) -> SourceStreamChunkRowWriter<'_> {
        SourceStreamChunkRowWriter {
            descs: &self.descs,
            builders: &mut self.builders,
            op_builder: &mut self.op_builder,
            vis_builder: &mut self.vis_builder,
            visible: true, // write visible rows by default
            row_meta: None,
        }
    }

    /// Consumes the builder and returns a [`StreamChunk`].
    pub fn finish(self) -> StreamChunk {
        StreamChunk::with_visibility(
            self.op_builder,
            self.builders
                .into_iter()
                .map(|builder| builder.finish().into())
                .collect(),
            self.vis_builder.finish(),
        )
    }

    /// Resets the builder and returns a [`StreamChunk`], while reserving `next_cap` capacity for
    /// the builders of the next [`StreamChunk`].
    #[must_use]
    pub fn take(&mut self, next_cap: usize) -> StreamChunk {
        let descs = std::mem::take(&mut self.descs); // we don't use `descs` in `finish`
        let builder = std::mem::replace(self, Self::with_capacity(descs, next_cap));
        builder.finish()
    }

    pub fn len(&self) -> usize {
        self.op_builder.len()
    }

    pub fn is_empty(&self) -> bool {
        self.op_builder.is_empty()
    }
}

/// `SourceStreamChunkRowWriter` is responsible to write one or more records to the [`StreamChunk`],
/// where each contains either one row (Insert/Delete) or two rows (Update) that can be written atomically.
///
/// Callers are supposed to call one of the `insert`, `delete` or `update` methods to write a record,
/// providing a closure that produces one or two [`Datum`]s by corresponding [`SourceColumnDesc`].
/// Specifically,
/// - only columns with [`SourceColumnType::Normal`] need to be handled;
/// - errors for non-primary key columns will be ignored and filled with default value instead;
/// - other errors will be propagated.
pub struct SourceStreamChunkRowWriter<'a> {
    descs: &'a [SourceColumnDesc],
    builders: &'a mut [ArrayBuilderImpl],
    op_builder: &'a mut Vec<Op>,
    vis_builder: &'a mut BitmapBuilder,

    /// Whether the rows written by this writer should be visible in output `StreamChunk`.
    visible: bool,

    /// An optional meta data of the original message.
    ///
    /// When this is set by `with_meta`, it'll be used to fill the columns of types other than [`SourceColumnType::Normal`].
    row_meta: Option<MessageMeta<'a>>,
}

impl<'a> SourceStreamChunkRowWriter<'a> {
    /// Set the meta data of the original message for this row writer.
    ///
    /// This should always be called except for tests.
    fn with_meta(mut self, row_meta: MessageMeta<'a>) -> Self {
        self.row_meta = Some(row_meta);
        self
    }

    /// Convert the row writer to invisible row writer.
    fn invisible(mut self) -> Self {
        self.visible = false;
        self
    }
}

/// The meta data of the original message for a row writer.
///
/// Extracted from the `SourceMessage`.
#[derive(Clone, Copy, Debug)]
pub struct MessageMeta<'a> {
    meta: &'a SourceMeta,
    split_id: &'a str,
    offset: &'a str,
}

impl MessageMeta<'_> {
    /// Extract the value for the given column.
    ///
    /// Returns `None` if the column is not a meta column.
    fn value_for_column(self, desc: &SourceColumnDesc) -> Option<Datum> {
        match desc.column_type {
            // Row id columns are filled with `NULL` here and will be filled with the real
            // row id generated by `RowIdGenExecutor` later.
            SourceColumnType::RowId => Datum::None.into(),
            // Extract the offset from the meta data.
            SourceColumnType::Offset => Datum::Some(self.offset.into()).into(),
            // Extract custom meta data per connector.
            SourceColumnType::Meta if let SourceMeta::Kafka(kafka_meta) = self.meta => {
                assert_eq!(
                    desc.name.as_str(),
                    KAFKA_TIMESTAMP_COLUMN_NAME,
                    "unexpected kafka meta column name"
                );
                kafka_meta
                    .timestamp
                    .map(|ts| {
                        risingwave_common::cast::i64_to_timestamptz(ts)
                            .unwrap()
                            .to_scalar_value()
                    })
                    .into()
            }
            SourceColumnType::Meta if let SourceMeta::DebeziumCdc(cdc_meta) = self.meta => {
                assert_eq!(
                    desc.name.as_str(),
                    TABLE_NAME_COLUMN_NAME,
                    "unexpected cdc meta column name"
                );
                Datum::Some(cdc_meta.full_table_name.as_str().into()).into()
            }

            // For other cases, return `None`.
            SourceColumnType::Meta | SourceColumnType::Normal => None,
        }
    }
}

trait OpAction {
    type Output;

    fn output_for(datum: Datum) -> Self::Output;

    fn apply(builder: &mut ArrayBuilderImpl, output: Self::Output);

    fn rollback(builder: &mut ArrayBuilderImpl);

    fn finish(writer: &mut SourceStreamChunkRowWriter<'_>);
}

struct OpActionInsert;

impl OpAction for OpActionInsert {
    type Output = Datum;

    #[inline(always)]
    fn output_for(datum: Datum) -> Self::Output {
        datum
    }

    #[inline(always)]
    fn apply(builder: &mut ArrayBuilderImpl, output: Datum) {
        builder.append(&output)
    }

    #[inline(always)]
    fn rollback(builder: &mut ArrayBuilderImpl) {
        builder.pop().unwrap()
    }

    #[inline(always)]
    fn finish(writer: &mut SourceStreamChunkRowWriter<'_>) {
        writer.append_op(Op::Insert);
    }
}

struct OpActionDelete;

impl OpAction for OpActionDelete {
    type Output = Datum;

    #[inline(always)]
    fn output_for(datum: Datum) -> Self::Output {
        datum
    }

    #[inline(always)]
    fn apply(builder: &mut ArrayBuilderImpl, output: Datum) {
        builder.append(&output)
    }

    #[inline(always)]
    fn rollback(builder: &mut ArrayBuilderImpl) {
        builder.pop().unwrap()
    }

    #[inline(always)]
    fn finish(writer: &mut SourceStreamChunkRowWriter<'_>) {
        writer.append_op(Op::Delete);
    }
}

struct OpActionUpdate;

impl OpAction for OpActionUpdate {
    type Output = (Datum, Datum);

    #[inline(always)]
    fn output_for(datum: Datum) -> Self::Output {
        (datum.clone(), datum)
    }

    #[inline(always)]
    fn apply(builder: &mut ArrayBuilderImpl, output: (Datum, Datum)) {
        builder.append(&output.0);
        builder.append(&output.1);
    }

    #[inline(always)]
    fn rollback(builder: &mut ArrayBuilderImpl) {
        builder.pop().unwrap();
        builder.pop().unwrap();
    }

    #[inline(always)]
    fn finish(writer: &mut SourceStreamChunkRowWriter<'_>) {
        writer.append_op(Op::UpdateDelete);
        writer.append_op(Op::UpdateInsert);
    }
}

impl SourceStreamChunkRowWriter<'_> {
    fn append_op(&mut self, op: Op) {
        self.op_builder.push(op);
        self.vis_builder.append(self.visible);
    }

    fn do_action<A: OpAction>(
        &mut self,
        mut f: impl FnMut(&SourceColumnDesc) -> AccessResult<A::Output>,
    ) -> AccessResult<()> {
        let mut parse_field = |desc: &SourceColumnDesc| {
            match f(desc) {
                Ok(output) => Ok(output),

                // Throw error for failed access to primary key columns.
                Err(e) if desc.is_pk => Err(e),
                // Ignore error for other columns and fill in `NULL` instead.
                Err(error) => {
                    // TODO: figure out a way to fill in not-null default value if user specifies one
                    // TODO: decide whether the error should not be ignored (e.g., even not a valid Debezium message)
                    // TODO: not using tracing span to provide `split_id` and `offset` due to performance concern,
                    //       see #13105
                    static LOG_SUPPERSSER: LazyLock<LogSuppresser> =
                        LazyLock::new(LogSuppresser::default);
                    if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                        tracing::warn!(
                            error = %error.as_report(),
                            split_id = self.row_meta.as_ref().map(|m| m.split_id),
                            offset = self.row_meta.as_ref().map(|m| m.offset),
                            column = desc.name,
                            suppressed_count,
                            "failed to parse non-pk column, padding with `NULL`"
                        );
                    }
                    Ok(A::output_for(Datum::None))
                }
            }
        };

        let mut wrapped_f = |desc: &SourceColumnDesc| {
            match (&desc.column_type, &desc.additional_column.column_type) {
                (&SourceColumnType::Offset | &SourceColumnType::RowId, _) => {
                    // SourceColumnType is for CDC source only.
                    Ok(A::output_for(
                        self.row_meta
                            .as_ref()
                            .and_then(|row_meta| row_meta.value_for_column(desc))
                            .unwrap(), // handled all match cases in internal match, unwrap is safe
                    ))
                }
                (&SourceColumnType::Meta, _)
                    if matches!(
                        &self.row_meta.map(|ele| ele.meta),
                        &Some(SourceMeta::Kafka(_) | SourceMeta::DebeziumCdc(_))
                    ) =>
                {
                    // SourceColumnType is for CDC source only.
                    return Ok(A::output_for(
                        self.row_meta
                            .as_ref()
                            .and_then(|row_meta| row_meta.value_for_column(desc))
                            .unwrap(), // handled all match cases in internal match, unwrap is safe
                    ));
                }
                (_, &Some(AdditionalColumnType::Timestamp(_))) => match self.row_meta {
                    Some(row_meta) => Ok(A::output_for(
                        extreact_timestamp_from_meta(row_meta.meta).unwrap_or(None),
                    )),
                    None => parse_field(desc), // parse from payload
                },
                (_, &Some(AdditionalColumnType::Partition(_))) => {
                    // the meta info does not involve spec connector
                    return Ok(A::output_for(
                        self.row_meta
                            .as_ref()
                            .map(|ele| ScalarImpl::Utf8(ele.split_id.to_string().into())),
                    ));
                }
                (_, &Some(AdditionalColumnType::Offset(_))) => {
                    // the meta info does not involve spec connector
                    return Ok(A::output_for(
                        self.row_meta
                            .as_ref()
                            .map(|ele| ScalarImpl::Utf8(ele.offset.to_string().into())),
                    ));
                }
                (_, &Some(AdditionalColumnType::HeaderInner(ref header_inner))) => {
                    return Ok(A::output_for(
                        self.row_meta
                            .as_ref()
                            .and_then(|ele| {
                                extract_header_inner_from_meta(
                                    ele.meta,
                                    header_inner.inner_field.as_ref(),
                                    header_inner.data_type.as_ref(),
                                )
                            })
                            .unwrap_or(None),
                    ))
                }
                (_, &Some(AdditionalColumnType::Headers(_))) => {
                    return Ok(A::output_for(
                        self.row_meta
                            .as_ref()
                            .and_then(|ele| extract_headers_from_meta(ele.meta))
                            .unwrap_or(None),
                    ))
                }
                (_, &Some(AdditionalColumnType::Filename(_))) => {
                    // Filename is used as partition in FS connectors
                    return Ok(A::output_for(
                        self.row_meta
                            .as_ref()
                            .map(|ele| ScalarImpl::Utf8(ele.split_id.to_string().into())),
                    ));
                }
                (_, _) => {
                    // For normal columns, call the user provided closure.
                    parse_field(desc)
                }
            }
        };

        // Columns that changes have been applied to. Used to rollback when an error occurs.
        let mut applied_columns = Vec::with_capacity(self.descs.len());

        let result = (self.descs.iter())
            .zip_eq_fast(self.builders.iter_mut())
            .try_for_each(|(desc, builder)| {
                wrapped_f(desc).map(|output| {
                    A::apply(builder, output);
                    applied_columns.push(builder);
                })
            });

        match result {
            Ok(_) => {
                A::finish(self);
                Ok(())
            }
            Err(e) => {
                for builder in applied_columns {
                    A::rollback(builder);
                }
                Err(e)
            }
        }
    }

    /// Write an `Insert` record to the [`StreamChunk`], with the given fallible closure that
    /// produces one [`Datum`] by corresponding [`SourceColumnDesc`].
    ///
    /// See the [struct-level documentation](SourceStreamChunkRowWriter) for more details.
    pub fn insert(
        &mut self,
        f: impl FnMut(&SourceColumnDesc) -> AccessResult<Datum>,
    ) -> AccessResult<()> {
        self.do_action::<OpActionInsert>(f)
    }

    /// Write a `Delete` record to the [`StreamChunk`], with the given fallible closure that
    /// produces one [`Datum`] by corresponding [`SourceColumnDesc`].
    ///
    /// See the [struct-level documentation](SourceStreamChunkRowWriter) for more details.
    pub fn delete(
        &mut self,
        f: impl FnMut(&SourceColumnDesc) -> AccessResult<Datum>,
    ) -> AccessResult<()> {
        self.do_action::<OpActionDelete>(f)
    }

    /// Write a `Update` record to the [`StreamChunk`], with the given fallible closure that
    /// produces two [`Datum`]s as old and new value by corresponding [`SourceColumnDesc`].
    ///
    /// See the [struct-level documentation](SourceStreamChunkRowWriter) for more details.
    pub fn update(
        &mut self,
        f: impl FnMut(&SourceColumnDesc) -> AccessResult<(Datum, Datum)>,
    ) -> AccessResult<()> {
        self.do_action::<OpActionUpdate>(f)
    }
}

/// Transaction control message. Currently only used by Debezium messages.
#[derive(Debug)]
pub enum TransactionControl {
    Begin { id: Box<str> },
    Commit { id: Box<str> },
}

/// The result of parsing a message.
#[derive(Debug)]
pub enum ParseResult {
    /// Some rows are parsed and written to the [`SourceStreamChunkRowWriter`].
    Rows,
    /// A transaction control message is parsed.
    TransactionControl(TransactionControl),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ParserFormat {
    CanalJson,
    Csv,
    Json,
    Maxwell,
    Debezium,
    DebeziumMongo,
    Upsert,
    Plain,
}

/// `ByteStreamSourceParser` is the entrypoint abstraction for parsing messages.
/// It consumes bytes of one individual message and produces parsed records.
///
/// It's used by [`ByteStreamSourceParserImpl::into_stream`]. `pub` is for benchmark only.
pub trait ByteStreamSourceParser: Send + Debug + Sized + 'static {
    /// The column descriptors of the output chunk.
    fn columns(&self) -> &[SourceColumnDesc];

    /// The source context, used to report parsing error.
    fn source_ctx(&self) -> &SourceContext;

    /// The format of the specific parser.
    fn parser_format(&self) -> ParserFormat;

    /// Parse one record from the given `payload` and write rows to the `writer`.
    ///
    /// Returns error if **any** of the rows in the message failed to parse.
    fn parse_one<'a>(
        &'a mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> impl Future<Output = ConnectorResult<()>> + Send + 'a;

    /// Parse one record from the given `payload`, either write rows to the `writer` or interpret it
    /// as a transaction control message.
    ///
    /// The default implementation forwards to [`ByteStreamSourceParser::parse_one`] for
    /// non-transactional sources.
    ///
    /// Returns error if **any** of the rows in the message failed to parse.
    fn parse_one_with_txn<'a>(
        &'a mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> impl Future<Output = ConnectorResult<ParseResult>> + Send + 'a {
        self.parse_one(key, payload, writer)
            .map_ok(|_| ParseResult::Rows)
    }

    fn emit_empty_row<'a>(&'a mut self, mut writer: SourceStreamChunkRowWriter<'a>) {
        _ = writer.insert(|_column| Ok(None));
    }
}

#[try_stream(ok = Vec<SourceMessage>, error = ConnectorError)]
async fn ensure_largest_at_rate_limit(stream: BoxSourceStream, rate_limit: u32) {
    #[for_await]
    for batch in stream {
        let mut batch = batch?;
        let mut start = 0;
        let end = batch.len();
        while start < end {
            let next = std::cmp::min(start + rate_limit as usize, end);
            yield std::mem::take(&mut batch[start..next].as_mut()).to_vec();
            start = next;
        }
    }
}

#[easy_ext::ext(SourceParserIntoStreamExt)]
impl<P: ByteStreamSourceParser> P {
    /// Parse a data stream of one source split into a stream of [`StreamChunk`].
    ///
    /// # Arguments
    /// - `data_stream`: A data stream of one source split.
    ///  To be able to split multiple messages from mq, so it is not a pure byte stream
    ///
    /// # Returns
    ///
    /// A [`ChunkSourceStream`] which is a stream of parsed messages.
    pub fn into_stream(self, data_stream: BoxSourceStream) -> impl ChunkSourceStream {
        let actor_id = self.source_ctx().actor_id;
        let source_id = self.source_ctx().source_id.table_id();

        // Ensure chunk size is smaller than rate limit
        let data_stream = if let Some(rate_limit) = &self.source_ctx().source_ctrl_opts.rate_limit {
            Box::pin(ensure_largest_at_rate_limit(data_stream, *rate_limit))
        } else {
            data_stream
        };

        // The parser stream will be long-lived. We use `instrument_with` here to create
        // a new span for the polling of each chunk.
        into_chunk_stream_inner(self, data_stream)
            .instrument_with(move || tracing::info_span!("source_parse_chunk", actor_id, source_id))
    }
}

/// Maximum number of rows in a transaction. If a transaction is larger than this, it will be force
/// committed to avoid potential OOM.
const MAX_ROWS_FOR_TRANSACTION: usize = 4096;

// TODO: when upsert is disabled, how to filter those empty payload
// Currently, an err is returned for non upsert with empty payload
#[try_stream(ok = StreamChunk, error = crate::error::ConnectorError)]
async fn into_chunk_stream_inner<P: ByteStreamSourceParser>(
    mut parser: P,
    data_stream: BoxSourceStream,
) {
    let columns = parser.columns().to_vec();

    let mut heartbeat_builder = SourceStreamChunkBuilder::with_capacity(columns.clone(), 0);
    let mut builder = SourceStreamChunkBuilder::with_capacity(columns, 0);

    struct Transaction {
        id: Box<str>,
        len: usize,
    }
    let mut current_transaction = None;

    #[for_await]
    for batch in data_stream {
        let batch = batch?;
        let batch_len = batch.len();

        let mut last_batch_not_yielded = false;
        if let Some(Transaction { len, id }) = &mut current_transaction {
            // Dirty state. The last batch is not yielded due to uncommitted transaction.
            if *len > MAX_ROWS_FOR_TRANSACTION {
                // Large transaction. Force commit.
                tracing::warn!(
                    id,
                    len,
                    "transaction is larger than {MAX_ROWS_FOR_TRANSACTION} rows, force commit"
                );
                *len = 0; // reset `len` while keeping `id`
                yield builder.take(batch_len);
            } else {
                last_batch_not_yielded = true
            }
        } else {
            // Clean state. Reserve capacity for the builder.
            assert!(builder.is_empty());
            let _ = builder.take(batch_len);
        }

        let process_time_ms = chrono::Utc::now().timestamp_millis();
        for (i, msg) in batch.into_iter().enumerate() {
            if msg.key.is_none() && msg.payload.is_none() {
                tracing::debug!(
                    offset = msg.offset,
                    "got a empty message, could be a heartbeat"
                );
                // Emit an empty invisible row for the heartbeat message.
                parser.emit_empty_row(heartbeat_builder.row_writer().invisible().with_meta(
                    MessageMeta {
                        meta: &msg.meta,
                        split_id: &msg.split_id,
                        offset: &msg.offset,
                    },
                ));
                continue;
            }

            // calculate process_time - event_time lag
            if let SourceMeta::DebeziumCdc(msg_meta) = &msg.meta {
                let lag_ms = process_time_ms - msg_meta.source_ts_ms;
                // report to promethus
                GLOBAL_SOURCE_METRICS
                    .direct_cdc_event_lag_latency
                    .with_label_values(&[&msg_meta.full_table_name])
                    .observe(lag_ms as f64);
            }

            let old_len = builder.len();
            match parser
                .parse_one_with_txn(
                    msg.key,
                    msg.payload,
                    builder.row_writer().with_meta(MessageMeta {
                        meta: &msg.meta,
                        split_id: &msg.split_id,
                        offset: &msg.offset,
                    }),
                )
                .await
            {
                // It's possible that parsing multiple rows in a single message PARTIALLY failed.
                // We still have to maintain the row number in this case.
                res @ (Ok(ParseResult::Rows) | Err(_)) => {
                    // Aggregate the number of new rows into the current transaction.
                    if let Some(Transaction { len, .. }) = &mut current_transaction {
                        let n_new_rows = builder.len() - old_len;
                        *len += n_new_rows;
                    }

                    if let Err(error) = res {
                        // TODO: not using tracing span to provide `split_id` and `offset` due to performance concern,
                        //       see #13105
                        static LOG_SUPPERSSER: LazyLock<LogSuppresser> =
                            LazyLock::new(LogSuppresser::default);
                        if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                            tracing::error!(
                                error = %error.as_report(),
                                split_id = &*msg.split_id,
                                offset = msg.offset,
                                suppressed_count,
                                "failed to parse message, skipping"
                            );
                        }

                        // report to error metrics
                        let context = parser.source_ctx();
                        GLOBAL_ERROR_METRICS.user_source_error.report([
                            error.variant_name().to_string(),
                            context.source_id.to_string(),
                            context.source_name.clone(),
                            context.fragment_id.to_string(),
                        ]);
                    }
                }

                Ok(ParseResult::TransactionControl(txn_ctl)) => match txn_ctl {
                    TransactionControl::Begin { id } => {
                        if let Some(Transaction { id: current_id, .. }) = &current_transaction {
                            tracing::warn!(current_id, id, "already in transaction");
                        }
                        tracing::debug!(id, "begin upstream transaction");
                        current_transaction = Some(Transaction { id, len: 0 });
                    }
                    TransactionControl::Commit { id } => {
                        let current_id = current_transaction.as_ref().map(|t| &t.id);
                        if current_id != Some(&id) {
                            tracing::warn!(?current_id, id, "transaction id mismatch");
                        }
                        tracing::debug!(id, "commit upstream transaction");
                        current_transaction = None;

                        if last_batch_not_yielded {
                            yield builder.take(batch_len - (i + 1));
                            last_batch_not_yielded = false;
                        }
                    }
                },
            }
        }

        // emit heartbeat for each message batch
        // we must emit heartbeat chunk before the data chunk,
        // otherwise the source offset could be backward due to the heartbeat
        if !heartbeat_builder.is_empty() {
            yield heartbeat_builder.take(0);
        }

        // If we are not in a transaction, we should yield the chunk now.
        if current_transaction.is_none() {
            yield builder.take(0);
        }
    }
}

pub trait AccessBuilder {
    async fn generate_accessor(&mut self, payload: Vec<u8>) -> ConnectorResult<AccessImpl<'_, '_>>;
}

#[derive(Debug)]
pub enum EncodingType {
    Key,
    Value,
}

#[derive(Debug)]
pub enum AccessBuilderImpl {
    Avro(AvroAccessBuilder),
    Protobuf(ProtobufAccessBuilder),
    Json(JsonAccessBuilder),
    Bytes(BytesAccessBuilder),
    DebeziumAvro(DebeziumAvroAccessBuilder),
    DebeziumJson(DebeziumJsonAccessBuilder),
    DebeziumMongoJson(DebeziumMongoJsonAccessBuilder),
}

impl AccessBuilderImpl {
    pub async fn new_default(
        config: EncodingProperties,
        kv: EncodingType,
    ) -> ConnectorResult<Self> {
        let accessor = match config {
            EncodingProperties::Avro(_) => {
                let config = AvroParserConfig::new(config).await?;
                AccessBuilderImpl::Avro(AvroAccessBuilder::new(config, kv)?)
            }
            EncodingProperties::Protobuf(_) => {
                let config = ProtobufParserConfig::new(config).await?;
                AccessBuilderImpl::Protobuf(ProtobufAccessBuilder::new(config)?)
            }
            EncodingProperties::Bytes(_) => {
                AccessBuilderImpl::Bytes(BytesAccessBuilder::new(config)?)
            }
            EncodingProperties::Json(config) => {
                AccessBuilderImpl::Json(JsonAccessBuilder::new(config)?)
            }
            _ => unreachable!(),
        };
        Ok(accessor)
    }

    pub async fn generate_accessor(
        &mut self,
        payload: Vec<u8>,
    ) -> ConnectorResult<AccessImpl<'_, '_>> {
        let accessor = match self {
            Self::Avro(builder) => builder.generate_accessor(payload).await?,
            Self::Protobuf(builder) => builder.generate_accessor(payload).await?,
            Self::Json(builder) => builder.generate_accessor(payload).await?,
            Self::Bytes(builder) => builder.generate_accessor(payload).await?,
            Self::DebeziumAvro(builder) => builder.generate_accessor(payload).await?,
            Self::DebeziumJson(builder) => builder.generate_accessor(payload).await?,
            Self::DebeziumMongoJson(builder) => builder.generate_accessor(payload).await?,
        };
        Ok(accessor)
    }
}

/// The entrypoint of parsing. It parses [`SourceMessage`] stream (byte stream) into [`StreamChunk`] stream.
/// Used by [`crate::source::into_chunk_stream`].
#[derive(Debug)]
pub(crate) enum ByteStreamSourceParserImpl {
    Csv(CsvParser),
    Json(JsonParser),
    Debezium(DebeziumParser),
    Plain(PlainParser),
    Upsert(UpsertParser),
    DebeziumMongoJson(DebeziumMongoJsonParser),
    Maxwell(MaxwellParser),
    CanalJson(CanalJsonParser),
}

impl ByteStreamSourceParserImpl {
    /// Converts [`SourceMessage`] stream into [`StreamChunk`] stream.
    pub fn into_stream(self, msg_stream: BoxSourceStream) -> impl ChunkSourceStream + Unpin {
        #[auto_enum(futures03::Stream)]
        let stream = match self {
            Self::Csv(parser) => parser.into_stream(msg_stream),
            Self::Json(parser) => parser.into_stream(msg_stream),
            Self::Debezium(parser) => parser.into_stream(msg_stream),
            Self::DebeziumMongoJson(parser) => parser.into_stream(msg_stream),
            Self::Maxwell(parser) => parser.into_stream(msg_stream),
            Self::CanalJson(parser) => parser.into_stream(msg_stream),
            Self::Plain(parser) => parser.into_stream(msg_stream),
            Self::Upsert(parser) => parser.into_stream(msg_stream),
        };
        Box::pin(stream)
    }
}

impl ByteStreamSourceParserImpl {
    pub async fn create(
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
    ) -> ConnectorResult<Self> {
        let CommonParserConfig { rw_columns } = parser_config.common;
        let protocol = &parser_config.specific.protocol_config;
        let encode = &parser_config.specific.encoding_config;
        match (protocol, encode) {
            (ProtocolProperties::Plain, EncodingProperties::Csv(config)) => {
                CsvParser::new(rw_columns, *config, source_ctx).map(Self::Csv)
            }
            (ProtocolProperties::DebeziumMongo, EncodingProperties::Json(_)) => {
                DebeziumMongoJsonParser::new(rw_columns, source_ctx).map(Self::DebeziumMongoJson)
            }
            (ProtocolProperties::Canal, EncodingProperties::Json(config)) => {
                CanalJsonParser::new(rw_columns, source_ctx, config).map(Self::CanalJson)
            }
            (ProtocolProperties::Native, _) => unreachable!("Native parser should not be created"),
            (ProtocolProperties::Upsert, _) => {
                let parser =
                    UpsertParser::new(parser_config.specific, rw_columns, source_ctx).await?;
                Ok(Self::Upsert(parser))
            }
            (ProtocolProperties::Plain, _) => {
                let parser =
                    PlainParser::new(parser_config.specific, rw_columns, source_ctx).await?;
                Ok(Self::Plain(parser))
            }
            (ProtocolProperties::Debezium(_), _) => {
                let parser =
                    DebeziumParser::new(parser_config.specific, rw_columns, source_ctx).await?;
                Ok(Self::Debezium(parser))
            }
            (ProtocolProperties::Maxwell, _) => {
                let parser =
                    MaxwellParser::new(parser_config.specific, rw_columns, source_ctx).await?;
                Ok(Self::Maxwell(parser))
            }
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ParserConfig {
    pub common: CommonParserConfig,
    pub specific: SpecificParserConfig,
}

impl ParserConfig {
    pub fn get_config(self) -> (Vec<SourceColumnDesc>, SpecificParserConfig) {
        (self.common.rw_columns, self.specific)
    }
}

#[derive(Debug, Clone, Default)]
pub struct CommonParserConfig {
    pub rw_columns: Vec<SourceColumnDesc>,
}

#[derive(Debug, Clone, Default)]
pub struct SpecificParserConfig {
    pub key_encoding_config: Option<EncodingProperties>,
    pub encoding_config: EncodingProperties,
    pub protocol_config: ProtocolProperties,
}

impl SpecificParserConfig {
    // for test only
    pub const DEFAULT_PLAIN_JSON: SpecificParserConfig = SpecificParserConfig {
        key_encoding_config: None,
        encoding_config: EncodingProperties::Json(JsonProperties {
            use_schema_registry: false,
            timestamptz_handling: None,
        }),
        protocol_config: ProtocolProperties::Plain,
    };
}

#[derive(Debug, Default, Clone)]
pub struct AvroProperties {
    pub use_schema_registry: bool,
    pub row_schema_location: String,
    pub client_config: SchemaRegistryAuth,
    pub aws_auth_props: Option<AwsAuthProps>,
    pub topic: String,
    pub enable_upsert: bool,
    pub record_name: Option<String>,
    pub key_record_name: Option<String>,
    pub name_strategy: PbSchemaRegistryNameStrategy,
    pub map_handling: Option<MapHandling>,
}

/// How to convert the map type from the input encoding to RisingWave's datatype.
///
/// XXX: Should this be `avro.map.handling.mode`? Can it be shared between Avro and Protobuf?
#[derive(Debug, Copy, Clone)]
pub enum MapHandling {
    Jsonb,
    // TODO: <https://github.com/risingwavelabs/risingwave/issues/13387>
    // Map
}

impl MapHandling {
    pub const OPTION_KEY: &'static str = "map.handling.mode";

    pub fn from_options(
        options: &std::collections::BTreeMap<String, String>,
    ) -> Result<Option<Self>, InvalidOptionError> {
        let mode = match options.get(Self::OPTION_KEY).map(std::ops::Deref::deref) {
            Some("jsonb") => Self::Jsonb,
            Some(v) => {
                return Err(InvalidOptionError {
                    message: format!("unrecognized {} value {}", Self::OPTION_KEY, v),
                })
            }
            None => return Ok(None),
        };
        Ok(Some(mode))
    }
}

#[derive(Debug, Default, Clone)]
pub struct ProtobufProperties {
    pub message_name: String,
    pub use_schema_registry: bool,
    pub row_schema_location: String,
    pub aws_auth_props: Option<AwsAuthProps>,
    pub client_config: SchemaRegistryAuth,
    pub enable_upsert: bool,
    pub topic: String,
    pub key_message_name: Option<String>,
    pub name_strategy: PbSchemaRegistryNameStrategy,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CsvProperties {
    pub delimiter: u8,
    pub has_header: bool,
}

#[derive(Debug, Default, Clone)]
pub struct JsonProperties {
    pub use_schema_registry: bool,
    pub timestamptz_handling: Option<TimestamptzHandling>,
}

#[derive(Debug, Default, Clone)]
pub struct BytesProperties {
    pub column_name: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub enum EncodingProperties {
    Avro(AvroProperties),
    Protobuf(ProtobufProperties),
    Csv(CsvProperties),
    Json(JsonProperties),
    MongoJson,
    Bytes(BytesProperties),
    Native,
    /// Encoding can't be specified because the source will determines it. Now only used in Iceberg.
    None,
    #[default]
    Unspecified,
}

#[derive(Debug, Default, Clone)]
pub enum ProtocolProperties {
    Debezium(DebeziumProps),
    DebeziumMongo,
    Maxwell,
    Canal,
    Plain,
    Upsert,
    Native,
    /// Protocol can't be specified because the source will determines it. Now only used in Iceberg.
    None,
    #[default]
    Unspecified,
}

impl SpecificParserConfig {
    // The validity of (format, encode) is ensured by `extract_format_encode`
    pub fn new(
        info: &StreamSourceInfo,
        with_properties: &HashMap<String, String>,
    ) -> ConnectorResult<Self> {
        let source_struct = extract_source_struct(info)?;
        let format = source_struct.format;
        let encode = source_struct.encode;
        // this transformation is needed since there may be config for the protocol
        // in the future
        let protocol_config = match format {
            SourceFormat::Native => ProtocolProperties::Native,
            SourceFormat::None => ProtocolProperties::None,
            SourceFormat::Debezium => {
                let debezium_props = DebeziumProps::from(&info.format_encode_options);
                ProtocolProperties::Debezium(debezium_props)
            }
            SourceFormat::DebeziumMongo => ProtocolProperties::DebeziumMongo,
            SourceFormat::Maxwell => ProtocolProperties::Maxwell,
            SourceFormat::Canal => ProtocolProperties::Canal,
            SourceFormat::Upsert => ProtocolProperties::Upsert,
            SourceFormat::Plain => ProtocolProperties::Plain,
            _ => unreachable!(),
        };

        let encoding_config = match (format, encode) {
            (SourceFormat::Plain, SourceEncode::Csv) => EncodingProperties::Csv(CsvProperties {
                delimiter: info.csv_delimiter as u8,
                has_header: info.csv_has_header,
            }),
            (SourceFormat::Plain, SourceEncode::Avro)
            | (SourceFormat::Upsert, SourceEncode::Avro) => {
                let mut config = AvroProperties {
                    record_name: if info.proto_message_name.is_empty() {
                        None
                    } else {
                        Some(info.proto_message_name.clone())
                    },
                    key_record_name: info.key_message_name.clone(),
                    name_strategy: PbSchemaRegistryNameStrategy::try_from(info.name_strategy)
                        .unwrap(),
                    use_schema_registry: info.use_schema_registry,
                    row_schema_location: info.row_schema_location.clone(),
                    map_handling: MapHandling::from_options(&info.format_encode_options)?,
                    ..Default::default()
                };
                if format == SourceFormat::Upsert {
                    config.enable_upsert = true;
                }
                if info.use_schema_registry {
                    config.topic.clone_from(get_kafka_topic(with_properties)?);
                    config.client_config = SchemaRegistryAuth::from(&info.format_encode_options);
                } else {
                    config.aws_auth_props = Some(
                        serde_json::from_value::<AwsAuthProps>(
                            serde_json::to_value(info.format_encode_options.clone()).unwrap(),
                        )
                        .map_err(|e| anyhow::anyhow!(e))?,
                    );
                }
                EncodingProperties::Avro(config)
            }
            (SourceFormat::Plain, SourceEncode::Protobuf)
            | (SourceFormat::Upsert, SourceEncode::Protobuf) => {
                if info.row_schema_location.is_empty() {
                    bail!("protobuf file location not provided");
                }
                let mut config = ProtobufProperties {
                    message_name: info.proto_message_name.clone(),
                    use_schema_registry: info.use_schema_registry,
                    row_schema_location: info.row_schema_location.clone(),
                    name_strategy: PbSchemaRegistryNameStrategy::try_from(info.name_strategy)
                        .unwrap(),
                    key_message_name: info.key_message_name.clone(),
                    ..Default::default()
                };
                if format == SourceFormat::Upsert {
                    config.enable_upsert = true;
                }
                if info.use_schema_registry {
                    config.topic.clone_from(get_kafka_topic(with_properties)?);
                    config.client_config = SchemaRegistryAuth::from(&info.format_encode_options);
                } else {
                    config.aws_auth_props = Some(
                        serde_json::from_value::<AwsAuthProps>(
                            serde_json::to_value(info.format_encode_options.clone()).unwrap(),
                        )
                        .map_err(|e| anyhow::anyhow!(e))?,
                    );
                }
                EncodingProperties::Protobuf(config)
            }
            (SourceFormat::Debezium, SourceEncode::Avro) => {
                EncodingProperties::Avro(AvroProperties {
                    record_name: if info.proto_message_name.is_empty() {
                        None
                    } else {
                        Some(info.proto_message_name.clone())
                    },
                    name_strategy: PbSchemaRegistryNameStrategy::try_from(info.name_strategy)
                        .unwrap(),
                    key_record_name: info.key_message_name.clone(),
                    row_schema_location: info.row_schema_location.clone(),
                    topic: get_kafka_topic(with_properties).unwrap().clone(),
                    client_config: SchemaRegistryAuth::from(&info.format_encode_options),
                    ..Default::default()
                })
            }
            (
                SourceFormat::Plain
                | SourceFormat::Debezium
                | SourceFormat::Maxwell
                | SourceFormat::Canal
                | SourceFormat::Upsert,
                SourceEncode::Json,
            ) => EncodingProperties::Json(JsonProperties {
                use_schema_registry: info.use_schema_registry,
                timestamptz_handling: TimestamptzHandling::from_options(
                    &info.format_encode_options,
                )?,
            }),
            (SourceFormat::DebeziumMongo, SourceEncode::Json) => {
                EncodingProperties::Json(JsonProperties {
                    use_schema_registry: false,
                    timestamptz_handling: None,
                })
            }
            (SourceFormat::Plain, SourceEncode::Bytes) => {
                EncodingProperties::Bytes(BytesProperties { column_name: None })
            }
            (SourceFormat::Native, SourceEncode::Native) => EncodingProperties::Native,
            (SourceFormat::None, SourceEncode::None) => EncodingProperties::None,
            (format, encode) => {
                bail!("Unsupported format {:?} encode {:?}", format, encode);
            }
        };
        Ok(Self {
            key_encoding_config: None,
            encoding_config,
            protocol_config,
        })
    }
}
