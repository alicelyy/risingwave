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

use std::marker::PhantomData;

use async_trait::async_trait;

use crate::sink::log_store::{LogStoreReadItem, TruncateOffset};
use crate::sink::{
    DummySinkCommitCoordinator, LogSinker, Result, Sink, SinkError, SinkLogReader, SinkParam,
    SinkWriterParam,
};

pub const BLACKHOLE_SINK: &str = "blackhole";
pub const TABLE_SINK: &str = "table";

pub trait TrivialSinkName: Send + 'static {
    const SINK_NAME: &'static str;
}

#[derive(Debug)]
pub struct BlackHoleSinkName;

impl TrivialSinkName for BlackHoleSinkName {
    const SINK_NAME: &'static str = BLACKHOLE_SINK;
}

pub type BlackHoleSink = TrivialSink<BlackHoleSinkName>;

#[derive(Debug)]
pub struct TableSinkName;

impl TrivialSinkName for TableSinkName {
    const SINK_NAME: &'static str = TABLE_SINK;
}

pub type TableSink = TrivialSink<TableSinkName>;

#[derive(Debug)]
pub struct TrivialSink<T: TrivialSinkName>(PhantomData<T>);

impl<T: TrivialSinkName> TryFrom<SinkParam> for TrivialSink<T> {
    type Error = SinkError;

    fn try_from(_value: SinkParam) -> std::result::Result<Self, Self::Error> {
        Ok(Self(PhantomData))
    }
}

impl<T: TrivialSinkName> Sink for TrivialSink<T> {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = Self;

    const SINK_NAME: &'static str = T::SINK_NAME;

    async fn new_log_sinker(&self, _writer_env: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(Self(PhantomData))
    }

    async fn validate(&self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl<T: TrivialSinkName> LogSinker for TrivialSink<T> {
    async fn consume_log_and_sink(self, log_reader: &mut impl SinkLogReader) -> Result<()> {
        loop {
            let (epoch, item) = log_reader.next_item().await?;
            match item {
                LogStoreReadItem::StreamChunk { chunk_id, .. } => {
                    log_reader.truncate(TruncateOffset::Chunk { epoch, chunk_id })?;
                }
                LogStoreReadItem::Barrier { .. } => {
                    log_reader.truncate(TruncateOffset::Barrier { epoch })?;
                }
                LogStoreReadItem::UpdateVnodeBitmap(_) => {}
            }
        }
    }
}
