// Copyright 2023 RisingWave Labs
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

use std::sync::Arc;

use futures_async_stream::try_stream;
use risingwave_common::array::stream_record::Record;
use risingwave_common::row::RowExt;

use crate::executor::error::StreamExecutorError;
use crate::executor::{ExecutorInfo, Message, MessageStream};

/// Streams wrapped by `update_check` will check whether the two rows of updates are next to each
/// other.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn update_check(info: Arc<ExecutorInfo>, input: impl MessageStream) {
    #[for_await]
    for message in input {
        let message = message?;

        if let Message::Chunk(chunk) = &message {
            for record in chunk.records() {
                // `chunk.records()` will check U-/U+ pairing
                if let Record::Update { old_row, new_row } = record {
                    debug_assert!(
                        old_row.project(&info.pk_indices) == new_row.project(&info.pk_indices),
                        "U- and U+ should have same stream key, U- row: {:?}, U+ row: {:?}, stream key: {:?}, executor id: {}",
                        old_row, new_row, info.pk_indices, info.identity
                    )
                }
            }
        }

        yield message;
    }
}

#[cfg(test)]
mod tests {
    use futures::{pin_mut, StreamExt};
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::StreamChunk;

    use super::*;
    use crate::executor::test_utils::MockSource;
    use crate::executor::Executor;

    #[should_panic]
    #[tokio::test]
    async fn test_not_next_to_each_other() {
        let (mut tx, source) = MockSource::channel(Default::default(), vec![]);
        tx.push_chunk(StreamChunk::from_pretty(
            "     I
            U-  114
            U-  514
            U+ 1919
            U+  810",
        ));

        let checked = update_check(source.info().into(), source.boxed().execute());
        pin_mut!(checked);

        checked.next().await.unwrap().unwrap(); // should panic
    }

    #[should_panic]
    #[tokio::test]
    async fn test_first_one_update_insert() {
        let (mut tx, source) = MockSource::channel(Default::default(), vec![]);
        tx.push_chunk(StreamChunk::from_pretty(
            "     I
            U+  114",
        ));

        let checked = update_check(source.info().into(), source.boxed().execute());
        pin_mut!(checked);

        checked.next().await.unwrap().unwrap(); // should panic
    }

    #[should_panic]
    #[tokio::test]
    async fn test_last_one_update_delete() {
        let (mut tx, source) = MockSource::channel(Default::default(), vec![]);
        tx.push_chunk(StreamChunk::from_pretty(
            "        I
            U-     114
            U+     514
            U- 1919810",
        ));

        let checked = update_check(source.info().into(), source.boxed().execute());
        pin_mut!(checked);

        checked.next().await.unwrap().unwrap(); // should panic
    }

    #[tokio::test]
    async fn test_empty_chunk() {
        let (mut tx, source) = MockSource::channel(Default::default(), vec![]);
        tx.push_chunk(StreamChunk::default());

        let checked = update_check(source.info().into(), source.boxed().execute());
        pin_mut!(checked);

        checked.next().await.unwrap().unwrap();
    }
}
