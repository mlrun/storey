# Copyright 2026 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import copy
from collections import defaultdict

from ..dtypes import StreamCompletion, _termination_obj
from ..flow import Flow


class Collector(Flow):
    """
    Collects streaming chunks and emits a single event once all chunks for a stream are received.
    Acts as a no-op passthrough for non-streaming events.

    This step accumulates chunks from upstream streaming steps until a StreamCompletion
    sentinel is received. Once all expected completions are received, it emits a single
    event containing all collected chunk bodies as a list.

    :param expected_completions: The number of StreamCompletion signals expected for a given
        event stream. Useful when there are upstream splits that duplicate streaming events.
        Defaults to 1.
    :type expected_completions: int
    :param name: Name of this step, as it should appear in logs. Defaults to class name (Collector).
    :type name: string
    """

    def __init__(self, expected_completions: int = 1, **kwargs):
        super().__init__(**kwargs)
        if expected_completions < 1:
            raise ValueError("expected_completions must be at least 1")
        self._expected_completions = expected_completions
        # Map from event id -> {"chunks": [], "completions": 0, "first_event": Event}
        self._collected_streams: dict[str, dict] = defaultdict(
            lambda: {"chunks": [], "completions": 0, "first_event": None}
        )

    async def _do(self, event):
        if event is _termination_obj:
            return await self._do_downstream(_termination_obj)

        # Handle StreamCompletion sentinel
        if isinstance(event, StreamCompletion):
            stream_id = event.original_event.id
            stream_data = self._collected_streams[stream_id]

            stream_data["completions"] += 1

            if stream_data["completions"] >= self._expected_completions:
                # Stream is complete - emit collected result
                # Use first_event if we have chunks, otherwise use original_event from completion (empty stream)
                base_event = stream_data["first_event"] or event.original_event
                collected_body = [chunk.body for chunk in stream_data["chunks"]]
                if len(collected_body) == 1:
                    collected_body = collected_body[0]

                # Copy the original event to preserve all attributes (important for offset management)
                collected_event = copy.copy(base_event)
                collected_event.body = collected_body
                # Clear streaming attributes
                if hasattr(collected_event, "streaming_step"):
                    del collected_event.streaming_step
                if hasattr(collected_event, "chunk_id"):
                    del collected_event.chunk_id
                await self._do_downstream(collected_event)

                # Clean up
                del self._collected_streams[stream_id]
            return None

        # Check if this is a streaming chunk (has streaming_step attribute)
        if hasattr(event, "streaming_step"):
            stream_id = event.id
            stream_data = self._collected_streams[stream_id]
            if stream_data["first_event"] is None:
                stream_data["first_event"] = event
            stream_data["chunks"].append(event)
            return None
        else:
            # Non-streaming event - pass through directly
            return await self._do_downstream(event)
