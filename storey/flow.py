# Copyright 2020 Iguazio
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
import asyncio
import copy
import datetime
import enum
import inspect
import multiprocessing
import os
import pickle
import time
import traceback
import uuid
from asyncio import Task
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Collection,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Set,
    Union,
)

import aiohttp

from .dtypes import (
    Event,
    FlowError,
    StreamChunk,
    StreamCompletion,
    StreamingError,
    V3ioError,
    _termination_obj,
    known_driver_schemes,
)
from .queue import AsyncQueue
from .table import Table
from .utils import _split_path, get_in, stringify_key, update_in


def _is_generator(obj) -> bool:
    """Check if an object is a sync or async generator."""
    return inspect.isgenerator(obj) or inspect.isasyncgen(obj)


class Flow:
    _legal_first_step = False

    def __init__(
        self,
        recovery_step=None,
        termination_result_fn=lambda x, y: x if x is not None else y,
        context=None,
        max_iterations: Optional[int] = None,
        **kwargs,
    ):
        self._outlets = []
        self._inlets = []

        self._recovery_step = recovery_step
        if recovery_step:
            if isinstance(recovery_step, dict):
                for step in recovery_step.values():
                    step._inlets.append(self)
            else:
                recovery_step._inlets.append(self)

        self._termination_result_fn = termination_result_fn
        self.context = context
        self.verbose = context and getattr(context, "verbose", False)
        self.logger = getattr(self.context, "logger", None) if self.context else None

        self._kwargs = kwargs
        self._full_event = kwargs.get("full_event")
        self._input_path = kwargs.get("input_path")
        self._result_path = kwargs.get("result_path")
        self._max_iterations = max_iterations
        self._runnable = False
        name = kwargs.get("name", None)
        if name:
            self.name = name
        else:
            self.name = type(self).__name__

        self._closeables = []
        self._selected_outlets: Optional[list[str]] = None
        self._create_name_to_outlet = True

    def _init(self):
        self._closeables = []
        self._termination_received = 0
        self._termination_result = None
        self._name_to_outlet = {}
        if self._method_is_overridden("select_outlets", Flow) and self._create_name_to_outlet:
            self._init_name_to_outlet()

    def _init_name_to_outlet(self):
        for outlet in self._outlets:
            if outlet.name in self._name_to_outlet:
                raise ValueError(f"Ambiguous outlet name '{outlet.name}' in step '{self.name}'")
            self._name_to_outlet[outlet.name] = outlet

    def _method_is_overridden(self, method_name: str, parent_cls):
        """Return True if the subclass overrides the given method."""
        return getattr(self.__class__, method_name) is not getattr(parent_cls, method_name)

    def to_dict(self, fields=None, exclude=None):
        """convert the step object to a python dictionary"""
        fields = fields or getattr(self, "_dict_fields", None)
        if not fields:
            fields = list(inspect.signature(self.__init__).parameters.keys())
        if exclude:
            fields = [field for field in fields if field not in exclude]

        meta_keys = [
            "context",
            "name",
            "input_path",
            "result_path",
            "full_event",
            "kwargs",
        ]
        args = {
            key: getattr(self, key) for key in fields if getattr(self, key, None) is not None and key not in meta_keys
        }
        # add storey kwargs or extra kwargs
        if "kwargs" in fields and (hasattr(self, "kwargs") or hasattr(self, "_kwargs")):
            kwargs = getattr(self, "kwargs", {}) or getattr(self, "_kwargs", {})
            for key, value in kwargs.items():
                if key not in meta_keys:
                    args[key] = value

        mod_name = self.__class__.__module__
        class_path = self.__class__.__qualname__
        if mod_name != "__main__":
            class_path = f"{mod_name}.{class_path}"
        struct = {
            "class_name": class_path,
            "name": self.name or self.__class__.__name__,
            "class_args": args,
        }

        for parameter_name in [
            ("kind", "_STEP_KIND"),
            "input_path",
            "result_path",
            "full_event",
        ]:
            if isinstance(parameter_name, tuple):
                parameter_name, field_name = parameter_name
            else:
                field_name = f"_{parameter_name}"
            if hasattr(self, field_name):
                field_value = getattr(self, field_name)
                if field_value is not None:
                    struct[parameter_name] = field_value
        return struct

    def _to_code(self, taken: Set[str]):
        class_name = type(self).__name__
        base_var_name = ""
        for c in class_name:
            if c.isupper() and base_var_name:
                base_var_name += "_"
            base_var_name += c.lower()
        i = 0
        while True:
            var_name = f"{base_var_name}{i}"
            if var_name not in taken:
                taken.add(var_name)
                break
            i += 1
        taken.add(var_name)
        param_list = []
        for key, value in self._kwargs.items():
            if isinstance(value, str):
                value = f"'{value}'"
            param_list.append(f"{key}={value}")
        param_str = ", ".join(param_list)
        step = f"{var_name} = {class_name}({param_str})"
        steps = [step]
        tos = []
        for outlet in self._outlets:
            outlet_var_name, outlet_steps, outlet_tos = outlet._to_code(taken)
            steps.extend(outlet_steps)
            tos.append(f"{var_name}.to({outlet_var_name})")
            tos.extend(outlet_tos)
        return var_name, steps, tos

    def to_code(self):
        _, steps, tos = self._to_code(set())
        result = "\n".join(steps)
        result += "\n\n"
        result += "\n".join(tos)
        result += "\n"
        return result

    def to(self, outlet):
        if outlet._legal_first_step:
            raise ValueError(f"{outlet.name} can only appear as the first step of a flow")
        self._outlets.append(outlet)
        outlet._inlets.append(self)
        return outlet

    def set_recovery_step(self, outlet):
        self._recovery_step = outlet
        return self

    def _get_recovery_step(self, exception):
        if isinstance(self._recovery_step, dict):
            return self._recovery_step.get(type(exception), None)
        else:
            return self._recovery_step

    def run(self, visited=None):
        if not self._legal_first_step and not self._runnable:
            raise ValueError("Flow must start with a source")

        # Initialize visited set once at the top (only for the root call)
        if visited is None:
            visited = set()

        # Detect cycles: if we've already visited this step, don't run it again
        if self in visited:
            return []
        self._init()
        visited.add(self)

        outlets = []
        outlets.extend(self._outlets)
        outlets.extend(self._get_recovery_steps())
        for outlet in outlets:
            outlet._runnable = True
            outlet_closeables = outlet.run(visited)
            self._closeables.extend(outlet_closeables)
        return self._closeables

    def _get_recovery_steps(self):
        if self._recovery_step:
            if isinstance(self._recovery_step, dict):
                return list(self._recovery_step.values())
            else:
                return [self._recovery_step]
        return []

    async def run_async(self):
        raise NotImplementedError

    async def _do(self, event):
        raise NotImplementedError

    async def _do_and_recover(self, event):
        try:
            self.check_and_update_iteration_number(event)
            return await self._do(event)
        except BaseException as ex:
            if getattr(ex, "_raised_by_storey_step", None) is not None:
                raise ex
            ex._raised_by_storey_step = self
            recovery_step = self._get_recovery_step(ex)
            if recovery_step is None:
                if self.context and hasattr(self.context, "push_error"):
                    message = traceback.format_exc()
                    if event._awaitable_result:
                        none_or_coroutine = event._awaitable_result._set_error(ex)
                        if none_or_coroutine:
                            await none_or_coroutine
                    if self.logger:
                        self.logger.error(f"Pushing error to error stream: {ex}\n{message}")
                    self.context.push_error(event, f"{ex}\n{message}", source=self.name)
                    return
                else:
                    raise ex
            event.origin_state = self.name
            event.error = ex
            return await recovery_step._do(event)

    @staticmethod
    def _event_string(event):
        result = "Event("
        if event.id:
            result += f"id={event.id}, "
        if getattr(event, "key", None):
            result += f"key={event.key}, "
        if getattr(event, "time", None):
            result += f"processing_time={event.processing_time}, "
        if getattr(event, "path", None):
            result += f"path={event.path}, "
        result += f"body={event.body})"
        return result

    def _should_terminate(self):
        return self._termination_received == len(self._inlets)

    def _deepcopy_event_for_outlet(self, event, target_obj, is_stream_completion: bool):
        """Deepcopy event while handling unpicklable attributes on target_obj.

        :param event: The event to deepcopy.
        :param target_obj: The object containing _awaitable_result and _original_events
                           (either the event itself or event.original_event for StreamCompletion).
        :param is_stream_completion: If True, copy target is event_copy.original_event,
                                     otherwise it's event_copy itself.

        :returns: The deepcopied event with unpicklable attributes restored.
        """
        awaitable_result = target_obj._awaitable_result
        target_obj._awaitable_result = None
        original_events = getattr(target_obj, "_original_events", None)
        target_obj._original_events = None

        event_copy = copy.deepcopy(event)
        copy_target = event_copy.original_event if is_stream_completion else event_copy
        copy_target._awaitable_result = awaitable_result
        copy_target._original_events = original_events

        target_obj._awaitable_result = awaitable_result
        target_obj._original_events = original_events
        return event_copy

    async def _do_downstream(self, event, outlets=None, select_outlets: bool = True):
        # Termination object and StreamCompletion should propagate to all outlets
        if not outlets and event is not _termination_obj and not isinstance(event, StreamCompletion) and select_outlets:
            outlet_names = self.select_outlets(event.body)
            outlets = self._check_outlets_by_names(outlet_names) if outlet_names else None
        outlets = self._outlets if outlets is None else outlets

        if not outlets:
            return
        if event is _termination_obj:
            if self.logger:
                outlet_names = ", ".join([outlet.name for outlet in outlets])
                self.logger.info(f"Forwarding termination signal from step '{self.name}' to steps: {outlet_names}")
            # Only propagate the termination object once we received one per inlet
            outlets[0]._termination_received += 1
            if outlets[0]._should_terminate():
                self._termination_result = await outlets[0]._do(_termination_obj)
            for outlet in outlets[1:] + self._get_recovery_steps():
                outlet._termination_received += 1
                if outlet._should_terminate():
                    self._termination_result = self._termination_result_fn(
                        self._termination_result, await outlet._do(_termination_obj)
                    )
            return self._termination_result
        # If there is more than one outlet, allow concurrent execution.
        tasks = []
        if len(outlets) > 1:
            # Deep copy event and create a task per outlet (except the first, which is awaited directly below)
            is_stream_completion = isinstance(event, StreamCompletion)
            target_obj = event.original_event if is_stream_completion else event
            for i in range(1, len(outlets)):
                event_copy = self._deepcopy_event_for_outlet(event, target_obj, is_stream_completion)
                tasks.append(asyncio.get_running_loop().create_task(outlets[i]._do_and_recover(event_copy)))
        if self.verbose and self.logger:
            step_name = self.name
            event_string = self._event_string(event)
            self.logger.debug(f"{step_name} -> {outlets[0].name} | {event_string}")
        await outlets[0]._do_and_recover(event)  # Optimization - avoids creating a task for the first outlet.
        for i, task in enumerate(tasks, start=1):
            if self.verbose and self.logger:
                self.logger.debug(f"{step_name} -> {outlets[i].name} | {event_string}")
            await task

    def _get_event_or_body(self, event):
        if self._full_event:
            return event
        elif self._input_path:
            if not hasattr(event.body, "__getitem__"):
                raise TypeError("input_path parameter supports only dict-like event bodies")
            return get_in(event.body, self._input_path)
        else:
            return event.body

    def _user_fn_output_to_event(self, event, fn_result):
        if self._full_event:
            return fn_result
        else:
            mapped_event = copy.copy(event)
            if self._result_path:
                if not hasattr(event.body, "__getitem__"):
                    raise TypeError("result_path parameter supports only dict-like event bodies")
                update_in(mapped_event.body, self._result_path, fn_result)
            else:
                mapped_event.body = fn_result
            return mapped_event

    def _check_step_in_flow(self, type_to_check, visited=None):
        # initialize the visited set once at the top
        if visited is None:
            visited = set()

        # detect cycles
        if self in visited:
            return False
        visited.add(self)

        # check this node
        if isinstance(self, type_to_check):
            return True

        # check outlets
        for outlet in self._outlets:
            if outlet._check_step_in_flow(type_to_check, visited):
                return True

        # check recovery step
        if isinstance(self._recovery_step, Flow):
            if self._recovery_step._check_step_in_flow(type_to_check, visited):
                return True

        elif isinstance(self._recovery_step, dict):
            for step in self._recovery_step.values():
                if step._check_step_in_flow(type_to_check, visited):
                    return True
        return False

    def check_and_update_iteration_number(self, event) -> Optional[Callable]:
        # Skip iteration counting in case of StreamCompletion
        if isinstance(event, StreamCompletion):
            return
        if hasattr(event, "_cyclic_counter") and self._max_iterations is not None:
            counter = self.get_iteration_counter(event)
            if counter >= self._max_iterations:
                raise RuntimeError(f"Max iterations exceeded in step '{self.name}' for event {event.id}")
            event._cyclic_counter[self.name] = counter + 1
        else:
            event._cyclic_counter = {self.name: 1}

    def get_iteration_counter(self, event):
        return getattr(event, "_cyclic_counter", {}).get(self.name, 0)

    def select_outlets(self, event) -> Optional[Collection[str]]:
        """
        Override this method to route events based on a custom logic. The default implementation will route all
        events to all outlets.
        """
        return None

    def _check_outlets_by_names(self, outlet_names: Collection[str]) -> list["Flow"]:
        outlets = []

        # Check for duplicates
        if len(set(outlet_names)) != len(outlet_names):
            raise ValueError(
                f"Invalid outlet selection for '{self.name}': duplicate outlet names were provided "
                f"({', '.join(outlet_names)})."
            )

        # Validate each outlet name
        for outlet_name in outlet_names:
            if outlet_name not in self._name_to_outlet:
                raise ValueError(
                    f"Invalid outlet '{outlet_name}' for '{self.name}'. "
                    f"Allowed outlets are: {', '.join(self._name_to_outlet)}."
                )
            outlets.append(self._name_to_outlet[outlet_name])

        return outlets


class WithUUID:
    def __init__(self):
        self._current_uuid_base = None
        self._current_uuid_count = 0

    def _get_uuid(self):
        if not self._current_uuid_base or self._current_uuid_count == 1024:
            self._current_uuid_base = uuid.uuid4().hex
            self._current_uuid_count = 0
        result = f"{self._current_uuid_base}-{self._current_uuid_count:04}"
        self._current_uuid_count += 1
        return result


class Choice(Flow):
    """
    Redirects each input element into any number of predetermined downstream steps. Override select_outlets()
    to route events to any number of downstream steps.
    """

    def _init(self):
        super()._init()
        # TODO: hacky way of supporting mlrun preview, which replaces targets with a DFTarget
        self._passthrough_for_preview = list(self._name_to_outlet) == ["dataframe"] if self._name_to_outlet else False

    async def _do(self, event):
        if event is _termination_obj or isinstance(event, StreamCompletion):
            return await self._do_downstream(event, select_outlets=False)

        event_body = event if self._full_event else event.body
        outlet_names = self.select_outlets(event_body)
        outlets = []
        if self._passthrough_for_preview:
            outlet = self._name_to_outlet["dataframe"]
            outlets.append(outlet)
        else:
            outlets = self._check_outlets_by_names(outlet_names)
        return await self._do_downstream(event, outlets=outlets, select_outlets=False)


class Recover(Flow):
    def __init__(self, exception_to_downstream, **kwargs):
        Flow.__init__(self, **kwargs)

        self._exception_to_downstream = exception_to_downstream

    async def _do(self, event):
        if not self._outlets or event is _termination_obj:
            return await super()._do_downstream(event)
        else:
            try:
                await super()._do_downstream(event)
            except BaseException as ex:
                typ = type(ex)
                if typ in self._exception_to_downstream:
                    await self._exception_to_downstream[typ]._do(event)
                else:
                    raise ex


class _StreamingStepMixin:
    """Mixin providing streaming support for steps that can emit generators.

    This mixin provides utility methods for detecting generators and emitting
    streaming chunks downstream. It should be used with Flow subclasses that
    want to support user-provided generator functions.
    """

    def _validate_not_already_streaming(self, event):
        """Ensure we're not streaming on top of an already streaming event.

        Raises StreamingError if the event already has a streaming_step attribute,
        indicating it came from an upstream streaming step without a Collector in between.
        """
        streaming_step = getattr(event, "streaming_step", None)
        if streaming_step:
            raise StreamingError(
                f"Streaming on top of streaming is not allowed. "
                f"Step '{self.name}' received a streaming event from '{streaming_step}'."
            )

    async def _emit_streaming_chunks(self, event, generator: Union[Generator, AsyncGenerator]) -> None:
        """Emit streaming chunks from a generator, then send StreamCompletion.

        :param event: The event that will be used to create chunk events.
        :param generator: A sync or async generator yielding chunk bodies.
        """
        self._validate_not_already_streaming(event)

        async def gen_to_async_gen(sync_gen):
            for item in sync_gen:
                yield item

        # If needed, wrap sync generator as async to unify iteration
        async_gen = gen_to_async_gen(generator) if inspect.isgenerator(generator) else generator

        chunk_id = 0
        async for chunk_body in async_gen:
            chunk_event = self._user_fn_output_to_event(event, chunk_body)
            chunk_event.streaming_step = self.name
            chunk_event.chunk_id = chunk_id
            await self._do_downstream(chunk_event)
            chunk_id += 1

        # Send completion signal
        await self._do_downstream(StreamCompletion(self.name, event))


class _UnaryFunctionFlow(Flow):
    def __init__(
        self, fn, long_running=None, pass_context=None, fn_select_outlets: Optional[Callable] = None, **kwargs
    ):
        super().__init__(**kwargs)
        if not callable(fn):
            raise TypeError(f"Expected a callable, got {type(fn)}")
        if asyncio.iscoroutinefunction(fn) and long_running:
            raise ValueError("long_running=True cannot be used in conjunction with a coroutine")
        self._long_running = long_running
        self._fn = fn
        self._pass_context = pass_context
        if fn_select_outlets and not callable(fn_select_outlets):
            raise TypeError(f"Expected fn_select_outlets to be callable, got {type(fn)}")
        self._outlets_selector = fn_select_outlets
        self._create_name_to_outlet = self._outlets_selector or self._method_is_overridden(
            "select_outlets", _UnaryFunctionFlow
        )

    async def _call(self, element, fn, pass_kwargs=True):
        if self._long_running:
            res = await asyncio.get_running_loop().run_in_executor(None, fn, element)
        else:
            kwargs = {}
            if self._pass_context:
                kwargs = {"context": self.context}
            if pass_kwargs:
                res = fn(element, **kwargs)
            else:
                res = fn(element)
        if asyncio.iscoroutinefunction(fn):
            res = await res
        return res

    async def _do_internal(self, element, fn_result):
        raise NotImplementedError()

    async def _do(self, event):
        # Forward termination object and StreamCompletion without processing
        if event is _termination_obj or isinstance(event, StreamCompletion):
            return await self._do_downstream(event)
        element = self._get_event_or_body(event)
        fn_result = await self._call(element, self._fn)
        await self._do_internal(event, fn_result)

    def select_outlets(self, event_body) -> Optional[Collection[str]]:
        if self._outlets_selector:
            return self._outlets_selector(event_body)
        else:
            return super().select_outlets(event_body)


class DropColumns(Flow):
    def __init__(self, columns, **kwargs):
        super().__init__(**kwargs)
        if not isinstance(columns, list):
            columns = [columns]
        self._columns = columns

    async def _do(self, event):
        if event is not _termination_obj:
            new_body = copy.copy(event.body)
            for column in self._columns:
                new_body.pop(column, None)
            event.body = new_body
        return await self._do_downstream(event)


class Map(_UnaryFunctionFlow, _StreamingStepMixin):
    """Maps, or transforms, incoming events using a user-provided function.

    :param fn: Function to apply to each event. Can also be a generator function
        (sync or async) to stream multiple chunks.
    :type fn: Function (Event=>Event) or generator function
    :param long_running: Whether fn is a long-running function. Long-running functions are run in an executor to
        avoid blocking other concurrent processing. Default is False. Cannot be used with async or generator functions.
    :type long_running: boolean
    :param name: Name of this step, as it should appear in logs. Defaults to class name (Map).
    :type name: string
    :param full_event: Whether user functions should receive and return Event objects (when True),
        or only the payload (when False). Defaults to False.
    :type full_event: boolean
    """

    async def _do_internal(self, event, fn_result):
        # Check if the result is a generator (streaming response)
        if _is_generator(fn_result):
            await self._emit_streaming_chunks(event, fn_result)
        else:
            mapped_event = self._user_fn_output_to_event(event, fn_result)
            await self._do_downstream(mapped_event)


class Filter(_UnaryFunctionFlow):
    """Filters events based on a user-provided function.

    :param fn: Function to decide whether to keep each event.
    :type fn: Function (Event=>boolean)
    :param long_running: Whether fn is a long-running function. Long-running functions are run in an executor to
        avoid blocking other concurrent processing. Default is False.
    :type long_running: boolean
    :param name: Name of this step, as it should appear in logs. Defaults to class name (Filter).
    :type name: string
    :param full_event: Whether user functions should receive and return Event objects (when True), or only the
        payload (when False). Defaults to False.
    :type full_event: boolean
    """

    async def _do_internal(self, event, keep):
        if keep:
            await self._do_downstream(event)


class FlatMap(_UnaryFunctionFlow):
    """Maps, or transforms, each incoming event into any number of events.

    :param fn: Function to transform each event to a list of events.
    :type fn: Function (Event=>list of Event)
    :param long_running: Whether fn is a long-running function. Long-running functions are run in an executor
        to avoid blocking other concurrent processing. Default is False.
    :type long_running: boolean
    :param name: Name of this step, as it should appear in logs. Defaults to class name (FlatMap).
    :type name: string
    :param full_event: Whether user functions should receive and return Event objects (when True), or only
        the payload (when False). Defaults to False.
    :type full_event: boolean
    """

    async def _do_internal(self, event, fn_result):
        for fn_result_element in fn_result:
            mapped_event = self._user_fn_output_to_event(event, fn_result_element)
            await self._do_downstream(mapped_event)


class Extend(_UnaryFunctionFlow):
    """Adds fields to each incoming event.

    :param fn: Function to transform each event to a dictionary. The fields in the returned dictionary are then added
        to the original event.
    :type fn: Function (Event=>Dict)
    :param long_running: Whether fn is a long-running function. Long-running functions are run in an executor to avoid
        blocking other concurrent processing. Default is False.
    :type long_running: boolean
    :param name: Name of this step, as it should appear in logs. Defaults to class name (Extend).
    :type name: string
    :param full_event: Whether user functions should receive and return Event objects (when True), or only the
        payload (when False). Defaults to False.
    :type full_event: boolean
    """

    async def _do_internal(self, event, fn_result):
        for key, value in fn_result.items():
            event.body[key] = value
        await self._do_downstream(event)


class _FunctionWithStateFlow(Flow):
    def __init__(self, initial_state, fn, group_by_key=False, **kwargs):
        super().__init__(**kwargs)
        if not callable(fn):
            raise TypeError(f"Expected a callable, got {type(fn)}")
        self._is_async = asyncio.iscoroutinefunction(fn)
        self._state = initial_state
        if isinstance(self._state, str):
            should_get_from_context = False
            for known_scheme in known_driver_schemes:
                if self._state.startswith(f"{known_scheme}://"):
                    should_get_from_context = True
                    break
            if should_get_from_context:
                if not self.context:
                    raise TypeError("Table can not be string if no context was provided to the step")
                self._state = self.context.get_table(self._state)
        self._fn = fn
        self._group_by_key = group_by_key

    def _init(self):
        super()._init()
        if hasattr(self._state, "close"):
            self._closeables = [self._state]

    async def _call(self, event):
        element = self._get_event_or_body(event)
        if self._group_by_key:
            safe_key = stringify_key(event.key)
            if isinstance(self._state, Table):
                key_data = await self._state._get_or_load_static_attributes_by_key(safe_key)
            else:
                key_data = self._state[event.key]
            res, new_state = self._fn(element, key_data)
            async with self._state._get_lock(safe_key):
                self._state._update_static_attrs(safe_key, new_state)
                self._state._pending_events.append(event)
            self._state._init_flush_task()
        else:
            res, self._state = self._fn(element, self._state)
        if self._is_async:
            res = await res
        return res

    async def _do_internal(self, element, fn_result):
        raise NotImplementedError()

    async def _do(self, event):
        if event is _termination_obj:
            return await self._do_downstream(_termination_obj)
        else:
            fn_result = await self._call(event)
            await self._do_internal(event, fn_result)


class MapWithState(_FunctionWithStateFlow):
    """Maps, or transforms, incoming events using a stateful user-provided function, and an initial state,
        which may be a database table.

    :param initial_state: Initial state for the computation. If group_by_key is True, this must be a dictionary or
        a Table object.
    :type initial_state: dictionary or Table if group_by_key is True. Any object otherwise.
    :param fn: A function to run on each event and the current state. Must yield an event and an updated state.
    :type fn: Function ((Event, state)=>(Event, state))
    :param group_by_key: Whether the state is computed by key. Optional. Default to False.
    :type group_by_key: boolean
    :param full_event: Whether fn will receive and return an Event object or only the body (payload).
        Optional. Defaults to False (body only).
    :type full_event: boolean
    """

    async def _do_internal(self, event, mapped_element):
        mapped_event = self._user_fn_output_to_event(event, mapped_element)
        await self._do_downstream(mapped_event)


class MapClass(Flow, _StreamingStepMixin):
    """Similar to Map, but instead of a function argument, this class should be extended and its do()
    method overridden.

    The do() method can also be a generator (sync or async) to stream multiple chunks.
    """

    def __init__(self, long_running=None, **kwargs):
        super().__init__(**kwargs)
        self._is_async = asyncio.iscoroutinefunction(self.do)
        self._is_async_gen = inspect.isasyncgenfunction(self.do)
        self._is_sync_gen = inspect.isgeneratorfunction(self.do)
        if (self._is_async or self._is_async_gen or self._is_sync_gen) and long_running:
            raise ValueError("long_running=True cannot be used in conjunction with a coroutine or generator do()")
        self._long_running = long_running
        self._filter = False
        self._create_name_to_outlet = True

    def filter(self):
        # used in the .do() code to signal filtering
        self._filter = True

    def do(self, event):
        raise NotImplementedError()

    async def _call(self, event):
        if self._long_running:
            res = await asyncio.get_running_loop().run_in_executor(None, self.do, event)
        else:
            res = self.do(event)
            if self._is_async:
                res = await res
        return res

    async def _do(self, event):
        # Forward termination object and StreamCompletion without processing
        if event is _termination_obj or isinstance(event, StreamCompletion):
            return await self._do_downstream(event)
        element = self._get_event_or_body(event)
        fn_result = await self._call(element)
        if not self._filter:
            # Check if the result is a generator (streaming response)
            if _is_generator(fn_result):
                await self._emit_streaming_chunks(event, fn_result)
            else:
                mapped_event = self._user_fn_output_to_event(event, fn_result)
                await self._do_downstream(mapped_event)
        else:
            self._filter = False  # clear the flag for future runs


class Rename(Flow):
    """
    Rename fields in event body.

    :param mapping: Dictionary from old name to new name.
    :param name: Name of this step, as it should appear in logs. Defaults to class name (Rename).
    :type name: string
    """

    def __init__(self, mapping: Dict[str, str], **kwargs):
        super().__init__(**kwargs)
        self.mapping = mapping

    async def _do(self, event):
        if event is not _termination_obj:
            for old_name, new_name in self.mapping.items():
                if old_name in event.body:
                    event.body[new_name] = event.body.get(old_name)
                    del event.body[old_name]
        return await self._do_downstream(event)


class ReifyMetadata(Flow):
    """
    Inserts event metadata into the event body.

    :param mapping: Dictionary from event attribute name to entry key in the event body (which must be a
        dictionary). Alternatively, an iterable of names may be provided, and these will be used as both
        attribute name and entry key.
    :param name: Name of this step, as it should appear in logs. Defaults to class name (ReifyMetadata).
    :type name: string
    """

    def __init__(self, mapping: Iterable[str], **kwargs):
        super().__init__(**kwargs)
        self.mapping = mapping

    async def _do(self, event):
        if event is not _termination_obj:
            if isinstance(self.mapping, dict):
                for attribute_name, entry_key in self.mapping.items():
                    event.body[entry_key] = getattr(event, attribute_name)
            else:
                for attribute_name in self.mapping:
                    event.body[attribute_name] = getattr(event, attribute_name)
        return await self._do_downstream(event)


class Complete(Flow):
    """
    Completes the AwaitableResult associated with incoming events.

    For non-streaming events, pushes the result to the AwaitableResult queue.
    For streaming events (events with a streaming_step attribute), wraps each chunk
    in a StreamChunk before pushing. StreamCompletion sentinels are pushed directly.

    :param name: Name of this step, as it should appear in logs. Defaults to class name (Complete).
    :type name: string
    :param full_event: Whether to complete with an Event object (when True) or only the payload
        (when False). Default to False.
    :type full_event: boolean
    """

    async def _do(self, event):
        termination_result = await self._do_downstream(event)
        if event is _termination_obj:
            return termination_result

        # Handle StreamCompletion sentinel - push to queue and propagate
        if isinstance(event, StreamCompletion):
            if event.original_event._awaitable_result:
                res = event.original_event._awaitable_result._set_result(event)
                if res:  # AsyncAwaitableResult returns a coroutine
                    await res
            return termination_result

        # Handle streaming chunk events (have streaming_step attribute)
        if event._awaitable_result:
            result = self._get_event_or_body(event)

            # wrap intermediate streaming result in StreamChunk
            is_streaming_step = getattr(event, "streaming_step", None)
            if is_streaming_step:
                result = StreamChunk(result)

            res = event._awaitable_result._set_result(result)
            if res:  # AsyncAwaitableResult returns a coroutine
                await res
        return termination_result


class Reduce(Flow):
    """
    Reduces incoming events into a single value which is returned upon the successful termination of the flow.

    :param initial_value: Starting value. When the first event is received, fn will be applied to the
        initial_value and that event.
    :type initial_value: object
    :param fn: Function to apply to the current value and each event.
    :type fn: Function ((object, Event) => object)
    :param name: Name of this step, as it should appear in logs. Defaults to class name (Reduce).
    :type name: string
    :param full_event: Whether user functions should receive and return Event objects (when True),
        or only the payload (when False). Defaults to False.
    :type full_event: boolean
    """

    def __init__(self, initial_value, fn, **kwargs):
        kwargs["initial_value"] = initial_value
        super().__init__(**kwargs)
        if not callable(fn):
            raise TypeError(f"Expected a callable, got {type(fn)}")
        self._is_async = asyncio.iscoroutinefunction(fn)
        self._fn = fn
        self._initial_value = initial_value

    def _init(self):
        super()._init()
        self._result = self._initial_value

    def to(self, outlet):
        raise ValueError("Reduce is a terminal step. It cannot be piped further.")

    async def _do(self, event):
        if event is _termination_obj:
            return self._result
        # Skip StreamCompletion - Reduce only processes actual event bodies
        if isinstance(event, StreamCompletion):
            return
        if self._full_event:
            elem = event
        else:
            elem = event.body
        res = self._fn(self._result, elem)
        if self._is_async:
            res = await res
        self._result = res


class HttpRequest:
    """A class representing an HTTP request, with method, url, body, and headers.

    :param method: HTTP method (e.g. GET).
    :type method: string
    :param url: Target URL (http and https schemes supported).
    :type url: string
    :param body: Request body.
    :type body: bytes or string
    :param headers: Request headers, in the form of a dictionary. Optional. Defaults to no headers.
    :type headers: dictionary, or None.
    """

    def __init__(self, method, url, body, headers: Optional[dict] = None):
        self.method = method
        self.url = url
        self.body = body
        if headers is None:
            headers = {}
        self.headers = headers


class HttpResponse:
    """A class representing an HTTP response, with a status code and body.

    :param body: Response body.
    :type body: bytes
    :param status: HTTP status code.
    :type status: int
    """

    def __init__(self, status, body):
        self.status = status
        self.body = body


class _ConcurrentJobExecution(Flow):
    _BACKOFF_MAX = 120
    _DEFAULT_MAX_IN_FLIGHT = 8

    def __init__(self, max_in_flight=None, retries=None, backoff_factor=None, **kwargs):
        Flow.__init__(self, **kwargs)
        if max_in_flight is not None and max_in_flight < 1:
            raise ValueError(f"max_in_flight may not be less than 1 (got {max_in_flight})")
        self.max_in_flight = max_in_flight
        self.retries = retries
        self.backoff_factor = backoff_factor

        self._queue_size = (max_in_flight or self._DEFAULT_MAX_IN_FLIGHT) - 1

    def _init(self):
        super()._init()
        self._q = None
        self._lazy_init_complete = False

    async def _worker(self):
        try:
            while True:
                # Allow event to be garbage collected
                job = None  # noqa
                event = None
                completed = None  # noqa
                try:
                    # If we don't handle the event before we remove it from the queue, the effective max_in_flight will
                    # be 1 higher than requested. Hence, we peek.
                    job = await self._q.peek()
                    if job is _termination_obj:
                        if self.logger:
                            self.logger.info(
                                f"Terminating ConcurrentJobExecution worker belonging to step '{self.name}'"
                            )
                        await self._q.get()
                        if self.logger:
                            self.logger.info(
                                f"Terminated ConcurrentJobExecution worker belonging to step '{self.name}'"
                            )
                        break
                    event = job[0]
                    completed = await job[1]
                    await self._handle_completed(event, completed)
                    await self._q.get()
                except BaseException as ex:
                    await self._q.get()
                    ex._raised_by_storey_step = self
                    recovery_step = self._get_recovery_step(ex)
                    try:
                        if recovery_step is not None:
                            event.origin_state = self.name
                            event.error = ex
                            await recovery_step._do(event)
                        else:
                            if event._awaitable_result:
                                none_or_coroutine = event._awaitable_result._set_error(ex)
                                if none_or_coroutine:
                                    await none_or_coroutine
                            if self.context and hasattr(self.context, "push_error"):
                                message = traceback.format_exc()
                                if self.logger:
                                    self.logger.error(f"Pushing error to error stream: {ex}\n{message}")
                                self.context.push_error(event, f"{ex}\n{message}", source=self.name)
                            else:
                                raise ex
                    except BaseException:
                        if not self._q.empty():
                            await self._q.get()
                        raise
        finally:
            await self._cleanup()

    async def _process_event(self, event):
        raise NotImplementedError()

    async def _handle_completed(self, event, response):
        raise NotImplementedError()

    async def _cleanup(self):
        pass

    async def _lazy_init(self):
        pass

    async def _process_event_with_retries(self, event):
        times_attempted = 0
        max_attempts = (self.retries or 0) + 1
        while True:
            try:
                return await self._process_event(event)
            except Exception as ex:
                times_attempted += 1
                attempts_left = max_attempts - times_attempted
                if self.logger:
                    self.logger.warn(f"{self.name} failed to process event ({attempts_left} retries left): {ex}")
                if attempts_left <= 0:
                    raise ex
                backoff_value = (self.backoff_factor or 1) * (2 ** (times_attempted - 1))
                backoff_value = min(self._BACKOFF_MAX, backoff_value)
                if backoff_value >= 0:
                    await asyncio.sleep(backoff_value)

    async def _do(self, event):
        if not self._lazy_init_complete:
            await self._lazy_init()
            self._lazy_init_complete = True

        if not self._q and self._queue_size > 0:
            self._q = AsyncQueue(self._queue_size)
            self._worker_awaitable = asyncio.get_running_loop().create_task(self._worker())

        if self._queue_size > 0 and self._worker_awaitable.done():
            await self._worker_awaitable
            raise FlowError("ConcurrentJobExecution worker has already terminated")

        if event is _termination_obj:
            if self._queue_size > 0:
                if self.logger:
                    self.logger.info(
                        f"Sending termination signal to ConcurrentJobExecution worker belonging to step '{self.name}'"
                    )
                await self._q.put(_termination_obj)
                if self.logger:
                    self.logger.info(
                        f"Awaiting termination of ConcurrentJobExecution worker belonging to step '{self.name}'"
                    )
                await self._worker_awaitable
            else:
                if self.logger:
                    self.logger.info(f"Terminating ConcurrentJobExecution step '{self.name}' without a worker")
                await self._cleanup()
                if self.logger:
                    self.logger.info(f"Terminated ConcurrentJobExecution step '{self.name}' without a worker")
            return await self._do_downstream(_termination_obj)
        else:
            coroutine = self._process_event_with_retries(event)
            if self._queue_size == 0:
                completed = await coroutine
                await self._handle_completed(event, completed)
            else:
                task = asyncio.get_running_loop().create_task(coroutine)
                await self._q.put((event, task))
                if self._worker_awaitable.done():
                    await self._worker_awaitable


class ConcurrentExecution(_ConcurrentJobExecution):
    """
    Inherit this class and override `process_event()` to process events concurrently.

    :param process_event: Function that will be run on each event

    :param concurrency_mechanism: One of:
      * "asyncio" (default) – for I/O implemented using asyncio
      * "threading" – for blocking I/O
      * "multiprocessing" – for processing-intensive tasks

    :param max_in_flight: Maximum number of events to be processed at a time (default 8)
    :param retries: Maximum number of retries per event (default 0)
    :param backoff_factor: Wait time in seconds before the first retry (default 1). Subsequent retries will each wait
      twice as the previous retry, up to a maximum of two minutes.
    :param pass_context: If False, the process_event function will be called with just one parameter (event). If True,
      the process_event function will be called with two parameters (event, context). Defaults to False.
    :param full_event: Whether event processor should receive and return Event objects (when True),
        or only the payload (when False). Defaults to False.
    """

    _supported_concurrency_mechanisms = ["asyncio", "threading", "multiprocessing"]

    def __init__(
        self,
        event_processor: Union[Callable[[Event], Any], Callable[[Event, Any], Any]],
        concurrency_mechanism=None,
        pass_context=None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        if concurrency_mechanism == "multiprocessing" and kwargs.get("full_event"):
            raise ValueError(
                'concurrency_mechanism="multiprocessing" may not be used in conjunction with full_event=True'
            )

        self._event_processor = event_processor

        if concurrency_mechanism and concurrency_mechanism not in self._supported_concurrency_mechanisms:
            raise ValueError(f"Concurrency mechanism '{concurrency_mechanism}' is not supported")

        if concurrency_mechanism == "multiprocessing" and pass_context:
            try:
                pickle.dumps(self.context)
            except Exception as ex:
                raise ValueError(
                    'When concurrency_mechanism="multiprocessing" is used in conjunction with '
                    "pass_context=True, context must be serializable"
                ) from ex

        self._executor = None
        if concurrency_mechanism == "threading":
            self._executor = ThreadPoolExecutor(max_workers=self.max_in_flight or self._DEFAULT_MAX_IN_FLIGHT)
        elif concurrency_mechanism == "multiprocessing":
            self._executor = ProcessPoolExecutor(max_workers=self.max_in_flight or self._DEFAULT_MAX_IN_FLIGHT)

        self._pass_context = pass_context

    async def _process_event(self, event):
        args = [event if self._full_event else event.body]

        if self._pass_context:
            args.append(self.context)
        if self._executor:
            result = await asyncio.get_running_loop().run_in_executor(self._executor, self._event_processor, *args)
        else:
            result = self._event_processor(*args)

        if asyncio.iscoroutine(result):
            result = await result

        if self._full_event:
            return result
        else:
            event.body = result
            return event

    async def _handle_completed(self, event, response):
        await self._do_downstream(response)


class SendToHttp(_ConcurrentJobExecution):
    """Joins each event with data from any HTTP source. Used for event augmentation.

    :param request_builder: Creates an HTTP request from the event. This request is then sent to its destination.
    :type request_builder: Function (Event=>HttpRequest)
    :param join_from_response: Joins the original event with the HTTP response into a new event.
    :type join_from_response: Function ((Event, HttpResponse)=>Event)
    :param name: Name of this step, as it should appear in logs. Defaults to class name (SendToHttp).
    :type name: string
    :param full_event: Whether user functions should receive and return Event objects (when True),
        or only the payload (when False). Defaults to False.
    :type full_event: boolean
    """

    def __init__(self, request_builder, join_from_response, **kwargs):
        super().__init__(**kwargs)
        self._request_builder = request_builder
        self._join_from_response = join_from_response

    def _init(self):
        super()._init()
        self._client_session = None

    async def _lazy_init(self):
        connector = aiohttp.TCPConnector()
        self._client_session = aiohttp.ClientSession(connector=connector)

    async def _cleanup(self):
        await self._client_session.close()

    async def _process_event(self, event):
        req = self._request_builder(event)
        return await self._client_session.request(req.method, req.url, headers=req.headers, data=req.body, ssl=False)

    async def _handle_completed(self, event, response):
        response_body = await response.text()
        joined_element = self._join_from_response(event.body, HttpResponse(response.status, response_body))
        if joined_element is not None:
            new_event = self._user_fn_output_to_event(event, joined_element)
            await self._do_downstream(new_event)


class _Batching(Flow):
    _do_downstream_per_event = True

    def __init__(
        self,
        max_events: Optional[int] = None,
        flush_after_seconds: Union[int, float, None] = None,
        key_field: Optional[Union[str, Callable[[Event], str]]] = None,
        drop_key_field=False,
        **kwargs,
    ):
        if max_events:
            kwargs["max_events"] = max_events
        if flush_after_seconds is not None:
            kwargs["flush_after_seconds"] = flush_after_seconds
        if isinstance(key_field, str):
            kwargs["key_field"] = key_field
        super().__init__(**kwargs)

        self._max_events = max_events
        self._flush_after_seconds = flush_after_seconds

        if self._flush_after_seconds is not None and self._flush_after_seconds < 0:
            raise ValueError("flush_after_seconds cannot be negative")

        self._extract_key: Optional[Callable[[Event], str]] = self._create_key_extractor(key_field, drop_key_field)

    def _init(self):
        super()._init()
        self._batch: Dict[Optional[str], List[Any]] = defaultdict(list)
        # Keep the original events that make up each batch
        self._batch_events: Dict[Optional[str], List[Any]] = defaultdict(list)
        self._batch_first_event_time: Dict[Optional[str], datetime.datetime] = {}
        self._batch_last_event_time: Dict[Optional[str], datetime.datetime] = {}
        self._batch_start_time: Dict[Optional[str], float] = {}
        self._timeout_task: Optional[Task] = None

    @staticmethod
    def _create_key_extractor(key_field, drop_key_field) -> Callable:
        if key_field is None:
            return lambda event: None
        elif callable(key_field):
            return key_field
        elif isinstance(key_field, str):
            if key_field.startswith("$"):
                attribute = key_field[1:]
                return lambda event: getattr(event, attribute)
            elif drop_key_field:
                return lambda event: event.body.pop(key_field)
            else:
                return lambda event: event.body[key_field]
        else:
            raise ValueError(f"Unsupported key_field type {type(key_field)}")

    async def _emit(self, batch, batch_key, batch_time, batch_events, last_event_time=None):
        raise NotImplementedError

    async def _terminate(self):
        pass

    async def _do(self, event):
        if event is _termination_obj:
            if self.logger:
                self.logger.info(f"Terminating Batching step '{self.name}': emitting all remaining batches")
            await self._emit_all()
            if self.logger:
                self.logger.info(f"Terminating Batching step '{self.name}': running custom termination code")
            await self._terminate()
            if self.logger:
                self.logger.info(f"Terminated Batching step '{self.name}'")
            return await self._do_downstream(_termination_obj)

        key = self._extract_key(event)

        if hasattr(self, "_get_event_time"):
            event_time = self._get_event_time(event)
        else:
            event_time = event.processing_time

        if len(self._batch[key]) == 0:
            self._batch_first_event_time[key] = event_time
            self._batch_start_time[key] = time.monotonic()
            self._batch_last_event_time[key] = event_time
        elif self._batch_last_event_time[key] < event_time:
            self._batch_last_event_time[key] = event_time

        if self._flush_after_seconds is not None and self._timeout_task is None:
            self._timeout_task = asyncio.get_running_loop().create_task(self._sleep_and_emit())

        self._batch[key].append(self._event_to_batch_entry(event))
        self._batch_events[key].append(event)

        if len(self._batch[key]) == self._max_events:
            await self._emit_batch(key)

        if self._do_downstream_per_event:
            await self._do_downstream(event)

    async def _sleep_and_emit(self):
        try:
            while self._batch:
                key = next(iter(self._batch.keys()))
                delta_seconds = time.monotonic() - self._batch_start_time[key]
                if delta_seconds < self._flush_after_seconds:
                    await asyncio.sleep(self._flush_after_seconds - delta_seconds)
                await self._emit_batch(key)
        except Exception:
            message = traceback.format_exc()
            if self.logger:
                self.logger.error(f"Failed to flush batch in step '{self.name}':\n{message}")

        self._timeout_task = None

    def _event_to_batch_entry(self, event):
        return self._get_event_or_body(event)

    async def _emit_batch(self, batch_key: Optional[str] = None):
        batch_to_emit = self._batch.pop(batch_key, None)
        if batch_to_emit is None:
            return
        batch_time = self._batch_first_event_time.pop(batch_key)
        last_event_time = self._batch_last_event_time.pop(batch_key)
        del self._batch_start_time[batch_key]
        try:
            await self._emit(batch_to_emit, batch_key, batch_time, self._batch_events[batch_key], last_event_time)
        finally:
            # whether we succeeded or failed, we are done with these events
            del self._batch_events[batch_key]

    async def _emit_all(self):
        for key in list(self._batch.keys()):
            await self._emit_batch(key)


class Batch(_Batching, WithUUID):
    """Batches events into lists of up to max_events events. Each emitted list contained max_events events, unless
    flush_after_seconds seconds have passed since the first event in the batch was received, at which the batch is
    emitted with potentially fewer than max_events event.

    :param max_events: Maximum number of events per emitted batch. Set to None to emit all events in one batch on flow
        termination.
    :param flush_after_seconds: Maximum number of seconds to wait before a batch is emitted.
    :param key: The key by which events are grouped. By default (None), events are not grouped.
        Other options may be:
        Set to '$x' to group events by the x attribute of the event. E.g. "$key" or "$path".
        set to other string 'str' to group events by Event.body[str].
        set a Callable[Any, Any] to group events by a a custom key extractor.
    """

    _do_downstream_per_event = False

    def __init__(self, *args, **kwargs):
        _Batching.__init__(self, *args, **kwargs)
        WithUUID.__init__(self)

    async def _emit(self, batch, batch_key, batch_time, batch_events, last_event_time=None):
        event = Event(batch, id=self._get_uuid())
        if not self._full_event:
            # Preserve reference to the original events to avoid early commit of offsets
            event._original_events = batch_events
        return await self._do_downstream(event)


class JoinWithV3IOTable(_ConcurrentJobExecution):
    """Joins each event with a V3IO table. Used for event augmentation.

    :param storage: Database driver.
    :type storage: Driver
    :param key_extractor: Function for extracting the key for table access from an event.
    :type key_extractor: Function (Event=>string)
    :param join_function: Joins the original event with relevant data received from V3IO.
    :type join_function: Function ((Event, dict)=>Event)
    :param table_path: Path to the table in V3IO.
    :type table_path: string
    :param attributes: A comma-separated list of attributes to be requested from V3IO.
        Defaults to '*' (all user attributes).
    :type attributes: string
    :param name: Name of this step, as it should appear in logs. Defaults to class name (JoinWithV3IOTable).
    :type name: string
    :param full_event: Whether user functions should receive and return Event objects (when True), or only
        the payload (when False). Defaults to False.
    :type full_event: boolean
    """

    def __init__(
        self,
        storage,
        key_extractor,
        join_function,
        table_path,
        attributes="*",
        **kwargs,
    ):
        kwargs["table_path"] = table_path
        kwargs["attributes"] = attributes
        super().__init__(**kwargs)

        self._storage = storage

        self._key_extractor = key_extractor
        self._join_function = join_function

        self._container, self._table_path = _split_path(table_path)
        self._attributes = attributes

    async def _process_event(self, event):
        key = str(self._key_extractor(self._get_event_or_body(event)))
        return await self._storage._get_item(self._container, self._table_path, key, self._attributes)

    async def _handle_completed(self, event, response):
        if response.status_code == 200:
            response_object = response.output.item
            joined = self._join_function(self._get_event_or_body(event), response_object)
            if joined is not None:
                new_event = self._user_fn_output_to_event(event, joined)
                await self._do_downstream(new_event)
        elif response.status_code == 404:
            return None
        else:
            raise V3ioError(f"Failed to get item. Response status code was {response.status_code}: {response.body}")

    async def _cleanup(self):
        await self._storage.close()


class JoinWithTable(_ConcurrentJobExecution):
    """Joins each event with data from the given table.

    :param table: A Table object or name to join with. If a table name is provided, it will be looked up in the context.
    :param key_extractor: Key's column name or a function for extracting the key, for table access from an event.
    :param attributes: A comma-separated list of attributes to be queried for. Defaults to all attributes.
    :param inner_join: Whether to drop events when the table does not have a matching entry (join_function won't be
        called in such a case). Defaults to False.
    :param join_function: Joins the original event with relevant data received from the storage. Event is dropped when
        this function returns None. Defaults to assume the event's body is a dict-like object and updating it.
    :param name: Name of this step, as it should appear in logs. Defaults to class name (JoinWithTable).
    :param full_event: Whether user functions should receive and return Event objects (when True), or only the
        payload (when False). Defaults to False.
    :param context: Context object that holds global configurations and secrets.
    """

    def __init__(
        self,
        table: Union[Table, str],
        key_extractor: Union[str, Callable[[Event], str]],
        attributes: Optional[List[str]] = None,
        inner_join: bool = False,
        join_function: Optional[Callable[[Any, Dict[str, object]], Any]] = None,
        **kwargs,
    ):
        if isinstance(table, str):
            kwargs["table"] = table
        if isinstance(key_extractor, str):
            kwargs["key_extractor"] = key_extractor
        if attributes:
            kwargs["attributes"] = attributes
        kwargs["inner_join"] = inner_join

        super().__init__(**kwargs)

        self._table = table
        if isinstance(table, str):
            if not self.context:
                raise TypeError("Table can not be string if no context was provided to the step")
            self._table = self.context.get_table(table)

        if key_extractor:
            if callable(key_extractor):
                self._key_extractor = key_extractor
            elif isinstance(key_extractor, str):
                if self._full_event:
                    self._key_extractor = lambda event: event.body[key_extractor]
                else:
                    self._key_extractor = lambda element: element[key_extractor]
            else:
                raise TypeError(f"key is expected to be either a callable or string but got {type(key_extractor)}")

        def default_join_fn(event, join_res):
            event.update(join_res)
            return event

        def default_join_fn_full_event(event, join_res):
            event.body.update(join_res)
            return event

        self._inner_join = inner_join
        self._join_function = join_function or (default_join_fn_full_event if self._full_event else default_join_fn)

        self._attributes = attributes or "*"

    def _init(self):
        super()._init()
        self._closeables = [self._table]

    async def _process_event(self, event):
        key = self._key_extractor(self._get_event_or_body(event))
        safe_key = stringify_key(key)
        return await self._table._get_or_load_static_attributes_by_key(safe_key, self._attributes)

    async def _handle_completed(self, event, response):
        if self._inner_join and not response:
            return
        joined = self._join_function(self._get_event_or_body(event), response)
        if joined is not None:
            new_event = self._user_fn_output_to_event(event, joined)
            await self._do_downstream(new_event)


def build_flow(steps):
    """Builds a flow from a list of steps, by chaining the steps according to their order in the list.
    Nested lists are used to represent branches in the flow.

    Examples:
        build_flow([step1, step2, step3])
        is equivalent to
        step1.to(step2).to(step3)

        build_flow([step1, [step2a, step2b], step3])
        is equivalent to
        step1.to(step2a)
        step1.to(step3)
        step2a.to(step2b)

    :param steps: a potentially nested list of steps
    :returns: the first step
    :rtype: Flow
    """
    if len(steps) == 0:
        raise ValueError("Cannot build an empty flow")
    first_step = steps[0]
    if isinstance(first_step, list):
        first_step = build_flow(first_step)
    cur_step = first_step
    for next_step in steps[1:]:
        if isinstance(next_step, list):
            cur_step.to(build_flow(next_step))
        else:
            cur_step.to(next_step)
            cur_step = next_step
    return first_step


class Context:
    """
    Context object that holds global secrets and configurations to be passed to relevant steps.

    :param initial_secrets: Initial dict of secrets.
    :param initial_parameters: Initial dict of parameters.
    :param initial_tables: Initial dict of tables.
    """

    def __init__(
        self,
        initial_secrets: Optional[Dict[str, str]] = None,
        initial_parameters: Optional[Dict[str, object]] = None,
        initial_tables: Optional[Dict[str, Table]] = None,
    ):
        self._secrets = initial_secrets or {}
        self._parameters = initial_parameters or {}
        self._tables = initial_tables or {}

    def get_param(self, key, default):
        return self._parameters.get(key, default)

    def set_param(self, key, value):
        self._parameters[key] = value

    def get_secret(self, key):
        return self._secrets.get(key, None)

    def set_secret(self, key, secret):
        self._secrets[key] = secret

    def get_table(self, key):
        return self._tables[key]

    def set_table(self, key, table):
        self._tables[key] = table


class _ParallelExecutionRunnableResult:
    def __init__(self, runnable_name: str, data: Any, runtime: float, timestamp: datetime.datetime):
        self.runnable_name = runnable_name
        self.data = data
        self.runtime = runtime
        self.timestamp = timestamp


class ParallelExecutionMechanisms(str, enum.Enum):
    process_pool = "process_pool"
    dedicated_process = "dedicated_process"
    thread_pool = "thread_pool"
    asyncio = "asyncio"
    shared_executor = "shared_executor"
    naive = "naive"

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value

    @classmethod
    def all(cls) -> list[str]:
        return [
            ParallelExecutionMechanisms.process_pool,
            ParallelExecutionMechanisms.dedicated_process,
            ParallelExecutionMechanisms.thread_pool,
            ParallelExecutionMechanisms.asyncio,
            ParallelExecutionMechanisms.shared_executor,
            ParallelExecutionMechanisms.naive,
        ]

    @classmethod
    def single_thread(cls) -> list[str]:
        return [
            ParallelExecutionMechanisms.asyncio,
            ParallelExecutionMechanisms.naive,
        ]

    @classmethod
    def process(cls) -> list[str]:
        return [
            ParallelExecutionMechanisms.process_pool,
            ParallelExecutionMechanisms.dedicated_process,
        ]

    @staticmethod
    def validate(execution_mechanism: str) -> None:
        if execution_mechanism not in ParallelExecutionMechanisms.all():
            raise ValueError(
                f"Execution mechanism '{execution_mechanism}' is invalid. It must be one of: "
                f"{ParallelExecutionMechanisms.all()}"
            )


class ParallelExecutionRunnable:
    """
    Runnable to be run by a ParallelExecution step.

    Subclasses must override the run() method, or run_async() in order to support execution_mechanism="asyncio",
    with user code that handles the event and returns a result.

    Subclasses may optionally override the init() method if the user's implementation of run() requires prior
    initialization.

    :param name: Runnable name
    """

    # ignore unused keyword arguments such as context which may be passed in by mlrun
    def __init__(self, name: str, raise_exception: bool = True, shared_runnable_name: Optional[str] = None, **kwargs):
        self.name = name
        self.shared_runnable_name = shared_runnable_name
        self._raise_exception = raise_exception

    def init(self) -> None:
        """Override this method to add initialization logic."""
        pass

    def run(self, body: Any, path: str, origin_name: Optional[str] = None) -> Any:
        """
        Override this method with the code this runnable should run. If execution_mechanism is "asyncio", override
        run_async() instead.

        :param body: Event body
        :param path: Event path
        :param origin_name: Name of the runnable that initiated this run, if applicable.
                Use especially when this runnable is shared between multiple parallel executions.
        """
        return body

    async def run_async(self, body: Any, path: str, origin_name: Optional[str] = None) -> Any:
        """
        If execution_mechanism is "asyncio", override this method with the code this runnable should run. Otherwise,
        override run() instead.

        :param body: Event body
        :param path: Event path
        :param origin_name: Name of the runnable that initiated this run, if applicable.
                Use especially when this runnable is shared between multiple parallel executions.
        """
        return body

    def _run(self, body: Any, path: str, origin_name: Optional[str] = None) -> Any:
        timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        start = time.monotonic()
        try:
            result = self.run(body, path, origin_name)
            # Return generator directly for streaming support
            if _is_generator(result):
                return result
            body = result
        except Exception as e:
            if self._raise_exception:
                raise e
            else:
                body = {"error": f"{type(e).__name__}: {e}"}
        end = time.monotonic()
        return _ParallelExecutionRunnableResult(origin_name or self.name, body, end - start, timestamp)

    async def _async_run(self, body: Any, path: str, origin_name: Optional[str] = None) -> Any:
        timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        start = time.monotonic()
        try:
            result = self.run_async(body, path, origin_name)

            # Return generator directly for streaming support
            if _is_generator(result):
                return result

            # Await if coroutine
            if asyncio.iscoroutine(result):
                result = await result

            body = result
        except Exception as e:
            if self._raise_exception:
                raise e
            else:
                body = {"error": f"{type(e).__name__}: {e}"}
        end = time.monotonic()
        return _ParallelExecutionRunnableResult(origin_name or self.name, body, end - start, timestamp)


_sval = None


def _set_global(sval):
    global _sval
    _sval = sval


def _static_run(*args, **kwargs):
    global _sval
    return _sval._run(*args, **kwargs)


class RunnableExecutor:
    """
    Manages and executes `ParallelExecutionRunnable` instances using various parallel execution mechanisms.

    :param max_processes: Maximum number of processes to spawn (excluding dedicated processes).
                         Defaults to the number of CPUs or 16 if undetectable.
    :param max_threads: Maximum number of threads to spawn. Defaults to 32.
    :param pool_factor: Multiplier to scale the number of process/thread workers per runnable. Defaults to 1.
    """

    def __init__(
        self,
        max_processes: Optional[int] = None,
        max_threads: Optional[int] = None,
        pool_factor: Optional[int] = None,
    ):
        self._runnable_by_name: dict[str, ParallelExecutionRunnable] = {}
        self._execution_mechanism_by_runnable_name: dict[str, str] = {}
        self.max_processes = max_processes or os.cpu_count() or 16
        self.max_threads = max_threads or 32
        self.pool_factor = pool_factor or 1
        self.num_processes = 0
        self.num_threads = 0

        self._process_executor_by_runnable_name = {}
        self._mp_context = multiprocessing.get_context("spawn")
        self._executors = {}

    def add_runnable(self, runnable: ParallelExecutionRunnable, execution_mechanism: str) -> None:
        """
        Registers a new runnable instance.

        :param runnable: A `ParallelExecutionRunnable` instance.
        :param execution_mechanism: Execution mechanism (See `ParallelExecution`).

        :raises ValueError: If a runnable with the same name is already registered.
        """
        ParallelExecutionMechanisms.validate(execution_mechanism)
        if runnable.name not in self._runnable_by_name:
            self._runnable_by_name[runnable.name] = runnable
            self._execution_mechanism_by_runnable_name[runnable.name] = execution_mechanism
        else:
            raise ValueError(f"ParallelExecutionRunnable name '{runnable.name}' is not unique")

    def init_runnable(self, runnable: Union[str, ParallelExecutionRunnable]) -> None:
        """
        Initializes a runnable and prepares it for execution.

        :param runnable: Either a runnable instance or the name of a registered runnable.

        :raises ValueError: If the named runnable does not exist or has an unsupported execution mechanism.
        """
        if isinstance(runnable, str):
            if runnable not in self._runnable_by_name:
                raise ValueError(f"{runnable} does not exist")
            else:
                runnable = self._runnable_by_name[runnable]

        runnable.init()
        self._runnable_by_name[runnable.name] = runnable

        execution_mechanism = self._execution_mechanism_by_runnable_name[runnable.name]

        # Check for streaming + process-based execution (incompatible combination)
        if execution_mechanism in ParallelExecutionMechanisms.process():
            is_streaming = inspect.isgeneratorfunction(runnable.run) or inspect.isasyncgenfunction(runnable.run_async)
            if is_streaming:
                raise StreamingError(
                    f"Streaming is not supported with process-based execution mechanisms. "
                    f"Runnable '{runnable.name}' uses '{execution_mechanism}'. "
                    f"Use 'thread_pool', 'asyncio', or 'naive' for streaming runnables."
                )

        if execution_mechanism == ParallelExecutionMechanisms.process_pool:
            self.num_processes += 1

        elif execution_mechanism == ParallelExecutionMechanisms.dedicated_process:
            self._process_executor_by_runnable_name[runnable.name] = ProcessPoolExecutor(
                max_workers=1,
                mp_context=self._mp_context,
                initializer=_set_global,
                initargs=(runnable,),
            )

        elif execution_mechanism == ParallelExecutionMechanisms.thread_pool:
            self.num_threads += 1

        elif execution_mechanism not in ParallelExecutionMechanisms.single_thread():
            raise ValueError(f"Unsupported execution mechanism: {execution_mechanism}")

    def init_executors(self):
        """
        Initializes thread and process pool executors based on configured runnables and resource limits.
        """
        if not self._executors:
            num_threads = min(self.num_threads * self.pool_factor, self.max_threads)
            num_processes = min(self.num_processes * self.pool_factor, self.max_processes)

            self._executors = {}
            if num_processes:
                self._executors[ParallelExecutionMechanisms.process_pool] = ProcessPoolExecutor(
                    max_workers=num_processes, mp_context=self._mp_context
                )
            if num_threads:
                self._executors[ParallelExecutionMechanisms.thread_pool] = ThreadPoolExecutor(max_workers=num_threads)

    def run_executor(
        self,
        runnable: Union[ParallelExecutionRunnable, str],
        runnables_encountered: set[int],
        event,
        origin_runnable_name: Optional[str] = None,
    ) -> asyncio.Future:
        """
        Executes the given runnable instance using the appropriate execution mechanism.

        :param runnable: Runnable instance or name to execute.
        :param runnables_encountered: Set of `id`s for runnables already executed in this cycle to prevent duplicates.
        :param event: The event input object to pass to the runnable.
        :param origin_runnable_name: Name of the proxy runnable that initiated this execution, if any.

        :return: An `asyncio.Future` representing the pending result.
        :raises ValueError: If the runnable was already executed or is not properly registered.
        :raises TypeError: If the input is neither a string nor a `ParallelExecutionRunnable`.
        """
        self.init_executors()
        if isinstance(runnable, str):
            runnable = self._runnable_by_name[runnable]
        elif not isinstance(runnable, ParallelExecutionRunnable):
            raise TypeError(f"Expected a ParallelExecutionRunnable or str, but got: {type(runnable).__name__}")

        if id(runnable) in runnables_encountered:
            raise ValueError(f"select_runnables() returned more than one outlet named '{runnable.name}'")

        execution_mechanism = self._execution_mechanism_by_runnable_name[runnable.name]

        input = (
            event.body if execution_mechanism in ParallelExecutionMechanisms.process() else copy.deepcopy(event.body)
        )

        if execution_mechanism == ParallelExecutionMechanisms.asyncio:
            future = asyncio.get_running_loop().create_task(
                runnable._async_run(input, event.path, origin_runnable_name)
            )
        elif execution_mechanism == ParallelExecutionMechanisms.naive:
            future = asyncio.get_running_loop().create_future()
            future.set_result(runnable._run(input, event.path, origin_runnable_name))
        elif execution_mechanism == ParallelExecutionMechanisms.dedicated_process:
            executor = self._process_executor_by_runnable_name[runnable.name]
            future = asyncio.get_running_loop().run_in_executor(
                executor,
                _static_run,
                input,
                event.path,
                origin_runnable_name,
            )
        else:
            executor = self._executors[execution_mechanism]
            future = asyncio.get_running_loop().run_in_executor(
                executor,
                runnable._run,
                input,
                event.path,
                origin_runnable_name,
            )
        return future


class ParallelExecution(Flow, _StreamingStepMixin):
    """
    Runs multiple jobs in parallel for each event.

    :param runnables: A list of ParallelExecutionRunnable instances.
    :param execution_mechanism_by_runnable_name: Mapping from each runnable name to the execution_mechanism that should
        run it. Must be one of:
    * "process_pool" – To run in a separate process from a process pool. This is appropriate for CPU or GPU intensive
        tasks as they would otherwise block the main process by holding Python's Global Interpreter Lock (GIL).
    * "dedicated_process" – To run in a separate dedicated process. This is appropriate for CPU or GPU intensive tasks
        that also require significant Runnable-specific initialization (e.g. a large model).
    * "thread_pool" – To run in a separate thread. This is appropriate for blocking I/O tasks, as they would otherwise
        block the main event loop thread.
    * "asyncio" – To run in an asyncio task. This is appropriate for I/O tasks that use asyncio, allowing the event
        loop to continue running while waiting for a response.
    * "shared_executor" – Reuses an external executor (typically managed by the flow or context) to execute the
        runnable. Should be used only if you have multiply `ParallelExecution` in the same flow and especially
        useful when:
        - You want to share a heavy resource like a large model loaded onto a GPU.
        - You want to centralize task scheduling or coordination for multiple lightweight tasks.
        - You aim to minimize overhead from creating new executors or processes/threads per runnable.
      The runnable is expected to be pre-initialized and reused across events, enabling efficient use of memory and
      hardware accelerators.
    * "naive" – To run in the main event loop. This is appropriate only for trivial computation and/or file I/O. It
        means that the runnable will not actually be run in parallel to anything else.
    :param max_processes: Maximum number of processes to spawn, not including dedicated ones. Defaults to the number of
      available CPUs, or 16 if number of CPUs can't be determined.
    :param max_threads: Maximum number of threads to start. Defaults to 32.

    Streaming support: If a single runnable is selected and returns a generator (sync or async),
    the result will be streamed as chunks. Streaming with multiple runnables or process-based
    execution mechanisms is not supported.
    """

    def __init__(
        self,
        runnables: list[ParallelExecutionRunnable],
        execution_mechanism_by_runnable_name: dict[str, str],
        max_processes: Optional[int] = None,
        max_threads: Optional[int] = None,
        pool_factor: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        if not runnables:
            raise ValueError("ParallelExecution cannot be instantiated without at least one runnable")

        for runnable in runnables:
            if not (execution_mechanism := execution_mechanism_by_runnable_name.get(runnable.name)):
                raise ValueError(f"No execution mechanism was specified for runnable '{runnable.name}'")
            ParallelExecutionMechanisms.validate(execution_mechanism)

        self.runnables = runnables
        self.registered_runnables = set(
            r.name if isinstance(r, ParallelExecutionRunnable) else r for r in self.runnables
        )
        self.execution_mechanism_by_runnable_name = execution_mechanism_by_runnable_name
        self.max_processes = max_processes or os.cpu_count() or 16
        self.max_threads = max_threads or 32
        self.pool_factor = pool_factor or 1

    def select_runnables(self, event) -> Optional[Union[list[str], list[ParallelExecutionRunnable]]]:
        """
        Given an event, returns a list of runnables (or a list of runnable names) to execute on it. It can also return
        None, in which case all runnables are executed on the event, which is also the default.

        :param event: Event object
        """
        pass

    def preprocess_event(self, event):
        """
        Given an event, preprocess it with user code.
        Runs before the runnable selector.
        Should return the new enriched event.
        :param event: Event object
        """
        return event

    def _init(self):
        super()._init()
        self.runnable_executor = RunnableExecutor(
            max_processes=self.max_processes, max_threads=self.max_threads, pool_factor=self.pool_factor
        )
        for runnable in self.runnables:
            execution_mechanism = self.execution_mechanism_by_runnable_name[runnable.name]
            self.runnable_executor.add_runnable(runnable=runnable, execution_mechanism=execution_mechanism)
            if execution_mechanism == ParallelExecutionMechanisms.shared_executor:
                self.context.executor.init_runnable(runnable=runnable.shared_runnable_name)
            else:
                self.runnable_executor.init_runnable(runnable=runnable)

        self.runnable_executor.init_executors()

    async def _do(self, event):
        # Forward termination object and StreamCompletion without processing
        if event is _termination_obj or isinstance(event, StreamCompletion):
            return await self._do_downstream(event)

        event = self.preprocess_event(event)
        runnables = self.select_runnables(event)
        if runnables is None:
            runnables = self.runnables
        self._verify_runnables(runnables)
        futures = []
        runnables_encountered = set()
        for runnable in runnables:
            runnable: ParallelExecutionRunnable = (
                runnable
                if isinstance(runnable, ParallelExecutionRunnable)
                else self.runnable_executor._runnable_by_name[runnable]
            )
            if self.execution_mechanism_by_runnable_name[runnable.name] == ParallelExecutionMechanisms.shared_executor:
                future = self.context.executor.run_executor(
                    runnable=runnable.shared_runnable_name,
                    runnables_encountered=runnables_encountered,
                    event=event,
                    origin_runnable_name=runnable.name,
                )
            else:
                future = self.runnable_executor.run_executor(
                    runnable=runnable, runnables_encountered=runnables_encountered, event=event
                )
            runnables_encountered.add(id(runnable))
            futures.append(future)
        results = await asyncio.gather(*futures)

        # Check for streaming response (only when a single runnable is selected)
        if len(runnables) == 1 and results:
            result = results[0]
            # Check if the result is a generator (streaming response)
            if _is_generator(result):
                await self._emit_streaming_chunks(event, result)
                return None

        # Non-streaming path
        if len(runnables) == 1:
            result: _ParallelExecutionRunnableResult = results[0]
            event.body = result.data if results else None

            metadata = {
                "microsec": result.runtime,
                "when": result.timestamp.isoformat(sep=" ", timespec="microseconds"),
            }
        else:
            # Check if any results are generators (not allowed with multiple runnables)
            for result in results:
                if _is_generator(result):
                    raise StreamingError(
                        "Streaming is not supported when multiple runnables are selected. "
                        "Streaming runnables must be the only runnable selected for an event."
                    )
            event.body = {result.runnable_name: result.data for result in results}
            metadata = {
                result.runnable_name: {
                    "microsec": result.runtime,
                    "when": result.timestamp.isoformat(sep=" ", timespec="microseconds"),
                }
                for result in results
            }

        if hasattr(event, "_metadata") and isinstance(event._metadata, dict):
            event._metadata.update(metadata)
        else:
            event._metadata = metadata
        return await self._do_downstream(event)

    def _verify_runnables(self, runnables: List[Union[str, ParallelExecutionRunnable]]):
        """Verifies that the provided runnables are valid and registered."""
        runnable_names = set()
        for runnable in runnables:
            if not isinstance(runnable, (str, ParallelExecutionRunnable)):
                raise TypeError(f"Expected a ParallelExecutionRunnable or str, but got: {type(runnable).__name__}")
            runnable_names.add(runnable.name if isinstance(runnable, ParallelExecutionRunnable) else runnable)
        if not runnable_names.issubset(self.registered_runnables):
            unregistered_runnables_string = ",".join(runnable_names - self.registered_runnables)
            raise ValueError(f"The following selected Runnables are not registered: {unregistered_runnables_string}")
