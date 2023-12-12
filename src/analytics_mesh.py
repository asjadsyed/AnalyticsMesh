#!/usr/bin/env python3

import enum
import logging
import random
import signal
import tempfile
import threading
from types import FrameType
from typing import Any, Dict, List, Optional, Self, Tuple, Type
import time
import os

import datasketches
import thrift.server.TNonblockingServer
import thrift.transport.TSocket
import thrift.transport.TTransport
import thrift.protocol.TBinaryProtocol

from gen.py.anti_entropy import AntiEntropy
import thrift_helper

ANTI_ENTROPY_INTERVAL = 1
ANTI_ENTROPY_MAX_CLIENTS = 3
ANTI_ENTROPY_TIMEOUT: Optional[int | float] = 1000
COMMITTER_INTERVAL = 5
LOGGER: logging.Logger = logging.getLogger(__name__)
LOG_K = 21


class DurabilityLevel(enum.StrEnum):
    STRICT = enum.auto()
    DELAYED = enum.auto()
    VOLATILE = enum.auto()


class AnalyticsMesh(AntiEntropy.Iface):
    def __init__(
        self,
        enable_server: bool,
        enable_client: bool,
        server_address: Optional[Tuple[str, int]] = None,
        client_addresses: Optional[List[Tuple[str, int]]] = None,
        sketch_file: Optional[str] = None,
        durability_level: DurabilityLevel = DurabilityLevel.VOLATILE,
        atomicity: Optional[bool] = None,
    ) -> None:
        self.enable_server = enable_server
        self.enable_client = enable_client
        self.server_address: Optional[Tuple[str, int]] = server_address
        if self.server_address is not None:
            host, port = self.server_address
            if host == "":
                raise ValueError("Host cannot be empty. Please specify a valid host.")
            if not 0 <= port <= 65535:
                raise ValueError(
                    f"Port must be an integer in the range 0-65535, '{port}' provided."
                )
        self.client_addresses: Optional[List[Tuple[str, int]]] = client_addresses
        self.sketch_file: Optional[str] = sketch_file
        self.durability_level = durability_level
        self.atomicity = atomicity or (
            self.durability_level != DurabilityLevel.VOLATILE
        )
        if self.durability_level == DurabilityLevel.VOLATILE:
            if self.atomicity:
                raise ValueError(
                    f"Atomicity cannot be enabled with {self.durability_level} durability. "
                    "Remove the atomicity flag or adjust the durability level."
                )
        else:
            if self.sketch_file is None:
                raise ValueError(
                    f"A sketch file is required with {self.durability_level} durability. "
                    "Provide a valid file path using the sketch_file argument "
                    f"or set durability_level to {DurabilityLevel.VOLATILE}."
                )
        self.sketch_file_dirname: Optional[str] = None
        self.sketch_file_basename: Optional[str] = None
        if self.sketch_file is not None:
            self.sketch_file_dirname, self.sketch_file_basename = os.path.split(
                sketch_file
            )
            self.sketch_file_dirname = self.sketch_file_dirname or "."
            if self.sketch_file_basename == "":
                raise ValueError("The sketch file name cannot be empty or a directory.")
            if os.path.isdir(self.sketch_file):
                raise ValueError(
                    f"The path '{sketch_file}' is a directory, not a file."
                )
            if os.path.exists(self.sketch_file) and not os.access(
                self.sketch_file, os.W_OK
            ):
                raise ValueError(f"The file '{sketch_file}' is not writable.")
            if not os.access(self.sketch_file_dirname, os.W_OK):
                raise ValueError(
                    f"No write permission in the directory '{self.sketch_file_dirname}'."
                )
        self.enable_committer = self.durability_level == DurabilityLevel.DELAYED
        self.continue_client = self.enable_client
        self.continue_committer = self.enable_committer
        self.is_dirty: bool = False
        self.server_thread: Optional[threading.Thread] = None
        self.client_thread: Optional[threading.Thread] = None
        self.committer_thread: Optional[threading.Thread] = None
        self.prev_signal_handlers: Dict[int, Optional[signal.Handlers]] = {}
        self.server: Optional[
            thrift.server.TNonblockingServer.TNonblockingServer
        ] = None
        self.sketch: datasketches.hll_sketch = datasketches.hll_sketch(LOG_K)

    def __enter__(self) -> Self:
        self.start_handler()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[Any],
    ) -> Optional[bool]:
        self.stop_handler()

    def reader_work(self) -> bool:
        if self.sketch_file is None:
            return False
        self_serialized = None
        try:
            with open(self.sketch_file, "rb") as fd:
                self_serialized = fd.read()
        except FileNotFoundError:
            return False
        self.sketch = datasketches.hll_sketch.deserialize(self_serialized)
        return True

    def committer_work(self) -> bool:
        # https://lwn.net/Articles/457667/
        if self.durability_level == DurabilityLevel.VOLATILE:
            return False
        if self.sketch_file is None:
            return False
        if self.is_dirty:
            if self.atomicity:
                temp_file_path = None
                try:
                    # 1. create a new temp file (on the same file system!)
                    with tempfile.NamedTemporaryFile(
                        mode="wb",
                        dir=self.sketch_file_dirname,
                        prefix=self.sketch_file_basename + "_",
                        suffix=".tmp",
                        delete=False,
                    ) as temp_file:
                        temp_file_path = temp_file.name

                        # 2. write data to the temp file
                        temp_file.write(self.sketch.serialize_compact())

                        # 3. fsync() the temp file
                        temp_file.flush()
                        os.fsync(temp_file.fileno())

                    # 4. rename the temp file to the appropriate name
                    os.replace(temp_file_path, self.sketch_file)

                    # 5. fsync() the containing directory
                    dir_fd = os.open(
                        self.sketch_file_dirname, os.O_RDONLY | os.O_DIRECTORY
                    )
                    try:
                        os.fsync(dir_fd)
                    finally:
                        os.close(dir_fd)

                except BaseException as e:
                    if temp_file_path is not None:
                        try:
                            os.remove(temp_file_path)
                        except FileNotFoundError:
                            logging.warning(
                                "Temporary file deletion failed: '%s' not found.",
                                temp_file_path,
                                exc_info=e,
                            )
                    raise
            else:
                with open(self.sketch_file, "wb") as file:
                    file.write(self.sketch.serialize_compact())
                    file.flush()
                    os.fsync(file.fileno())

            self.is_dirty = False
            return True
        return False

    def committer_work_periodic(self) -> None:
        while self.continue_committer:
            time.sleep(COMMITTER_INTERVAL)
            self.committer_work()

    def start_committer(self) -> threading.Thread:
        committer_thread = threading.Thread(
            target=self.committer_work_periodic,
            daemon=True,
        )
        committer_thread.start()
        return committer_thread

    def update_sketch(self, datum: int | float | str) -> None:
        self.sketch.update(datum)
        self.is_dirty = True
        if self.durability_level == DurabilityLevel.STRICT:
            self.committer_work()

    def signal_handler(self, signum: int, frame: Optional[FrameType]) -> None:
        if self.durability_level == DurabilityLevel.DELAYED:
            self.committer_work()
        if signum in self.prev_signal_handlers:
            curr_handler = signal.getsignal(signum)
            if curr_handler == self.signal_handler:
                prev_handler = self.prev_signal_handlers[signum]
                signal.signal(signum, prev_handler)
                os.kill(os.getpid(), signum)
                signal.signal(signum, self.signal_handler)

    def start_handler(self) -> None:
        self.reader_work()
        for sig in (
            signal.SIGINT,
            signal.SIGTERM,
            signal.SIGQUIT,
            signal.SIGABRT,
            signal.SIGHUP,
            signal.SIGTSTP,
            signal.SIGPWR,
        ):
            self.prev_signal_handlers[sig] = signal.signal(sig, self.signal_handler)
        if self.enable_server:
            if self.server_thread is None:
                self.server_thread = self.start_server(self.server_address)
            else:
                LOGGER.warning(
                    "The server thread is already running. Ignoring duplicate start request."
                )
        if self.enable_client:
            if self.client_thread is None:
                self.client_thread = self.start_client(self.client_addresses)
            else:
                LOGGER.warning(
                    "The client thread is already running. Ignoring duplicate start request."
                )
        if self.enable_committer:
            if self.committer_thread is None:
                self.committer_thread = self.start_committer()
            else:
                LOGGER.warning(
                    "The committer thread is already running. Ignoring duplicate start request."
                )

    def client_work(self, client_addresses: List[Tuple[str, int]]) -> None:
        addresses = random.sample(
            client_addresses,
            min(len(client_addresses), ANTI_ENTROPY_MAX_CLIENTS),
        )

        for host, port in addresses:
            self.try_anti_entropy(host, port)

    def client_work_periodic(self, client_addresses: List[Tuple[str, int]]) -> None:
        while self.continue_client:
            self.client_work(client_addresses)
            time.sleep(ANTI_ENTROPY_INTERVAL)

    def start_client(self, client_addresses: List[Tuple[str, int]]) -> threading.Thread:
        client_thread = threading.Thread(
            target=self.client_work_periodic,
            args=(client_addresses,),
            daemon=True,
        )
        client_thread.start()
        return client_thread

    def server_work(self, address: Tuple[str, int]) -> None:
        host, port = address
        handler = self
        processor = AntiEntropy.Processor(handler)
        transport = thrift.transport.TSocket.TServerSocket(host=host, port=port)
        pfactory = thrift.protocol.TBinaryProtocol.TBinaryProtocolFactory()
        self.server = thrift.server.TNonblockingServer.TNonblockingServer(
            processor,
            transport,
            pfactory,
        )
        self.server.serve()

    def start_server(self, address: Tuple[str, int]) -> threading.Thread:
        server_thread = threading.Thread(
            target=self.server_work, args=(address,), daemon=True
        )
        server_thread.start()
        return server_thread

    def stop_handler(self) -> None:
        self.stop_server()
        self.stop_client()
        self.stop_committer()
        if self.durability_level == DurabilityLevel.DELAYED:
            self.committer_work()
        for sig, prev_handler in self.prev_signal_handlers.items():
            curr_handler = signal.getsignal(sig)
            if curr_handler == self.signal_handler:
                signal.signal(sig, prev_handler)

    def stop_server(self) -> None:
        if self.enable_server:
            while self.server is None:
                time.sleep(0.01)
            self.server.stop()

    def stop_client(self) -> None:
        self.continue_client = False

    def stop_committer(self) -> None:
        self.continue_committer = False

    def merge(self, other: datasketches.hll_sketch) -> datasketches.hll_sketch:
        union: datasketches.hll_union = datasketches.hll_union(LOG_K)
        union.update(self.sketch)
        union.update(other)
        return union.get_result()

    def imerge(self, other: datasketches.hll_sketch) -> None:
        LOGGER.debug("The set has ~%r elements", self.sketch.get_estimate())
        merged_sketch = self.merge(other)
        self.is_dirty = self.is_dirty or (
            merged_sketch.serialize_compact() != self.sketch.serialize_compact()
        )
        self.sketch = merged_sketch
        if self.durability_level == DurabilityLevel.STRICT:
            self.committer_work()
        LOGGER.debug("The set has ~%r elements", self.sketch.get_estimate())

    def push(self, other_serialized: bytes) -> None:
        other_deserialized: datasketches.hll_sketch = (
            datasketches.hll_sketch.deserialize(other_serialized)
        )
        self.imerge(other_deserialized)

    def pull(self) -> bytes:
        self_serialized: bytes = self.sketch.serialize_compact()
        return self_serialized

    def push_pull(self, client: thrift_helper.ThriftHelper) -> None:
        self_serialized: bytes = self.sketch.serialize_compact()
        client.push(self_serialized)
        other_serialized: bytes = client.pull()
        other_deserialized = datasketches.hll_sketch.deserialize(other_serialized)
        self.imerge(other_deserialized)

    def anti_entropy(self, host: str, port: int) -> None:
        client = thrift_helper.ThriftHelper(
            host,
            port,
            AntiEntropy.Client,
            reliable=False,
            timeout=ANTI_ENTROPY_TIMEOUT,
        )
        self.push_pull(client)

    def try_anti_entropy(self, *args: Any, **kwargs: Any) -> bool:
        try:
            self.anti_entropy(*args, **kwargs)
            return True
        except thrift.transport.TTransport.TTransportException:
            return False
