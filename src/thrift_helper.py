#!/usr/bin/env python3

import functools
import logging
import time
from typing import Any, Callable, Optional

import thrift.transport.TSocket
import thrift.transport.TTransport
import thrift.protocol.TBinaryProtocol


LOGGER: logging.Logger = logging.getLogger(__name__)


class ThriftHelper:
    def __init__(
        self,
        host: str,
        port: int,
        client_class,
        reliable: bool = False,
        timeout: Optional[int | float] = None,
    ) -> None:
        self._host = host
        self._port = port
        self._reliable = reliable
        (
            self._socket_transport,
            self._buffered_transport,
            self._client,
        ) = self._get_cached_client(self._host, self._port, client_class, timeout)

    @staticmethod
    @functools.cache
    def _get_cached_client(
        host: str, port: int, client_class, timeout: Optional[int | float] = None
    ):
        socket_transport = thrift.transport.TSocket.TSocket(host, port)
        socket_transport.setTimeout(timeout)
        buffered_transport = thrift.transport.TTransport.TFramedTransport(
            socket_transport
        )
        protocol = thrift.protocol.TBinaryProtocol.TBinaryProtocol(buffered_transport)
        client = client_class(protocol)
        return socket_transport, buffered_transport, client

    def _invoke(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        try:
            if not self._buffered_transport.isOpen():
                self._buffered_transport.open()
            method = getattr(self._client, method_name)
            return method(*args, **kwargs)
        except thrift.transport.TTransport.TTransportException:
            self._buffered_transport.close()
            raise

    def _invoke_best_effort_broadcast(
        self, method_name: str, *args: Any, **kwargs: Any
    ) -> Any:
        try:
            return self._invoke(method_name, *args, **kwargs)
        except thrift.transport.TTransport.TTransportException:
            LOGGER.debug(
                "Thrift networking exception",
                exc_info=True,
            )
            raise

    def _invoke_reliable_broadcast(
        self, method_name: str, *args: Any, **kwargs: Any
    ) -> Any:
        while True:
            try:
                return self._invoke(method_name, *args, **kwargs)
            except thrift.transport.TTransport.TTransportException:
                LOGGER.debug(
                    "Thrift networking exception, retrying",
                    exc_info=True,
                )
                time.sleep(1)

    def __getattr__(self, attr: str) -> Callable[..., Any]:
        wrapped = getattr(self._client, attr)

        @functools.wraps(wrapped)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return (
                self._invoke_reliable_broadcast(attr, *args, **kwargs)
                if self._reliable
                else self._invoke_best_effort_broadcast(attr, *args, **kwargs)
            )

        return wrapper
