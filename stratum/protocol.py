"""Generic protocol primitives"""
import asyncio  # new module
import socket
from abc import abstractmethod
from datetime import datetime

import stringcase
from event_bus import EventBus

from stratum.network import Connection
from stratum.v2.message_types import FRAME
from stratum.v2.messages import (
    msg_type_class_map,
)


class Message:
    """Generic message that accepts visitors and dispatches their processing."""

    class VisitorMethodNotImplemented(Exception):
        """Custom handling to report if visitor method is missing"""

        def __init__(self, method_name):
            self.method_name = method_name

        def __str__(self):
            return self.method_name

    def __init__(self, req_id=None):
        self.req_id = req_id

    def accept(self, visitor):
        """Call visitor method based on the actual message type."""
        method_name = 'visit_{}'.format(stringcase.snakecase(type(self).__name__))
        try:
            visit_method = getattr(visitor, method_name)
        except AttributeError:
            raise self.VisitorMethodNotImplemented(method_name)

        visit_method(self)

    def _format(self, content):
        return '{}({})'.format(type(self).__name__, content)

    def to_frame(self):
        payload = self.to_bytes()
        # self.__class__.__name__ will return the derived class name
        frame = FRAME(0x0, self.__class__.__name__, payload)
        return frame

    # accepts an already decrypted message
    @staticmethod
    def from_frame(raw: bytes):
        extension_type = raw[0:1]
        msg_type = raw[2]  # U8
        msg_length = raw[3:5]  # U24
        raw = raw[6:]  # remove the common bytes

        msg_class = msg_type_class_map[msg_type]
        decoded_msg = msg_class.from_bytes(raw)
        return decoded_msg

    @abstractmethod
    def to_bytes(self):
        pass

    @abstractmethod
    def from_bytes(self):
        pass


class RequestRegistry:
    """Generates unique request ID for messages and provides simple registry"""

    def __init__(self):
        self.next_req_id = 0
        self.requests = dict()

    def push(self, req: Message):
        """Assigns a unique request ID to a message and registers it"""
        req.req_id = self.__next_req_id()
        assert (
            self.requests.get(req.req_id) is None
        ), 'BUG: request ID already present {}'.format(req.req_id)
        self.requests[req.req_id] = req

    def pop(self, req_id):
        return self.requests.pop(req_id, None)

    def __next_req_id(self):
        curr_req_id = self.next_req_id
        self.next_req_id += 1
        return curr_req_id


class ConnectionProcessor:
    """Receives and dispatches a message on a single connection."""

    def __init__(
        self,
        name: str,
        bus: EventBus,
        connection: Connection
    ):
        self.name = name
        self.bus = bus
        self.connection = connection
        self.request_registry = RequestRegistry()
        self.receive_loop_process = None

    def terminate(self):
        self.receive_loop_process.interrupt()

    def send_request(self, req):
        """Register the request and send it down the line"""
        self.request_registry.push(req)
        self._send_msg(req)

    @abstractmethod
    def _send_msg(self, msg):
        pass

    @abstractmethod
    def _recv_msg(self):
        pass

    @abstractmethod
    def _on_invalid_message(self, msg):
        pass

    def _emit_aux_msg_on_bus(self, log_msg: str):
        self.bus.emit(self.name, datetime.now(), self.connection.uid, log_msg)

    def _emit_protocol_msg_on_bus(self, log_msg: str, msg: Message):
        self._emit_aux_msg_on_bus('{}: {}'.format(log_msg, msg))

    async def receive_loop(self):
        """Receive process for a particular connection dispatches each received message
        """
        while True:
            try:
                messages = await self.connection.receive()
                for msg in messages:
                    msg.accept(self)
            except socket.timeout as e:
                print(e)
            except Message.VisitorMethodNotImplemented as e:
                print(
                    "{} doesn't implement:{}() for".format(type(self).__name_, e),
                    msg,
                )
                self._on_invalid_message(msg)
            await asyncio.sleep(0)


class UpstreamConnectionProcessor(ConnectionProcessor):
    """Processes messages flowing through an upstream node

    This class only determines the direction in which it accesses the connection.
    """

    def _send_msg(self, msg):
        self.connection.incoming.put(msg)

    def _recv_msg(self):
        return self.connection.outgoing.get()

    @abstractmethod
    def _on_invalid_message(self, msg):
        pass


class DownstreamConnectionProcessor(ConnectionProcessor):
    """Processes messages flowing through a downstream node

    This class only determines the direction in which it accesses the connection.
    Also, the downstream processor is able to initiate the shutdown of the connection.
    """

    def _send_msg(self, msg):
        self.connection.send_msg(msg)

    def _recv_msg(self):
        return self.connection.incoming.get()

    def disconnect(self):
        """Downstream node may initiate disconnect

        """
        self.connection.disconnect()

    @abstractmethod
    def _on_invalid_message(self, msg):
        pass
