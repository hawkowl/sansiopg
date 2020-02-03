import struct

import attr

from .messages import parse_from_buffer


@attr.s
class ParserFeed(object):

    _server_encoding = attr.ib()
    _buffer = attr.ib(default=b"", init=False)

    def feed(self, input):

        self._buffer += input

        messages = []

        while True:
            if len(self._buffer) < 5:
                # We can't even get the message, so, return
                return messages

            # Get the length of the message
            (msg_len,) = struct.unpack("!i", self._buffer[1:5])

            # Check if we have the whole message
            if len(self._buffer) < msg_len + 1:
                print("Dont have for", self._buffer)
                return messages

            # If we do, split it up
            msg = self._buffer[0 : msg_len + 1]
            self._buffer = self._buffer[msg_len + 1 :]

            messages.append(parse_from_buffer(msg, self._server_encoding))
