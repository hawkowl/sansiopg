import attr
from twisted.internet.protocol import Protocol
from twisted.internet import defer

import struct
from .messages import (
    StartupMessage,
    parse_from_buffer,
    AuthenticationOk,
    Query,
    BackendKeyData,
    DataRow,
    CommandComplete,
    Parse, ParameterStatus, Bind, ReadyForQuery, Describe, Notice, Error, Flush
)


@attr.s
class ParserFeed(object):

    _buffer = attr.ib(default=b"")

    def feed(self, input):

        self._buffer += input

        messages = []

        while True:
            if len(self._buffer) < 5:
                # We can't even get the message, so, return
                return messages

            # Get the length of the message
            msg_len, = struct.unpack("!i", self._buffer[1:5])

            # Check if we have the whole message
            if len(self._buffer) < msg_len + 1:
                print("Dont have for", self._buffer)
                return messages

            # If we do, split it up
            msg = self._buffer[0 : msg_len + 1]
            self._buffer = self._buffer[msg_len + 1 :]

            messages.append(parse_from_buffer(msg))


@attr.s
class PostgreSQLClientProtocol(Protocol):

    username = attr.ib()
    _on_message = attr.ib()
    _parser = attr.ib(factory=ParserFeed)

    def send(self, msg):
        print(">>> " + repr(msg.ser()))
        self.transport.write(msg.ser())

    def connectionMade(self):
        s = StartupMessage(parameters={"user": self.username})

        self.send(s)

    def dataReceived(self, data):
        print("<<< " + repr(data))

        messages = self._parser.feed(data)

        for i in messages:
            self._on_message(i)

    def sendQuery(self, query):
        q = Query(query)
        self.send(q)

    def sendParse(self, query, name=""):
        p = Parse(name, query)
        self.send(p)

        f = Flush()
        self.send(f)

    def sendBind(self, bind):
        b = Bind("", "test", bind, None)
        self.send(b)

class PostgresConnection(object):
    def connect(self, endpoint, username):

        self._queryDeferred = None

        self._pg = PostgreSQLClientProtocol(username, self._onMessage)

        self._connectedDeferred = defer.Deferred()

        cf = Factory.forProtocol(lambda: self._pg)
        endpoint.connect(cf)

        return self._connectedDeferred

    def query(self, query):
        self._pg.sendQuery(query)

        self._dataRows = []
        self._queryDeferred = defer.Deferred()
        return self._queryDeferred

    async def extQuery(self, query, vals):

        self._parsedDeferred = defer.Deferred()
        self._pg.sendParse(query)

        await self._parsedDeferred

        self._bindDeferred = defer.Deferred()
        self._pg.sendBind(vals)

        await self._bindDeferred

    def _addDataRow(self, msg):
        self._dataRows.append(msg.values)

    def _collate(self):
        """
        Collate the responses of a query.
        """
        return self._dataRows

    def _onMessage(self, message):

        if isinstance(message, ReadyForQuery) and self._connectedDeferred:
            self._connectedDeferred.callback(True)
            self._connectedDeferred = None
            return
        elif isinstance(message, ParameterStatus):
            pass
        elif isinstance(message, DataRow):
            self._addDataRow(message)

        elif isinstance(message, CommandComplete):
            self._queryDeferred.callback(self._collate())

        elif isinstance(message, Notice):
            pass
        elif isinstance(message, Error):
            print(message)
            self._pg.transport.loseConnection()

        else:
            print("Not handling", message)


if __name__ == "__main__":

    from twisted.internet import endpoints, reactor
    from twisted.internet.protocol import Factory
    from twisted.internet.task import react

    async def main(reactor):

        e = endpoints.HostnameEndpoint(reactor, "localhost", 5432)
        conn = PostgresConnection()

        await conn.connect(e, "hawkowl")

        resp = await conn.extQuery("SELECT usename FROM pg_user WHERE usename = $1", tuple(b'4'))

        print("Got return!")
        print(resp)

    react(lambda r: defer.ensureDeferred(main(r)))
