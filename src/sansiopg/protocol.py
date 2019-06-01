import attr
from twisted.internet.protocol import Protocol
from twisted.internet import defer
from collections import namedtuple

from enum import Enum
import struct
from .messages import (
    StartupMessage,
    FormatType,
    DataType,
    AuthenticationOk,
    Query,
    BackendKeyData,
    BindParam,
    Execute,
    DataRow,
    Sync,
    CommandComplete,
    RowDescription,
    Parse,
    ParameterStatus,
    Bind,
    ReadyForQuery,
    Describe,
    Notice,
    Error,
    Flush,
    ParseComplete,
    BindComplete,
)
from .parser import ParserFeed


@attr.s
class PostgreSQLClientProtocol(Protocol):

    username = attr.ib()
    _on_message = attr.ib()
    _parser = attr.ib(factory=ParserFeed)

    def send(self, msg):
        print(">>> " + repr(msg))
        self.transport.write(msg.ser())

    def connectionMade(self):
        s = StartupMessage(parameters={"user": self.username})

        self.send(s)

    def dataReceived(self, data):
        messages = self._parser.feed(data)

        for i in messages:
            print("<<< " + repr(i))
            self._on_message(i)

    def sendQuery(self, query):
        q = Query(query)
        self.send(q)

    def sendParse(self, query, name=""):
        p = Parse(name, query)
        self.send(p)
        self.flush()

    def sendDescribe(self, name=""):
        d = Describe("")
        self.send(d)
        self.flush()

    def sendBind(self, bind):
        b = Bind("", "", bind, None)
        self.send(b)
        self.flush()

    def flush(self):
        f = Flush()
        self.send(f)

    def sendSync(self):
        s = Sync()
        self.send(s)

    def sendExecute(self, portal, rows_to_return=0):
        e = Execute(portal, rows_to_return)
        self.send(e)
        self.flush()


class NotARealStateMachine(Enum):

    DISCONNECTED = 0
    WAITING_FOR_READY = 1
    WAITING_FOR_PARSE = 2
    WAITING_FOR_DESCRIBE = 7
    WAITING_FOR_BIND = 3
    READY = 4
    NEEDS_AUTH = 5
    WAITING_FOR_QUERY_COMPLETE = 6


def _int_to_postgres(val):
    return BindParam(0, str(val).encode("utf8"))


def _str_to_postgres(val):
    return BindParam(0, val.encode("utf8"))


def _text_text_from_postgres(val):
    return val.decode("utf8")


def _bool_text_from_postgres(val):
    if val == b"t":
        return True
    elif val == b"f":
        return False
    else:
        raise ValueError()


class PostgresConnection(object):
    def __init__(self):
        self._converters = {
            int: _int_to_postgres,
            str: _str_to_postgres,
            (DataType.NAME, FormatType.TEXT): _text_text_from_postgres,
            (DataType.TEXT, FormatType.TEXT): _text_text_from_postgres,
            (DataType.BOOL, FormatType.TEXT): _bool_text_from_postgres,
        }

    def convertToPostgres(self, value):

        try:
            conv = self._converters.get(type(value))
            return conv(value)
        except:
            print("Can't convert ", value)
            raise ValueError()

    def convertFromPostgres(self, value, row_format):

        try:
            conv = self._converters.get((row_format.data_type, row_format.format_code))
            return conv(value)
        except:
            # print("Can't convert ", value, row_format)
            # print("Falling back to text decode")
            if row_format.format_code == FormatType.TEXT:
                return value.decode("utf8")

    def connect(self, endpoint, username):

        self._state = NotARealStateMachine.WAITING_FOR_READY
        self._waiting = None

        self._pg = PostgreSQLClientProtocol(username, self._onMessage)

        self._waiting = defer.Deferred()

        cf = Factory.forProtocol(lambda: self._pg)
        endpoint.connect(cf)

        return self._waiting

    async def extQuery(self, query, vals):

        self._dataRows = []
        self._desc = None

        self._waiting = defer.Deferred()
        self._state = NotARealStateMachine.WAITING_FOR_PARSE
        self._pg.sendParse(query)

        await self._waiting

        self._waiting = defer.Deferred()
        self._state = NotARealStateMachine.WAITING_FOR_DESCRIBE
        self._pg.sendDescribe()

        await self._waiting

        bind_vals = [self.convertToPostgres(x) for x in vals]

        self._waiting = defer.Deferred()
        self._state = NotARealStateMachine.WAITING_FOR_BIND
        self._pg.sendBind(bind_vals)

        await self._waiting

        self._waiting = defer.Deferred()
        self._state = NotARealStateMachine.WAITING_FOR_QUERY_COMPLETE
        self._pg.sendExecute("")

        await self._waiting

        self._waiting = defer.Deferred()
        self._state = NotARealStateMachine.WAITING_FOR_READY
        self._pg.sendSync()

        await self._waiting

        return self._collate()

    def _addDataRow(self, msg):
        self._dataRows.append(msg.values)

    def _collate(self):
        """
        Collate the responses of a query.
        """

        res = namedtuple("Result", [x.field_name.decode('utf8') for x in self._desc.values])

        resp = []

        for i in self._dataRows:
            row = []
            for x, form in zip(i, self._desc.values):
                row.append(self.convertFromPostgres(x, form))
            resp.append(res(*row))

        return resp

    def _onMessage(self, message):

        # These can come at any time
        if isinstance(message, Notice):
            return
        elif isinstance(message, Error):
            print(message)
            self._pg.transport.loseConnection()
            return

        if self._state == NotARealStateMachine.WAITING_FOR_READY:
            if isinstance(message, ReadyForQuery):
                self._state = NotARealStateMachine.READY
                self._waiting, waiting = None, self._waiting
                waiting.callback(True)
            elif isinstance(message, ParameterStatus):
                # Don't care
                pass
            elif isinstance(message, AuthenticationOk):
                # Don't care
                pass
            elif isinstance(message, BackendKeyData):
                # Don't care
                pass
            else:
                print("Do not understand!")
                print(message)

        elif self._state == NotARealStateMachine.WAITING_FOR_PARSE:
            if isinstance(message, ParseComplete):
                self._state = NotARealStateMachine.READY
                self._waiting, waiting = None, self._waiting
                waiting.callback(True)
            else:
                print("Do not understand!")
                print(message)

        elif self._state == NotARealStateMachine.WAITING_FOR_DESCRIBE:
            if isinstance(message, RowDescription):
                self._desc = message
                self._state = NotARealStateMachine.READY
                self._waiting, waiting = None, self._waiting
                waiting.callback(True)

        elif self._state == NotARealStateMachine.WAITING_FOR_BIND:
            if isinstance(message, BindComplete):
                self._state = NotARealStateMachine.READY
                self._waiting, waiting = None, self._waiting
                waiting.callback(True)
            else:
                print("Do not understand!")
                print(message)

        elif self._state == NotARealStateMachine.WAITING_FOR_QUERY_COMPLETE:
            if isinstance(message, CommandComplete):
                self._state = NotARealStateMachine.WAITING_FOR_READY
                self._waiting, waiting = None, self._waiting
                waiting.callback(True)
            elif isinstance(message, DataRow):
                self._addDataRow(message)
            else:
                print("Do not understand!")
                print(message)

        else:
            print("current state", self._state)
            print("message", message)


if __name__ == "__main__":

    from twisted.internet import endpoints, reactor
    from twisted.internet.protocol import Factory
    from twisted.internet.task import react

    async def main(reactor):

        e = endpoints.HostnameEndpoint(reactor, "localhost", 5432)
        conn = PostgresConnection()

        r = await conn.connect(e, "hawkowl")

        resp = await conn.extQuery("SELECT * FROM pg_user", tuple())

        print("Result:", resp)

    react(lambda r: defer.ensureDeferred(main(r)))
