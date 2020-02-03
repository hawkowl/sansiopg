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
from .conversion import Converter


@attr.s
class PostgreSQLClientProtocol(Protocol):

    username = attr.ib()
    _on_message = attr.ib()
    _parser = attr.ib(factory=ParserFeed)
    _waiter = attr.ib()

    def register_waiter(self):
        if self._waiter:
            raise Exception()

        self._waiter = defer.Deferred()
        return self._waiter

    def clear_waiter(self):
        if self._waiter:
            if not self._waiter.result:
                raise Exception()
        self._waiter = None

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

from automat import MethodicalMachine

@attr.s
class PostgresConnection(object):

    _machine = MethodicalMachine()

    _converter = attr.ib(factory=Converter)
    _targets = attr.ib(factory=list)

    @_machine.state(initial=True)
    def DISCONNECTED(self):
        """
        Not connected.
        """

    @_machine.state()
    def WAITING_FOR_READY(self):
        pass

    @_machine.state()
    def WAITING_FOR_PARSE(self):
        pass

    @_machine.state()
    def WAITING_FOR_DESCRIBE(self):
        pass

    @_machine.state()
    def WAITING_FOR_BIND(self):
        pass

    @_machine.state()
    def READY(self):
        pass

    @_machine.state()
    def NEEDS_AUTH(self):
        pass

    @_machine.state()
    def WAITING_FOR_QUERY_COMPLETE(self):
        pass


    @_machine.input()
    def connect(self, endpoint, username):
        pass

    @_machine.output()
    def do_connect(self, endpoint, username):
        from twisted.internet.protocol import Factory

        self._pg = PostgreSQLClientProtocol(username, self._onMessage)

        connected = defer.Deferred()
        cf = Factory.forProtocol(lambda: self._pg)
        return endpoint.connect(cf)

    @_machine.output()
    def wait_for_ready(self, endpoint, username):
        self._ready_callback = defer.Deferred()
        return self._ready_callback


    @_machine.input()
    def _REMOTE_READY_FOR_QUERY(self, message):
        pass

    @_machine.input()
    def _REMOTE_PARSE_COMPLETE(self, message):
        pass

    @_machine.output()
    def on_ready(self, message):
        self._ready_callback.callback(message.backend_status)

    DISCONNECTED.upon(connect, enter=WAITING_FOR_READY, outputs=[do_connect, wait_for_ready])

    WAITING_FOR_READY.upon(_REMOTE_PARSE_COMPLETE, enter=WAITING_FOR_DESCRIBE, output=[on_ready])


    async def extQuery(self, query, vals):

        self._dataRows = []
        self._desc = None

        self._state = NotARealStateMachine.WAITING_FOR_PARSE
        self._pg.sendParse(query)


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

        for row in self._desc.values:
            if row.field_name == b"?column?":
                row.field_name = b"anonymous"

        res = namedtuple(
            "Result", [x.field_name.decode("utf8") for x in self._desc.values]
        )

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
                print("Do not understand!", self._state)
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
                print("Do not understand!", self._state)
                print(message)

        elif self._state == NotARealStateMachine.WAITING_FOR_QUERY_COMPLETE:
            if isinstance(message, CommandComplete):
                self._state = NotARealStateMachine.WAITING_FOR_READY
                self._waiting, waiting = None, self._waiting
                waiting.callback(True)
            elif isinstance(message, DataRow):
                self._addDataRow(message)
            else:
                print("Do not understand!", self._state)
                print(message)

        else:
            print("current state", self._state)
            print("message", message)
