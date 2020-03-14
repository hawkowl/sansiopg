import re
import struct
from collections import namedtuple
from enum import Enum

import attr
from automat import MethodicalMachine
from twisted.internet import defer
from twisted.internet.protocol import Protocol

from .conversion import Converter
from .messages import (
    AuthenticationOk,
    BackendKeyData,
    Bind,
    BindComplete,
    BindParam,
    CommandComplete,
    DataRow,
    DataType,
    Describe,
    Error,
    Execute,
    Flush,
    FormatType,
    Notice,
    ParameterStatus,
    Close,
    Parse,
    ParseComplete,
    PasswordMessage,
    NoData,
    Query,
    ReadyForQuery,
    RowDescription,
    StartupMessage,
    Sync,
)
from .parser import ParserFeed

_convert_to_underscores_lmao = re.compile(r"(?<!^)(?=[A-Z])")


@attr.s
class PostgreSQLClientProtocol(Protocol):

    database = attr.ib()
    username = attr.ib()
    _on_message = attr.ib()
    _debug = attr.ib(default=False)
    _encoding = attr.ib(default="utf8")
    _parser = attr.ib()

    @_parser.default
    def _parser_build(self):
        return ParserFeed(self._encoding)

    def send(self, msg):
        if self._debug:
            print(">>> " + repr(msg))
        self.transport.write(msg.ser())

    def connectionMade(self):
        s = StartupMessage(
            parameters={"user": self.username, "database": self.database},
            encoding=self._encoding,
        )

        self.send(s)

    def dataReceived(self, data):
        messages = self._parser.feed(data)

        for i in messages:
            if self._debug:
                print("<<< " + repr(i))
            self._on_message(i)

    def sendQuery(self, query):
        q = Query(self._encoding, query)
        self.send(q)

    def sendParse(self, query, name=""):
        p = Parse(self._encoding, name, query)
        self.send(p)
        self.flush()

    def sendDescribe(self, name=""):
        d = Describe(self._encoding, "")
        self.send(d)
        self.flush()

    def sendBind(self, bind):
        b = Bind(self._encoding, "", "", bind, None)
        self.send(b)
        self.flush()

    def flush(self):
        f = Flush()
        self.send(f)

    def sendSync(self):
        s = Sync()
        self.send(s)

    def sendExecute(self, portal, rows_to_return=0):
        e = Execute(self._encoding, portal, rows_to_return)
        self.send(e)
        self.flush()

    def sendAuth(self, password):
        m = PasswordMessage(self._encoding, password)
        self.send(m)
        self.flush()

    def close(self):
        m = Close(self._encoding, "P", "")
        self.send(m)
        self.flush()

    def sync(self):
        m = Sync()
        self.send(m)
        self.flush()


def _get_last_collector(results):

    results = list(results)

    for res in results:
        if not isinstance(res, defer.Deferred):
            results.remove(res)

    r = defer.DeferredList(list(results), fireOnOneErrback=True, consumeErrors=True)
    r.addCallback(lambda res: res[-1][-1])
    return r


@attr.s
class Transaction:

    _conn = attr.ib()

    def begin(self):
        return self._conn.execute("BEGIN", [])

    def commit(self):
        return self._conn.execute("COMMIT", [])

    def rollback(self):
        return self._conn.execute("ROLLBACK", [])

    async def __aenter__(self):
        await self.begin()
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        if tb is None:
            await self.commit()
        else:
            await self.rollback()


@attr.s
class PostgresConnection(object):

    _machine = MethodicalMachine()

    encoding = attr.ib(default="utf8")
    _converter = attr.ib(factory=Converter)
    _dataRows = attr.ib(factory=list, init=False, repr=False)
    _auth = attr.ib(default=None, init=False, repr=False)
    _parameters = attr.ib(factory=dict, init=False)

    @_machine.state(initial=True)
    def DISCONNECTED(self):
        """
        Not connected.
        """

    @_machine.state()
    def CONNECTING(self):
        pass

    @_machine.state()
    def WAITING_FOR_AUTH(self):
        pass

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
    def WAITING_FOR_CLOSE(self):
        pass

    @_machine.state()
    def READY(self):
        pass

    @_machine.state()
    def NEEDS_AUTH(self):
        pass

    @_machine.state()
    def WAITING_FOR_COMMAND_COMPLETE(self):
        pass

    @_machine.state()
    def COMMAND_COMPLETE(self):
        pass

    @_machine.input()
    def _REMOTE_READY(self, message):
        pass

    @_machine.input()
    def _REMOTE_PARSE_COMPLETE(self, message):
        pass

    @_machine.input()
    def _REMOTE_PARSE_COMPLETE(self, message):
        pass

    @_machine.input()
    def _REMOTE_ROW_DESCRIPTION(self, message):
        pass

    @_machine.input()
    def _REMOTE_BIND_COMPLETE(self, message):
        pass

    @_machine.input()
    def _REMOTE_COMMAND_COMPLETE(self, message):
        pass

    @_machine.input()
    def _REMOTE_DATA_ROW(self, message):
        pass

    @_machine.input()
    def _REMOTE_NO_DATA(self, message):
        pass

    @_machine.input()
    def _REMOTE_AUTHENTICATION_OK(self, message):
        pass

    @_machine.input()
    def _REMOTE_AUTHENTICATION_CLEARTEXT_PASSWORD(self, message):
        pass

    @_machine.input()
    def _REMOTE_CLOSE_COMPLETE(self, message):
        pass

    @_machine.input()
    def _REMOTE_PARAMETER_STATUS(self, message):
        pass

    def _wait_for_ready(self, *args, **kwargs):
        self._ready_callback = defer.Deferred()
        return self._ready_callback

    @_machine.input()
    def connect(self, endpoint, database, username, password=None):
        pass

    @_machine.output()
    def do_connect(self, endpoint, database, username, password=None):
        from twisted.internet.protocol import Factory

        if password:
            self._auth = password

        self._pg = PostgreSQLClientProtocol(
            database, username, self._onMessage, encoding=self.encoding
        )

        connected = defer.Deferred()
        cf = Factory.forProtocol(lambda: self._pg)
        return endpoint.connect(cf)

    @_machine.output()
    def _wait_for_ready_on_connect(self, database, username, password):
        return self._wait_for_ready()

    @_machine.output()
    def _on_connected(self, message):
        if self._ready_callback:
            self._ready_callback.callback(message.backend_status)

    DISCONNECTED.upon(
        connect,
        enter=CONNECTING,
        outputs=[do_connect, _wait_for_ready_on_connect],
        collector=_get_last_collector,
    )

    @_machine.output()
    def _send_auth_plaintext(self, message):
        self._pg.sendAuth(self._auth)

        # Let's not store this in memory.
        self._auth = None

    CONNECTING.upon(
        _REMOTE_AUTHENTICATION_CLEARTEXT_PASSWORD,
        enter=WAITING_FOR_AUTH,
        outputs=[_send_auth_plaintext],
    )

    WAITING_FOR_AUTH.upon(
        _REMOTE_AUTHENTICATION_OK, enter=WAITING_FOR_READY, outputs=[]
    )

    CONNECTING.upon(_REMOTE_AUTHENTICATION_OK, enter=WAITING_FOR_READY, outputs=[])

    @_machine.output()
    def _register_parameter(self, message):
        self._parameters[message.name] = message.val

        if message.name == "server_encoding":
            self._pg._encoding = message.val

    WAITING_FOR_READY.upon(
        _REMOTE_PARAMETER_STATUS, enter=WAITING_FOR_READY, outputs=[_register_parameter]
    )

    WAITING_FOR_READY.upon(
        _REMOTE_READY, enter=READY, outputs=[_on_connected]
    )

    COMMAND_COMPLETE.upon(
        _REMOTE_READY, enter=READY, outputs=[_on_connected]
    )

    @_machine.input()
    def query(self, query, vals):
        pass

    @_machine.output()
    def _do_query(self, query, vals):
        self._currentQuery = query
        self._currentVals = vals
        self._dataRows = []
        self._ready_callback = defer.Deferred()
        self._pg.sendParse(query)

    @_machine.output()
    def _wait_for_result(self, query, vals):
        self._result_callback = defer.Deferred()
        self._result_callback.addCallback(lambda x: self._collate())
        return self._result_callback

    @_machine.output()
    def _wait_for_ready_on_query(self, query, vals):
        return self._wait_for_ready()

    READY.upon(
        query,
        enter=WAITING_FOR_PARSE,
        outputs=[_do_query, _wait_for_ready_on_query, _wait_for_result],
        collector=_get_last_collector,
    )

    def execute(self, command, args=[]):
        d = self.query(command, args)
        d.addCallback(lambda x: None)
        return d

    @_machine.output()
    def _do_send_describe(self, message):
        self._pg.sendDescribe()

    WAITING_FOR_PARSE.upon(
        _REMOTE_PARSE_COMPLETE, enter=WAITING_FOR_DESCRIBE, outputs=[_do_send_describe]
    )

    @_machine.output()
    def _on_row_description(self, message):
        self._currentDescription = message.values

    @_machine.output()
    def _do_bind(self, message):
        bind_vals = [self._converter.to_postgres(x) for x in self._currentVals]
        self._pg.sendBind(bind_vals)

    WAITING_FOR_DESCRIBE.upon(
        _REMOTE_ROW_DESCRIPTION,
        enter=WAITING_FOR_BIND,
        outputs=[_on_row_description, _do_bind],
    )

    WAITING_FOR_DESCRIBE.upon(
        _REMOTE_NO_DATA, enter=WAITING_FOR_BIND, outputs=[_do_bind]
    )

    @_machine.output()
    def _on_bind_complete(self, message):
        self._pg.sendExecute("")

    WAITING_FOR_BIND.upon(
        _REMOTE_BIND_COMPLETE,
        enter=WAITING_FOR_COMMAND_COMPLETE,
        outputs=[_on_bind_complete],
    )

    @_machine.output()
    def _store_row(self, message):
        self._addDataRow(message)

    WAITING_FOR_COMMAND_COMPLETE.upon(
        _REMOTE_DATA_ROW, enter=WAITING_FOR_COMMAND_COMPLETE, outputs=[_store_row]
    )

    @_machine.output()
    def _on_command_complete(self, message):
        self._currentQuery = None
        self._currentVals = None
        self._result_callback.callback(True)
        self._pg.sync()

    WAITING_FOR_COMMAND_COMPLETE.upon(
        _REMOTE_COMMAND_COMPLETE,
        enter=COMMAND_COMPLETE,
        outputs=[_on_command_complete],
    )

    def _addDataRow(self, msg):
        self._dataRows.append(msg.values)

    def _collate(self):
        """
        Collate the responses of a query.
        """
        if not self._dataRows:
            return []

        for row in self._currentDescription:
            if row.field_name == b"?column?":
                row.field_name = b"anonymous"

        res = namedtuple(
            "Result", [x.field_name.decode("utf8") for x in self._currentDescription]
        )

        resp = []

        for i in self._dataRows:
            row = []
            for x, form in zip(i, self._currentDescription):
                row.append(self._converter.from_postgres(x, form))
            resp.append(res(*row))

        self._dataRows.clear()
        self._currentDescription = None

        return resp

    @_machine.input()
    def close(self):
        pass

    @_machine.output()
    def _do_close(self):
        if not self._ready_callback:
            self._ready_callback = defer.Deferred()
        self._pg.close()
        return self._ready_callback

    READY.upon(
        close,
        enter=WAITING_FOR_CLOSE,
        outputs=[_do_close],
        collector=_get_last_collector,
    )

    WAITING_FOR_CLOSE.upon(_REMOTE_CLOSE_COMPLETE, enter=WAITING_FOR_READY, outputs=[])

    def _onMessage(self, message):

        # These can come at any time
        if isinstance(message, Notice):
            return
        elif isinstance(message, Error):
            print(message)
            self._pg.transport.loseConnection()
            return

        rem = _convert_to_underscores_lmao.sub("_", message.__class__.__name__).upper()
        func = getattr(self, "_REMOTE_" + rem, None)

        if func is None:
            print(f"Ignoring incoming message {message} as {rem}")
            return

        func(message)
        return

    def new_transaction(self):
        return Transaction(self)
