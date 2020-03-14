import re
from collections import namedtuple

import attr
from automat import MethodicalMachine

from .conversion import Converter
from .messages import Notice, Error

_convert_to_underscores_lmao = re.compile(r"(?<!^)(?=[A-Z])")


def _get_last_collector(results):

    try:
        from twisted.internet.defer import Deferred, DeferredList
    except ImportError:
        Deferred = None

    results = list(results)

    if Deferred in map(type, results):
        for res in results:
            if not isinstance(res, Deferred):
                results.remove(res)

        r = DeferredList(list(results), fireOnOneErrback=True, consumeErrors=True)
        r.addCallback(lambda res: res[-1][-1])
        return r

    return results


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

    _io_impl = attr.ib()
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
    def RECEIVING_COPY_DATA(self):
        pass

    @_machine.state()
    def EXECUTING(self):
        pass

    @_machine.state()
    def WAITING_FOR_COPY_OUT_RESPONSE(self):
        pass

    @_machine.state()
    def COPY_OUT_COMPLETE(self):
        pass

    @_machine.state()
    def COMMAND_COMPLETE(self):
        pass

    @_machine.input()
    def _REMOTE_READY_FOR_QUERY(self, message):
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

    @_machine.input()
    def _REMOTE_COPY_OUT_RESPONSE(self, message):
        pass

    @_machine.input()
    def _REMOTE_COPY_DATA(self, message):
        pass

    @_machine.input()
    def _REMOTE_COPY_DONE(self, message):
        pass

    def _wait_for_ready(self, *args, **kwargs):
        self._ready_callback = self._io_impl.make_callback()
        return self._ready_callback

    @_machine.input()
    def connect(self, endpoint, database, username, password=None):
        pass

    @_machine.output()
    def do_connect(self, endpoint, database, username, password=None):

        if password:
            self._auth = password

        return self._io_impl.connect(self, endpoint, database, username)

    @_machine.output()
    def _wait_for_ready_on_connect(self, database, username, password):
        return self._wait_for_ready()

    @_machine.output()
    def _on_connected(self, message):
        if self._ready_callback:
            self._io_impl.trigger_callback(self._ready_callback, message.backend_status)

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
        _REMOTE_READY_FOR_QUERY, enter=READY, outputs=[_on_connected]
    )

    COMMAND_COMPLETE.upon(_REMOTE_READY_FOR_QUERY, enter=READY, outputs=[_on_connected])

    @_machine.input()
    def query(self, query, vals):
        pass

    @_machine.output()
    def _do_query(self, query, vals):
        self._currentQuery = query
        self._currentVals = vals
        self._dataRows = []
        self._ready_callback = self._io_impl.make_callback()
        self._pg.sendParse(query)

    @_machine.output()
    def _wait_for_result(self, query, vals):
        self._result_callback = self._io_impl.make_callback()
        self._io_impl.add_callback(self._result_callback, lambda x: self._collate())
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
        self._io_impl.add_callback(d, lambda x: None)
        return d

    @_machine.output()
    def _do_send_describe(self, message):
        self._pg.sendDescribe()

    WAITING_FOR_PARSE.upon(
        _REMOTE_PARSE_COMPLETE, enter=WAITING_FOR_DESCRIBE, outputs=[_do_send_describe]
    )

    @_machine.output()
    def _on_row_description(self, message):
        print(message.values)
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
    def _send_execute(self, message):
        self._pg.sendExecute("")

    WAITING_FOR_BIND.upon(
        _REMOTE_BIND_COMPLETE, enter=EXECUTING, outputs=[_send_execute]
    )

    @_machine.output()
    def _store_row(self, message):
        self._addDataRow(message)

    EXECUTING.upon(_REMOTE_DATA_ROW, enter=EXECUTING, outputs=[_store_row])

    @_machine.output()
    def _on_command_complete(self, message):
        self._currentQuery = None
        self._currentVals = None
        self._io_impl.trigger_callback(self._result_callback, True)
        self._pg.sync()

    EXECUTING.upon(
        _REMOTE_COMMAND_COMPLETE, enter=COMMAND_COMPLETE, outputs=[_on_command_complete]
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
            self._ready_callback = self._io_impl.make_callback()
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

    @_machine.input()
    def copy_out(self, target, table=None, query=None):
        pass

    @_machine.output()
    def _do_copy_out(self, target, table=None, query=None):

        self._copy_out_func = target

        if table is not None and query is not None:
            raise Exception("Only one must be provided")

        if table:
            target_query = "COPY " + table.replace('"', '""') + " TO STDOUT"
        elif query:
            target_query = "COPY (" + query + ") TO STDOUT"

        # target_query += " WITH (FORMAT binary)"

        self._currentQuery = query
        self._currentVals = []
        self._ready_callback = self._io_impl.make_callback()
        self._pg.sendParse(target_query)

        return self._ready_callback

    READY.upon(
        copy_out,
        enter=WAITING_FOR_PARSE,
        outputs=[_do_copy_out],
        collector=_get_last_collector,
    )

    @_machine.output()
    def _on_copy_out_response(self, message):
        pass

    EXECUTING.upon(
        _REMOTE_COPY_OUT_RESPONSE,
        enter=RECEIVING_COPY_DATA,
        outputs=[_on_copy_out_response],
    )

    @_machine.output()
    def _on_copy_data(self, message):
        self._copy_out_func(message)

    RECEIVING_COPY_DATA.upon(
        _REMOTE_COPY_DATA, enter=RECEIVING_COPY_DATA, outputs=[_on_copy_data]
    )

    RECEIVING_COPY_DATA.upon(_REMOTE_COPY_DONE, enter=COPY_OUT_COMPLETE, outputs=[])

    @_machine.output()
    def _on_copy_out_complete(self, message):
        # self._io_impl.trigger_callback(self._copy_out_complete_callback, True)
        pass

    COPY_OUT_COMPLETE.upon(
        _REMOTE_COMMAND_COMPLETE,
        enter=COMMAND_COMPLETE,
        outputs=[_on_copy_out_complete],
    )
