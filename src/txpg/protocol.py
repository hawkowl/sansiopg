import attr

from twisted.internet import defer
from twisted.internet.protocol import Protocol, Factory
from sansiopg.messages import (
    Bind,
    Describe,
    Execute,
    Flush,
    Close,
    Parse,
    PasswordMessage,
    Query,
    StartupMessage,
    Sync,
)


from sansiopg.parser import ParserFeed


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


@attr.s
class TwistedIOImplementation:

    debug = attr.ib(default=False)

    def connect(self, connection, endpoint, database, username, password=None):

        connection._pg = PostgreSQLClientProtocol(
            database,
            username,
            connection._onMessage,
            encoding=connection.encoding,
            debug=self.debug,
        )
        cf = Factory.forProtocol(lambda: connection._pg)

        return endpoint.connect(cf)

    def make_callback(self):
        return defer.Deferred()

    def trigger_callback(self, future, result):
        return future.callback(result)

    def add_callback(self, future, callback):
        future.addCallback(callback)
