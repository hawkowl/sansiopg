import enum
import struct
from enum import Enum

import attr


class FormatType(Enum):
    TEXT = 0
    BINARY = 1


class DataType(Enum):
    BOOL = 16
    NAME = 19
    INT4 = 23
    TEXT = 25
    OID = 26
    ABSTIME = 702
    _TEXT = 1009


class FrontendMessageType(Enum):
    BIND = b"B"
    CLOSE = b"C"
    DESCRIBE = b"D"
    EXECUTE = b"E"
    FLUSH = b"H"
    PARSE = b"P"
    PASSWORD_MESSAGE = b"p"
    SYNC = b"S"
    QUERY = b"Q"
    UNKNOWN = None

    def _missing_(value):
        return FrontendMessageType.UNKNOWN


class BackendTransactionStatus(Enum):

    IDLE = b"I"
    IN_TRANSACTION = b"T"
    IN_ERRORED_TRANSACTION = b"E"


@enum.unique
class BackendMessageType(Enum):
    COMMAND_COMPLETE = b"C"
    DATA_ROW = b"D"
    ERROR = b"E"
    BACKEND_KEY_DATA = b"K"
    NOTICE = b"N"
    AUTHENTICATION_REQUEST = b"R"
    PARAMETER_STATUS = b"S"
    ROW_DESCRIPTION = b"T"
    PARAMETER_DESCRIPTION = b"t"
    READY_FOR_QUERY = b"Z"
    NO_DATA = b"n"
    PARSE_COMPLETE = b"1"
    BIND_COMPLETE = b"2"
    CLOSE_COMPLETE = b"3"
    UNKNOWN = None

    def _missing_(value):
        return BackendMessageType.UNKNOWN


@attr.s
class StartupMessage(object):

    _encoding = attr.ib()
    protocol_version_number = attr.ib(default=196608)
    parameters = attr.ib(default={})

    def ser(self):

        res = []

        res.append(struct.pack("!i", self.protocol_version_number))

        for key, val in self.parameters.items():
            res.append(key.encode(self._encoding))
            res.append(b"\0")
            res.append(val.encode(self._encoding))
            res.append(b"\0")

        res.append(b"\0")

        msg = b"".join(res)
        return struct.pack("!i", len(msg) + 4) + msg


@attr.s
class ReadyForQuery(object):
    backend_status = attr.ib()

    @classmethod
    def deser(cls, buf, server_encoding):
        backend_status = BackendTransactionStatus(buf[5:])

        return cls(backend_status=backend_status)


@attr.s
class Query(object):

    _encoding = attr.ib()
    query = attr.ib()

    def ser(self):

        res = [self.query.encode(self._encoding), b"\0"]

        msg = b"".join(res)
        return MessageType.QUERY + struct.pack("!i", len(msg) + 4) + msg


@attr.s
class Parse(object):

    _encoding = attr.ib()
    prepared_statement_name = attr.ib()
    query = attr.ib()

    def ser(self):

        res = [
            self.prepared_statement_name.encode(self._encoding),
            b"\0",
            self.query.encode(self._encoding),
            b"\0",
        ]

        # We don't support prespecifying types
        res.append(struct.pack("!h", 0))

        msg = b"".join(res)
        return FrontendMessageType.PARSE.value + struct.pack("!i", len(msg) + 4) + msg


@attr.s
class ParseComplete(object):
    @classmethod
    def deser(cls, buf, server_encoding):
        return cls()


@attr.s
class Describe(object):

    _encoding = attr.ib()
    prepared_statement_name = attr.ib()

    def ser(self):

        res = [b"S", self.prepared_statement_name.encode(self._encoding), b"\0"]

        msg = b"".join(res)
        return (
            FrontendMessageType.DESCRIBE.value + struct.pack("!i", len(msg) + 4) + msg
        )


@attr.s
class BindParam(object):

    format_code = attr.ib()
    value = attr.ib()


@attr.s
class Bind(object):

    _encoding = attr.ib()
    destination_portal = attr.ib()
    prepared_statement = attr.ib()
    parameters = attr.ib()
    result_format_codes = attr.ib()

    def ser(self):

        res = []

        res.append(self.destination_portal.encode(self._encoding))
        res.append(b"\0")
        res.append(self.prepared_statement.encode(self._encoding))
        res.append(b"\0")

        # No input format codes
        res.append(struct.pack("!h", 0))

        res.append(struct.pack("!h", len(self.parameters)))

        for p in self.parameters:
            res.append(struct.pack("!i", len(p.value)))
            res.append(p.value)

        # No result format codes
        res.append(struct.pack("!h", 0))

        msg = b"".join(res)
        return FrontendMessageType.BIND.value + struct.pack("!i", len(msg) + 4) + msg


@attr.s
class BindComplete(object):
    @classmethod
    def deser(cls, buf, server_encoding):
        return cls()


@attr.s
class Sync(object):
    def ser(self):
        return FrontendMessageType.SYNC.value + struct.pack("!i", 4)


@attr.s
class Execute(object):

    _encoding = attr.ib()
    portal_name = attr.ib()
    rows_to_return = attr.ib()

    def ser(self):

        res = [self.portal_name.encode(self._encoding), b"\0"]

        res.append(struct.pack("!i", self.rows_to_return))

        msg = b"".join(res)
        return FrontendMessageType.EXECUTE.value + struct.pack("!i", len(msg) + 4) + msg


@attr.s
class NoData:
    @classmethod
    def deser(cls, buf, server_encoding):
        return cls()


@attr.s
class IndividualRow(object):
    field_name = attr.ib()
    data_type = attr.ib(converter=DataType)
    type_modifier = attr.ib()
    format_code = attr.ib(converter=FormatType)


@attr.s
class RowDescription(object):

    values = attr.ib()

    @classmethod
    def deser(cls, buf, server_encoding):

        (col_values,) = struct.unpack("!h", buf[5:7])
        content = buf[7:]

        vals = []

        for x in range(col_values):

            field_name, rest = content.split(b"\0", 1)

            (
                table_obj_id,
                col_attr_num,
                data_type,
                data_type_size,
                type_modifier,
                format_code,
            ) = struct.unpack("!ihihih", rest[:18])

            content = rest[18:]

            vals.append(
                IndividualRow(
                    field_name=field_name,
                    data_type=data_type,
                    type_modifier=type_modifier,
                    format_code=format_code,
                )
            )

        return cls(values=tuple(vals))


@attr.s
class ParameterDescription:

    object_ids = attr.ib()

    @classmethod
    def deser(cls, buf, server_encoding):
        (parameter_count,) = struct.unpack("!h", buf[5:7])
        ids = struct.unpack("!" + "i" * parameter_count, buf[7:])
        return cls(object_ids=ids)


@attr.s
class DataRow(object):

    values = attr.ib()

    @classmethod
    def deser(cls, buf, server_encoding):

        (col_values,) = struct.unpack("!h", buf[5:7])
        content = buf[7:]

        vals = []

        for x in range(col_values):
            (length_of_next,) = struct.unpack("!i", content[0:4])
            col_val = content[4 : length_of_next + 4]
            vals.append(col_val)
            content = content[length_of_next + 4 :]

        return cls(values=tuple(vals))


@attr.s
class CommandComplete(object):

    cmd = attr.ib()

    @classmethod
    def deser(cls, buf, server_encoding):
        content = buf[5:-1].decode(server_encoding)
        return cls(cmd=content)


@attr.s
class ParameterStatus(object):

    name = attr.ib()
    val = attr.ib()

    @classmethod
    def deser(cls, buf, server_encoding):

        key, val = buf[5:-1].split(b"\0")

        key = key.decode(server_encoding)
        val = val.decode(server_encoding)

        return cls(name=key, val=val)


@attr.s
class BackendKeyData(object):

    process_id = attr.ib()
    secret_key = attr.ib()

    @classmethod
    def deser(cls, buf, server_encoding):
        proc_id, secret_key = struct.unpack("!ii", buf[5:])
        return cls(process_id=proc_id, secret_key=secret_key)


@attr.s
class ErrorField(object):
    error_type = attr.ib()
    error_text = attr.ib()


@attr.s
class Error(object):
    fields = attr.ib()

    @classmethod
    def deser(cls, buf, server_encoding):

        fields = []
        content = buf[5:]

        while content:

            field_type = content[0:1]

            if field_type == b"\0":
                break

            msg, content = content[1:].split(b"\0", 1)

            fields.append(ErrorField(error_type=field_type, error_text=msg))

        return cls(fields=fields)


@attr.s
class Notice(object):

    fields = attr.ib()

    @classmethod
    def deser(cls, buf, server_encoding):

        fields = []
        content = buf[5:]

        while content:

            field_type = content[0:1]

            if field_type == b"\0":
                break

            msg, content = content[1:].split(b"\0", 1)

            fields.append(ErrorField(error_type=field_type, error_text=msg))

        return cls(fields=fields)


@attr.s
class Flush(object):
    def ser(self):
        return FrontendMessageType.FLUSH.value + struct.pack("!i", 4)


@attr.s
class Close(object):

    _encoding = attr.ib()
    close_type = attr.ib()
    name = attr.ib()

    def ser(self):

        name_enc = self.name.encode(self._encoding) + b"\0"
        type_enc = self.close_type.encode(self._encoding)

        return (
            FrontendMessageType.CLOSE.value
            + struct.pack("!i", 4 + len(name_enc) + 1)
            + type_enc
            + name_enc
        )


@attr.s
class CloseComplete:
    @classmethod
    def deser(cls, buf, server_encoding):
        return cls()


@attr.s
class Unknown(object):
    """
    Something I haven't implemented yet.
    """

    content = attr.ib()

    @classmethod
    def deser(cls, buf, server_encoding):
        return cls(content=buf)


@attr.s
class PasswordMessage(object):

    _encoding = attr.ib()
    password = attr.ib()

    def ser(self):
        encoded = self.password.encode(self._encoding) + b"\0"
        return (
            FrontendMessageType.PASSWORD_MESSAGE.value
            + struct.pack("!i", len(encoded) + 4)
            + encoded
        )


@attr.s
class AuthenticationOk(object):
    pass


@attr.s
class AuthenticationCleartextPassword(object):
    pass


AuthenticationRequestTypes = {0: AuthenticationOk, 3: AuthenticationCleartextPassword}


@attr.s
class AuthenticationRequest(object):
    @classmethod
    def deser(cls, buf, server_encoding):
        (typ,) = struct.unpack("!i", buf[5:9])

        try:
            return AuthenticationRequestTypes[typ]()
        except ValueError:
            return Unknown.deser(buf)


class Parser(Enum):

    COMMAND_COMPLETE = CommandComplete
    CLOSE_COMPLETE = CloseComplete
    DATA_ROW = DataRow
    ERROR = Error
    BACKEND_KEY_DATA = BackendKeyData
    NOTICE = Notice
    AUTHENTICATION_REQUEST = AuthenticationRequest
    PARAMETER_STATUS = ParameterStatus
    ROW_DESCRIPTION = RowDescription
    PARAMETER_DESCRIPTION = ParameterDescription
    READY_FOR_QUERY = ReadyForQuery
    PARSE_COMPLETE = ParseComplete
    BIND_COMPLETE = BindComplete
    NO_DATA = NoData
    UNKNOWN = Unknown


def parse_from_buffer(buf, server_encoding):

    msg_type = BackendMessageType(buf[0:1])
    parser = Parser[msg_type.name].value
    return parser.deser(buf, server_encoding)
