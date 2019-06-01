import attr

import struct
from enum import Enum


class FormatType(Enum):
    TEXT = 0
    BINARY = 1


class DataType(Enum):
    NAME = 19
    INT4 = 23
    TEXT = 25


@attr.s
class StartupMessage(object):

    protocol_version_number = attr.ib(default=196608)
    parameters = attr.ib(default={})

    def ser(self):

        res = []

        res.append(struct.pack("!i", self.protocol_version_number))

        for key, val in self.parameters.items():
            res.append(key.encode("utf8"))
            res.append(b"\0")
            res.append(val.encode("utf8"))
            res.append(b"\0")

        res.append(b"\0")

        msg = b"".join(res)
        return struct.pack("!i", len(msg) + 4) + msg

@attr.s
class ReadyForQuery(object):
    backend_status = attr.ib()

    @classmethod
    def deser(cls, buf):
        backend_status = buf[5:]

        return cls(backend_status=backend_status)

@attr.s
class Query(object):

    query = attr.ib()

    def ser(self):

        res = [self.query.encode("utf8"), b"\0"]

        msg = b"".join(res)
        return b"Q" + struct.pack("!i", len(msg) + 4) + msg


@attr.s
class Parse(object):

    prepared_statement_name = attr.ib()
    query = attr.ib()

    def ser(self):

        res = [
            self.prepared_statement_name.encode("utf8"),
            b"\0",
            self.query.encode("utf8"),
            b"\0",
        ]

        # We don't support prespecifying types
        res.append(struct.pack("!h", 0))

        msg = b"".join(res)
        return b"P" + struct.pack("!i", len(msg) + 4) + msg

@attr.s
class Describe(object):

    prepared_statement_name = attr.ib()

    def ser(self):

        res = [
            b"S",
            self.prepared_statement_name.encode("utf8"),
            b"\0",
        ]

        msg = b"".join(res)
        return b"D" + struct.pack("!i", len(msg) + 4) + msg

@attr.s
class BindParam(object):

    format_code = attr.ib()
    value = attr.ib()


@attr.s
class Bind(object):

    destination_portal = attr.ib()
    prepared_statement = attr.ib()
    parameters = attr.ib()
    result_format_codes = attr.ib()


    def ser(self):

        res = []

        res.append(self.destination_portal.encode('utf8'))
        res.append(b"\0")
        res.append(self.prepared_statement.encode('utf8'))
        res.append(b"\0")

        # No result format codes
        res.append(struct.pack("!h", 0))

        res.append(struct.pack("!h", len(self.parameters)))

        for p in self.parameters:
            res.append(struct.pack("!i", len(p.value)))
            res.append(p.value)

        # No result format codes
        res.append(struct.pack("!h", 0))


        msg = b"".join(res)
        return b"B" + struct.pack("!i", len(msg) + 4) + msg


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
    def deser(cls, buf):

        col_values, = struct.unpack("!h", buf[5:7])
        content = buf[7:]

        vals = []

        for x in range(col_values):

            field_name, rest = content.split(b"\0", 1)

            print(field_name)

            table_obj_id, col_attr_num, data_type, data_type_size, type_modifier, format_code = struct.unpack(
                "!ihihih", rest[:18]
            )

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
class DataRow(object):

    values = attr.ib()

    @classmethod
    def deser(cls, buf):

        col_values, = struct.unpack("!h", buf[5:7])
        content = buf[7:]

        vals = []

        for x in range(col_values):
            length_of_next, = struct.unpack("!i", content[0:4])
            col_val = content[4 : length_of_next + 4]
            vals.append(col_val)

        return cls(values=tuple(vals))


@attr.s
class CommandComplete(object):

    cmd = attr.ib()

    @classmethod
    def deser(cls, buf):
        content = buf[5:-1].decode("utf8")
        return cls(cmd=content)


@attr.s
class AuthenticationOk(object):
    pass


@attr.s
class ParameterStatus(object):

    name = attr.ib()
    val = attr.ib()

    @classmethod
    def deser(cls, buf):

        key, val = buf[5:-1].split(b"\0")

        key = key.decode("utf8")
        val = val.decode("utf8")

        return cls(name=key, val=val)


@attr.s
class BackendKeyData(object):

    process_id = attr.ib()
    secret_key = attr.ib()

    @classmethod
    def deser(cls, buf):
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
    def deser(cls, buf):

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
    def deser(cls, buf):

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
        return b"H" + struct.pack("!i", 4)

@attr.s
class Unknown(object):
    """
    Something I haven't implemented yet.
    """

    content = attr.ib()

    @classmethod
    def deser(self, buf):
        return self(buf)


def _parse_authentication_object(buf):

    typ, = struct.unpack("!i", buf[5:9])

    if typ == 0:
        return AuthenticationOk()
    print(typ)


def parse_from_buffer(buf):

    msg_type = buf[0:1]

    if msg_type == b"R":
        return _parse_authentication_object(buf)
    elif msg_type == b"S":
        msg = ParameterStatus
    elif msg_type == b"D":
        msg = DataRow
    elif msg_type == b"C":
        msg = CommandComplete
    elif msg_type == b"K":
        msg = BackendKeyData
    elif msg_type == b"T":
        msg = RowDescription
    elif msg_type == b"Z":
        msg = ReadyForQuery
    elif msg_type == b"E":
        msg = Error
    elif msg_type == b"N":
        msg = Notice
    else:
        msg = Unknown

    return msg.deser(buf)
