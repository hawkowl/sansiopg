from .messages import BindParam, DataType, FormatType


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


def _int_text_from_postgres(val):
    return int(val.decode("ascii"))


_DEFAULT_CONVERTERS_FROM_POSTGRES = {
    (DataType.NAME, FormatType.TEXT): _text_text_from_postgres,
    (DataType.TEXT, FormatType.TEXT): _text_text_from_postgres,
    (DataType.BOOL, FormatType.TEXT): _bool_text_from_postgres,
    (DataType.INT4, FormatType.TEXT): _int_text_from_postgres,
}
_DEFAULT_CONVERTERS_TO_POSTGRES = {
    int: _int_to_postgres,
    str: _str_to_postgres,
}


class Converter(object):
    def __init__(self):
        self._from_postgres = dict(_DEFAULT_CONVERTERS_FROM_POSTGRES)
        self._to_postgres = dict(_DEFAULT_CONVERTERS_TO_POSTGRES)

    def to_postgres(self, value):
        try:
            conv = self._to_postgres.get(type(value))
            return conv(value)
        except:
            print("Can't convert ", value)
            raise ValueError()

    def from_postgres(self, value, row_format):
        try:
            conv = self._from_postgres.get(
                (row_format.data_type, row_format.format_code)
            )
            return conv(value)
        except:
            # print("Can't convert ", value, row_format)
            # print("Falling back to text decode")
            if row_format.format_code == FormatType.TEXT:
                return value.decode("utf8")
