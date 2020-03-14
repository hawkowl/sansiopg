from .protocol import TwistedIOImplementation

from sansiopg.protocol import PostgresConnection


def new_connection(encoding="utf8", debug=True):
    return PostgresConnection(TwistedIOImplementation(debug=debug), encoding=encoding)
