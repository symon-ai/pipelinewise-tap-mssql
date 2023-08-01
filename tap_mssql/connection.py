#!/usr/bin/env python3

import backoff
# Workaround fix https://github.com/pymssql/pymssql/issues/705
# import _scproxy #caused "Module not found" error while running in linux container
import pymssql
from tap_mssql.symon_exception import SymonException

import singer
import ssl

LOGGER = singer.get_logger()


@backoff.on_exception(backoff.expo, pymssql.Error, max_tries=5, factor=2)
def connect_with_backoff(connection):
    warnings = []
    with connection.cursor() as cur:
        if warnings:
            LOGGER.info(
                (
                    "Encountered non-fatal errors when configuring session that could "
                    "impact performance:"
                )
            )
        for w in warnings:
            LOGGER.warning(w)

    return connection


class MSSQLConnection(pymssql.Connection):
    def __init__(self, config):
        args = {
            "user": config["user"],
            "password": config["password"],
            "server": config["host"],
            "database": config["database"],
            "charset": "utf8",
            "port": config.get("port", "1433"),
            "tds_version": config.get("tds_version", "8.0"),
        }
        try:
            conn = pymssql._mssql.connect(**args)
        except pymssql._mssql.MSSQLDatabaseException as e:
            message = str(e)
            # pymssql throws same error for both wrong credentials and wrong database
            if f"Login failed for user '{config['user']}'" in message:
                raise SymonException('The username and password provided are incorrect. Please try again.', 'odbc.AuthenticationFailed')
            if "Adaptive Server is unavailable or does not exist" in message:
                if "timed out" in message:
                    raise SymonException('Timed out connecting to database. Please ensure all the connection form values are correct.', 'odbc.ConnectionTimeout')
                raise SymonException(f'Sorry, we couldn\'t connect to the host "{config["host"]}". Please ensure all the connection form values are correct.', 'odbc.ConnectionFailed')
            raise
        except pymssql._mssql.MSSQLDriverException as e:
            # invalid host url like 'asd' raises this error, but it might not be the only reason for this error
            if "Connection to the database failed for an unknown reason" in str(e):
                raise SymonException(f'Sorry, we couldn\'t connect to the host "{config["host"]}". Please ensure all the connection form values are correct.', 'odbc.ConnectionFailed')
            raise

        super().__init__(conn, False, True)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        del exc_info
        self.close()


def make_connection_wrapper(config):
    class ConnectionWrapper(MSSQLConnection):
        def __init__(self, *args, **kwargs):
            super().__init__(config)

            connect_with_backoff(self)

    return ConnectionWrapper
