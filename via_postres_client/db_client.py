import logging
from typing import (
    Any,
    List,
    Tuple,
)

import psycopg2


class TConnectionStatus:
    def __init__(
        self,
        connection: Any = None,
        is_connected: bool = False,
    ):
        self.connection: Any = connection
        self.is_connected: bool = is_connected

    def set_connection(self, connection: Any):
        self.connection = connection

    def set_is_connected(self, is_connected: bool):
        self.is_connected = is_connected


class TConnectionStatusBuilder:
    def __init__(self):
        self.conn_status = TConnectionStatus()

    def connection(self, conn: Any) -> "TConnectionStatusBuilder":
        self.conn_status.set_connection(conn)
        return self

    def is_connected(self, is_conn: bool) -> "TConnectionStatusBuilder":
        self.conn_status.set_is_connected(is_conn)
        return self

    def build(self):
        return self.conn_status


class TDBClient:
    def __init__(
        self,
        dbname: str = None,
        user: str = None,
        password: str = None,
        host: str = "127.0.0.1",
        port: str = "8888",
        forward_connect: bool = False,
    ):
        assert isinstance(password, str), \
            "Password should be passed as string"
        assert isinstance(port, str), \
            "Port should be passed as string"

        self.dbname: str = dbname
        self.user: str = user
        self.password: str = password
        self.host: str = host
        self.port: int = port
        self.conn_status: TConnectionStatus = None
        self.cursor: Any = None
        if forward_connect:
            self.connect()

    def connect(self) -> None:
        if self.conn_status and self.conn_status.is_connected:
            logging.info(f"Already connected to '{self.dbname}'")
            return
        self.conn_status = TConnectionStatusBuilder() \
                                .connection(self.__postgres_conn()) \
                                .is_connected(True) \
                                .build()
        self.cursor = self.conn_status.connection.cursor()

    def execute(
        self,
        query: str = "",
        fetch_all: bool = False,
    ) -> List[Tuple[Any]]:
        if not self.conn_status or not self.conn_status.is_connected:
            self.connect()

        execution_res: List[Tuple[Any]] = []
        try:
            self.cursor.execute(query)
            if fetch_all and self.cursor.rowcount > 0:
                execution_res = self.cursor.fetchall()
            self.conn_status.connection.commit()
        except AttributeError as err:
            logging.warning(f"{self.__get_error_module(err)}: {err}")
        except psycopg2.errors.SyntaxError as err:
            err_repr: str = '. '.join(err.__str__().split('\n')[:2])
            logging.error(
                f"{self.__get_error_module(err)}: {err_repr}",
                exc_info=True,
            )
        except psycopg2.errors.UndefinedTable as err:
            err_repr: str = '. '.join(err.__str__().split('\n')[:2])
            logging.warning(
                f"{self.__get_error_module(err)}: {err_repr}",
                exc_info=True,
            )
        except psycopg2.errors.UndefinedColumn as err:
            err_repr: str = '. '.join(err.__str__().split('\n')[:2])
            logging.warning(
                f"{self.__get_error_module(err)}: {err_repr}",
                exc_info=True,
            )
        except psycopg2.ProgrammingError as err:
            logging.warning(
                f"{self.__get_error_module(err)}: {err.__str__().strip()}",
                exc_info=True,
            )

        return execution_res

    def create_table(self, table_name: str, **kwargs) -> None:
        query: str = f"create table if not exists {table_name} ("
        for idx, (name, type) in enumerate(kwargs.items()):
            query += f"{name} {type}"
            if idx < len(kwargs) - 1: query += ", "
        query += ");"
        self.execute(query)

    def drop_table(self, table_name: str) -> None:
        self.execute(f"drop table if exists {table_name} cascade;")

    def insert_into(
        self,
        table_name: str,
        schema: str,
        values: List[str],
    ) -> None:
        query: str = f"insert into {table_name} ({schema}) values %s;"
        value_string: str = ','.join(['%s'] * len(values[0]))
        value_data: str = ','.join(
            self.cursor.mogrify(f"({value_string})", item).decode('utf-8')
            for item in values
        )
        query = query % value_data
        self.execute(query)

    def close(self):
        try:
            self.conn_status.connection.close()
            self.cursor.close()
            logging.info(f"Closed cursor for '{self.dbname}'")
        except AttributeError as err:
            logging.warning(f"{self.__get_error_module(err)}: {err}")
        except psycopg2.InterfaceError as err:
            logging.info(
                f"{self.__get_error_module(err)}: {err.__str__().strip()}",
            )

    def __postgres_conn(self) -> Any | None:
        conn: Any = None
        try:
            conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )
        except psycopg2.Error as err:
            logging.warning(err.__str__().strip())
        else:
            logging.info(f"Successfully connected to '{self.dbname}'")

        return conn

    def __get_error_module(self, err: Any):
        return f"{err.__class__.__module__}.{err.__class__.__name__}"
