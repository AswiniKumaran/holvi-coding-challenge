import select
from datetime import datetime
from dataclasses import dataclass, field

import psycopg2
import psycopg2.extras


@dataclass
class DBConnection:
    """
    This class represents a database connection, and has some helper
    methods to work with the database. You can add more if you want to,
    or use something completely different, like an ORM or directly psycopg2.
    """

    hostname: str = field(default="shared-db")
    port: int = field(default=5432)
    username: str = field(default="shared")
    password: str = field(default="shared")
    database: str = field(default="shared")
    autocommit: bool = field(default=True)

    def __post_init__(self):
        psycopg2.extras.register_uuid()
        self.connection = psycopg2.connect(
            f"postgres://{self.username}:{self.password}@{self.hostname}:{self.port}/{self.database}"
        )
        self.connection.autocommit = self.autocommit

    def close(self):
        self.connection.close()

    def begin_transaction(self):
        self.connection.autocommit = False

    def rollback_transaction(self):
        self.connection.rollback()
        self.connection.autocommit = self.autocommit

    def commit_transaction(self):
        self.connection.commit()
        self.connection.autocommit = self.autocommit

    def fetch_results(self, sql, params=None):
        """
        Run the given query, with given params, returning all rows.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    def fetch_one(self, sql, params=None):
        """
        Returns a single row of a SELECT Query or an INSERT or UPDATE query with
        an RETURNING clause.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone()

    def execute(self, sql, params=None):
        """
        Execute a query returning no results.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
