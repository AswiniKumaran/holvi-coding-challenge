import os
from typing import Union
from abc import ABC, abstractmethod
from psycopg_pool import ConnectionPool
import logging
from datetime import datetime

from psycopg_pool.abc import CT

from models import HolviReceivedPayout, FailedTransaction

logger = logging.getLogger(__name__)
class HandleDBOperation(ABC):
    connection_pool:Union[ConnectionPool, None] = None
    def __init__(self, query):
        self.query = query

    @classmethod
    def setup_db_connection_pool(cls) -> Union[ConnectionPool, None]:
        connection_info = (
            f"host={os.environ.get('DB_HOSTNAME', '127.0.0.1')} "
            f"dbname={os.environ.get('DB_DATABASE', 'shared')} "
            f"user={os.environ.get('DB_USERNAME', 'shared')} "
            f"password={os.environ.get('DB_PASSWORD', '')} "
        )
        try:
            connection_pool = ConnectionPool(conninfo=connection_info)
            logger.info(f"Connection pool created for {connection_info}")
            HandleDBOperation.connection_pool = connection_pool
            return connection_pool
        except Exception as e:
            logger.exception(f"Failed to create database connection pool: {e}")
            raise e

    def perform_db_operation(self, transaction: dict, last_attempted=None):
        if not self.connection_pool:
            raise RuntimeError("No database connection pool created")
        with self.connection_pool.connection() as connection:
            try:
                self.perform(transaction, connection, last_attempted)
                connection.commit()
            except Exception as e:
                connection.rollback()
                raise e

    @abstractmethod
    def perform(self, transaction: dict, connection: CT, last_attempted_at: Union[datetime, None] = None):
        pass


class AddHolviTransaction(HandleDBOperation):
    def perform(self, transaction, connection, last_attempted_at=None):
        try:
            self.query.insert(connection=connection, payout=HolviReceivedPayout(
                expenzy_uuid=transaction['id'],
                amount=transaction['amount'],
                recipient_account_identifier=transaction['recipient_account_identifier'],
                create_time=transaction['create_time'],
            ))
        except Exception as e:
            logger.exception(f"Failed to add Holvi transaction {transaction['id']}: {e}")


class AddFailedTransaction(HandleDBOperation):
    def perform(self, transaction, connection, last_attempted_at=None):
        assert last_attempted_at is not None
        try:
            self.query.insert(connection=connection, payout=FailedTransaction(
                transaction_uuid=transaction['id'],
                last_attempted_at=last_attempted_at,
            ))
        except Exception as e:
            logger.exception(f"Failed to add failed transaction {transaction['id']}: {e}")

