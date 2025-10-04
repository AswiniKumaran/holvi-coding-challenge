import os
from typing import Union, TypeVar
from abc import ABC, abstractmethod
from psycopg_pool import ConnectionPool
import logging
from datetime import datetime
from uuid import UUID
from psycopg import Connection

from models import HolviReceivedPayout, FailedTransaction, HolviPayoutQuery, FailedTransactionQuery

logger = logging.getLogger(__name__)
T = TypeVar('T')


class DBOperationHandler(ABC):
    connection_pool: Union[ConnectionPool, None] = None

    def __init__(self, query: Union[HolviPayoutQuery, FailedTransactionQuery]) -> None:
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
            DBOperationHandler.connection_pool = connection_pool
            return connection_pool
        except Exception as e:
            logger.exception(f"Failed to create database connection pool: {e}")
            raise e

    def perform_db_operation(self, transaction: Union[HolviReceivedPayout, UUID], last_attempted: datetime = None):
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
    def perform(self, transaction: T, connection: Connection,
                last_attempted_at: Union[datetime, None] = None):
        pass


class AddHolviTransaction(DBOperationHandler):
    def perform(self, transaction: HolviReceivedPayout, connection: Connection,
                last_attempted_at: Union[datetime, None] = None):
        try:
            self.query.insert(connection=connection, payout=HolviReceivedPayout(
                expenzy_uuid=transaction.expenzy_uuid,
                amount=transaction.amount,
                recipient_account_identifier=transaction.recipient_account_identifier,
                create_time=transaction.create_time,
            ))
        except Exception as e:
            logger.exception(f"Failed to add Holvi transaction {transaction.expenzy_uuid}: {e}")


class AddFailedTransaction(DBOperationHandler):
    def perform(self, transaction: UUID, connection: Connection,
                last_attempted_at: Union[datetime, None] = None):
        if not last_attempted_at:
            raise ValueError('last_attempted_at cannot be None')
        try:
            self.query.insert(connection=connection, payout=FailedTransaction(
                transaction_uuid=transaction,
                last_attempted_at=last_attempted_at,
            ))
        except Exception as e:
            logger.exception(f"Failed to add failed transaction_id {transaction}: {e}")
