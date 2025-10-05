import datetime
import uuid
from decimal import Decimal

from psycopg.errors import UniqueViolation
from pydantic import BaseModel, field_validator
import logging

logger = logging.getLogger(__name__)

class HolviReceivedPayout(BaseModel):
    expenzy_uuid: uuid.UUID
    create_time: datetime.datetime
    amount: Decimal
    recipient_account_identifier: str

    @field_validator('create_time', mode="before")
    def parse_create_time(cls, value):
        if isinstance(value, str):
            # Define the exact format of the incoming string
            format_string = '%a, %d %b %Y %H:%M:%S %Z'
            return datetime.datetime.strptime(value, format_string)
        return value

class HolviPayoutQuery:
    def insert(self, connection, payout):
        try:
            connection.execute(
                "INSERT INTO holvi_received_payout(create_time, expenzy_uuid, amount, recipient_account_identifier) "
                "     VALUES (%s, %s, %s, %s)",
                (
                    payout.create_time,
                    payout.expenzy_uuid,
                    payout.amount,
                    payout.recipient_account_identifier,
                ),
            )
        except UniqueViolation as e:
            print(f"Expenzy uuid {payout.expenzy_uuid} already exists")

class FailedTransaction(BaseModel):
    transaction_uuid: uuid.UUID
    last_attempted_at: datetime.datetime

class FailedTransactionQuery:
    def insert(self, connection, failed_attempt: FailedTransaction):
        try:
            connection.execute(
                "INSERT INTO failed_transaction_update(transaction_id, last_attempted_at) "
                "     VALUES (%s, %s)",
                (failed_attempt.transaction_uuid, failed_attempt.last_attempted_at),
            )
        except UniqueViolation as e:
            logger.warning(f"Transaction uuid {uuid} already exists")