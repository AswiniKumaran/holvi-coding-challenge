from datetime import datetime
import random
import decimal
from dataclasses import dataclass, field
import uuid


@dataclass
class Payout:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    create_time: datetime = field(default_factory=datetime.now)
    amount: decimal.Decimal = field(
        default_factory=lambda: decimal.Decimal(random.randrange(100, 10000)) / 100
    )
    recipient_account_identifier: str = "4321"
    state: str = "notifying"
    # Realistically, there would be much more data for a payout, for
    # exercise no need to add more fields


class PayoutQuery:
    def insert(self, connection, payout):
        connection.execute(
            "INSERT INTO expenzy_payout(id, create_time, amount, recipient_account_identifier, state) "
            "     VALUES (%s, %s, %s, %s, %s)",
            (
                payout.id,
                payout.create_time,
                payout.amount,
                payout.recipient_account_identifier,
                payout.state,
            ),
        )
        return payout

    def fetch(self, connection, state):
        if state:
            results = connection.fetch_results(
                """
                SELECT id, create_time, amount, recipient_account_identifier, state
                  FROM expenzy_payout
                 WHERE state = %s ORDER BY create_time DESC
            """,
                (state,),
            )
        else:
            results = connection.fetch_results(
                """
                SELECT id, create_time, amount, recipient_account_identifier, state
                  FROM expenzy_payout ORDER BY create_time DESC
            """
            )
        return [Payout(*row) for row in results]

    def update_state_by_id(self, connection, state, id):
        results = connection.fetch_results(
            """
            UPDATE expenzy_payout set state = %s, state_update_count = state_update_count + 1
             WHERE id = %s RETURNING id, create_time, amount, recipient_account_identifier, state
        """,
            (state, id),
        )
        return [Payout(*row) for row in results]
