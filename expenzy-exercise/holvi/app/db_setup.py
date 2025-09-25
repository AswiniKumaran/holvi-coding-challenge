import os

from database import DBConnection


db_connection = DBConnection(
    hostname=os.environ.get("DB_HOSTNAME", "127.0.0.1"),
    username=os.environ.get("DB_USERNAME", "holvi"),
    password=os.environ.get("DB_PASSWORD", "holvi"),
    database=os.environ.get("DB_DATABASE", "holvi"),
)

db_connection.begin_transaction()
if os.getenv("RESET_DB"):
    db_connection.execute("DROP TABLE IF EXISTS holvi_received_payout;")

db_connection.execute(
    """
CREATE TABLE IF NOT EXISTS holvi_received_payout(
    id serial primary key,
    expenzy_uuid uuid not null,
    create_time timestamptz not null,
    amount numeric(16, 2) not null,
    recipient_account_identifier varchar(20) not null
);
"""
)
db_connection.commit_transaction()
db_connection.close()
