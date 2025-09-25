import os

from database import DBConnection


# Database & user can be created with the following SQL:
# create user expenzy;
# alter user expenzy password 'expenzy';
# create database expenzy owner expenzy;

connection = DBConnection(
    hostname=os.environ.get("DB_HOSTNAME", "127.0.0.1")
)
connection.begin_transaction()
if os.getenv("RESET_DB"):
    connection.execute("drop table if exists expenzy_payout")

connection.execute(
    """
create table if not exists expenzy_payout(
    id uuid primary key,
    create_time timestamptz not null,
    amount numeric(16, 2) not null,
    recipient_account_identifier varchar(20) not null,
    state varchar(10) not null,
    state_update_count integer not null default 0
);
"""
)

connection.commit_transaction()
connection.close()
