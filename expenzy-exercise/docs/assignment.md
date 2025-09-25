# Welcome to Expenzy exercise!

In this exercise you will be implementing an integration to Expenzy, a third party 
partner of Holvi's.

Expenzy is a service where Holvi customer can record expense information,
such as travel costs. Once they have filled in the expense information,
they can request a payout. When a payout is requested payment information
is transmitted to Holvi over an integration.

You have been asked to implement the integration to Expenzy. The goal of
the integration is to fetch payouts recorded in Expenzy to Holvi's database
for further processing, and do so without recording duplicates or missing
payouts.

Note that Expenzy's API offering is intentionally far from perfect, starting from
questionable use of HTTP methods and status codes. As part of the exercise you
should also come up with ideas how you might want to improve Expenzy's API offering.

The technologies used in this exercise have been chosen to make the setup
as simple as possible, and are not representative of Holvi's tech stack.

See included expenzy.jpg for an overview of the setup. In essence, you'll need to
implement fetching transactions using the Expenzy API, insert them to database,
and update Expenzy.

## Expenzy API offering

Expenzy server is running at http://expenzy-server:5001. Below is what Holvi got from Expenzy
for API documentation.

### POST /api/transaction/

Fetches transactions from Expenzy as JSON list. 

The payload for each transaction has the following fields:

 - `id`: uuid of transaction, unique on Expenzy side
 - `amount`: string with two decimal places
 - `create_time`: iso timestamp
 - `recipient_account_identifier`: who should receive the payout?
 - `state`: one of notifying, processing

Initially the state is notifying, and Holvi should set it to processing once the
payout has been recorded on Holvi side.

URL query parameter `state` can be used to filter transactions by state.
      
### POST /api/transaction/uuid/

This API can be used to update state of a transaction. Use this to update the state
of the transaction to processing once transaction has been received on Holvi side.

This API uses form parameters, and the only allowed parameter is state. On successful
update it returns a JSON list containing the transaction object. An empty list is
returned in case of failure.

### Webhook notification

After each payout created on Expenzy side, Expenzy will send a webhook to Holvi.
The notification about new payout will be sent to GET http://holvi-api:5002/expenzy/webhook/,
without any data in the payload. The call is done purely to notify new payouts are
available in the transaction API.

### Notes on the API

Payouts and notifications can happen concurrently. Unfortunately Expenzy's state
update API isn't entirely reliable, and it randomly fails with 500 error.

## Holvi Server

The payouts are to be recorded in holvi_received_payout table (see holvi/app/db_setup.py
for schema). Also, the state on Expenzy side must be updated to `processing` once Holvi
has received the transaction.

Note that you can alter the database schema for the holvi_received_payout table.
A small addition to the schema might make reliability easier to achieve.

## Running the services

See README.md in the root directory.

## Working on the task

Webhooks notifications are received by the expenzy_webhook() method in holvi/app/http_api.py.
The exercise task is to implement the integration starting from here, so practically fetching
the transactions from Expenzy server, storing them in holvi_received_payout table, and updating
the state to Expenzy, and doing so in a way which does not cause duplicated or missed payouts.

There are multiple ways to tackle the issues raising from Expenzy's API design,
concurrency and unreliability of Expenzy's API. You don't have to do anything fancy,
but if you feel like it, you can use other technologies than Flask and raw SQL,
you can use advanced database features, or for example use asyncio or simulate
a worker process with threading.Queue. Installing additional libraries, or
additional services on top of PostgreSQL is all ok, too.

Take note on producing easy to read and well structured code.

Start running with CONCURRENCY=1, GENERATION_ATTEMPTS=1, SLEEP_BETWEEN_PAYOUT=0.02,
and then incrementally go for more generation attempts and concurrency while having
less sleep in between.

After each run, verify with db_check.py that all is good:
  `docker-compose exec -e GENERATION_ATTEMPTS=100 holvi-api python db_check.py`

## After the exercise

Please submit the exercise by creating a private Github repo and invite holvi-recruiting-exercise.

We'll review your submission, and then setup an exercise review & interview. In the review
we'll ask you what kind of issues you spotted in the API, and also use the exercise as
basis to discuss your approach on things like observability, security, Also,
given more time, what would you have done next?

Finally, we hope you have fun with the exercise, and please do give us feedback!
