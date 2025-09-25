# Expenzy API integration

## Assignment

See docs/assignment.md (or .pdf if you prefer).

## Requirements

* A relatively recent version of Docker (tested: 24.0.7) and Docker-Compose (tested: 2.23.1)

## How to run

`RESET_DB=true docker-compose up --build` (or RESET_DB='' in case you want to restart without nuking the DB)

## Producing payouts

`docker-compose exec -e CONCURRENCY=1 -e GENERATION_ATTEMPTS=1 -e SLEEP_BETWEEN_PAYOUT=0.02 expenzy-server python producer.py`

## Checking results

To check the results and see if all payouts ended up in Holvi's database:

`docker-compose exec holvi-api python db_check.py`
