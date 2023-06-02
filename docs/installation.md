# Installation

To install run
```
poetry install
```
or if you don't use poetry, you can instead run

```
pip install .
```

To use the database you need a download token to read it or an ingest token to read and write/upload to it.

To get a read token run
```
get_token.sh
```

The output token should be set as an environment variable using
```
export PSRDB_TOKEN=tokenhere
export PSRDB_URL=https://pulsars.org.au/api/graphql/