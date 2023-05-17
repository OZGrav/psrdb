# meerdb
Command line interface and python package for interacting with the MeerTime data portal

## Installation

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
```

## How to use

The command line interface can be used to list various tables in the database.
The first arguments is the table name and the second is the action.
For example if I wanted to list all observations of pulsar J1652-4838 I could use:

```
meerdb observations list --pulsar J1652-4838
```

If you're unsure which arguments you require you can use `-h` to list your options.