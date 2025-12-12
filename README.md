# psrdb

[![Documentation Status](https://readthedocs.org/projects/psrdb/badge/?version=latest)](https://psrdb.readthedocs.io/en/latest/?badge=latest)
![example workflow](https://github.com/OZGrav/psrdb/actions/workflows/pytest.yaml/badge.svg)

Command line interface and python package for interacting with the MeerTime data portal

## Installation

To install `psrdb` run the following command:

```
pip install psrdb
```

To interact with the database you need to get an account on the [pulsars.org.au](https://pulsars.org.au) website.
Once you have an account you then need to generate a token on this [page](https://pulsars.org.au/api-tokens/).

Set this token using the following command (you can put this in your `~/.bashrc`):
```
export PSRDB_TOKEN=tokenhere
```

If you want to the current unreleased version, you can clone the repository and run

```
poetry install
```

or if you don't use poetry, you can instead run

```
pip install .
```

## How to use

Instructions and examples of how to use `psrdb` can be found in the [documentation](https://psrdb.readthedocs.io/en/latest/how_to_use.html).

## Running Tests

This project uses pytest as the testing framework. To run the tests:

```bash
poetry run pytest
```
