# Installation

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

## Optional

If you want to change the default url of https://pulsars.org.au/api/, you can change it by setting:

```
export PSRDB_URL=https://customurl.com/api/
```

It is likely that only developers will need to set this option.
