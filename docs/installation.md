# Installation

To install run
```
poetry install
```
or if you don't use poetry, you can instead run

```
pip install .
```

To interact with the database you need to get an account on the https://pulsars.org.au/ website.
Once you have an account you then need to generate a token on the page https://pulsars.org.au/token_generate/.

Set this token using the following command (you can put this in your `~/.bashrc`):
```
export PSRDB_TOKEN=tokenhere
```

## Optional

If you want to change the default url of https://pulsars.org.au/api/, you can change it by setting:

```
export PSRDB_URL=https://customurl.com/api/
```

It is likely that only developers will need to set this option.