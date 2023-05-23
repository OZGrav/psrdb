# Database structure

The full database structure and how it relates can be seen [here](https://dbdocs.io/nickaswainston/meertime_dataportal?view=relationships).
If it is out of date you can update it by running

```bash
mysqldump -u mysql_user -p meertime --no-data > meertime_format.sql
```

To get an up to date SQL format and then you can import it to [dbdiagram](https://dbdiagram.io/home)