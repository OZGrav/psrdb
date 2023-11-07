# How to use

The command line interface can be used to interact with the various tables in the database.
The first arguments is the table name and the second is the action.
For example if I wanted to list all observations of pulsar J1652-4838 I could use:

```
psrdb observation list --pulsar J1652-4838
```

If you're unsure which arguments you require you can use `-h` to list your options.

Most tables have the commands `download`, `list`, `create`, `update` and `delete`.
Most users will only use `download` and `list` because `create`, `update` and `delete` is only used by the processing pipelines and admins.

The `download` command will download data from the tables to a csv. 
The two tables that you are most likely to use this command on is `toa` and `pulsar_fold_result`