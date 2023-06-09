# Observation Ingest


This documentation will describe how observation data is uploaded to the MeerTime database.
It will first describe telescope specific steps to create a meertime.json and then how to upload the json.


## Telescope specific ingest



### MeerKAT ingest

The design of the pulsar backend of MeerKAT (PTUSE) is described [here](https://ui.adsabs.harvard.edu/abs/2020PASA...37...28B/abstract).

For each MeerKAT observation, two directories are created `/fred/oz005/kronos/<beam_num>/<utc>/<jname>` and `/fred/oz005/timing/<jname>/<utc>/<beam_num>/<frequency>`.
The directories are split by `<beam_num>` as sometimes MeerKAT observe multiple sources at the same time (UTC) with different beams.

In `/fred/oz005/kronos/<beam_num>/<utc>/<jname>` there is a `obs.header` file which contains the configuration that the PTUSE uses to observe a source, most of the information we require is in this file.
There is also a `obs.info` file which has information about how the PTUSE split the `dspsr` command into subbands, this info does not need to be uploaded.
The `obs.results` has a signal to noise ratio result of the folded result and the length of the observation in seconds, if the backend has not had enough time to finish the process required for this calculation the file may not exist and will need to be regenerated.

In `/fred/oz005/timing/<jname>/<utc>/<beam_num>/<frequency>` it will also have the `obs.header`, `obs.info` and `obs.results` file.
The `metadata.json` is generated to upload the data to the MeeKAT data portal.
This directory also contains all the archive files that output from the PTUSE in 8 second files.





