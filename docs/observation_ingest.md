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

The `meertime.json` file required for ingest is created with the `psrdb/scripts/generate_meerkat_json.py` script.
The inputs of the script are the `obs.header` file (this is where it gets most of the metadata) and the beam_num which is worked out from the directory structure.
The script also works out the calibration type ("pre" or "post") from the UTC and finds and calibration files that are on disk.
It uses the frequency summed archive in `/fred/oz005/kronos/<beam_num>/<utc>/<jname>/freq.sum` to work out the observation length and the ephemeris used for folding.


## Uploading the observation

To upload an observation we use the `metadata.json` and the script `psrdb/scripts/ingest_obs.py`.

An example of the `metadata.json` file is shown below

```
{
 "pulsarName": "J1705-1903",
 "telescopeName": "MeerKAT",
 "projectCode": "SCI-20180516-MB-05",
 "delaycal_id": "20201022-0018",
 "cal_type": "pre",
 "cal_location": null,
 "frequency": 1283.58203125,
 "bandwidth": 856.0,
 "nchan": 1024,
 "beam": 3,
 "nant": 61,
 "nantEff": 61,
 "npol": 2,
 "obsType": "fold",
 "utcStart": "2020-10-22-16:24:55",
 "raj": "17:05:43.8502743",
 "decj": "-19:03:41.32025",
 "duration": 1999.5421942429905,
 "nbit": 8,
 "tsamp": 1.196261682242991,
 "foldNbin": 1024,
 "foldNchan": 1024,
 "foldTsubint": 8,
 "filterbankNbit": null,
 "filterbankNpol": null,
 "filterbankNchan": null,
 "filterbankTsamp": null,
 "filterbankDm": null,
 "ephemerisText": "PSRJ \t J1705-1903\nRAJ  \t  17:05:43.8502743  \t  2.846e-4\nDECJ  \t  -19:03:41.32025  \t  3.999e-2\nF0  \t  403.17844370811329346  \t  1.289e-9\nDM  \t  57.50571096535851744  \t  5.052e-5\nF1  \t  -3.4428715379610950e-15  \t  6.438e-18\nPEPOCH  \t  56618  \t  \nPOSEPOCH  \t  56618  \t  \nDMEPOCH  \t  56618  \t  \nPMRA  \t  -4.3811121114291140e+0  \t  1.3305\nPMDEC  \t  -1.7888684789156335e+1  \t  13.2010\nPX  \t  0  \t  \nBINARY  \t  ELL1  \t  \nPB  \t  0.18395403344503906874  \t  3.598e-8\nA1  \t  0.10436244224177347356  \t  1.698e-6\nPBDOT  \t  -3.7178429899716451e-12  \t  1.539e-11\nTASC  \t  56582.21217308180756000000  \t  2.285e-4\nEPS1  \t  7.9300304759274633e-5  \t  1.779e-5\nEPS2  \t  -3.3596418853323134e-5  \t  7.178e-6\nTZRMJD  \t  58855.25408581254015800000  \t  \nTZRFRQ  \t  944.52099999999995816000  \t  \nTZRSITE  \t  meerkat  \t  \nEPHVER  \t  5  \t  \nCLK  \t  TT(TAI)  \t  \nUNITS  \t  TCB  \t  \nTIMEEPH  \t  IF99  \t  \nT2CMETHOD  \t  IAU2000B  \t  \nCORRECT_TROPOSPHERE  \t  N  \t  \nEPHEM  \t  DE421  \t  \n"
}
```





