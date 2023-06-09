#!/bin/bash

# Telescopes
psrdb telescope create MeerKAT
psrdb telescope create MONS

# Main projects
psrdb mainproject create MeerKAT MeerTIME
psrdb mainproject create MeerKAT Trapum
psrdb mainproject create MeerKAT Commissioning
psrdb mainproject create MeerKAT Unknown
psrdb mainproject create MONS    MONSPSR

# Projects
psrdb project create Trapum          SCI-20180923-MK-04      None            548 ""
psrdb project create MeerTIME        SCI-20180516-MB-02      TPA             548 "Thousand Pulsar Array"
psrdb project create MeerTIME        SCI-20180516-MB-05      PTA             548 "Pulsar Timing Array"
psrdb project create MeerTIME        SCI-20180516-MB-03      RelBin          548 "Relativistic Binaries"
psrdb project create MeerTIME        SCI-20180516-MB-04      GC              548 "Globular Clusters"
psrdb project create Commissioning   COM-20200429-SB-01      unknown         548 ""
psrdb project create Commissioning   COM-20200402-SB-01      unknown         548 ""
psrdb project create Commissioning   COM-20180801-SB-01      unknown         548 ""
psrdb project create Trapum          SCI-20180923-MK-01      unknown         548 ""
psrdb project create MeerTIME        SCI-20200222-MB-01      unknown         548 ""
psrdb project create Commissioning   COM-PTUSE-TEST          unknown         548 ""
psrdb project create Commissioning   COM-20200929-AF-01      unknown         548 ""
psrdb project create Commissioning   COM-20190902-MS-01      unknown         548 ""
psrdb project create Commissioning   COM-20191023-MS-01      unknown         548 ""
psrdb project create MeerTIME        SCI-20180516-MB-01      unknown         548 ""
psrdb project create Commissioning   COM-20191129-MG-01      unknown         548 ""
psrdb project create Commissioning   COM-20201001-DH-01      unknown         548 ""
psrdb project create Commissioning   COM-20180614-MG-01      unknown         548 ""
psrdb project create MeerTIME        SCI-20180516-MB-06      ngc6440         548 "Measuring the Shapiro delay in PSR J1748-2021B"
psrdb project create MeerTIME        SCI-20180516-MB-99      phaseups        548 "MeerTIME Phase Up observations"
psrdb project create Commissioning   COM-20190912-MS-01      unknown         548 ""
psrdb project create MeerTIME        SCI-2018-0516-MB-03     unknown         548 ""
psrdb project create MeerTIME        SCI-2018-0516-MB-02     unknown         548 ""
psrdb project create Trapum          DDT-20200506-BS-01      unknown         548 ""
psrdb project create Unknown         DDT-20190905-MC-01      unknown         548 ""
psrdb project create Commissioning   COM-20191019-MG-01      unknown         548 ""
psrdb project create Commissioning   COM-20190917-MS-01      unknown         548 ""
psrdb project create Unknown         EXT-20210822-OW-01      unknown         548 ""
psrdb project create Trapum          SCI-20180923-MK-06      unknown         548 ""
psrdb project create Unknown         EXT-20210822-OW-02      unknown         548 ""
psrdb project create Unknown         EXT-20220104-SB-01      unknown         548 ""
psrdb project create Unknown         SCI-20200703-MK-02      unknown         548 ""
psrdb project create Unknown         EXT-20220714-SG-01      unknown         548 ""
psrdb project create MONSPSR         MONSPSR_TIMING          MONSPSR_TIMING  548 ""
psrdb project create Unknown         ENG-20221026-PK-01      unknown         548 ""
psrdb project create Unknown         EXT-20230510-SB-01      unknown         548 ""

# Pulsar
psrdb pulsar create J1705-1903 ""

# Ephemeris
psrdb ephemeris create J1705-1903 /home/nick/code/meertime_dataportal/backend/dataportal/tests/test_data/ephem_J1705-1903_same_1.eph SCI-20180516-MB-05 ""
psrdb ephemeris create J1705-1903 /home/nick/code/meertime_dataportal/backend/dataportal/tests/test_data/ephem_J1705-1903_same_2.eph SCI-20180516-MB-05 ""
psrdb ephemeris create J1705-1903 /home/nick/code/meertime_dataportal/backend/dataportal/tests/test_data/ephem_J1705-1903_diff.eph SCI-20180516-MB-05 ""

