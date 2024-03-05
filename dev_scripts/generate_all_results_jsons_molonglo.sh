#!/bin/bash

set -e


# Upload all results, images and toas
for path in $(find /fred/oz002/ldunn/meertime_dataportal/data/post/ -type f -name "obs.header"); do
    echo "PATH: $PATH"
    IFS="/"
    # Split the path into an array
    read -ra directories <<< "$path"
    echo "directories: $directories"
    utc=${directories[-2]}
    jname=${directories[-3]}
    echo "janme utc: $jname $utc"
    if [ -f "/fred/oz002/ldunn/meertime_dataportal/data/post/${jname}/${utc}/results.json" ]; then
        python ingest_molonglo_results.py --pulsar ${jname} --date ${utc}  && returncode=$? || returncode=$?
        if [ $returncode -eq 0 ]; then
            echo "Results uploaded"
        else
            echo "Trying again in 30 seconds"
            sleep 30
        fi
    fi
done

# Download those ToAs and create residuals and upload them

for path in $(find /fred/oz002/ldunn/meertime_dataportal/data/post/ -maxdepth 1 -mindepth 1 -type d -name "J*"); do
    echo "path: $path"
    IFS="/"
    # Split the path into an array
    read -ra directories <<< "$path"
    echo "directories: $directories"
    jname=${directories[-1]}
    echo "jname: $jname"
    psrdb toa download ${jname} --project MONSPSR_TIMING --nchan 1 --npol 1
    /fred/oz005/users/nswainst/code/nf-core-meerpipe/bin/tempo2_wrapper.sh toa_${jname}_nchan1_npol1.tim /fred/oz002/ldunn/meertime_dataportal/data/pars/${jname}.par
    if [ -f "toa_${jname}_nchan1_npol1.tim.residual" ]; then
        echo -e "\\nUpload the residuals\\n--------------------------\\n"
        psrdb residual create toa_${jname}_nchan1_npol1.tim.residual
    fi
done