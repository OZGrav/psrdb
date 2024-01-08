#!/bin/bash

set -e

for path in $(find /fred/oz002/ldunn/meertime_dataportal/data/post -type f -name "obs.header"); do
    if [ "${path}" == "/fred/oz002/ldunn/meertime_dataportal/data/post/J0922+0638/2021-09-23-23:10:59/obs.header" ]; then
        continue
    fi
    if [ -e "${path%%obs.header}/meertime.json" ]; then
        echo "Skipping $path"
    else
        echo "Making meetime.json for $path"
        EXIT_CODE=0
        generate_molonglo_json $path -o ${path%%obs.header} || EXIT_CODE=$?
        if [ "$EXIT_CODE" -ne 42 ]; then
            ingest_obs ${path%%obs.header}/meertime.json
        fi
    fi
done