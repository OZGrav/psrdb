#!/bin/bash

set -e

for path in $(find /fred/oz002/ldunn/meertime_dataportal/data/post -type f -name "obs.header"); do
    if [ -e "${path%%obs.header}/meertime.json" ]; then
        echo "Skipping $path"
    else
        echo "Making meetime.json for $path"
        EXIT_CODE=0
        generate_meerkat_json $path -o ${path%%obs.header} || EXIT_CODE=$?
        if [ "$EXIT_CODE" -ne 42 ]; then
            ingest_obs ${path%%obs.header}/meertime.json
        fi
    fi
done