#!/bin/bash

set -e

for path in $(find /fred/oz005/timing -type f -name "obs.header"); do
    IFS="/"
    # Split the path into an array
    read -ra directories <<< "$path"
    beam=${directories[-3]}
    utc=${directories[-4]}
    jname=${directories[-5]}
    IFS=" "
    if [ -e "${path%%obs.header}/meertime.json" ]; then
        echo "Skipping $path"
    else
        echo "Making meetime.json for $path"
        EXIT_CODE=0
        generate_meerkat_json $path $beam -o ${path%%obs.header} || EXIT_CODE=$?
        if [ "$EXIT_CODE" -ne 42 ]; then
            ingest_obs ${path%%obs.header}/meertime.json
        fi
    fi
done