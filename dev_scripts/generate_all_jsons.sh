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
    echo "Making meetime.json for $path"
    generate_meerkat_json $path $beam -o ${path%%obs.header}
    ingest_obs ${path%%obs.header}/meertime.json
done