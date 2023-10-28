#!/bin/bash

set -e

old_name="$1"
new_name="$2"

mkdir -p /fred/oz005/timing/${new_name}

for path in $(find /fred/oz005/timing/${old_name} -type f -name "obs.header"); do
    IFS="/"
    # Split the path into an array
    read -ra directories <<< "$path"
    freq=${directories[-2]}
    beam=${directories[-3]}
    utc=${directories[-4]}
    jname=${directories[-5]}
    IFS=" "
    echo "Working on UTC:${utc} beam:${beam}"
    mv /fred/oz005/timing/${old_name}/${utc} /fred/oz005/timing/${new_name}
    sed -i "s/SOURCE              ${old_name}/SOURCE              ${new_name}/" /fred/oz005/timing/${new_name}/${utc}/${beam}/${freq}/obs.header
    mv /fred/oz005/kronos/${beam}/${utc}/${old_name} /fred/oz005/kronos/${beam}/${utc}/${new_name}
    sed -i "s/SOURCE              ${old_name}/SOURCE              ${new_name}/" /fred/oz005/kronos/${beam}/${utc}/${new_name}/obs.header
done