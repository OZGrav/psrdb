set -e

for path in $(find /fred/oz005/timing -type f -name "obs.header"); do
    IFS="/"
    # Split the path into an array
    read -ra directories <<< "$path"
    beam=${directories[-3]}
    utc=${directories[-4]}
    jname=${directories[-5]}
    IFS=" "
    echo "making ${utc}_${beam}_${jname}.json"
    poetry run generate_meerkat_json $path $beam -o test_jsons -n "${utc}_${beam}_${jname}.json"
done