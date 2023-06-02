IFS=$'\n'
for class in $(grep -m 1 class ../psrdb/*/*py); do
    location=$(echo $class | cut -d ':' -f 1 | sed 's&/&.&g')
    name=$(echo $class | cut -d ':' -f 2 | cut -d '(' -f 1)
    name=${name:6}
    echo ""
    echo $name

    length=${#name}
    dashes=""
    for ((i=0; i<length; i++))
    do
        dashes+="-"
    done
    echo $dashes
    echo ""
    echo ".. autoclass:: ${location:3:-3}.${name}"
    echo "   :members:"
done