#!/bin/bash

if [ "$#" != "1" ]; then
    echo "$0 <payload_file>"
    exit 1
fi

curl -i -H "Content-Type: application/json" -X POST \
    -d @$1 \
     https://app-gw.cis.gov.pl/api-devel/submit
