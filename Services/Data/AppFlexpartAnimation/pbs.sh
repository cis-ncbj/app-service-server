#!/bin/bash

module load python-tools

server=/mnt/home/kgomulski/utils/
export PATH=$PATH:$server/ffmpeg
export PYTHONPATH=$PYTHONPATH:/mnt/home/kgomulski/tools/python-tornado/lib/python2.7/site-packages
postprocess_scripts=$server/ResultServer

python $postprocess_scripts/animation.py \
            --path @@{CIS_CHAIN0}/output \
            --age @@{age} \
            --spc @@{spc} \
            --z @@{z} \
            --min @@{min} \
            --max @@{max} \
            --scale @@{scale} \
            --format @@{format}  
cp metadata.json maps/
cd maps
tar -zcvf maps.tar.gz *
