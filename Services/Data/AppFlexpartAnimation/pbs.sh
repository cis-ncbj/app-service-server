#!/bin/bash

#PBS -l nodes=1:ppn=10,walltime=1:00:00

module load python-analysis/1.4-x86_64-gcc46-python27-mkl
module load python-tools

server=/mnt/home/kgomulski/utils/
export PATH=$PATH:$server/ffmpeg
postprocess_scripts=$server/ResultServer

$server/env/bin/python $postprocess_scripts/animation.py \
            --path @@{CIS_CHAIN0}/output \
            --age @@{age} \
            --spc @@{spc} \
            --z @@{z} \
            --min @@{min} \
            --max @@{max} \
            --scale @@{scale} \
            --format @@{format} \
            --workers $NCPUS

cp metadata.json maps/
cd maps
ls | grep -P "^tmp_[0-9]{4}.png$" | xargs -d"\n" rm
tar -zcvf maps.tar.gz * 
