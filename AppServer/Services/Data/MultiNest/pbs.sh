#!/bin/bash

cd $PBS_O_WORKDIR
mkdir chains

source /mnt/home/kklimaszewski/MultiNest/PyMultiNest/setup.sh

/mnt/home/kklimaszewski/MultiNest/PyMultiNest/MultinestEvaluator.py 2>&1 | tee progress.log 

exit $?
