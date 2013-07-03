#!/bin/bash
#PBS -N MultiNest

echo "#####" | tee -a progress.log
echo "Start: $(date)" | tee -a progress.log

echo "Inicjalizajca ..." | tee -a progress.log
cd $PBS_O_WORKDIR
mkdir chains

source $HOME/MultiNest/PyMultiNest/setup.sh

echo "#####" | tee -a progress.log
echo "Obliczenia ..." | tee -a progress.log
$HOME/MultiNest/PyMultiNest/MultinestEvaluator.py 2>&1 | tee -a progress.log 

echo "#####" | tee -a progress.log
echo "Stop: $(date)" | tee -a progress.log
exit $?
