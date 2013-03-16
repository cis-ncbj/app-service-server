#!/bin/bash
#PBS -l nodes=@@{PBS_NODES}:ppn=@@{PBS_PPN}

./python/run.py

