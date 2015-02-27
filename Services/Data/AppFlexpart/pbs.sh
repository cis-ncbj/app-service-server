#!/bin/bash

#PBS -l nodes=1:ppn=10,walltime=5:00:00
#PBS -l mem=6GB

#setenv LD_LIBRARY_PATH /mnt/opt/tools/slc6/binutils/2.22/lib:/mnt/opt/tools/slc6/netcdf/4.3.0/lib

module load python
module load netcdf/4.3.0-pgi

cd $PBS_O_WORKDIR

ln -s /mnt/home/mjkorycki/models/slc6/FLEXPART_WRF/src_flexwrf_v3.1/data/IGBP_int1.dat $PWD/IGBP_int1.dat
ln -s /mnt/home/mjkorycki/models/slc6/FLEXPART_WRF/src_flexwrf_v3.1/data/OH_7lev_agl.dat $PWD/OH_7lev_agl.dat
ln -s /mnt/home/mjkorycki/models/slc6/FLEXPART_WRF/src_flexwrf_v3.1/data/surfdata.t $PWD/surfdata.t
ln -s /mnt/home/mjkorycki/models/slc6/FLEXPART_WRF/src_flexwrf_v3.1/data/surfdepo.t $PWD/surfdepo.t
ln -s /mnt/home/mjkorycki/models/slc6/FLEXPART_WRF/src_flexwrf_v3.1/flexwrf31_pgi_omp $PWD/flexwrf31_pgi_omp
#chmod 777 *.dat
#chmod 777 *.t

mkdir output
cd meteo
python get_meteo_data.py
cd ..

echo "=====================FORMER PATHNAMES FILE===================" > flexwrf.input
echo "$PWD/output/" >> flexwrf.input
echo "$PWD/meteo/" >> flexwrf.input
echo "$PWD/meteo/AVAILABLE" >> flexwrf.input
echo "=============================================================" >> flexwrf.input
cat input >> flexwrf.input

export OMP_NUM_THREADS=10
ulimit -s unlimited
#limit stacksize unlimited
pwd
./flexwrf31_pgi_omp flexwrf.input
