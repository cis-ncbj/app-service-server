#!/bin/bash

#PBS -l nodes=1:ppn=10,walltime=5:00:00
#PBS -q i12h
#PBS -l mem=6GB

#setenv LD_LIBRARY_PATH /mnt/opt/tools/slc6/binutils/2.22/lib:/mnt/opt/tools/slc6/netcdf/4.3.0/lib

module load python
module load netcdf/4.3.0-pgi

cd $PBS_O_WORKDIR

ln -s /mnt/home/mjkorycki/models/slc6/FLEXPART_WRF/src_flexwrf_v3.1/data/IGBP_int1.dat $PBS_O_WORKDIR/IGBP_int1.dat
ln -s /mnt/home/mjkorycki/models/slc6/FLEXPART_WRF/src_flexwrf_v3.1/data/OH_7lev_agl.dat $PBS_O_WORKDIR/OH_7lev_agl.dat
ln -s /mnt/home/mjkorycki/models/slc6/FLEXPART_WRF/src_flexwrf_v3.1/data/surfdata.t $PBS_O_WORKDIR/surfdata.t
ln -s /mnt/home/mjkorycki/models/slc6/FLEXPART_WRF/src_flexwrf_v3.1/data/surfdepo.t $PBS_O_WORKDIR/surfdepo.t
ln -s /mnt/home/mjkorycki/models/slc6/FLEXPART_WRF/src_flexwrf_v3.1/flexwrf31_pgi_omp $PBS_O_WORKDIR/flexwrf31_pgi_omp
#chmod 777 *.dat
#chmod 777 *.t

mkdir output
cd meteo
python get_meteo_data.py
cd ..

echo "=====================FORMER PATHNAMES FILE===================" > flexwrf.input
echo "$PBS_O_WORKDIR/output/" >> flexwrf.input
echo "$PBS_O_WORKDIR/meteo/" >> flexwrf.input
echo "$PBS_O_WORKDIR/meteo/AVAILABLE" >> flexwrf.input
echo "=============================================================" >> flexwrf.input
cat input >> flexwrf.input

export OMP_NUM_THREADS=10
#limit stacksize unlimited

./flexwrf31_pgi_omp flexwrf.input
