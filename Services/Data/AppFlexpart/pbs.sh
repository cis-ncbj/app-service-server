#!/bin/bash

#PBS -l nodes=1:ppn=10,walltime=5:00:00
#PBS -l mem=6GB



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

cat input >> flexwrf.input

export OMP_NUM_THREADS=10
ulimit -s unlimited
#limit stacksize unlimited
./flexwrf31_pgi_omp flexwrf.input

postprocess_scripts=/mnt/home/kgomulski/utils/ResultServer

rm *.t
rm *.dat
rm flexwrf31_pgi_omp

cd meteo

rm *.nc

cd ../output 


module load python-basemap
module load python-tools
python $postprocess_scripts/postprocess.py

tar -zcvf output.tar.gz *
#check if there are any results
ls -l metadata.json
