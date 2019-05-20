#!/bin/bash
#description     calculate PSI on hadoop cluster using spark-submit
#author		 Alex Tselikov

#set env values
PYSPARK_DRIVER_PYTHON=/opt/anaconda3/bin/python
PYSPARK_PYTHON=/opt/anaconda3/bin/python

#run script - set actual date!
/opt/users/vchq/spark-2.0.1-bin-hadoop2.7/bin/spark-submit \
--master yarn --queue adhoc --num-executors 50 \
calc_psi_tables.py \
201709