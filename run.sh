#!/bin/bash

#Historico de facebook anuncios
cd /u01/app/oracle/tools/home/oracle/Hist_anunc_fcbk/ && /usr/hdp/current/spark-client/bin/spark-submit ServicioAnuncio.py --jars ojdbc7-12.1.0.2.jar
