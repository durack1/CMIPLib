#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Documentation for make_cmip5_xml2:
-----
Created on Wed Apr 11 10:59:24 2018

Paul J. Durack and Stephen Po-Chedley 11th April 2018

Script is meant to create xml library in several parts:
    + Find all CMIP5 directory paths 
    + Write paths and filesystem datestamps to SQL database
    + Create xmls
    + Running the script subsequently will only update xml files
      that correspond to directories in which the files have been 
      modified (using the SQL DB as a reference)

|  SP  11 Apr 2018  -   Initial outline/functionality of xml processing
|  SP  16 Jun 2018  -   Updated library and database, speed improvements, added  
                        functionality to retire paths

@author: pochedls
"""

import sys, os
sys.path.append('lib/')
import CMIPLib
import time 
from joblib import Parallel, delayed
import multiprocessing
import datetime
from tqdm import tqdm # conda install tqdm

print('Started on: ', time.ctime()) # start time for reference
t00 = time.time() # time whole script

xmlOutputDir = '/work/cmip5-dyn/'
updatePaths = False

# Define search directories
data_directories = ['/p/css03/cmip5_css01/data/cmip5/output1/', '/p/css03/cmip5_css01/data/cmip5/output2/',
                    '/p/css03/cmip5_css02/data/cmip5/output1', '/p/css03/cmip5_css02/data/cmip5/output2', 
                    '/p/css03/scratch/cmip5/', '/p/css03/scratch/published-latest/cmip5/',
                    '/p/css03/scratch/published-latest/cmip5/cmip5_css01/scratch/cmip5/',
                    '/p/css03/scratch/published-older/cmip5/', '/p/css03/scratch/should-publish/cmip5/',
                    '/p/css03/scratch/unknown-dset/cmip5/', '/p/css03/scratch/unknown-status/cmip5/',
                    '/p/css03/scratch/unknown-status/cmip5/', '/p/css03/scratch/obsolete/cmip5/',
                    '/p/css03/esgf_publish/cmip5/']

num_processors = 20

var_in = ['snc','snd','snw','tpf','pflw', 'sic','sim','sit','snc','snd', 'agessc','cfc11','dissic','evs','ficeberg',\
    'friver','hfds','hfls','hfss','mfo','mlotst','omlmax','ph','pr','rlds', 'rhopoto','rsds','sfriver','so','soga',\
    'sos','tauuo','tauvo','thetao','thetaoga','tos','uo','vo','vsf','vsfcorr', 'vsfevap','vsfpr','vsfriver','wfo',\
    'wfonocorr','zos','zostoga', 'cropfrac','evspsblsoi','evspsblveg','gpp','lai','mrfso','mrro','mrros','mrso',\
    'mrsos','tran','tsl', 'areacella','areacello','basin','deptho','mrsofc','orog','sftgif','sftlf','sftof','volcello', \
    'cl','clcalipso','cli','clisccp','clivi','clt','clw','clwvi','evspsbl','hfls','hfss','hur','hurs', 'hus','huss',\
    'mc','pr','prc','prsn','prw','ps','psl','rlds','rldscs','rlus','rluscs','rlut', 'rlutcs','rsds','rsdscs','rsdt',\
    'rsus','rsuscs','rsut','rsutcs','sbl','sci','sfcWind', 'ta','tas','tasmax','tasmin','tauu','tauv','ts','ua','uas',\
    'va','vas','wap','zg']   
temporal = ['fx','mon']
exps = ['1pctCO2','abrupt4xCO2','amip','amip4K','amip4xCO2','amipFuture','historical','historicalExt', \
        'historicalGHG','historicalMisc','historicalNat','past1000','piControl','rcp26','rcp45','rcp60',\
        'rcp85', 'sstClim','sstClim4xCO2']

if updatePaths:
    ## update SQL DB - parallelize here
    for parent in data_directories:
        t0 = time.time() # start timer
        CMIPLib.updateSqlDb(parent)
        t1 = time.time() # end timer
        print(t1-t0) # print timing
        print()

    t1 = time.time()
    total = t1-t00
    print(total)
else:
    print('Using existing path information in database...')

# get directories to scan
print('Getting directories to scan...')
# change input lists to strings for query
var_in = '\'' + '\', \''.join(var_in) + '\''
temporal = '\'' + '\', \''.join(temporal) + '\''
exps = '\'' + '\', \''.join(exps) + '\''
# create query 
q = "select path from paths where variable in (" + var_in + ") and experiment in (" + exps + ") and tfreq in (" + temporal + ") and ((xmlFile is NULL or xmlFile = \'None\') or (xmlwritedatetime < modified or xmlwritedatetime is NULL));"
q = "select path from paths where variable in (" + var_in + ") and experiment in (" + exps + ") and tfreq in (" + temporal + ") and (xmlFile is NULL);"
q = "select path from paths where variable in (" + var_in + ") and experiment in (" + exps + ") and tfreq in (" + temporal + ") and cdscanerror like 'No write%';"
# get directories
queryResult = CMIPLib.sqlQuery(q)
dirs_to_scan = list(queryResult[:,0])
print(str(len(dirs_to_scan)) + ' directories to scan...')

print('Starting directory scanning...')
print('Started on: ', time.ctime()) # start time for reference
results = Parallel(n_jobs=num_processors)(delayed(CMIPLib.process_path)(xmlOutputDir, inpath)\
           for (inpath) in tqdm(dirs_to_scan))

print('Finished on: ', time.ctime()) # start time for reference




