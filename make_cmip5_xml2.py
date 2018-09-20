#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Documentation for make_cmip5_xml2:
-----
Created on Wed Apr 11 10:59:24 2018

Paul J. Durack and Stephen Po-Chedley 11th April 2018

Command line usage: 
    Help: ./make_cmip5_xml2.py --help
    Example: ./make_cmip5_xml2.py -p 'True' -s 'False' -n 1

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
|  SP  20 Sep 2018  -   Python 3 ready, argument parsing, various defined scan modes, 
                        track bad paths in database, parallelized path scanning

@author: pochedls
"""

import sys, os
sys.path.append('lib/')
import CMIPLib
import time 
import numpy as np
from joblib import Parallel, delayed
import multiprocessing
import datetime
from tqdm import tqdm # conda install tqdm
import argparse
try:
    __IPYTHON__
except NameError:
    INIPYTHON = False
else:
    INIPYTHON = True

print('Started on: ', time.ctime()) # start time for reference
t00 = time.time() # time whole script

# function to parse boolean
def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

if INIPYTHON == False: # Look for cmd line arguments if we are NOT in Ipython

    parser = argparse.ArgumentParser()

    # Optional arguments
    parser.add_argument('-p', '--updatePaths', type=str2bool,
                        default=True,
                        help="Flag (TRUE/FALSE) to update SQL database (default is TRUE)")
    parser.add_argument('-s', '--updateScans', type=str2bool,
                        default=True,
                        help="Flag (TRUE/FALSE) to run cdscan (default is TRUE)")    
    parser.add_argument('-out', '--xmlOutputDir', type=str,
                        default = '/work/cmip-dyn/',
                        help="Base output directory for xml files (default /work/cmip-dyn/)")
    parser.add_argument('-n', '--numProcessors', type=int,
                        help="Number of processors for creating xml files (default 20)")

    args = parser.parse_args()

    updatePaths = args.updatePaths
    updateScans = args.updateScans
    xmlOutputDir = args.xmlOutputDir
    numProcessors = args.numProcessors

else:

    updatePaths = True
    updateScans = False
    xmlOutputDir = '/work/cmip-dyn/'
    numProcessors = 20

# Define search directories
data_directories = ['/p/css03/cmip5_css01/data/cmip5/output1/', '/p/css03/cmip5_css01/data/cmip5/output2/',
                    '/p/css03/cmip5_css02/data/cmip5/output1/', '/p/css03/cmip5_css02/data/cmip5/output2/', 
                    '/p/css03/scratch/cmip5/', '/p/css03/scratch/published-latest/cmip5/',
                    '/p/css03/scratch/published-latest/cmip5/cmip5_css01/scratch/cmip5/',
                    '/p/css03/scratch/published-older/cmip5/', '/p/css03/scratch/should-publish/cmip5/',
                    '/p/css03/scratch/unknown-dset/cmip5/', '/p/css03/scratch/unknown-status/cmip5/',
                    '/p/css03/scratch/obsolete/cmip5/', '/p/css03/esgf_publish/cmip5/', 
                    '/p/css03/esgf_publish/CMIP6/CMIP/']


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


# for testing
# data_directories = ['/p/css03/cmip5_css02/data/cmip5/output2/', '/p/css03/scratch/cmip5/', '/p/css03/esgf_publish/CMIP6/CMIP/']
# CMIPLib.updateSqlDb('/p/css03/cmip5_css02/data/cmip5/output1/NCAR/CCSM4/past1000/mon/atmos/Amon/r1i1p1/')
# CMIPLib.updateSqlDb('/p/css03/esgf_publish/CMIP6/CMIP/')
# q = "select path from paths where variable = \'tas\';"
# queryResult = CMIPLib.sqlQuery(q)
# dirs_to_scan = list(queryResult[:,0])
# for path in dirs_to_scan:
#     print(path)
#     CMIPLib.process_path(xmlOutputDir, path)


if updatePaths:
    # grab the right number of processors
    if len(data_directories) > numProcessors:
        nfscan = numProcessors
    else:
        nfscan = len(data_directories)
    print('Using ' + str(nfscan) + ' processors to check directories...', end='\n \n')
    results = Parallel(n_jobs=nfscan)(delayed(CMIPLib.updateSqlDb)(parent)\
           for (parent) in data_directories)
    # print results
    headers = ['New', 'Modified', 'Ignored', 'New written', 'Updated']
    matrix = np.zeros((len(results), len(headers)))
    for i, row in enumerate(results):
        print(row[0])
        for j in range(len(headers)):
            print('    ' + headers[j] + ': ' + str(row[j + 1]), end='')
            matrix[i, j] = row[j + 1]
        print()
    msum = np.sum(matrix, axis=0)
    print('Total')
    for j in range(len(headers)):
        print('    ' + headers[j] + ': ' + str(int(msum[j])), end='')

    # print timing
    t1 = time.time()
    total = t1-t00
    print(end='\n \n'); 
    print(str(int(total)) + ' seconds.', end='\n \n');



# change input lists to strings for query
var_in = '\'' + '\', \''.join(var_in) + '\''
temporal = '\'' + '\', \''.join(temporal) + '\''
exps = '\'' + '\', \''.join(exps) + '\''
# create query 
q = "select path from paths where variable in (" + var_in + ") and experiment in (" + exps + ") and tfreq in (" + temporal + ") and ((xmlFile is NULL or xmlFile = \'None\') or (xmlwritedatetime < modified or xmlwritedatetime is NULL));"
# q = "select path from paths where variable in (" + var_in + ") and experiment in (" + exps + ") and tfreq in (" + temporal + ") and (xmlFile is NULL);"
# used this to run all files with any no write error
# q = "select path from paths where variable in (" + var_in + ") and experiment in (" + exps + ") and tfreq in (" + temporal + ") and cdscanerror like 'No write%';"
# used this to run no write files
# q = "select path from paths where variable in (" + var_in + ") and experiment in (" + exps + ") and tfreq in (" + temporal + ") and cdscanerror = 'No write';"
# used this to get newer fgoals-g2 xmls (which have same version number as the old files)
# q = "select path from paths where variable in (" + var_in + ") and experiment=\'historical\' and model = \'FGOALS-g2\' and tfreq = \'mon\' and ((xmlFile is NULL or xmlFile = 'None') or (xmlwritedatetime < modified or xmlwritedatetime is NULL)) and path not like \'%esgf_publish%\';"
# q = 'select path from paths where mip_era = \'CMIP6\';'

# get directories
if updateScans:
    # get directories to scan
    print('Getting directories to scan...')
    queryResult = CMIPLib.sqlQuery(q)
    dirs_to_scan = list(queryResult[:,0])
    print(str(len(dirs_to_scan)) + ' directories to scan...')

    print('Starting directory scanning...')
    print('Started on: ', time.ctime()) # start time for reference
    results = Parallel(n_jobs=numProcessors)(delayed(CMIPLib.process_path)(xmlOutputDir, inpath)\
               for (inpath) in tqdm(dirs_to_scan))

print('Finished on: ', time.ctime()) # start time for reference




