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
    Production: ./make_cmip5_xml2.py -p 'True' -r 'True' -s 'True' -c 'True' -n 20
    Subset: ./make_cmip5_xml2.py -p 'False' -r 'False' -s 'True' -c 'False' -n 20 -m 'historical.mon.tro3'

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
|  SP  21 Sep 2018  -   added gridlabel code, functionality to include database stats

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
print()
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
                        default = 20,
                        help="Number of processors for creating xml files (default 20)")
    parser.add_argument('-l', '--lastTouch', type=int,
                        default = 24,
                        help="Number of hours since a directory was modified to process it")  
    parser.add_argument('-c', '--countStats', type=str2bool,
                        default = False,
                        help="Boolean to record statistics on xml database")  
    parser.add_argument('-r', '--retirePaths', type=str2bool,
                        default = True,
                        help="Boolean to look for paths that no longer exist")                                                    
    parser.add_argument('-m', '--mode', type=str,
                        default = '',
                        help="Mode to specify the what to cdscan:\
                                    experiment.frequency.variable")

    args = parser.parse_args()

    updatePaths = args.updatePaths
    updateScans = args.updateScans
    xmlOutputDir = args.xmlOutputDir
    numProcessors = args.numProcessors
    lastTouch = args.lastTouch
    countStats = args.countStats
    mode = args.mode
    retirePaths = args.retirePaths

else:
    retirePaths = True
    updatePaths = False
    updateScans = True
    xmlOutputDir = '/work/cmip-dyn/'
    numProcessors = 20
    lastTouch = 24
    countStats = True
    mode = ''

# Define search directories
data_directories = ['/p/css03/cmip5_css01/data/cmip5/output1/', '/p/css03/cmip5_css01/data/cmip5/output2/',
                    '/p/css03/cmip5_css02/data/cmip5/output1/', '/p/css03/cmip5_css02/data/cmip5/output2/', 
                    '/p/css03/scratch/cmip5/', '/p/css03/scratch/published-latest/cmip5/',
                    '/p/css03/scratch/published-latest/cmip5/cmip5_css01/scratch/cmip5/',
                    '/p/css03/scratch/published-older/cmip5/', '/p/css03/scratch/should-publish/cmip5/',
                    '/p/css03/scratch/unknown-dset/cmip5/', '/p/css03/scratch/unknown-status/cmip5/',
                    '/p/css03/scratch/obsolete/cmip5/', '/p/css03/esgf_publish/cmip5/', 
                    '/p/css03/esgf_publish/CMIP6/CMIP/', '/p/css03/scratch/cmip6/']


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

if mode.find('.') >= 0:
    x = mode.split('.')
    if x[0] != '*':
        exps = [x[0]]
    temporal = [x[1]]
    if x[2] != '*':
        var_in = [x[2]]

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

if retirePaths:
    print('Checking for retired directories...')
    print('Started on: ', time.ctime(), end='\n \n') # start time for reference
    q = 'select path, xmlFile from paths where xmlFile is not NULL and xmlFile != \'None\' and retired = 0;'
    queryResult = CMIPLib.sqlQuery(q)
    for i in range(len(queryResult)):
        p = queryResult[i][0]
        f = queryResult[i][1]
        if not os.path.exists(p):
            # remove from database
            CMIPLib.retireDirectory(p)
            # delete xml file
            if os.path.exists(f):
                os.system('rm ' + f)

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
# q = "select path from paths where variable in (" + var_in + ") and experiment in (" + exps + ") and frequency in (" + temporal + ") and ((xmlFile is NULL or xmlFile = \'None\') or (xmlwritedatetime < modified or xmlwritedatetime is NULL)) and TIMESTAMPDIFF(HOUR, modified, now()) > " + str(lastTouch) + " ;"
# q = "select path from paths where variable in (" + var_in + ") and experiment in (" + exps + ") and tfreq in (" + temporal + ") and (xmlFile is NULL);"
# used this to run all files with any no write error
# q = "select path from paths where variable in (" + var_in + ") and experiment in (" + exps + ") and tfreq in (" + temporal + ") and cdscanerror like 'No write%';"
# used this to run no write files
# q = "select path from paths where variable in (" + var_in + ") and experiment in (" + exps + ") and tfreq in (" + temporal + ") and cdscanerror = 'No write';"
# used this to get newer fgoals-g2 xmls (which have same version number as the old files)
# q = "select path from paths where variable in (" + var_in + ") and experiment=\'historical\' and model = \'FGOALS-g2\' and tfreq = \'mon\' and ((xmlFile is NULL or xmlFile = 'None') or (xmlwritedatetime < modified or xmlwritedatetime is NULL)) and path not like \'%esgf_publish%\';"
# q = 'select path from paths where mip_era = \'CMIP6\';'
q = "select path from paths where variable in (" + var_in + ") and experiment in (" + exps + ") and frequency in (" + temporal + ") and ((xmlFile is NULL or xmlFile = \'None\') or (xmlwritedatetime < modified or xmlwritedatetime is NULL)) and retired = 0 and (ignored = 0 OR ignored is NULL) and TIMESTAMPDIFF(HOUR, modified, now()) > " + str(lastTouch) + " ;"
q = "select path from paths where variable in (" + var_in + ") and experiment in (" + exps + ") and frequency in (" + temporal + ") and ((xmlFile is NULL or xmlFile = \'\' or xmlFile = 'None') or (xmlwritedatetime < modified or xmlwritedatetime is NULL)) and retired = 0 and (ignored = 0 OR ignored is NULL) and TIMESTAMPDIFF(HOUR, modified, now()) > " + str(lastTouch) + ";"

# q = "select path from paths where institute = \'IPSL\' and variable = \'tas\' and member like \'r1i1p1%\' and experiment = \'piControl\' and model like \'IPSL-CM%A-LR\' and frequency = \'mon\' and path like \'%esgf_publish%\'";


# get directories
if updateScans:
    # get directories to scan
    print('Getting directories to scan...')
    queryResult = CMIPLib.sqlQuery(q)
    if len(queryResult) > 0:
        dirs_to_scan = list(queryResult[:,0])
        print(str(len(dirs_to_scan)) + ' directories to scan...')
        if len(dirs_to_scan) < numProcessors:
            numProcessors = len(dirs_to_scan)
        print('Using ' + str(numProcessors) + ' processors to scan directories...', end='\n \n')
        print('Starting directory scanning...')
        print('Started on: ', time.ctime()) # start time for reference
        results = Parallel(n_jobs=numProcessors)(delayed(CMIPLib.process_path)(xmlOutputDir, inpath)\
                   for (inpath) in tqdm(dirs_to_scan))
    else:
        print('No directories found...')

if countStats:
    print('Writing statistics to database', end='\n\n')
    q = []
    q.append("INSERT INTO stats (indicator, value, datetime) VALUES (\'cmip5 directories\', (select count(*) as n from paths where mip_era = \'CMIP5\' and retired=0), now());")
    q.append("INSERT INTO stats (indicator, value, datetime) VALUES (\'cmip6 directories\', (select count(*) as n from paths where mip_era = \'CMIP6\' and retired=0), now());")
    q.append("INSERT INTO stats (indicator, value, datetime) VALUES (\'cmip5 xml files\', (select count(*) as n from paths where mip_era = \'CMIP5\' and xmlFile is NOT NULL and xmlFile != 'None' and retired=0), now());")
    q.append("INSERT INTO stats (indicator, value, datetime) VALUES (\'cmip6 xml files\', (select count(*) as n from paths where mip_era = \'CMIP6\' and xmlFile is NOT NULL and xmlFile != 'None' and retired=0), now());")
    q.append("INSERT INTO stats (indicator, value, datetime) VALUES (\'undefined vertical grid (cmip5)\', (select count(*) as n from paths where mip_era = \'CMIP5\' and gridLabel like \'%-%-x-%\' and retired=0), now());")
    q.append("INSERT INTO stats (indicator, value, datetime) VALUES (\'undefined vertical grid (cmip6)\', (select count(*) as n from paths where mip_era = \'CMIP6\' and gridLabel like \'%-%-x-%\' and retired=0), now());")    
    for query in q:
        queryResult = CMIPLib.sqlInsertQuery(query)

print('Finished on: ', time.ctime()) # start time for reference




