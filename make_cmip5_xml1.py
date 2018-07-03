#!/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 23 09:40:47 2012

Paul J. Durack 23rd January 2012

This script builds *.xml files for all "interesting" model variables

PJD 24 Mar 2014     - Added tauuo and tauvo variables to ocn realm
...
PJD 22 Feb 2018     - Update to point at CSS03 mounts
PJD  9 Apr 2018     - Updated to point to all CSS03 mounts (Jeff's list)
PJD 22 May 2018     - Added new path, following Jeff update
PJD 31 May 2018     - Added new path, following Sasha update
PJD 19 Jun 2018     - Updated all print functions for python3
                    - TODO:
                    Add check to ensure CSS/GDO systems are online, if not abort - use sysCallTimeout function
                    sysCallTimeout(['ls','/cmip5_gdo2/'],5.) ; http://stackoverflow.com/questions/13685239/check-in-python-script-if-nfs-server-is-mounted-and-online
                    Add model masternodes
                    Fix issue with no valid files being recorded
                    Placing read test in pathToFile will trim out issues with 0-sized files and read permissions, so reporting may need to be relocated
                    Add counters for lat1 vs lat0
                    Report new runtimeError in cdscan - problems with overlapping times, issue in combineKeys function - Report to Jeff/Charles
                    Added demo code from Charles to convert processes and queues into lists of objects thereby subverting hard-coding and parallel limits
                    update for gdo2_data (~8k; 2.2hrs) and css02_scratch (~25k; 7hrs) to scour using multiple threads each - what is the IO vs wait difference?
                     consider using multiprocess.pool to achieve this, so full loads until job(s) are completed
                    Consider using multiprocess.pool (which loads up processes concurrently) rather than multiprocess.Process
                    Fix duplicate versions - add flag for latest or deprecated - awaiting Bob to create index file as esgquery_index wont cope with 40k queries
                     Conditionally purge xmls with earlier version number (should sort so last generated file is latest)
                    [durack1@crunchy output1]$ pwd
                    /cmip5_gdo2/data/cmip5/output1
                    [durack1@crunchy output1]$ ncdump -h NOAA-GFDL/GFDL-CM3/historical/mon/atmos/Amon/r1i1p1/tas/1/tas_Amon_GFDL-CM3_historical_r1i1p1_186001-186412.nc | grep tracking_id
                    :tracking_id = "9ab415bd-adf7-4dcf-a13c-e9530d5efe41" ;
                    # Should also test an deprecated file for "False" response
                    import urllib2
                    req = urllib2.Request(url='http://pcmdi9.llnl.gov/esg-search/search?',data='type=File&query=tracking_id:9ab415bd-adf7-4dcf-a13c-e9530d5efe41&fields=latest')
                    query = urllib2.urlopen(req)
                    print query.read()

@author: durack1
"""
from __future__ import print_function ; # Python2->3 conversion
import argparse,cPickle,datetime,gc,glob,gzip,os,shlex,sys,time
sys.path.append('lib/')
from CMIPLib import checkPID,logWrite,pathToFile,xmlLog,xmlWrite
from durolib import mkDirNoOSErr,writeToLog #sysCallTimeout
from multiprocessing import Process,Manager
from socket import gethostname
from string import replace
from subprocess import call,Popen,PIPE
#import cdms2 as cdm

#%%
##### Set batch mode processing, console printing on/off and multiprocess loading #####
batch       = True ; # True = on, False = off
batch_print = False ; # Write log messages to console - suppress from cron daemon ; True = on, False = off
threadCount = 40 ; # ~36hrs xml creation solo ; 50hrs xml creation crunchy & oceanonly in parallel
##### Set batch mode processing, console printing on/off and multiprocess loading #####

# Set time counter and grab timestamp
start_time  = time.time() ; # Set time counter
time_format = datetime.datetime.now().strftime('%y%m%d_%H%M%S')

# Set conditional whether files are created or just numbers are calculated
if batch:
    parser = argparse.ArgumentParser()
    parser.add_argument('makefiles',metavar='str',type=str,help='\'makefiles\' as a command line argument will write xml files, \'report\' will produce a logfile with path information')
    args = parser.parse_args()
    if (args.makefiles == 'makefiles'):
       make_xml = 1 ; # 1 = make files
       print(time_format)
       print('** Write mode - new *.xml files will be written **')
    elif (args.makefiles == 'report'):
       make_xml = 0 ; # 0 = don't make files, just report
       print(time_format)
       print('** Report mode - no *.xml files will be written **')
else:
    make_xml = 1
    print(time_format)
    print('** Non-batch mode - new *.xml files will be written **')

# Set directories
host_name = gethostname()
if host_name in {'crunchy.llnl.gov','oceanonly.llnl.gov','grim.llnl.gov'}:
    trim_host = replace(host_name,'.llnl.gov','')
    if batch:
        #host_path = '/work/cmip5/' ; # BATCH MODE - oceanonly 130605
        host_path = '/work/cmip5-test/' ; # BATCH MODE - oceanonly 180222
        log_path = os.path.join(host_path,'_logs')
    else:
        host_path = '/work/durack1/Shared/cmip5/tmp/' ; # WORK_MODE_TEST - oceanonly 130605 #TEST#
        log_path = host_path
    #cdat_path = sys.executable.replace('python','') ; # Update to system-called python install
    cdat_path = '/usr/local/uvcdat/latest/bin/' ; # Reinstate old "stable" version
else:
    print('** HOST UNKNOWN, aborting.. **')
    sys.exit()

# Change directory to host
os.chdir(host_path)

# Set logfile attributes
time_format = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
pypid = str(os.getpid()) ; # Returns calling python instance, so master also see os.getppid() - Parent
logfile = os.path.join(log_path,"".join([time_format,'_make_cmip5_xml-',trim_host,'-threads',str(threadCount),'-PID',pypid,'.log']))
if ~os.path.exists(log_path):
    mkDirNoOSErr(log_path)
# Logging the explicit searched data path
os.chdir('/p/css03')
cmd = 'df -h | grep cmip5'
#sysCallTimeout(cmd,5) ; # Test for network connectivity and fail if /cmip5_css02 not alive
p = Popen(cmd,shell=True,stdout=PIPE,stderr=PIPE)
out1,err = p.communicate()
cmd = 'df -h | grep css03'
p = Popen(cmd,shell=True,stdout=PIPE,stderr=PIPE)
out2,err = p.communicate()
out = ''.join([out1,out2])
writeToLog(logfile,"".join(['TIME: ',time_format]))
writeToLog(logfile,"".join(['MASTER PID: ',pypid]))
print("".join(['master pid',pypid])) ; # Write master PID to sendmail/stdout
writeToLog(logfile,"".join(['UV-CDAT: ',sys.executable]))
print(''.join(['UV-CDAT: ',sys.executable])) ; # Write UV-CDAT/python path to sendmail/stdout
writeToLog(logfile,"".join(['HOSTNAME: ',host_name]))
writeToLog(logfile,"".join(['FUNCTION: ','scandir.walk']))
writeToLog(logfile,"".join(['SOURCEFILES:\n',out]))
del(trim_host,time_format,cmd,p,out,err)
gc.collect()

# Generate queue objects
manager0 = Manager()
## Mine for paths and files
queue1 = manager0.Queue(maxsize=0)
p1 = Process(target=pathToFile,args=('/p/css03/cmip5_css01/data/cmip5/output1/',start_time,queue1))
p1.start() ; print(''.join(['p1 pid: ',str(p1.ident)]))
queue2 = manager0.Queue(maxsize=0)
p2 = Process(target=pathToFile,args=('/p/css03/cmip5_css01/data/cmip5/output2/',start_time,queue2))
p2.start() ; print(''.join(['p2 pid: ',str(p2.ident)]))
queue3 = manager0.Queue(maxsize=0)
p3 = Process(target=pathToFile,args=('/p/css03/cmip5_css02/data/cmip5/output1/',start_time,queue3))
p3.start() ; print(''.join(['p3 pid: ',str(p3.ident)]))
queue4 = manager0.Queue(maxsize=0)
p4 = Process(target=pathToFile,args=('/p/css03/cmip5_css02/data/cmip5/output2/',start_time,queue4))
p4.start() ; print(''.join(['p4 pid: ',str(p4.ident)]))
# Plus Jeff's list
queue5 = manager0.Queue(maxsize=0)
p5 = Process(target=pathToFile,args=('/p/css03/scratch/cmip5/output1/',start_time,queue5))
p5.start() ; print(''.join(['p5 pid: ',str(p5.ident)]))
queue6 = manager0.Queue(maxsize=0)
p6 = Process(target=pathToFile,args=('/p/css03/scratch/published-latest/cmip5/output1/',start_time,queue6))
p6.start() ; print(''.join(['p6 pid: ',str(p6.ident)]))
queue7 = manager0.Queue(maxsize=0)
p7 = Process(target=pathToFile,args=('/p/css03/scratch/published-latest/cmip5/cmip5_css01/scratch/cmip5/output1/',start_time,queue7))
p7.start() ; print(''.join(['p7 pid: ',str(p7.ident)]))
queue8 = manager0.Queue(maxsize=0)
p8 = Process(target=pathToFile,args=('/p/css03/scratch/published-older/cmip5/output1/',start_time,queue8))
p8.start() ; print(''.join(['p8 pid: ',str(p8.ident)]))
queue9 = manager0.Queue(maxsize=0)
p9 = Process(target=pathToFile,args=('/p/css03/scratch/should-publish/cmip5/output1/',start_time,queue9))
p9.start() ; print(''.join(['p9 pid: ',str(p9.ident)]))
queue10 = manager0.Queue(maxsize=0)
p10 = Process(target=pathToFile,args=('/p/css03/scratch/unknown-dset/cmip5/output1/',start_time,queue10))
p10.start() ; print(''.join(['p10 pid: ',str(p10.ident)]))
queue11 = manager0.Queue(maxsize=0)
p11 = Process(target=pathToFile,args=('/p/css03/scratch/unknown-status/cmip5/output/',start_time,queue11))
p11.start() ; print(''.join(['p11 pid: ',str(p11.ident)]))
queue12 = manager0.Queue(maxsize=0)
p12 = Process(target=pathToFile,args=('/p/css03/scratch/unknown-status/cmip5/output1/',start_time,queue12))
p12.start() ; print(''.join(['p12 pid: ',str(p12.ident)]))
# Jeff new path
queue13 = manager0.Queue(maxsize=0)
p13 = Process(target=pathToFile,args=('/p/css03/scratch/obsolete/cmip5/output1/',start_time,queue13))
p13.start() ; print(''.join(['p13 pid: ',str(p13.ident)]))
# Sasha new publication path
queue14 = manager0.Queue(maxsize=0)
p14 = Process(target=pathToFile,args=('/p/css03/esgf_publish/cmip5/output1/',start_time,queue14))
p14.start() ; print(''.join(['p14 pid: ',str(p14.ident)]))

# Consider parallelising css02_scratch in particular - queue object doesn't play with p.map
'''
http://stackoverflow.com/questions/7588620/os-walk-multiple-directories-at-once
http://stackoverflow.com/questions/141291/how-to-list-only-yop-level-directories-in-python
from multiprocessing import Pool
folder = '/cmip5_css02/scratch/cmip5/output1/'
paths = [os.path.join(folder,path) for path in os.listdir(folder) if os.path.isdir(os.path.join(folder,path))]
p = Pool(len(paths))
[data_outfiles,data_outfiles_paths,time_since_start,i1,i2,len_vars] = p.map(pathToFile,paths)

130227 - Charles, create a list of queues and a list of processes through which to iterate and build the full
         outfiles and outfile_paths variables - then pass this as many paths as possible

def job(path,id):
    start_time  = time.time() ; # Set time counter
    queue1      = manager0.Queue(maxsize=0)
    p1          = Process(target=pathToFile,args=(path,start_time,queue1))
    p1.start()
    print "".join(['p1 pid: ',str(p1.ident)])
    return p1,queue1

queues=[] ; processes=[]
for path,id in zip(paths,ids):
    p,q = job(path,id)
    processes.append(p)
    queues.append(q)

for i,p in enumerate(processes):
    q   = queues[i]
    id  = ids[i]
    p.join()
    [css02_scratch_outfiles,css02_scratch_outfiles_paths,time_since_start,i1,i2,len_vars] = q.get_nowait()
    logWrite(logfile,time_since_start,id,i1,css02_scratch_outfiles,len_vars)
    outfiles.extend()
    outfiles_paths.extend()
'''

# Write to logfile
p1.join()
[p1_outfiles,p1_outfiles_paths,time_since_start,i1,i2,len_vars] = queue1.get_nowait()
logWrite(logfile,time_since_start,'css01_output1',i1,p1_outfiles,len_vars)
p2.join()
[p2_outfiles,p2_outfiles_paths,time_since_start,i1,i2,len_vars] = queue2.get_nowait()
logWrite(logfile,time_since_start,'css01_output2',i1,p2_outfiles,len_vars)
p3.join()
[p3_outfiles,p3_outfiles_paths,time_since_start,i1,i2,len_vars] = queue3.get_nowait()
logWrite(logfile,time_since_start,'css02_output1',i1,p3_outfiles,len_vars)
p4.join()
[p4_outfiles,p4_outfiles_paths,time_since_start,i1,i2,len_vars] = queue4.get_nowait()
logWrite(logfile,time_since_start,'css02_output2',i1,p4_outfiles,len_vars)
p5.join()
[p5_outfiles,p5_outfiles_paths,time_since_start,i1,i2,len_vars] = queue5.get_nowait()
logWrite(logfile,time_since_start,'scratch/cmip5/output1/',i1,p5_outfiles,len_vars)
p6.join()
[p6_outfiles,p6_outfiles_paths,time_since_start,i1,i2,len_vars] = queue6.get_nowait()
logWrite(logfile,time_since_start,'scratch/published-latest/cmip5/output1/',i1,p6_outfiles,len_vars)
p7.join()
[p7_outfiles,p7_outfiles_paths,time_since_start,i1,i2,len_vars] = queue7.get_nowait()
logWrite(logfile,time_since_start,'scratch/published-latest/cmip5/cmip5_css01/scratch/cmip5/output1/',i1,p7_outfiles,len_vars)
p8.join()
[p8_outfiles,p8_outfiles_paths,time_since_start,i1,i2,len_vars] = queue8.get_nowait()
logWrite(logfile,time_since_start,'scratch/published-older/cmip5/output1/',i1,p8_outfiles,len_vars)
p9.join()
[p9_outfiles,p9_outfiles_paths,time_since_start,i1,i2,len_vars] = queue9.get_nowait()
logWrite(logfile,time_since_start,'scratch/should-publish/cmip5/output1/',i1,p9_outfiles,len_vars)
p10.join()
[p10_outfiles,p10_outfiles_paths,time_since_start,i1,i2,len_vars] = queue10.get_nowait()
logWrite(logfile,time_since_start,'scratch/unknown-dset/cmip5/output1/',i1,p10_outfiles,len_vars)
p11.join()
[p11_outfiles,p11_outfiles_paths,time_since_start,i1,i2,len_vars] = queue11.get_nowait()
logWrite(logfile,time_since_start,'scratch/unknown-status/cmip5/output/',i1,p11_outfiles,len_vars)
p12.join()
[p12_outfiles,p12_outfiles_paths,time_since_start,i1,i2,len_vars] = queue12.get_nowait()
logWrite(logfile,time_since_start,'scratch/unknown-status/cmip5/output1/',i1,p12_outfiles,len_vars)
p13.join()
[p13_outfiles,p13_outfiles_paths,time_since_start,i1,i2,len_vars] = queue13.get_nowait()
logWrite(logfile,time_since_start,'scratch/obsolete/cmip5/output1/',i1,p13_outfiles,len_vars)
p14.join()
[p14_outfiles,p14_outfiles_paths,time_since_start,i1,i2,len_vars] = queue14.get_nowait()
logWrite(logfile,time_since_start,'esgf_publish/cmip5/output1/',i1,p14_outfiles,len_vars)

# Generate master lists from sublists
outfiles_paths = list(p1_outfiles_paths)
outfiles_paths.extend(p2_outfiles_paths)
outfiles_paths.extend(p3_outfiles_paths)
outfiles_paths.extend(p4_outfiles_paths)
outfiles_paths.extend(p5_outfiles_paths)
outfiles_paths.extend(p6_outfiles_paths)
outfiles_paths.extend(p7_outfiles_paths)
outfiles_paths.extend(p8_outfiles_paths)
outfiles_paths.extend(p9_outfiles_paths)
outfiles_paths.extend(p10_outfiles_paths)
outfiles_paths.extend(p11_outfiles_paths)
outfiles_paths.extend(p12_outfiles_paths)
outfiles_paths.extend(p13_outfiles_paths)
outfiles_paths.extend(p14_outfiles_paths)

outfiles = list(p1_outfiles)
outfiles.extend(p2_outfiles)
outfiles.extend(p3_outfiles)
outfiles.extend(p4_outfiles)
outfiles.extend(p5_outfiles)
outfiles.extend(p6_outfiles)
outfiles.extend(p7_outfiles)
outfiles.extend(p8_outfiles)
outfiles.extend(p9_outfiles)
outfiles.extend(p10_outfiles)
outfiles.extend(p11_outfiles)
outfiles.extend(p12_outfiles)
outfiles.extend(p13_outfiles)
outfiles.extend(p14_outfiles)

# Sort lists by outfiles
outfilesAndPaths = zip(outfiles,outfiles_paths)
outfilesAndPaths.sort() ; # sort by str value forgetting case - key=str.lower; requires str object
del(outfiles,outfiles_paths)
gc.collect()
outfiles,outfiles_paths = zip(*outfilesAndPaths)

# Truncate duplicates from lists
outfiles_new = []; outfiles_paths_new = []; counter = 0
for count,testfile in enumerate(outfiles):
    if count < len(outfiles)-1:
        if counter == 0:
            # Deal with first file instance
            outfiles_new.append(outfiles[counter])
            outfiles_paths_new.append(outfiles_paths[counter])
        # Create first file to check
        file1 = outfiles_new[counter]
        # Create second file to check
        file2 = outfiles[count+1]
        if file1 == file2:
            continue
        else:
            outfiles_new.append(file2)
            outfiles_paths_new.append(outfiles_paths[count+1])
            counter = counter+1

# For debugging save to file
time_format = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
outFile = os.path.join(log_path,"".join([time_format,'_list_outfiles.cpkl']))
f1 = open(outFile,'wb')
cPickle.dump([outfiles,outfiles_new,outfiles_paths,outfiles_paths_new],f1)
f1.close()
fIn = open(outFile,'rb')
gzfile = replace(outFile,'.cpkl','.cpkl.gz')
f2 = gzip.open(gzfile,'wb')
f2.writelines(fIn)
f2.close()
fIn.close()
os.remove(outFile)
del(outFile,gzfile,f1,f2,fIn,time_format,i1,i2,len_vars,time_since_start) ; gc.collect()

# Reallocate variables
outfiles = outfiles_new
outfiles_paths = outfiles_paths_new
del(count,counter,testfile,outfiles_new,outfiles_paths_new)

'''
# Debug code for duplicate removal/checking
import os,pickle
picklefile = '/work/durack1/Shared/cmip5/tmp/130228_024214_list_outfiles.pickle'
f = open(picklefile,'r')
outfiles,outfiles_new,outfiles_paths,outfiles_paths_new = pickle.load(f)
f.close()

for count,path in enumerate(outfiles_paths_new):
    if any( (bad in path) for bad in badstuff):
        print count,path

130225 1344: Check for remaining duplicates
counter = 1
for count,testfile in enumerate(outfiles):
    if count < len(outfiles):
        file1 = testfile
        file2 = outfiles[count+1]
        if file1 == file2:
            print counter,count,file1
            print counter,count,outfiles_paths[count]
            print counter,count+1,file2
            print counter,count+1,outfiles_paths[count+1]
            print '----------'
            counter = counter+1
'''

#%% Check whether running for file reporting or xml generation:
if make_xml:
    # Check to ensure previous xml creation run has successfully completed or terminated
    logFiles = glob.glob(os.path.join(log_path,'*.log*')) ; logFiles.sort()
    logCount = len(logFiles)-1
    # First check current process is running
    logFile = logFiles[logCount]
    PID = logFile.split('-')
    if PID[-1].split('.')[-1] == 'gz':
        extIndex = -3
    else:
        extIndex = -2
    PID = replace(PID[-1].split('.')[extIndex],'PID','')
    if checkPID(PID):
        reportStr = ''.join(['** make_cmip5_xml.py run (PID: ',str(PID),') starting, querying for existing previous process **'])
        print(reportStr)
        writeToLog(logfile,reportStr)
        logCount = logCount-1 ; # decrement count by 1
    else:
        pass

    # Check previous process existence - assumes no 'test' logs have been created
    logFile = logFiles[logCount]
    PID = logFile.split('-')
    if PID[-1].split('.')[-1] == 'gz':
        extIndex = -3
    else:
        extIndex = -2
    PID = replace(PID[-1].split('.')[extIndex],'PID','')
    if checkPID(PID):
        reportStr = ''.join(['** previous make_cmip5_xml.py run (PID: ',str(PID),') still active, terminating current process **'])
        print(reportStr)
        writeToLog(logfile,reportStr)
        sys.exit()
    else:
        reportStr = ''.join(['** previous make_cmip5_xml.py run (PID: ',str(PID),') not found, continuing current process **'])
        print(reportStr)
        writeToLog(logfile,reportStr)

    del(extIndex,logFiles,logCount,logFile,PID,reportStr) ; gc.collect()

    # Create counters for xml_good and xml_bad
    xmlGood,xmlBad1,xmlBad2,xmlBad3,xmlBad4,xmlBad5 = [1 for _ in range(6)]
    # Deal with existing *.xml files
    o = glob.glob("".join([host_path,'*/*/mo/*/*.xml']))
    xml_count1 = len(o)
    o = glob.glob("".join([host_path,'*/fx/*/*.xml']))
    xml_count2 = len(o)
    xml_count = int(xml_count1)+int(xml_count2);
    print(''.join(['** Updating ',format(xml_count,"1d"),' existing *.xml files **']))
    writeToLog(logfile,"".join(['** Updating ',format(xml_count,"1d"),' existing *.xml files **']))
    # Catch errors with system commands
    cmd = "".join(['rm -rf ',host_path,'*/*/mo_new'])
    fnull = open(os.devnull,'w')
    p = call(cmd,stdout=fnull,shell=True)
    fnull.close()
    cmd = "".join(['rm -rf ',host_path,'*/fx_new'])
    fnull = open(os.devnull,'w')
    p = call(cmd,stdout=fnull,shell=True)
    fnull.close()
    cmd = "".join(['rm -rf ',host_path,'*/*/fx_new']) ; # Cleanup issues with invalid fx_var entries
    fnull = open(os.devnull,'w')
    p = call(cmd,stdout=fnull,shell=True)
    fnull.close()
    print('** Generating new *.xml files **')
    writeToLog(logfile,"** Generating new *.xml files **")
    i = 0

    # Loop through all inpaths and outfiles
    while i <= (len(outfiles)-threadCount):
        # Case where full threads are used
        threads = threadCount
        # Create queue and pool variables
        queue0 = manager0.Queue() ; pool = []
        for n in range(threads):
            p =  Process(target=xmlWrite,args=(outfiles_paths[i+n],outfiles[i+n],host_path,cdat_path,start_time,queue0))
            p.start() ; pool.append(p)

        # Wait for processes to terminate
        for p in pool:
            p.join()

        # Get data back from queue object
        inpaths = [] ; outfileNames = [] ; fileZeros = [] ; fileWarnings = [] ; fileNoReads = [] ; fileNoWrites = [] ; fileNones = [] ; errorCodes = [] ; time_since_starts = []
        while not queue0.empty():
            [inpath,outfileName,fileZero,fileWarning,fileNoRead,fileNoWrite,fileNone,errorCode,time_since_start] = queue0.get_nowait()
            inpaths.append(inpath)
            outfileNames.append(outfileName)
            fileZeros.append(fileZero)
            fileWarnings.append(fileWarning)
            fileNoReads.append(fileNoRead)
            fileNoWrites.append(fileNoWrite)
            fileNones.append(fileNone)
            errorCodes.append(errorCode)
            time_since_starts.append(time_since_start)

        # Purge queue and pool object
        del(queue0,pool) ; gc.collect()
        # Sort lists by time_since_start
        tmp = zip(time_since_starts,inpaths,outfileNames,fileZeros,fileWarnings,fileNoReads,fileNoWrites,fileNones,errorCodes)
        tmp.sort() ; # sort by str value forgetting case - key=str.lower; requires str object
        time_since_starts,inpaths,outfileNames,fileZeros,fileWarnings,fileNoReads,fileNoWrites,fileNones,errorCodes = zip(*tmp)
        # Loop through inputs and log
        for n in range(threads):
            [xmlBad1,xmlBad2,xmlBad3,xmlBad4,xmlBad5,xmlGood] = xmlLog(logfile,fileZeros[n],fileWarnings[n],fileNoWrites[n],fileNoReads[n],fileNones[n],errorCodes[n],batch_print,inpaths[n],outfileNames[n],time_since_starts[n],(i+n),xmlBad1,xmlBad2,xmlBad3,xmlBad4,xmlBad5,xmlGood)

        # Increment counter and check for completion
        i = i + threads
        if i == len(outfiles):
            break

    else:
        # Case where partial threads are used
        threads = len(outfiles)-i
        # Create queue and pool variables
        queue0 = manager0.Queue() ; pool = []
        for n in range(threads):
            p =  Process(target=xmlWrite,args=(outfiles_paths[i+n],outfiles[i+n],host_path,cdat_path,start_time,queue0))
            p.start() ; pool.append(p)

        # Wait for processes to terminate
        for p in pool:
            p.join()

        # Get data back from queue object
        inpaths = [] ; outfileNames = [] ; fileZeros = [] ; fileWarnings = [] ; fileNoReads = [] ; fileNoWrites = [] ; fileNones = [] ; errorCodes = [] ; time_since_starts = []
        while not queue0.empty():
            [inpath,outfileName,fileZero,fileWarning,fileNoRead,fileNoWrite,fileNone,errorCode,time_since_start] = queue0.get_nowait()
            inpaths.append(inpath)
            outfileNames.append(outfileName)
            fileZeros.append(fileZero)
            fileWarnings.append(fileWarning)
            fileNoReads.append(fileNoRead)
            fileNoWrites.append(fileNoWrite)
            fileNones.append(fileNone)
            errorCodes.append(errorCode)
            time_since_starts.append(time_since_start)

        # Purge queue and pool object
        del(queue0,pool) ; gc.collect()
        # Sort lists by time_since_start
        tmp = zip(time_since_starts,inpaths,outfileNames,fileZeros,fileWarnings,fileNoReads,fileNoWrites,fileNones,errorCodes)
        tmp.sort() ; # sort by str value forgetting case - key=str.lower; requires str object
        time_since_starts,inpaths,outfileNames,fileZeros,fileWarnings,fileNoReads,fileNoWrites,fileNones,errorCodes = zip(*tmp)
        # Loop through inputs and log
        for n in range(threads):
            [xmlBad1,xmlBad2,xmlBad3,xmlBad4,xmlBad5,xmlGood] = xmlLog(logfile,fileZeros[n],fileWarnings[n],fileNoWrites[n],fileNoReads[n],fileNones[n],errorCodes[n], batch_print, inpaths[n],outfileNames[n],time_since_starts[n],(i+n),xmlBad1,xmlBad2,xmlBad3,xmlBad4,xmlBad5,xmlGood)

        # Increment counter
        i = i + threads

    # Create master list of xmlBad and log final to file and console
    xmlBad = xmlBad1+xmlBad2+xmlBad3+xmlBad4+xmlBad5
    print(''.join(['** Complete for \'data\' & \'scratch\' sources; Total outfiles: ',format(len(outfiles),"01d"),' **']))
    print(''.join(['** XML file count - Good: ',format(xmlGood-1,"1d"),' **']))
    print(''.join(['** XML file count - Bad/skipped: ',format(xmlBad-5,"1d"),'; bad1 (cdscan - zero files): ',format(xmlBad1-1,"1d"),'; bad2 (cdscan - warning specified): ',format(xmlBad2-1,"1d"),'; bad3 (read perms): ',format(xmlBad3-1,"1d"),'; bad4 (no outfile): ',format(xmlBad4-1,"1d"),'; bad5 (no infiles): ',format(xmlBad5-1,"1d")]))
    writeToLog(logfile,"".join(['** make_cmip5_xml.py complete for \'data\' & \'scratch\' sources; Total outfiles: ',format(len(outfiles),"01d"),' **']))
    writeToLog(logfile,"".join(['** XML file count - Good: ',format(xmlGood-1,"1d"),' **']))
    writeToLog(logfile,"".join(['** XML file count - Bad/skipped: ',format(xmlBad-5,"1d"),'; bad1 (cdscan - zero files): ',format(xmlBad1-1,"1d"),'; bad2 (cdscan - warning specified): ',format(xmlBad2-1,"1d"),'; bad3 (read perms): ',format(xmlBad3-1,"1d"),'; bad4 (no outfile): ',format(xmlBad4-1,"1d"),'; bad5 (no infiles): ',format(xmlBad5-1,"1d"),' **']))

    # Once run is complete, and xmlGood > 1e5, archive old files and move new files into place
    if xmlGood > 1e5:
        time_now = datetime.datetime.now()
        time_format = time_now.strftime("%y%m%d_%H%M%S")
        # Ensure /cmip5 directory is cwd
        os.chdir(host_path)
        # Archive old files
        cmd = "".join(['_archive/7za a ',host_path,'_archive/',time_format,'_cmip5_xml.7z */*/*/*/*.xml -t7z'])
        #cmd = "".join(['_archive/7za a ',host_path,'_archive/',time_format,'_cmip5_xml.7z */*/mo/*/*.xml -t7z']) ; # Only backup old *.xml files
        args = shlex.split(cmd) ; # Create input argument of type list - shell=False requires this, shell=True tokenises (lists) and runs this
        fnull = open(os.devnull,'w')
        #fnull = open("".join([time_format,'_7za.log']),'w') ; # Debug binary being called from script
        print('args1:',args)
        p = call(args,stdout=fnull,shell=False) ; # call runs in the foreground, so script will wait for termination
        fnull.close()
        # Purge old files [durack1@crunchy cmip5]$ rm -rf */*/mo
        cmd = 'rm -rf */*/mo'
        fnull = open(os.devnull,'w')
        p = call(cmd,stdout=fnull,shell=True)
        fnull.close()
        fnull = open(os.devnull,'w')
        cmd = 'rm -rf fx/fx'
        p = call(cmd,stdout=fnull,shell=True)
        cmd = 'rm -rf */fx' ; # Purge existing subdirs beneath $EXPERIMENT/fx
        p = call(cmd,stdout=fnull,shell=True)
        fnull.close()
        # Move new files into place
        cmd = 'find */*/mo_new -maxdepth 0 -exec sh -c \'mv -f `echo {}` `echo {} | sed s/mo_new/mo/`\' \;'
        fnull = open(os.devnull,'w')
        p = call(cmd,stdout=fnull,shell=True)
        fnull.close()
        cmd = 'find fx/fx_new -maxdepth 0 -exec sh -c \'mv -f `echo {}` `echo {} | sed s/fx_new/fx/`\' \;'
        fnull = open(os.devnull,'w')
        p = call(cmd,stdout=fnull,shell=True)
        fnull.close()
        # Wash new directories with fresh permissions
        cmd = 'chmod 755 -R */*/mo' ; # Pete G needs x to list directories
        fnull = open(os.devnull,'w')
        p = call(cmd,stdout=fnull,shell=True)
        fnull.close()
        cmd = 'chmod 755 -R */fx'
        fnull = open(os.devnull,'w')
        p = call(cmd,stdout=fnull,shell=True)
        fnull.close()
        del(time_now,cmd,fnull,p)
        gc.collect()
        #[durack1@crunchy cmip5]$ find */*/mo_new -maxdepth 0 -exec sh -c 'mv -f `echo {}` `echo {} | sed s/mo_new/mo/`' \;
        #[durack1@crunchy cmip5]$ ls -d1 */*/mo_new | sed -e 'p;s/mo_new/mo/' | xargs -n 2 mv
        # Report migration success to prompt and log
        print(''.join(['** Archive and migration complete from */*/*_new to */*/*, archive file: ',host_path,'_archive/',time_format,'_cmip5_xml.7z **']))
        writeToLog(logfile,"".join(['** Archive and migration complete from */*/*_new to */*/*,\n archive file: ',host_path,'_archive/',time_format,'_cmip5_xml.7z **']))
    else:
        print(''.join(['** XML count too low: ',format(xmlGood-1,"1d") ,', archival, purging and migration halted **']))
        writeToLog(logfile,"".join(['** XML count too low: ',format(xmlGood-1,"1d") ,', archival, purging and migration halted **']))

    # Run complete, now compress logfile
    fIn = open(logfile, 'rb')
    gzfile = replace(logfile,'.log','.log.gz')
    fOut = gzip.open(gzfile, 'wb')
    fOut.writelines(fIn)
    fOut.close()
    fIn.close()
    os.remove(logfile)

else:
    print('** make_cmip5_xml.py run in report mode **')
    writeToLog(logfile,"** make_cmip5_xml.py run in report mode **")
