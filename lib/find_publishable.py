#!/opt/python-2.7/bin/python

# This file has functions useful for finding publishable data.
# On aimsdtn6, type "module load python" before running this.

import sys, sqlite3, pdb
from pprint import pprint
import debug

class ds_info():
    def __init__( self, name, version=None, status=None, publishable=None, latest_version=None,
                  version_from=None, path=None ):
        self.name = name
        self.version = version
        # ... 'version' can be anything, but typically it is a string extracted from a database,
        # and may differ from the version component of the path, if any.
        self.status = status
        self.publishable = publishable
        self.latest_version = latest_version  # possible values: None, True or False
        self.version_from = version_from  # possible values: None, "path", "ESGF"
        self.path = path
    def __repr__( self ):
        return "name=%s, version=%s, path=%s" %\
            (self.name,self.version,self.path)
    def __str__( self ):
        return self.__repr__()

def ds_status( dataset ):
    """Returns status and other useful information about a dataset in CSS-01 or CSS-02.
    This information is extracted from the old database egreplica.db.
    The status information is returned as a list of ds_info objects, a list because there may be
    multiple matches to the specified dataset, or no matches.
    The dataset should be specified as a string, e.g.
    'cmip5.output1.IPSL.IPSL-CM5A-LR.historical.mon.atmos.Amon.r4i1p1'
    The SQL wildcards '%' and '_' are allowed.
    A dataset with status>=100 is publishable if the latest version.
    One with status>=110 has probably already been published.
    "Publishable" means that all files are present and have good checksums.
    This functiondoes not address the issue of whether a more recent version of the dataset
    exists; ds_latest() does that.
    For now, the database location is hard-wired.
    """
    conn = sqlite3.connect("/home/painter/db/egreplica.db")
    c = conn.cursor()
    if dataset.find('%')<0 and dataset.find('_')<0:
        # no wildcards
        cmd = "SELECT name, version, status FROM datasets WHERE name='%s'" % dataset
    else:
        cmd = "SELECT name, version, status FROM datasets WHERE name LIKE '%s'" % dataset
    c.execute( cmd )
    results = c.fetchall()
    infos = [ ds_info( r[0], version=r[1], status=r[2] ) for r in results ]
    infos = sorted( infos, key=(lambda x: x.name ) )
    conn.close()
    return infos

def ds_latest( dataset ):
    """Returns the latest version(s) of the specified dataset.
    This information is extracted from a *_latest database computed from Jason's script
    esg.py, and converted to SQLite.  Thus it can only return information on what has been
    published on the node referenced by esg.py, normally aims3.llnl.gov.
    The dataset should be specified as a string, e.g.
    'cmip5.output1.IPSL.IPSL-CM5A-LR.historical.mon.atmos.Amon.r4i1p1'
    The SQL wildcards '%' and '_' are allowed.
    We return a list of ds_info objects, each containing dataset name and version
    number for the latest version of that name.  And :latest_version is set to True.
    For now, the database location is hard-wired.
    """
    conn = sqlite3.connect("/home/painter/db/aims3.llnl.gov_latest.db")
    c = conn.cursor()
    if dataset.find('%')<0 and dataset.find('_')<0:
        # no wildcards
        cmd = "SELECT name, version FROM dsets WHERE name='%s'" % dataset
    else:
        cmd = "SELECT name, version FROM dsets WHERE name LIKE '%s'" % dataset
    c.execute( cmd )
    results = c.fetchall()
    infos = [ ds_info( r[0], version=r[1], latest_version=True ) for r in results ]
    infos = sorted( infos, key=(lambda x: x.name ) )
    conn.close()
    return infos

def ds_replace( dataset ):
    """like ds_latest() but instead of *_latest.db it references *_replace.db.
    That is, it returns a list of ds_info objects for datasets which we have published
    but do not have the latest version.  The attribute :latest_version is set to False.
    """
    # *_replace.db identifies datasets we have published which are not the latest
    # version, and for which we do not have the latest version
    conn = sqlite3.connect("/home/painter/db/aims3.llnl.gov_replace.db")
    c = conn.cursor()
    cmd = "SELECT name, version FROM dsets WHERE name LIKE '%s'" % dataset
    c.execute( cmd )
    results = c.fetchall()
    infosr = [ ds_info( r[0], version=r[1], latest_version=False ) for r in results ]
    infosr = sorted( infosr, key=(lambda x: x.name ) )
    conn.close()

def ds_pubver( dataset ):
    """If the specified dataset has been published on aims3.llnl.gov (or whatever
    server was used to compute the databases referenced below), then this function
    will return its version number and other information.  The input dataset may
    include SQL wildcards. So the return value will always be a list of ds_info objects.
    In each object, the name will be a dataset name, and the version will be present
    (and not None) if the dataset is found.  For each object, the attribute :latest_version
    will be set to True or False.
    """
    return ds_latest(dataset) + ds_replace(dataset)

def ds_pubvers( datasets ):
    """Like ds_pubver, but the input is a list of datasets (specified as strings)"""
    dsll = [ ds_latest[ds] for ds in datasets ]  # list of lists of ds_info objects
    dsl  = [ dsi for sublist in dsll for dsi in sublist ] # flatten the list of lists
    dsrr = [ ds_replace[ds] for ds in datasets ]  # list of lists of ds_info objects
    dsr  = [ dsi for sublist in dsrr for dsi in sublist ] # flatten the list of lists
    return dsl + dsr    # list of ds_info objects

def vnum( version ):
    """Convert a version string to a number, and return it.  If the string begings with 'v',
    delete that first."""
    if version is None:
        return version
    elif (type(version) is str or type(version) is unicode) and version[0]=='v':
        return int( version[1:] )
    else:
        return int(version)

def setup_databases():
    """Sets up the databases we need: the old replication database and the ones generated by seg.py.
    They should have already been converted to SQLite format.  The file locations are wired in here.
    """
    conn = sqlite3.connect("/home/painter/db/egreplica.db")
    sc = conn.cursor()
    cmda = "ATTACH DATABASE '%s' AS %s" % ("/home/painter/db/aims3.llnl.gov_latest.db",'lc')
    sc.execute(cmda)
    cmda = "ATTACH DATABASE '%s' AS %s" % ("/home/painter/db/aims3.llnl.gov_missing.db",'mc')
    sc.execute(cmda)
    cmda = "ATTACH DATABASE '%s' AS %s" % ("/home/painter/db/aims3.llnl.gov_replace.db",'rc')
    sc.execute(cmda)
    return conn, sc

def match_databases(  sc, ec, dataset, version, path ):
    """Finds dataset matches between the old replication database (specified here as sc, cursor
    result found from it), and an ESGF-based database (specified here with a string-valued name ec
    and a string for the path to the datavase).  If version is not None, the matches will be
    restricted to datasets of that version.  And it is assumed that such a version comes from
    the path.
    """
    # Narrow them down to ones which the old database says we have, and are good:
    version_num = vnum(version)
    cmdmatch = ' '.join(
        ["SELECT dsets.name, dsets.version, datasets.version, datasets.status",
         "FROM %s.dsets, datasets",
         "WHERE dsets.name IN (SELECT name FROM datasets WHERE name LIKE '%s')",
         "AND dsets.name LIKE datasets.name AND datasets.status>=100"]) % (ec,dataset)
    if version is None:
        version_from = None
    else:
        cmdmatch = cmdmatch + " AND ( dsets.version=%s OR dsets.version='v'||%s )"%(version_num,version_num)\
            +"AND (datasets.version=%s OR datasets.version='v'||%s )"%(version_num,version_num)
        version_from = "path"
    sc.execute( cmdmatch )
    resultsl = sc.fetchall()
    infos = [ ds_info( r[0], version=r[2], version_from=version_from, status=r[3],
                       publishable=(vnum(r[2])>=vnum(r[1])), path=path )
              for r in resultsl ]
    return infos

def ds_publishable( dataset, version=None, omit_published=True, path=None,
                    db_egreplica=None ):
    """Determines whether the specified dataset(s) are publishable.
    What is returned is a list of ds_info objects, with a :publishable slot set to True or False.
    The dataset should be specified as a string, e.g.
    'cmip5.output1.IPSL.IPSL-CM5A-LR.historical.mon.atmos.Amon.r4i1p1'
    The SQL wildcards '%' and '_' are allowed.
    "Publishable" means that all files are present and have good checksums, and the dataset
    is the latest version.  Datasets which we have already published do not count as "publishable"
    unless an optional argument is set thus: omit_published=False.  If the dataset is not
    present on ESGF at all, it is assumed to be the latest version.
    If a version (other than None) be specified, only datasets matching that version will be
    considered.  And it is assumed that such a version came from the file path on CSS-xx.
    If a path is provided, it will be used in constructing the ds_info object.
    This function is based on the egreplica.db database for dataset status (i.e., whether complete
    and all files have good checksums) and databases generated from esg.py output
    for the latest available version of the dataset.
    The databases used may be provided in the keyword argument db_egreplica.  This should be
    in SQLite format, representing a port of the old replication database, to which are attached
    ports of the following csv files generated by Jason's script esg.py: *_latest.csv,
    *_missing.csv, *_replace.csv ; with names lc, mc, rc respectively.
    This function does not check the actual files, e.g. whether their directory also contains
    garbage files or directories.
    """
    version_num = vnum(version)

    # Our old replication database, which says what we have (on CSS-01, CSS-03)
    # whether it has good checksums, etc.:
    if db_egreplica is None:
        conn,sc = setup_databases()  # will happen rarely, I expect db_egreplica to be provided
    else:
        sc = db_egreplica

    # Datasets on ESGF which we have published in the latest version:
    infosl = match_databases( sc, 'lc', dataset, version_num, path )

    # Datasets on ESGF which we have not published:
    infosm = match_databases( sc, 'mc', dataset, version_num, path )

    # Datasets on ESGF which we have published in an older version:
    infosr = match_databases( sc, 'rc', dataset, version_num, path)

    # Which good datasets have we missed?  These would not be on ESGF at all:
    # This would include data which we have failed to publish for which we are the original
    # publisher (Russia, Korea, Brazil, all these are ok except INPE decadal data);
    # and data which only we had replicated before the original publisher went offline (FIO,
    # and there are such cases), and data which had been withdrawn or lost by the original
    # publisher (this also has happend, but I can't distinguish between withdrawn and lost).
    if omit_published==True:
        cmdnomatch = ' '.join(
            ["SELECT name, version, status FROM datasets",
             "WHERE name NOT IN (SELECT name FROM mc.dsets WHERE NAME LIKE '%s')",
             "AND   name NOT IN (SELECT name FROM rc.dsets WHERE NAME LIKE '%s')",
             "AND   name LIKE '%s'",
             "AND status>=100"]) % (dataset,dataset,dataset)
    else:
        cmdnomatch = ' '.join(
            ["SELECT name, version, status FROM datasets",
             "WHERE name NOT IN (SELECT name FROM lc.dsets WHERE NAME LIKE '%s')",
             "AND   name NOT IN (SELECT name FROM mc.dsets WHERE NAME LIKE '%s')",
             "AND   name NOT IN (SELECT name FROM rc.dsets WHERE NAME LIKE '%s')",
             "AND   name LIKE '%s'",
             "AND status>=100"]) % (dataset,dataset,dataset,dataset)
    if version is None:
        version_from = None
    else:
        cmdnomatch = cmdnomatch + " AND ( datasets.version=%s OR datasets.version='v'||%s )"\
            % (version_num,version_num)
        version_from = "path"
    sc.execute( cmdnomatch )
    resultsnm = sc.fetchall()
    infosnm = [ ds_info( r[0], version=r[1], version_from=version_from, status=r[2],
                         publishable=True, path=path )
              for r in resultsnm ]

    if False:
        print "  The first of %s datasets in egreplica which are good and have been published:"\
            % len(infosl)
        for r in infosl[:20]:
            print r.name, r.version, '\t', r.publishable
        print "  The first of %s datasets in egreplica which are good but obsolete versions, and have been published:"\
            % len(infosr)
        for r in infosr[:20]:
            print r.name, r.version, '\t', r.publishable
        print "  The first of %s datasets in egreplica which are good and have not been published:"\
            % len(infosm)
        for r in infosm[:20]:
            print r.name, r.version, '\t', r.publishable
        print "  The first of %s datasets in egreplica which are good and don't exist on ESGF:"\
            % len(infosnm)
        for r in infosnm[:100]:
            print r.name, r.version, '\t', r.publishable

    # infos_pub is everything which looks publishable, except where we have already published the
    # latest version.  Thus the contents infosl don't belong in it.  Identify datasets by name only;
    # for this purpose the version and other attributes should be ignored. If omit_publised=False,
    # there is no harm in leaving infosl members in infos_pub.
    infos_pub = infosm + infosr + infosnm
    if omit_published is True:
        namesl = [ ds.name.upper() for ds in infosl ]
        infos_pub = [ ds for ds in infos_pub if ds.name.upper() not in namesl ]
        infosl = []

    # Note: there also are issues with from FIO, where the original publisher is off the net and
    # different sources use different cases for the model name, confusing this and most software.

    infos = infosl + infos_pub
    return infos

def path2dataset( path ):
    """Given a string which possibly includes a path for a single dataset, this function
    (tries to) deduce the dataset name and version.  It returns them both, as well as a flag
    for whether the data was in /scratch/ or /data/, in a 3-tuple (values may be None).
    This function only works for data in /scratch/ or /data/ directories, in the DRS-like
    form we have in CSS-01 and CSS-02.
    """
    if len(path)>1 and path[-1]==':':
        path = path[:-1]
    pathsplit = path.split('/scratch/')
    datascratch = 'scratch'
    if len(pathsplit)<2:
        pathsplit = path.split('/data/')
        datascratch = 'data'
    if len(pathsplit)<2: return (None,None,None,None)
    drspath = pathsplit[1].split('/')[0:10]
    dsetpath = drspath[0:9]
    # a few sanity checks to make sure this looks like a dataset:
    if len(dsetpath)<9: return (None,None,None,None)
    if dsetpath[0].lower()!='cmip5': return (None,None,None,None)
    if dsetpath[5] not in ['yr','mon','day', '6hr', '3hr', 'subhr', 'monClim', 'fx']:
        return (None,None,None,None)
    if dsetpath[8][0]!='r': return (None,None,None,None)
    # Look for a dataset version number
    if len(drspath)==9:
        dsetver = None
    else:
        if drspath[9][0]=='v':
            #drspath[9] = drspath[9][1:]
            vnum_str = drspath[9][1:]
        else:
            vnum_str = drspath[9]
        try:
            #vnum = int(drspath[9])
            throwaway = int(vnum_str)# just checking that vnum_str looks like an int
            dsetver = drspath[9]
        except:
            dsetver = None
            if datascratch=='data':
                drspath = drspath[0:9]
    return ( '.'.join(dsetpath), dsetver, datascratch, '/'.join([pathsplit[0],datascratch,
                                                                '/'.join(drspath)] ))

def remove_duplicates( seq, idfun=None, idfun2=None ):
    # adapted from https://www.peterbe.com/plog/uniqifiers-benchmark
    # order preserving.  If idfun2 is provided, it will be used to mark
    # a _second_ item as present, besides the presently encountered one.
    if idfun is None:
        def idfun(x): return x
    seen = {}
    result = []
    for item in seq:
        marker = idfun(item)
        if marker in seen: continue
        seen[marker] = 1
        seen[ idfun2(item) ] = 1
        result.append(item)
    return result

def paths2datasets( lslines ):
    """Input is a list of lines from 'ls -lR' output, many of which are paths which
    implicitly include specification of a dataset.  We return a list of (name,vers,datascratch)
    tuples where name is the name of a dataset (dots between shards) and vers is the
    dataset's version name, or None.  datascratch tells you whether the data is in /data/
    or /scratch/.  If the dataset's path does not include any .nc files, then it is not returned"""
    dset_ls = [] # list of tuples to be returned - describe dataset
    dset_dc = {} # dictionary of tuples, the key is dataset name
    lnos = {}    # first and last line numbers (in lslines), the key is dataset name
    last_dsetname = None
    for lno,line in enumerate(lslines):
        (dsetname,dsetver,datascratch,path) = path2dataset(line)
        if dsetname!=last_dsetname and last_dsetname is not None and lnos[last_dsetname][1] is None:
            # handles some non-DRS-compliant paths we have
            lnos[last_dsetname] = (lnos[last_dsetname][0], lno-3)
        if dsetname is not None:
            dset_dc[dsetname] = (dsetname,dsetver,datascratch,path)
            if dsetname not in lnos:
                lnomin = lno
                lnos[dsetname] = (lno,None)
            else:
                lnos[dsetname] = ( lnomin, lno )
        last_dsetname = dsetname
    for dsetname in dset_dc.keys():
        for lno in range(lnos[dsetname][0],lnos[dsetname][1]+3):
            havenc = False # havenc is whether the dataset's path has .nc files
            if lslines[lno].find('.nc')>0:
                havenc = True
        if havenc is True:
            dset_ls.append( dset_dc[dsetname] )
    return dset_ls
    
    # This paths2datasets works, but doesn't return line numbers, hence it can't figure out whether
    # a path has data.  And it doesn't take advantage of the known ordering of ls -lR output:
    #dsets = []
    #for lno,line in enumerate(lslines):
    #    (dsetname,dsetver,datascratch,path) = path2dataset(line)
    #    if dsetname is not None:
    #        dsets.append( (dsetname,dsetver,datascratch,path) )
    #dsets.sort( key=(lambda x: x[1]), reverse=True)  # if the version x[1]=None, it goes to the end
    #        #...and therefore the following order-preserving removal won't keep x with x[1]=None
    #        # if possible (given that idfun2 ensures that version=None will be marked as present
    #        # if any version appears).  Thus we remember the version if it's there.
    #dsets = remove_duplicates( dsets, idfun=(lambda x: (x[0],x[1])),
    #                           idfun2=(lambda x: (x[0],None)) )
    # this is how to sort if we want to: dsets.sort( key=(lambda x: x[0]) )
    #return dsets

def lsfile2datasets( lsfile ):
    """Input is the name of a file containing 'ls -lR' output.  This just calls
    paths2datasets on the file's contents."""
    conn, sc = setup_databases()
    with open(lsfile) as f:
        lsfile_str = f.read()
    lslines = lsfile_str.split('\n')
    dsets = paths2datasets(lslines)# list of tuples.  name,version,data/scratch,path
    #... Each element of dsets represents a dataset in the file system.
    # Note that if the version here isn't None, it came from the path.
    ds_infos = [ ds_publishable(dset[0],version=dset[1],path=dset[3],db_egreplica=sc) for dset in dsets ]
    # ...list of ds_info objects,including name,version,latest_version
    #.. Each element of ds_infos represents a dataset which is publishable but has not
    # been published on aims3.llnl.gov.
    conn.close()

    ds_infos_flat = [ dsi for sublist in ds_infos for dsi in sublist ] # flatten the list of lists
    return ds_infos_flat

if __name__ == '__main__':
    if len( sys.argv ) > 1:
        # sys.argv[1] could either be a dataset string e.g. cmip5.output1.IPSL....
        # or the name of a file which is "ls -lR..." output.
        dataset = sys.argv[1]
        # pprint( ds_status( dataset ) )
        # pprint( ds_latest( dataset ) )
        # pprint( ds_publishable( dataset ) )
        # ds_publishable( dataset )
        pprint( lsfile2datasets( dataset ) )
    else:
        print "please provide a dataset name"

