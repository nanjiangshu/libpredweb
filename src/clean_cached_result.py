#!/usr/bin/env python

import sys
import os
import sqlite3
import shutil
import argparse
import fcntl

from datetime import datetime
from pytz import timezone
import tempfile
from libpredweb import myfunc
from libpredweb import webserver_common as webcom

TZ = webcom.TZ

progname = os.path.basename(sys.argv[0])
rootname_progname = os.path.splitext(progname)[0]


def clean_cached_result(MAX_KEEP_DAYS, g_params):  # {{{
    """Clean out-dated cached result"""
    path_log = g_params['path_log']
    path_cache = g_params['path_cache']
    logfile = f"{path_log}/{progname}.log"
    errfile = f"{path_log}/{progname}.err"

    db = f"{path_log}/cached_job_finished_date.sqlite3"
    tmpdb = tempfile.mktemp(prefix=f"{db}_")

    webcom.loginfo(f"copy db {db} to tmpdb {tmpdb}", logfile)
    try:
        shutil.copyfile(db, tmpdb)
    except OSError:
        webcom.loginfo(f"Failed to copy {db} to {tmpdb}.", errfile)
        return 1

    md5listfile = f"{path_log}/cache_to_delete.md5list"
    con = sqlite3.connect(tmpdb)
    webcom.loginfo(f"output the outdated md5 list to {md5listfile}", logfile)

    tablename = "data"

    with con:
        cur = con.cursor()
        fpout = open(md5listfile, "w")
        nn_mag = cur.execute(f"SELECT md5, date_finish FROM {tablename}")
        cnt = 0
        chunk_size = 1000
        while True:
            result = nn_mag.fetchmany(chunk_size)
            if not result:
                break
            else:
                for row in result:
                    cnt += 1
                    md5_key = row[0]
                    finish_date_str = row[1]
                    finish_date = webcom.datetime_str_to_time(finish_date_str)
                    current_time = datetime.now(timezone(TZ))
                    timeDiff = current_time - finish_date
                    if timeDiff.days > MAX_KEEP_DAYS:
                        fpout.write(f"{md5_key}\n")
        fpout.close()

        # delete cached result folder and delete the record
        webcom.loginfo("Delete cached result folder and delete the record", logfile)

        hdl = myfunc.ReadLineByBlock(md5listfile)
        lines = hdl.readlines()
        cnt = 0
        while lines is not None:
            for line in lines:
                line = line.strip()
                if line != "":
                    cnt += 1
                    md5_key = line

                    subfoldername = md5_key[:2]
                    cachedir = os.path.join(path_cache, subfoldername, md5_key)
                    zipfile_cache = cachedir + ".zip"
                    if os.path.exists(zipfile_cache):
                        try:
                            os.remove(zipfile_cache)
                            webcom.loginfo(f"rm {zipfile_cache}", logfile)
                            cmd_d = f"DELETE FROM {tablename} WHERE md5 = '{md5_key}'"
                            cur.execute(cmd_d)
                        except Exception as e:
                            webcom.loginfo(f"Failed to delete with errmsg {e}", errfile)
                            pass

            lines = hdl.readlines()
        hdl.close()

        webcom.loginfo(f"VACUUM the database {tmpdb}", logfile)
        cur.execute("VACUUM")

    # copy back
    webcom.loginfo(f"cp tmpdb {tmpdb} -> db {db}", logfile)
    try:
        shutil.copyfile(tmpdb, db)
    except Exception as e:
        webcom.loginfo(f"Failed to copy {tmpdb} to {db} with {e}", errfile)
        return 1

    webcom.loginfo(f"delete tmpdb {tmpdb}", logfile)
    try:
        os.remove(tmpdb)
    except Exception as e:
        webcom.loginfo(f"Failed to delete {tmpdb} with {e}", errfile)
        return 1

    return 0
# }}}


def main(g_params):  # {{{
    """main procedure"""
    parser = argparse.ArgumentParser(
            description='Clean outdated cached results',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog=f'''\
Created 2018-10-21, updated 2022-03-10, Nanjiang Shu

Examples:
    {progname} -max-keep-day 360
''')
    parser.add_argument('-i', metavar='JSONFILE', dest='jsonfile',
                        type=str, required=True,
                        help='Provide the Json file with all parameters')
    parser.add_argument('-max-keep-day', metavar='INT', dest='max_keep_days',
                        default=360, type=int, required=False,
                        help='The age of the cached result to be kept,\
                             (default: 360)')
    args = parser.parse_args()

    MAX_KEEP_DAYS = args.max_keep_days
    jsonfile = args.jsonfile

    if not os.path.exists(jsonfile):
        print(f"Jsonfile {jsonfile} does not exist. Exit {progname}!",
              file=sys.stderr)
        return 1

    g_params.update(webcom.LoadJsonFromFile(jsonfile))

    lockname = f"{rootname_progname}.lock"
    lock_file = os.path.join(g_params['path_log'], lockname)
    g_params['lockfile'] = lock_file
    fp = open(lock_file, 'w')
    try:
        fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        webcom.loginfo(f"Another instance of {progname} is running",
                       g_params['gen_logfile'])
        return 1

    status = clean_cached_result(MAX_KEEP_DAYS, g_params)
    if os.path.exists(lock_file):
        try:
            os.remove(lock_file)
        except OSError:
            webcom.loginfo(f"Failed to delete lock_file {lock_file}",
                           g_params['gen_logfile'])
    return status

def InitGlobalParameter():#{{{
    g_params = {}
    g_params['lockfile'] = ""
    return g_params
#}}}

if __name__ == '__main__':
    g_params = InitGlobalParameter()
    try:
        status = main(g_params) 
    except Exception as e:
        webcom.loginfo("Error occurred: " + str(e), g_params['gen_logfile'])
        status = 1
    finally:
        if os.path.exists(g_params['lockfile']):
            try:
                os.remove(g_params['lockfile'])
            except:
                webcom.loginfo("Failed to delete lockfile %s\n" % (g_params['lockfile']), g_params['gen_logfile'])  
    sys.exit(status)
