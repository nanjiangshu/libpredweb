#!/usr/bin/env python
"""Run server statistics"""

import sys
import os
import argparse
import fcntl
import time
from geoip import geolite2
import pycountry
import numpy
import sqlite3

from libpredweb import myfunc
from libpredweb import webserver_common as webcom
from libpredweb import dataprocess

progname = os.path.basename(sys.argv[0])
rootname_progname = os.path.splitext(progname)[0]


def run_statistics(g_params):  # {{{
    """Server usage analysis"""
    name_server = g_params['name_server']
    logfile = g_params['logfile']
    errfile = g_params['errfile']
    webserver_root = g_params['webserver_root']
    run_statistics_basic(webserver_root, logfile, errfile)
    if name_server.lower() == "topcons2":
        run_statistics_topcons2(webserver_root, logfile, errfile)
    return 0
# }}}


def run_statistics_basic(webserver_root, logfile, errfile):  # {{{
    """Function for qd_fe to run usage statistics for the web-server usage
    """
    path_static = os.path.join(webserver_root, "proj", "pred", "static")
    path_log = os.path.join(path_static, 'log')
    path_result = os.path.join(path_static, 'result')
    path_stat = os.path.join(path_log, 'stat')
    binpath_plot = os.path.join(webserver_root, "env", "bin")

    # 1. calculate average running time, only for those sequences with time.txt
    # show also runtime of type and runtime -vs- seqlength
    webcom.loginfo("Run basic usage statistics...\n", logfile)
    allfinishedjoblogfile = f"{path_log}/all_finished_job.log"
    runtimelogfile = f"{path_log}/jobruntime.log"
    runtimelogfile_finishedjobid = f"{path_log}/jobruntime_finishedjobid.log"
    allsubmitjoblogfile = f"{path_log}/all_submitted_seq.log"
    if not os.path.exists(path_stat):
        os.mkdir(path_stat)

    allfinishedjobidlist = myfunc.ReadIDList2(allfinishedjoblogfile,
                                              col=0, delim="\t")
    runtime_finishedjobidlist = myfunc.ReadIDList(runtimelogfile_finishedjobid)
    toana_jobidlist = list(set(allfinishedjobidlist) -
                           set(runtime_finishedjobidlist))

    db_allfinished = f"{path_log}/all_finished_job.sqlite3"
    db_allsubmitted = f"{path_log}/all_submitted_job.sqlite3"
    sql_tablename = "data"

    for jobid in toana_jobidlist:
        runtimeloginfolist = []
        rstdir = "%s/%s" % (path_result, jobid)
        outpath_result = "%s/%s" % (rstdir, jobid)
        finished_seq_file = "%s/finished_seqs.txt" % (outpath_result)
        lines = []
        if os.path.exists(finished_seq_file):
            lines = myfunc.ReadFile(finished_seq_file).split("\n")
        for line in lines:
            strs = line.split("\t")
            if len(strs) >= 7:
                str_seqlen = strs[1]
                str_numTM = strs[2]
                str_isHasSP = strs[3]
                source = strs[4]
                if source == "newrun":
                    subfolder = strs[0]
                    timefile = f"{outpath_result}/{subfolder}/time.txt"
                    try:
                        if (os.path.exists(timefile)
                                and os.path.getsize(timefile) > 0):
                            txt = myfunc.ReadFile(timefile).strip()
                            try:
                                ss2 = txt.split(";")
                                runtime_str = ss2[1]
                                database_mode = ss2[2]
                                runtimeloginfolist.append("\t".join(
                                    [
                                        jobid, subfolder,
                                        source, runtime_str, database_mode,
                                        str_seqlen,
                                        str_numTM, str_isHasSP
                                    ]))
                            except IndexError:
                                sys.stderr.write("bad timefile %s\n" % (timefile))
                    except ValueError:
                        sys.stderr.write("bad timefile %s\n" % (timefile))


        if runtimeloginfolist:
            # items for the elelment of the list
            # jobid, seq_no, newrun_or_cached, runtime,
            # mtd_profile, seqlen, numTM, iShasSP
            myfunc.WriteFile("\n".join(runtimeloginfolist)+"\n",
                             runtimelogfile, "a", True)
        myfunc.WriteFile(jobid+"\n", runtimelogfile_finishedjobid, "a", True)

# 2. get numseq_in_job vs count_of_jobs, logscale in x-axis
#    get numseq_in_job vs waiting time (time_start - time_submit)
#    get numseq_in_job vs finish time  (time_finish - time_submit)

    allfinished_job_dict = myfunc.ReadFinishedJobLog(allfinishedjoblogfile)
    countjob_country = {}  # ['country'] = [numseq, numjob, ip_set]
    outfile_numseqjob = f"{path_stat}/numseq_of_job.stat.txt"
    outfile_numseqjob_web = f"{path_stat}/numseq_of_job.web.stat.txt"
    outfile_numseqjob_wsdl = f"{path_stat}/numseq_of_job.wsdl.stat.txt"
    countjob_numseq_dict = {}  # count the number jobs for each numseq
    countjob_numseq_dict_web = {}  # numJob for each numseq submitted via web
    countjob_numseq_dict_wsdl = {}  # numJob for each numseq submitted via wsdl

    waittime_numseq_dict = {}
    waittime_numseq_dict_web = {}
    waittime_numseq_dict_wsdl = {}

    finishtime_numseq_dict = {}
    finishtime_numseq_dict_web = {}
    finishtime_numseq_dict_wsdl = {}

    con_f = sqlite3.connect(db_allfinished)
    cur_f = con_f.cursor()
    myfunc.CreateSQLiteTableAllFinished(cur_f, tablename=sql_tablename)
    cur_f.execute('BEGIN;')

    webcom.loginfo("create all finished sql db...\n", logfile)
    for jobid in allfinished_job_dict:
        li = allfinished_job_dict[jobid]
        numseq = -1
        try:
            numseq = int(li[4])
        except (IndexError, ValueError):
            pass
        try:
            method_submission = li[5]
        except IndexError:
            method_submission = ""

        ip = ""
        try:
            ip = li[2]
        except IndexError:
            pass

        country = "N/A"  # this is slow
        try:
            match = geolite2.lookup(ip)
            country = pycountry.countries.get(alpha_2=match.country).name
        except Exception:   # pylint: disable=broad-except
            pass
        if country != "N/A":
            if country not in countjob_country:
                # [numseq, numjob, ip_set]
                countjob_country[country] = [0, 0, set([])]
            if numseq != -1:
                countjob_country[country][0] += numseq
            countjob_country[country][1] += 1
            countjob_country[country][2].add(ip)

        submit_date_str = li[6]
        start_date_str = li[7]
        finish_date_str = li[8]

        # Write SQL for allfinished{{{
        row = {}
        row['jobid'] = jobid
        row['status'] = li[0]
        row['jobname'] = li[1]
        row['email'] = li[3]
        row['ip'] = ip
        row['country'] = country
        row['method_submission'] = method_submission
        row['numseq'] = numseq
        row['submit_date'] = submit_date_str
        row['start_date'] = start_date_str
        row['finish_date'] = finish_date_str
        myfunc.WriteSQLiteAllFinished(cur_f, tablename=sql_tablename,
                                      data=[row])
# }}}

        if numseq != -1:
            if numseq not in countjob_numseq_dict:
                countjob_numseq_dict[numseq] = 0
            countjob_numseq_dict[numseq] += 1
            if method_submission == "web":
                if numseq not in countjob_numseq_dict_web:
                    countjob_numseq_dict_web[numseq] = 0
                countjob_numseq_dict_web[numseq] += 1
            if method_submission == "wsdl":
                if numseq not in countjob_numseq_dict_wsdl:
                    countjob_numseq_dict_wsdl[numseq] = 0
                countjob_numseq_dict_wsdl[numseq] += 1

#           # calculate waittime and finishtime
            is_valid_submit_date = True
            is_valid_start_date = True
            is_valid_finish_date = True
            try:
                submit_date = webcom.datetime_str_to_time(submit_date_str)
            except ValueError:
                is_valid_submit_date = False
            try:
                start_date = webcom.datetime_str_to_time(start_date_str)
            except ValueError:
                is_valid_start_date = False
            try:
                finish_date = webcom.datetime_str_to_time(finish_date_str)
            except ValueError:
                is_valid_finish_date = False

            if is_valid_submit_date and is_valid_start_date:
                waittime_sec = (start_date - submit_date).total_seconds()
                if numseq not in waittime_numseq_dict:
                    waittime_numseq_dict[numseq] = []
                waittime_numseq_dict[numseq].append(waittime_sec)
                if method_submission == "web":
                    if numseq not in waittime_numseq_dict_web:
                        waittime_numseq_dict_web[numseq] = []
                    waittime_numseq_dict_web[numseq].append(waittime_sec)
                if method_submission == "wsdl":
                    if numseq not in waittime_numseq_dict_wsdl:
                        waittime_numseq_dict_wsdl[numseq] = []
                    waittime_numseq_dict_wsdl[numseq].append(waittime_sec)
            if is_valid_submit_date and is_valid_finish_date:
                finishtime_sec = (finish_date - submit_date).total_seconds()
                if numseq not in finishtime_numseq_dict:
                    finishtime_numseq_dict[numseq] = []
                finishtime_numseq_dict[numseq].append(finishtime_sec)
                if method_submission == "web":
                    if numseq not in finishtime_numseq_dict_web:
                        finishtime_numseq_dict_web[numseq] = []
                    finishtime_numseq_dict_web[numseq].append(finishtime_sec)
                if method_submission == "wsdl":
                    if numseq not in finishtime_numseq_dict_wsdl:
                        finishtime_numseq_dict_wsdl[numseq] = []
                    finishtime_numseq_dict_wsdl[numseq].append(finishtime_sec)

    con_f.commit()
    con_f.close()

    # output countjob by country
    outfile_countjob_by_country = f"{path_stat}/countjob_by_country.txt"
    # sort by numseq in descending order
    li_countjob = sorted(list(countjob_country.items()),
                         key=lambda x: x[1][0], reverse=True)
    li_str = []
    li_str.append("#Country\tNumSeq\tNumJob\tNumIP")
    for li in li_countjob:
        li_str.append("%s\t%d\t%d\t%d" % (li[0], li[1][0], li[1][1], len(li[1][2])))
    myfunc.WriteFile(("\n".join(li_str)+"\n").encode('utf-8'),
                     outfile_countjob_by_country, "wb", True)

    flist = [outfile_numseqjob,
             outfile_numseqjob_web,
             outfile_numseqjob_wsdl]
    dictlist = [countjob_numseq_dict,
                countjob_numseq_dict_web,
                countjob_numseq_dict_wsdl]
    for i, outfile in enumerate(flist):
        dt = dictlist[i]
        sortedlist = sorted(list(dt.items()), key=lambda x: x[0])
        try:
            fpout = open(outfile, "w")
            fpout.write("%s\t%s\n" % ('numseq', 'count'))
            for j in range(len(sortedlist)):
                nseq = sortedlist[j][0]
                count = sortedlist[j][1]
                fpout.write("%d\t%d\n" % (nseq, count))
            fpout.close()
            # plotting
            if os.path.exists(outfile) and sortedlist:
                cmd = [f"{binpath_plot}/plot_numseq_of_job.sh", outfile]
                webcom.RunCmd(cmd, logfile, errfile)
        except IOError:
            continue
    cmd = [f"{binpath_plot}/plot_numseq_of_job_mtp.sh",
           "-web", outfile_numseqjob_web,
           "-wsdl", outfile_numseqjob_wsdl]
    webcom.RunCmd(cmd, logfile, errfile)

# 5. output num-submission time series with different bins
# (day, week, month, year)
    con_s = sqlite3.connect(db_allsubmitted)
    cur_s = con_s.cursor()
    myfunc.CreateSQLiteTableAllSubmitted(cur_s, tablename=sql_tablename)
    cur_s.execute('BEGIN;')

    webcom.loginfo("create all submitted sql db...\n", logfile)
    hdl = myfunc.ReadLineByBlock(allsubmitjoblogfile)
    # ["name" numjob, numseq, numjob_web, numseq_web,numjob_wsdl, numseq_wsdl]
    dict_submit_day = {}
    dict_submit_week = {}
    dict_submit_month = {}
    dict_submit_year = {}
    if not hdl.failure:
        lines = hdl.readlines()
        while lines is not None:
            for line in lines:
                strs = line.split("\t")
                if len(strs) < 8:
                    continue
                submit_date_str = strs[0]
                numseq = 0
                try:
                    numseq = int(strs[3])
                except (IndexError, ValueError):
                    pass
                method_submission = strs[7]
                is_valid_submit_date = True
                try:
                    submit_date = webcom.datetime_str_to_time(submit_date_str)
                except ValueError:
                    is_valid_submit_date = False
                if is_valid_submit_date:  # {{{
                    day_str = submit_date_str.split()[0]
                    (beginning_of_week, end_of_week) = myfunc.week_beg_end(submit_date)
                    week_str = beginning_of_week.strftime("%Y-%m-%d")
                    month_str = submit_date.replace(day=1).strftime("%Y-%m-%d")
                    year_str = submit_date.replace(month=1, day=1).strftime("%Y-%m-%d")
                    day = int(day_str.replace("-", ""))
                    week = int(submit_date.strftime("%Y%V"))
                    month = int(submit_date.strftime("%Y%m"))
                    year = int(submit_date.year)
                    if day not in dict_submit_day:
                        # all   web  wsdl
                        dict_submit_day[day] = [day_str] + 6*[0]
                    if week not in dict_submit_week:
                        dict_submit_week[week] = [week_str] + 6*[0]
                    if month not in dict_submit_month:
                        dict_submit_month[month] = [month_str] + 6*[0]
                    if year not in dict_submit_year:
                        dict_submit_year[year] = [year_str] + 6*[0]
                    dict_submit_day[day][1] += 1
                    dict_submit_day[day][2] += numseq
                    dict_submit_week[week][1] += 1
                    dict_submit_week[week][2] += numseq
                    dict_submit_month[month][1] += 1
                    dict_submit_month[month][2] += numseq
                    dict_submit_year[year][1] += 1
                    dict_submit_year[year][2] += numseq
                    if method_submission == "web":
                        dict_submit_day[day][3] += 1
                        dict_submit_day[day][4] += numseq
                        dict_submit_week[week][3] += 1
                        dict_submit_week[week][4] += numseq
                        dict_submit_month[month][3] += 1
                        dict_submit_month[month][4] += numseq
                        dict_submit_year[year][3] += 1
                        dict_submit_year[year][4] += numseq
                    if method_submission == "wsdl":
                        dict_submit_day[day][5] += 1
                        dict_submit_day[day][6] += numseq
                        dict_submit_week[week][5] += 1
                        dict_submit_week[week][6] += numseq
                        dict_submit_month[month][5] += 1
                        dict_submit_month[month][6] += numseq
                        dict_submit_year[year][5] += 1
                        dict_submit_year[year][6] += numseq
# }}}
                # Write to SQL{{{
                row = {}
                row['jobid'] = strs[1]
                row['jobname'] = strs[5]
                row['ip'] = strs[2]
                row['method_submission'] = method_submission
                row['numseq'] = numseq
                row['submit_date'] = submit_date_str
                row['email'] = strs[6]
                myfunc.WriteSQLiteAllSubmitted(cur_s, tablename=sql_tablename,
                                               data=[row])
# }}}
            lines = hdl.readlines()
        hdl.close()

    con_s.commit()
    con_s.close()

    li_submit_day = []
    li_submit_week = []
    li_submit_month = []
    li_submit_year = []
    li_submit_day_web = []
    li_submit_week_web = []
    li_submit_month_web = []
    li_submit_year_web = []
    li_submit_day_wsdl = []
    li_submit_week_wsdl = []
    li_submit_month_wsdl = []
    li_submit_year_wsdl = []
    dict_list = [dict_submit_day, dict_submit_week, dict_submit_month,
                 dict_submit_year]
    li_list = [li_submit_day, li_submit_week, li_submit_month, li_submit_year,
               li_submit_day_web, li_submit_week_web, li_submit_month_web,
               li_submit_year_web, li_submit_day_wsdl, li_submit_week_wsdl,
               li_submit_month_wsdl, li_submit_year_wsdl]

    for i in range(len(dict_list)):
        dt = dict_list[i]
        sortedlist = sorted(list(dt.items()), key=lambda x: x[0])
        for j in range(3):
            li = li_list[j*4+i]
            k1 = j*2 + 1
            k2 = j*2 + 2
            for kk in range(len(sortedlist)):
                items = sortedlist[kk]
                if items[1][k1] > 0 or items[1][k2] > 0:
                    li.append([items[1][0], items[1][k1], items[1][k2]])

    outfile_submit_day = f"{path_stat}/submit_day.stat.txt"
    outfile_submit_week = f"{path_stat}/submit_week.stat.txt"
    outfile_submit_month = f"{path_stat}/submit_month.stat.txt"
    outfile_submit_year = f"{path_stat}/submit_year.stat.txt"
    outfile_submit_day_web = f"{path_stat}/submit_day_web.stat.txt"
    outfile_submit_week_web = f"{path_stat}/submit_week_web.stat.txt"
    outfile_submit_month_web = f"{path_stat}/submit_month_web.stat.txt"
    outfile_submit_year_web = f"{path_stat}/submit_year_web.stat.txt"
    outfile_submit_day_wsdl = f"{path_stat}/submit_day_wsdl.stat.txt"
    outfile_submit_week_wsdl = f"{path_stat}/submit_week_wsdl.stat.txt"
    outfile_submit_month_wsdl = f"{path_stat}/submit_month_wsdl.stat.txt"
    outfile_submit_year_wsdl = f"{path_stat}/submit_year_wsdl.stat.txt"
    flist = [outfile_submit_day, outfile_submit_week, outfile_submit_month,
             outfile_submit_year,
             outfile_submit_day_web, outfile_submit_week_web,
             outfile_submit_month_web, outfile_submit_year_web,
             outfile_submit_day_wsdl, outfile_submit_week_wsdl,
             outfile_submit_month_wsdl, outfile_submit_year_wsdl]
    for i in range(len(flist)):
        outfile = flist[i]
        li = li_list[i]
        try:
            fpout = open(outfile, "w")
            fpout.write("%s\t%s\t%s\n" % ('Date', 'numjob', 'numseq'))
            for j in range(len(li)):     # name    njob   nseq
                fpout.write("%s\t%d\t%d\n" % (li[j][0], li[j][1], li[j][2]))
            fpout.close()
        except IOError:
            pass
        # plotting
        if os.path.exists(outfile) and li:  # have at least one record
            # if os.path.basename(outfile).find('day') == -1:
            # extends date time series for missing dates
            freq = dataprocess.date_range_frequency(os.path.basename(outfile))
            try:
                dataprocess.extend_data(outfile,
                                        value_columns=['numjob', 'numseq'],
                                        freq=freq, outfile=outfile)
            except Exception as e:
                webcom.loginfo(f"Failed to extend data for {outfile} with errmsg: {e}",
                               errfile)
                pass
            cmd = [f"{binpath_plot}/plot_numsubmit.sh", outfile]
            webcom.RunCmd(cmd, logfile, errfile)

    # output waittime vs numseq_of_job
    # output finishtime vs numseq_of_job
    outfile_waittime_nseq = f"{path_stat}/waittime_nseq.stat.txt"
    outfile_waittime_nseq_web = f"{path_stat}/waittime_nseq_web.stat.txt"
    outfile_waittime_nseq_wsdl = f"{path_stat}/waittime_nseq_wsdl.stat.txt"
    outfile_finishtime_nseq = f"{path_stat}/finishtime_nseq.stat.txt"
    outfile_finishtime_nseq_web = f"{path_stat}/finishtime_nseq_web.stat.txt"
    outfile_finishtime_nseq_wsdl = f"{path_stat}/finishtime_nseq_wsdl.stat.txt"

    outfile_avg_waittime_nseq = f"{path_stat}/avg_waittime_nseq.stat.txt"
    outfile_avg_waittime_nseq_web = f"{path_stat}/avg_waittime_nseq_web.stat.txt"
    outfile_avg_waittime_nseq_wsdl = f"{path_stat}/avg_waittime_nseq_wsdl.stat.txt"
    outfile_avg_finishtime_nseq = f"{path_stat}/avg_finishtime_nseq.stat.txt"
    outfile_avg_finishtime_nseq_web = f"{path_stat}/avg_finishtime_nseq_web.stat.txt"
    outfile_avg_finishtime_nseq_wsdl = f"{path_stat}/avg_finishtime_nseq_wsdl.stat.txt"

    outfile_median_waittime_nseq = f"{path_stat}/median_waittime_nseq.stat.txt"
    outfile_median_waittime_nseq_web = f"{path_stat}/median_waittime_nseq_web.stat.txt"
    outfile_median_waittime_nseq_wsdl = f"{path_stat}/median_waittime_nseq_wsdl.stat.txt"
    outfile_median_finishtime_nseq = f"{path_stat}/median_finishtime_nseq.stat.txt"
    outfile_median_finishtime_nseq_web = f"{path_stat}/median_finishtime_nseq_web.stat.txt"
    outfile_median_finishtime_nseq_wsdl = f"{path_stat}/median_finishtime_nseq_wsdl.stat.txt"

    flist1 = [outfile_waittime_nseq, outfile_waittime_nseq_web,
              outfile_waittime_nseq_wsdl, outfile_finishtime_nseq,
              outfile_finishtime_nseq_web, outfile_finishtime_nseq_wsdl]

    flist2 = [outfile_avg_waittime_nseq, outfile_avg_waittime_nseq_web,
              outfile_avg_waittime_nseq_wsdl, outfile_avg_finishtime_nseq,
              outfile_avg_finishtime_nseq_web, outfile_avg_finishtime_nseq_wsdl]

    flist3 = [outfile_median_waittime_nseq,
              outfile_median_waittime_nseq_web,
              outfile_median_waittime_nseq_wsdl,
              outfile_median_finishtime_nseq,
              outfile_median_finishtime_nseq_web,
              outfile_median_finishtime_nseq_wsdl]

    dict_list = [waittime_numseq_dict, waittime_numseq_dict_web,
                 waittime_numseq_dict_wsdl, finishtime_numseq_dict,
                 finishtime_numseq_dict_web, finishtime_numseq_dict_wsdl]

    for i in range(len(flist1)):
        dt = dict_list[i]
        outfile1 = flist1[i]
        outfile2 = flist2[i]
        outfile3 = flist3[i]
        sortedlist = sorted(list(dt.items()), key=lambda x: x[0])
        try:
            fpout = open(outfile1, "w")
            fpout.write("%s\t%s\n" % ('numseq', 'time'))
            for j in range(len(sortedlist)):
                nseq = sortedlist[j][0]
                li_time = sortedlist[j][1]
                for k in range(len(li_time)):
                    fpout.write("%d\t%f\n" % (nseq, li_time[k]))
            fpout.close()
        except IOError:
            pass
        try:
            fpout = open(outfile2, "w")
            fpout.write("%s\t%s\n" % ('numseq', 'time'))
            for j in range(len(sortedlist)):
                nseq = sortedlist[j][0]
                li_time = sortedlist[j][1]
                avg_time = myfunc.FloatDivision(sum(li_time), len(li_time))
                fpout.write("%d\t%f\n" % (nseq, avg_time))
            fpout.close()
        except IOError:
            pass
        try:
            fpout = open(outfile3, "w")
            fpout.write("%s\t%s\n" % ('numseq', 'time'))
            for j in range(len(sortedlist)):
                nseq = sortedlist[j][0]
                li_time = sortedlist[j][1]
                median_time = numpy.median(li_time)
                fpout.write("%d\t%f\n" % (nseq, median_time))
            fpout.close()
        except IOError:
            pass

    # plotting
    flist = flist1
    for i in range(len(flist)):
        outfile = flist[i]
        if os.path.exists(outfile):
            cmd = [f"{binpath_plot}/plot_nseq_waitfinishtime.sh", outfile]
            webcom.RunCmd(cmd, logfile, errfile)
    flist = flist2 + flist3
    for i in range(len(flist)):
        outfile = flist[i]
        if os.path.exists(outfile):
            cmd = [f"{binpath_plot}/plot_avg_waitfinishtime.sh", outfile]
            webcom.RunCmd(cmd, logfile, errfile)
# }}}


def run_statistics_topcons2(webserver_root, logfile, errfile):  # {{{
    """Server usage analysis specifically for topcons2"""
    path_log = os.path.join(webserver_root, 'proj', 'pred', 'static', 'log')
    path_stat = os.path.join(path_log, 'stat')
    binpath_plot = os.path.join(webserver_root, "env", "bin")
    runtimelogfile = f"{path_log}/jobruntime.log"

    webcom.loginfo("Run usage statistics for TOPCONS2...\n", logfile)
    # get longest predicted seq
    # get query with most TM helics
    # get query takes the longest time
    extreme_runtimelogfile = f"{path_log}/stat/extreme_jobruntime.log"

    longestlength = -1
    mostTM = -1
    longestruntime = -1.0
    line_mostTM = ""
    line_longestruntime = ""

    # 3. get running time vs sequence length
    cntseq = 0
    cnt_hasSP = 0
    outfile_runtime = f"{path_stat}/length_runtime.stat.txt"
    outfile_runtime_pfam = f"{path_stat}/length_runtime.pfam.stat.txt"
    outfile_runtime_cdd = f"{path_stat}/length_runtime.cdd.stat.txt"
    outfile_runtime_uniref = f"{path_stat}/length_runtime.uniref.stat.txt"
    outfile_runtime_avg = f"{path_stat}/length_runtime.stat.avg.txt"
    outfile_runtime_pfam_avg = f"{path_stat}/length_runtime.pfam.stat.avg.txt"
    outfile_runtime_cdd_avg = f"{path_stat}/length_runtime.cdd.stat.avg.txt"
    outfile_runtime_uniref_avg = f"{path_stat}/length_runtime.uniref.stat.avg.txt"
    li_length_runtime = []
    li_length_runtime_pfam = []
    li_length_runtime_cdd = []
    li_length_runtime_uniref = []
    dict_length_runtime = {}
    dict_length_runtime_pfam = {}
    dict_length_runtime_cdd = {}
    dict_length_runtime_uniref = {}
    li_length_runtime_avg = []
    li_length_runtime_pfam_avg = []
    li_length_runtime_cdd_avg = []
    li_length_runtime_uniref_avg = []
    hdl = myfunc.ReadLineByBlock(runtimelogfile)
    if not hdl.failure:
        lines = hdl.readlines()
        while lines is not None:
            for line in lines:
                strs = line.split("\t")
                if len(strs) < 8:
                    continue
                # jobid = strs[0]
                # seqidx = strs[1]
                runtime = -1.0
                try:
                    runtime = float(strs[3])
                except (IndexError, ValueError):
                    pass
                mtd_profile = strs[4]
                lengthseq = -1
                try:
                    lengthseq = int(strs[5])
                except (IndexError, ValueError):
                    pass

                numTM = -1
                try:
                    numTM = int(strs[6])
                except (IndexError, ValueError):
                    pass
                isHasSP = strs[7]

                cntseq += 1
                if isHasSP == "True":
                    cnt_hasSP += 1

                if runtime > longestruntime:
                    line_longestruntime = line
                    longestruntime = runtime
                if lengthseq > longestlength:
                    line_longestseq = line
                    longestlength = lengthseq
                if numTM > mostTM:
                    mostTM = numTM
                    line_mostTM = line

                if lengthseq != -1:
                    li_length_runtime.append([lengthseq, runtime])
                    if lengthseq not in dict_length_runtime:
                        dict_length_runtime[lengthseq] = []
                    dict_length_runtime[lengthseq].append(runtime)
                    if mtd_profile == "pfam":
                        li_length_runtime_pfam.append([lengthseq, runtime])
                        if lengthseq not in dict_length_runtime_pfam:
                            dict_length_runtime_pfam[lengthseq] = []
                        dict_length_runtime_pfam[lengthseq].append(runtime)
                    elif mtd_profile == "cdd":
                        li_length_runtime_cdd.append([lengthseq, runtime])
                        if lengthseq not in dict_length_runtime_cdd:
                            dict_length_runtime_cdd[lengthseq] = []
                        dict_length_runtime_cdd[lengthseq].append(runtime)
                    elif mtd_profile == "uniref":
                        li_length_runtime_uniref.append([lengthseq, runtime])
                        if lengthseq not in dict_length_runtime_uniref:
                            dict_length_runtime_uniref[lengthseq] = []
                        dict_length_runtime_uniref[lengthseq].append(runtime)
            lines = hdl.readlines()
        hdl.close()

    li_content = []
    for line in [line_mostTM, line_longestseq, line_longestruntime]:
        li_content.append(line)
    myfunc.WriteFile("\n".join(li_content)+"\n", extreme_runtimelogfile,
                     "w", True)

    # get lengthseq -vs- average_runtime
    dict_list = [dict_length_runtime, dict_length_runtime_pfam,
                 dict_length_runtime_cdd, dict_length_runtime_uniref]
    li_list = [li_length_runtime_avg, li_length_runtime_pfam_avg,
               li_length_runtime_cdd_avg, li_length_runtime_uniref_avg]
    li_sum_runtime = [0.0]*len(dict_list)
    for i in range(len(dict_list)):
        dt = dict_list[i]
        li = li_list[i]
        for lengthseq in dt:
            avg_runtime = sum(dt[lengthseq])/float(len(dt[lengthseq]))
            li.append([lengthseq, avg_runtime])
            li_sum_runtime[i] += sum(dt[lengthseq])

    avg_runtime = myfunc.FloatDivision(li_sum_runtime[0],
                                       len(li_length_runtime))
    avg_runtime_pfam = myfunc.FloatDivision(li_sum_runtime[1],
                                            len(li_length_runtime_pfam))
    avg_runtime_cdd = myfunc.FloatDivision(li_sum_runtime[2],
                                           len(li_length_runtime_cdd))
    avg_runtime_uniref = myfunc.FloatDivision(li_sum_runtime[3],
                                              len(li_length_runtime_uniref))

    li_list = [li_length_runtime, li_length_runtime_pfam,
               li_length_runtime_cdd, li_length_runtime_uniref,
               li_length_runtime_avg, li_length_runtime_pfam_avg,
               li_length_runtime_cdd_avg, li_length_runtime_uniref_avg]
    flist = [outfile_runtime, outfile_runtime_pfam, outfile_runtime_cdd,
             outfile_runtime_uniref, outfile_runtime_avg,
             outfile_runtime_pfam_avg, outfile_runtime_cdd_avg,
             outfile_runtime_uniref_avg]
    for i in range(len(flist)):
        outfile = flist[i]
        li = li_list[i]
        sortedlist = sorted(li, key=lambda x: x[0])
        try:
            fpout = open(outfile, "w")
            fpout.write("%s\t%s\n" % ('lengthseq', 'runtime'))
            for j in range(len(sortedlist)):
                lengthseq = sortedlist[j][0]
                runtime = sortedlist[j][1]
                fpout.write("%d\t%f\n" % (lengthseq, runtime))
            fpout.close()
        except IOError:
            continue

    outfile_avg_runtime = f"{path_stat}/avg_runtime.stat.txt"
    try:
        fpout = open(outfile_avg_runtime, "w")
        fpout.write("%s\t%f\n" % ("All", avg_runtime))
        fpout.write("%s\t%f\n" % ("Pfam", avg_runtime_pfam))
        fpout.write("%s\t%f\n" % ("CDD", avg_runtime_cdd))
        fpout.write("%s\t%f\n" % ("Uniref", avg_runtime_uniref))
        fpout.close()
    except IOError:
        pass
    if os.path.exists(outfile_avg_runtime):
        cmd = [f"{binpath_plot}/plot_avg_runtime.sh", outfile_avg_runtime]
        webcom.RunCmd(cmd, logfile, errfile)

    flist = [outfile_runtime, outfile_runtime_pfam,
             outfile_runtime_cdd, outfile_runtime_uniref]
    for outfile in flist:
        if os.path.exists(outfile):
            cmd = [f"{binpath_plot}/plot_length_runtime.sh", outfile]
            webcom.RunCmd(cmd, logfile, errfile)

    cmd = [f"{binpath_plot}/plot_length_runtime_mtp.sh", "-pfam",
           outfile_runtime_pfam, "-cdd", outfile_runtime_cdd, "-uniref",
           outfile_runtime_uniref, "-sep-avg"]
    webcom.RunCmd(cmd, logfile, errfile)

# 4. analysis for those predicted with signal peptide
    outfile_hasSP = f"{path_stat}/noSP_hasSP.stat.txt"
    content = "%s\t%d\t%f\n%s\t%d\t%f\n" % ("\"Without SP\"",
                                            cntseq-cnt_hasSP,
                                            myfunc.FloatDivision(cntseq-cnt_hasSP, cntseq),
                                            "\"With SP\"",
                                            cnt_hasSP,
                                            myfunc.FloatDivision(cnt_hasSP, cntseq))
    myfunc.WriteFile(content, outfile_hasSP, "w", True)
    cmd = [f"{binpath_plot}/plot_nosp_sp.sh", outfile_hasSP]
    webcom.RunCmd(cmd, logfile, errfile)

# }}}


def main():  # {{{
    """main procedure"""
    parser = argparse.ArgumentParser(description='Run server statistics',
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog='''\
Created 2022-03-05, updated 2022-03-05, Nanjiang Shu

Examples:
    %s -i run_server_statistics.json
''' % (sys.argv[0]))
    parser.add_argument('-i', metavar='JSONFILE', dest='jsonfile',
                        type=str, required=True,
                        help='Provide the Json file with all parameters')
    # parser.add_argument('-v', dest='verbose', nargs='?', type=int, default=0,
    #                    const=1,
    #                   help='show verbose information, (default: 0)')

    args = parser.parse_args()

    jsonfile = args.jsonfile

    if not os.path.exists(jsonfile):
        print(f"Jsonfile {jsonfile} does not exist. Exit {progname}!",
              file=sys.stderr)
        return 1

    g_params = {}
    g_params.update(webcom.LoadJsonFromFile(jsonfile))

    lockname = f"{rootname_progname}.lock"
    lock_file = os.path.join(g_params['path_log'], lockname)
    g_params['lockfile'] = lock_file
    fp = open(lock_file, 'w')
    try:
        fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        webcom.loginfo(f"Another instance of {progname} is running",
                       g_params['logfile'])
        return 1

    if 'DEBUG_LOCK_FILE' in g_params and g_params['DEBUG_LOCK_FILE']:
        time.sleep(g_params['SLEEP_INTERVAL']*6)
    status = run_statistics(g_params)
    if os.path.exists(lock_file):
        try:
            os.remove(lock_file)
        except OSError:
            webcom.loginfo(f"Failed to delete lock_file {lock_file}",
                           g_params['logfile'])
    return status
# }}}


if __name__ == '__main__':
    sys.exit(main())
