#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Description:
A collection of classes and functions for the qd_fe.py

Author: Nanjiang Shu (nanjiang.shu@scilifelab.se)

Address: Science for Life Laboratory Stockholm, Box 1031, 17121 Solna, Sweden
"""

import os
import sys
from . import myfunc
from . import webserver_common as webcom
from . import dataprocess
import math
import numpy
import random
import time
from datetime import datetime
# from pytz import timezone
from geoip import geolite2
import pycountry
import shutil
from suds.client import Client
import json
import hashlib
from .timeit import timeit


@timeit
def RunStatistics(g_params):  # {{{
    """Server usage analysis"""
    bsname = "run_server_statistics"
    lockname = f"{bsname}.lock"
    lock_file = os.path.join(g_params['path_log'], lockname)
    path_tmp = os.path.join(g_params['path_static'], "tmp")
    gen_logfile = g_params['gen_logfile']
    gen_errfile = g_params['gen_errfile']
    name_server = g_params['name_server']
    if not os.path.exists(lock_file):
        jsonfile = os.path.join(path_tmp, f"{bsname}.json")
        myfunc.WriteFile(json.dumps(g_params, sort_keys=True), jsonfile, "w")
        binpath_script = os.path.join(g_params['webserver_root'], "env", "bin")
        py_scriptfile = os.path.join(binpath_script, f"{bsname}.py")
        bash_scriptfile = f"{path_tmp}/${bsname}-{name_server}.sh"
        code_str_list = []
        code_str_list.append("#!/bin/bash")
        cmdline = f"python {py_scriptfile} -i {jsonfile}"
        code_str_list.append(cmdline)
        code = "\n".join(code_str_list)
        myfunc.WriteFile(code, bash_scriptfile, mode="w", isFlush=True)
        os.chmod(bash_scriptfile, 0o755)
        os.chdir(path_tmp)
        cmd = ['sbatch', bash_scriptfile]
        cmdline = " ".join(cmd)
        verbose = False
        if 'DEBUG' in g_params and g_params['DEBUG']:
            verbose = True
            webcom.loginfo(f"Run cmdline: {cmdline}", gen_logfile)
        (isSubmitSuccess, t_runtime) = webcom.RunCmd(cmd,
                                                     gen_logfile,
                                                     gen_errfile,
                                                     verbose)
        if 'DEBUG' in g_params and g_params['DEBUG']:
            webcom.loginfo("isSubmitSuccess: {isSubmitSuccess}", gen_logfile)
# }}}


@timeit
def RunStatistics_obselete(g_params):  # {{{
    """Server usage analysis"""
    name_server = g_params['name_server']
    gen_logfile = g_params['gen_logfile']
    gen_errfile = g_params['gen_errfile']
    webserver_root = g_params['webserver_root']
    RunStatistics_basic(webserver_root, gen_logfile, gen_errfile)
    if name_server.lower() == "topcons2":
        RunStatistics_topcons2(webserver_root, gen_logfile, gen_errfile)
# }}}


@timeit
def RunStatistics_basic(webserver_root, gen_logfile, gen_errfile):  # {{{
    """Function for qd_fe to run usage statistics for the web-server usage
    """
    path_log = os.path.join(webserver_root, 'proj', 'pred', 'static', 'log')
    path_result = os.path.join(
            webserver_root, 'proj', 'pred', 'static', 'result')
    path_stat = os.path.join(path_log, 'stat')
    binpath_plot = os.path.join(webserver_root, "env", "bin")

    # 1. calculate average running time, only for those sequences with time.txt
    # show also runtime of type and runtime -vs- seqlength
    webcom.loginfo("Run basic usage statistics...\n", gen_logfile)
    allfinishedjoblogfile = f"{path_log}/all_finished_job.log"
    runtimelogfile = f"{path_log}/jobruntime.log"
    runtimelogfile_finishedjobid = f"{path_log}/jobruntime_finishedjobid.log"
    allsubmitjoblogfile = f"{path_log}/all_submitted_seq.log"
    if not os.path.exists(path_stat):
        os.mkdir(path_stat)

    allfinishedjobidlist = myfunc.ReadIDList2(
            allfinishedjoblogfile, col=0, delim="\t")
    runtime_finishedjobidlist = myfunc.ReadIDList(runtimelogfile_finishedjobid)
    toana_jobidlist = list(
            set(allfinishedjobidlist) - set(runtime_finishedjobidlist))

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

        if len(runtimeloginfolist) > 0:
            # items for the elelment of the list
            # jobid, seq_no, newrun_or_cached, runtime,
            # mtd_profile, seqlen, numTM, iShasSP
            myfunc.WriteFile(
                    "\n".join(runtimeloginfolist)+"\n",
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
        except Exception:
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
            isValidSubmitDate = True
            isValidStartDate = True
            isValidFinishDate = True
            try:
                submit_date = webcom.datetime_str_to_time(submit_date_str)
            except ValueError:
                isValidSubmitDate = False
            try:
                start_date = webcom.datetime_str_to_time(start_date_str)
            except ValueError:
                isValidStartDate = False
            try:
                finish_date = webcom.datetime_str_to_time(finish_date_str)
            except ValueError:
                isValidFinishDate = False

            if isValidSubmitDate and isValidStartDate:
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
            if isValidSubmitDate and isValidFinishDate:
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

    # output countjob by country
    outfile_countjob_by_country = f"{path_stat}/countjob_by_country.txt"
    # sort by numseq in descending order
    li_countjob = sorted(
            list(countjob_country.items()),
            key=lambda x: x[1][0], reverse=True)
    li_str = []
    li_str.append("#Country\tNumSeq\tNumJob\tNumIP")
    for li in li_countjob:
        li_str.append(
                "%s\t%d\t%d\t%d" % (li[0], li[1][0], li[1][1], len(li[1][2])))
    myfunc.WriteFile(
            ("\n".join(li_str)+"\n").encode('utf-8'),
            outfile_countjob_by_country, "wb", True)

    flist = [
            outfile_numseqjob, outfile_numseqjob_web, outfile_numseqjob_wsdl
            ]
    dictlist = [
            countjob_numseq_dict, countjob_numseq_dict_web,
            countjob_numseq_dict_wsdl
            ]
    for i in range(len(flist)):
        dt = dictlist[i]
        outfile = flist[i]
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
            if os.path.exists(outfile) and len(sortedlist) > 0:
                cmd = [f"{binpath_plot}/plot_numseq_of_job.sh", outfile]
                webcom.RunCmd(cmd, gen_logfile, gen_errfile)
        except IOError:
            continue
    cmd = [
            f"{binpath_plot}/plot_numseq_of_job_mtp.sh",
            "-web", outfile_numseqjob_web,
            "-wsdl", outfile_numseqjob_wsdl
            ]
    webcom.RunCmd(cmd, gen_logfile, gen_errfile)

# 5. output num-submission time series with different bins
# (day, week, month, year)
    hdl = myfunc.ReadLineByBlock(allsubmitjoblogfile)
    dict_submit_day = {}   # ["name" numjob, numseq, numjob_web, numseq_web,numjob_wsdl, numseq_wsdl]
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
                isValidSubmitDate = True
                try:
                    submit_date = webcom.datetime_str_to_time(submit_date_str)
                except ValueError:
                    isValidSubmitDate = False
                if isValidSubmitDate:#{{{
                    day_str = submit_date_str.split()[0]
                    (beginning_of_week, end_of_week) = myfunc.week_beg_end(submit_date)
                    week_str = beginning_of_week.strftime("%Y-%m-%d")
                    month_str = submit_date.replace(day=1).strftime("%Y-%m-%d")
                    year_str = submit_date.replace(month=1, day=1).strftime("%Y-%m-%d")
                    day = int(day_str.replace("-", ""))
                    week = int(submit_date.strftime("%Y%V"))
                    month = int(submit_date.strftime("%Y%m"))
                    year = int(submit_date.year)
                    if not day in dict_submit_day:
                                                #all   web  wsdl
                        dict_submit_day[day] = [day_str, 0,0,0,0,0,0]
                    if not week in dict_submit_week:
                        dict_submit_week[week] = [week_str, 0,0,0,0,0,0]
                    if not month in dict_submit_month:
                        dict_submit_month[month] = [month_str, 0,0,0,0,0,0]
                    if not year in dict_submit_year:
                        dict_submit_year[year] = [year_str, 0,0,0,0,0,0]
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
#}}}
            lines = hdl.readlines()
        hdl.close()

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
    dict_list = [
            dict_submit_day, dict_submit_week, dict_submit_month,
            dict_submit_year]
    li_list = [
            li_submit_day, li_submit_week, li_submit_month, li_submit_year,
            li_submit_day_web, li_submit_week_web, li_submit_month_web,
            li_submit_year_web, li_submit_day_wsdl, li_submit_week_wsdl,
            li_submit_month_wsdl, li_submit_year_wsdl
            ]

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
    flist = [
            outfile_submit_day, outfile_submit_week, outfile_submit_month,
            outfile_submit_year,
            outfile_submit_day_web, outfile_submit_week_web,
            outfile_submit_month_web, outfile_submit_year_web,
            outfile_submit_day_wsdl, outfile_submit_week_wsdl,
            outfile_submit_month_wsdl, outfile_submit_year_wsdl
            ]
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
        if os.path.exists(outfile) and len(li) > 0:  # have at least one record
            # if os.path.basename(outfile).find('day') == -1:
            # extends date time series for missing dates
            freq = dataprocess.date_range_frequency(os.path.basename(outfile))
            try:
                dataprocess.extend_data(
                        outfile, value_columns=['numjob', 'numseq'],
                        freq=freq, outfile=outfile)
            except Exception as e:
                webcom.loginfo(
                        "Failed to extend data for file %s with errmsg: %s" % (
                            outfile, str(e)), gen_errfile)
                pass
            cmd = [f"{binpath_plot}/plot_numsubmit.sh", outfile]
            webcom.RunCmd(cmd, gen_logfile, gen_errfile)

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

    flist1 = [
            outfile_waittime_nseq, outfile_waittime_nseq_web,
            outfile_waittime_nseq_wsdl, outfile_finishtime_nseq,
            outfile_finishtime_nseq_web, outfile_finishtime_nseq_wsdl
            ]

    flist2 = [
            outfile_avg_waittime_nseq, outfile_avg_waittime_nseq_web,
            outfile_avg_waittime_nseq_wsdl, outfile_avg_finishtime_nseq,
            outfile_avg_finishtime_nseq_web, outfile_avg_finishtime_nseq_wsdl
            ]

    flist3 = [
            outfile_median_waittime_nseq, outfile_median_waittime_nseq_web,
            outfile_median_waittime_nseq_wsdl, outfile_median_finishtime_nseq,
            outfile_median_finishtime_nseq_web, outfile_median_finishtime_nseq_wsdl
            ]

    dict_list = [
            waittime_numseq_dict, waittime_numseq_dict_web,
            waittime_numseq_dict_wsdl, finishtime_numseq_dict,
            finishtime_numseq_dict_web, finishtime_numseq_dict_wsdl
            ]

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
            webcom.RunCmd(cmd, gen_logfile, gen_errfile)
    flist = flist2 + flist3
    for i in range(len(flist)):
        outfile = flist[i]
        if os.path.exists(outfile):
            cmd = [f"{binpath_plot}/plot_avg_waitfinishtime.sh", outfile]
            webcom.RunCmd(cmd, gen_logfile, gen_errfile)
# }}}


@timeit
def RunStatistics_topcons2(webserver_root, gen_logfile, gen_errfile):  # {{{
    """Server usage analysis specifically for topcons2"""
    path_log = os.path.join(webserver_root, 'proj', 'pred', 'static', 'log')
    path_stat = os.path.join(path_log, 'stat')
    binpath_plot = os.path.join(webserver_root, "env", "bin")
    runtimelogfile = f"{path_log}/jobruntime.log"

    webcom.loginfo("Run usage statistics for TOPCONS2...\n", gen_logfile)
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
    myfunc.WriteFile("\n".join(li_content)+"\n", extreme_runtimelogfile, "w", True)

    # get lengthseq -vs- average_runtime
    dict_list = [
            dict_length_runtime, dict_length_runtime_pfam,
            dict_length_runtime_cdd, dict_length_runtime_uniref
            ]
    li_list = [
            li_length_runtime_avg, li_length_runtime_pfam_avg,
            li_length_runtime_cdd_avg, li_length_runtime_uniref_avg
            ]
    li_sum_runtime = [0.0]*len(dict_list)
    for i in range(len(dict_list)):
        dt = dict_list[i]
        li = li_list[i]
        for lengthseq in dt:
            avg_runtime = sum(dt[lengthseq])/float(len(dt[lengthseq]))
            li.append([lengthseq, avg_runtime])
            li_sum_runtime[i] += sum(dt[lengthseq])

    avg_runtime = myfunc.FloatDivision(li_sum_runtime[0], len(li_length_runtime))
    avg_runtime_pfam = myfunc.FloatDivision(li_sum_runtime[1], len(li_length_runtime_pfam))
    avg_runtime_cdd = myfunc.FloatDivision(li_sum_runtime[2], len(li_length_runtime_cdd))
    avg_runtime_uniref = myfunc.FloatDivision(li_sum_runtime[3], len(li_length_runtime_uniref))

    li_list = [
            li_length_runtime, li_length_runtime_pfam,
            li_length_runtime_cdd, li_length_runtime_uniref,
            li_length_runtime_avg, li_length_runtime_pfam_avg,
            li_length_runtime_cdd_avg, li_length_runtime_uniref_avg]
    flist = [
            outfile_runtime, outfile_runtime_pfam, outfile_runtime_cdd,
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

    outfile_avg_runtime = "%s/avg_runtime.stat.txt" % (path_stat)
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
        webcom.RunCmd(cmd, gen_logfile, gen_errfile)

    flist = [
            outfile_runtime, outfile_runtime_pfam, outfile_runtime_cdd,
            outfile_runtime_uniref]
    for outfile in flist:
        if os.path.exists(outfile):
            cmd = [f"{binpath_plot}/plot_length_runtime.sh", outfile]
            webcom.RunCmd(cmd, gen_logfile, gen_errfile)

    cmd = [
            f"{binpath_plot}/plot_length_runtime_mtp.sh", "-pfam",
            outfile_runtime_pfam, "-cdd", outfile_runtime_cdd, "-uniref",
            outfile_runtime_uniref, "-sep-avg"]
    webcom.RunCmd(cmd, gen_logfile, gen_errfile)

# 4. analysis for those predicted with signal peptide
    outfile_hasSP = f"{path_stat}/noSP_hasSP.stat.txt"
    content = "%s\t%d\t%f\n%s\t%d\t%f\n" % (
            "\"Without SP\"", cntseq-cnt_hasSP,
            myfunc.FloatDivision(cntseq-cnt_hasSP, cntseq),
            "\"With SP\"", cnt_hasSP, myfunc.FloatDivision(cnt_hasSP, cntseq))
    myfunc.WriteFile(content, outfile_hasSP, "w", True)
    cmd = [
            f"{binpath_plot}/plot_nosp_sp.sh", outfile_hasSP]
    webcom.RunCmd(cmd, gen_logfile, gen_errfile)

# }}}


@timeit
def CreateRunJoblog(loop, isOldRstdirDeleted, g_params):#{{{
    """Create the index file for the jobs to be run
    """
    gen_logfile = g_params['gen_logfile']
    # gen_errfile = g_params['gen_errfile']
    name_server = g_params['name_server']

    webcom.loginfo("CreateRunJoblog for server %s..."%(name_server), gen_logfile)

    path_static = g_params['path_static']
    # path_cache = g_params['path_cache']

    path_result = os.path.join(path_static, 'result')
    path_log = os.path.join(path_static, 'log')

    submitjoblogfile = f"{path_log}/submitted_seq.log"
    runjoblogfile = f"{path_log}/runjob_log.log"
    finishedjoblogfile = f"{path_log}/finished_job.log"

    # Read entries from submitjoblogfile, checking in the result folder and
    # generate two logfiles:
    #   1. runjoblogfile
    #   2. finishedjoblogfile
    # when loop == 0, for unfinished jobs, regenerate finished_seqs.txt
    hdl = myfunc.ReadLineByBlock(submitjoblogfile)
    if hdl.failure:
        return 1

    finished_job_dict = {}
    if os.path.exists(finishedjoblogfile):
        finished_job_dict = myfunc.ReadFinishedJobLog(finishedjoblogfile)

    # these two list try to update the finished list and submitted list so that
    # deleted jobs will not be included, there is a separate list started with
    # all_xxx which keeps also the historical jobs
    new_finished_list = []  # Finished or Failed
    new_submitted_list = []

    new_runjob_list = []    # Running
    new_waitjob_list = []    # Queued
    lines = hdl.readlines()
    while lines is not None:
        for line in lines:
            strs = line.split("\t")
            if len(strs) < 8:
                continue
            submit_date_str = strs[0]
            jobid = strs[1]
            ip = strs[2]
            numseq_str = strs[3]
            jobname = strs[5]
            email = strs[6].strip()
            method_submission = strs[7]
            start_date_str = ""
            finish_date_str = ""
            rstdir = os.path.join(path_result, jobid)

            numseq = 1
            try:
                numseq = int(numseq_str)
            except ValueError:
                pass

            isRstFolderExist = False
            if not isOldRstdirDeleted or os.path.exists(rstdir):
                isRstFolderExist = True

            if isRstFolderExist:
                new_submitted_list.append([jobid, line])

            if jobid in finished_job_dict:
                if isRstFolderExist:
                    li = [jobid] + finished_job_dict[jobid]
                    new_finished_list.append(li)
                continue

            status = webcom.get_job_status(jobid, numseq, path_result)
            if 'DEBUG_JOB_STATUS' in g_params and g_params['DEBUG_JOB_STATUS']:
                webcom.loginfo("status(%s): %s"%(jobid, status), gen_logfile)

            starttagfile = "%s/%s"%(rstdir, "runjob.start")
            finishtagfile = "%s/%s"%(rstdir, "runjob.finish")
            if os.path.exists(starttagfile):
                start_date_str = myfunc.ReadFile(starttagfile).strip()
            if os.path.exists(finishtagfile):
                finish_date_str = myfunc.ReadFile(finishtagfile).strip()

            li = [jobid, status, jobname, ip, email, numseq_str,
                    method_submission, submit_date_str, start_date_str,
                    finish_date_str]
            if status in ["Finished", "Failed"]:
                new_finished_list.append(li)

            isValidSubmitDate = True
            try:
                submit_date = webcom.datetime_str_to_time(submit_date_str)
            except ValueError:
                isValidSubmitDate = False

            if isValidSubmitDate:
                current_time = datetime.now(submit_date.tzinfo)
                timeDiff = current_time - submit_date
                queuetime_in_sec = timeDiff.seconds
            else:
                queuetime_in_sec = g_params['UPPER_WAIT_TIME_IN_SEC'] + 1

            # for servers not in the list ["topcons2"] all jobs are handled by the qd_fe
            if (name_server.lower() not in ["topcons2"]
                or (numseq > 1
                    or method_submission == "wsdl" 
                    or queuetime_in_sec > g_params['UPPER_WAIT_TIME_IN_SEC'])):
                if status == "Running":
                    new_runjob_list.append(li)
                elif status == "Wait":
                    new_waitjob_list.append(li)
        lines = hdl.readlines()
    hdl.close()

# rewrite logs of submitted jobs
    li_str = []
    for li in new_submitted_list:
        li_str.append(li[1])
    if len(li_str)>0:
        myfunc.WriteFile("\n".join(li_str)+"\n", submitjoblogfile, "w", True)
    else:
        myfunc.WriteFile("", submitjoblogfile, "w", True)

# rewrite logs of finished jobs
    li_str = []
    for li in new_finished_list:
        li = [str(x) for x in li]
        li_str.append("\t".join(li))
    if len(li_str) > 0:
        myfunc.WriteFile("\n".join(li_str)+"\n", finishedjoblogfile, "w", True)
    else:
        myfunc.WriteFile("", finishedjoblogfile, "w", True)
# rewrite logs of finished jobs for each IP
    new_finished_dict = {}
    for li in new_finished_list:
        ip = li[3]
        if not ip in new_finished_dict:
            new_finished_dict[ip] = []
        new_finished_dict[ip].append(li)
    for ip in new_finished_dict:
        finished_list_for_this_ip = new_finished_dict[ip]
        divide_finishedjoblogfile = "%s/divided/%s_finished_job.log"%(path_log, ip)
        li_str = []
        for li in finished_list_for_this_ip:
            li = [str(x) for x in li]
            li_str.append("\t".join(li))
        if len(li_str)>0:
            myfunc.WriteFile("\n".join(li_str)+"\n", divide_finishedjoblogfile, "w", True)
        else:
            myfunc.WriteFile("", divide_finishedjoblogfile, "w", True)

# update allfinished jobs
    allfinishedjoblogfile = "%s/all_finished_job.log"%(path_log)
    allfinished_jobid_set = set(myfunc.ReadIDList2(allfinishedjoblogfile, col=0, delim="\t"))
    li_str = []
    for li in new_finished_list:
        li = [str(x) for x in li]
        jobid = li[0]
        if not jobid in allfinished_jobid_set:
            li_str.append("\t".join(li))
    if len(li_str)>0:
        myfunc.WriteFile("\n".join(li_str)+"\n", allfinishedjoblogfile, "a", True)

# update all_submitted jobs
    allsubmitjoblogfile = "%s/all_submitted_seq.log"%(path_log)
    allsubmitted_jobid_set = set(myfunc.ReadIDList2(allsubmitjoblogfile, col=1, delim="\t"))
    li_str = []
    for li in new_submitted_list:
        jobid = li[0]
        if not jobid in allsubmitted_jobid_set:
            li_str.append(li[1])
    if len(li_str)>0:
        myfunc.WriteFile("\n".join(li_str)+"\n", allsubmitjoblogfile, "a", True)

# write logs of running and queuing jobs
# the queuing jobs are sorted in descending order by the suq priority
# frist get numseq_this_user for each jobs
# format of numseq_this_user: {'jobid': numseq_this_user}
    numseq_user_dict = webcom.GetNumSeqSameUserDict(new_runjob_list + new_waitjob_list)

# now append numseq_this_user and priority score to new_waitjob_list and
# new_runjob_list

    for joblist in [new_waitjob_list, new_runjob_list]:
        for li in joblist:
            jobid = li[0]
            ip = li[3]
            email = li[4].strip()
            rstdir = "%s/%s"%(path_result, jobid)
            outpath_result = "%s/%s"%(rstdir, jobid)

            # if loop == 0 , for new_waitjob_list and new_runjob_list
            # regenerate finished_seqs.txt
            runjob_lockfile = "%s/%s.lock"%(rstdir, "runjob.lock")
            if 'DEBUG' in g_params and g_params['DEBUG'] and os.path.exists(runjob_lockfile):
                webcom.loginfo("runjob_lockfile %s exists. "%(runjob_lockfile), gen_logfile)
            if loop == 0 and os.path.exists(outpath_result) and not os.path.exists(runjob_lockfile):#{{{
                finished_seq_file = "%s/finished_seqs.txt"%(outpath_result)
                finished_idx_file = "%s/finished_seqindex.txt"%(rstdir)
                finished_idx_set = set([])

                finished_seqs_idlist = []
                if os.path.exists(finished_seq_file):
                    finished_seqs_idlist = myfunc.ReadIDList2(finished_seq_file, col=0, delim="\t")
                finished_seqs_idset = set(finished_seqs_idlist)
                finished_info_list = []
                queryfile = "%s/query.fa"%(rstdir)
                (seqIDList, seqAnnoList, seqList) = myfunc.ReadFasta(queryfile)
                try:
                    dirlist = os.listdir(outpath_result)
                except Exception as e:
                    webcom.loginfo("Failed to os.listdir(%s) with errmsg=%s"%(outpath_result, str(e)), gen_logfile)
                for dd in dirlist:
                    if dd.find("seq_") == 0:
                        origIndex_str = dd.split("_")[1]
                        finished_idx_set.add(origIndex_str)

                    if dd.find("seq_") == 0 and dd not in finished_seqs_idset:
                        origIndex = int(dd.split("_")[1])
                        outpath_this_seq = "%s/%s"%(outpath_result, dd)
                        timefile = "%s/time.txt"%(outpath_this_seq)
                        runtime = webcom.ReadRuntimeFromFile(timefile, default_runtime=0.0)
                        # get origIndex and then read description the description list
                        try:
                            description = seqAnnoList[origIndex].replace('\t', ' ')
                        except:
                            description = "seq_%d"%(origIndex)
                        try:
                            seq = seqList[origIndex]
                        except:
                            seq = ""
                        info_finish = webcom.GetInfoFinish(name_server, outpath_this_seq,
                                origIndex, len(seq), description,
                                source_result="newrun", runtime=runtime)
                        finished_info_list.append("\t".join(info_finish))
                if len(finished_info_list)>0:
                    myfunc.WriteFile("\n".join(finished_info_list)+"\n", finished_seq_file, "a", True)
                if len(finished_idx_set) > 0:
                    myfunc.WriteFile("\n".join(list(finished_idx_set))+"\n", finished_idx_file, "w", True)
                else:
                    myfunc.WriteFile("", finished_idx_file, "w", True)
            #}}}

            try:
                numseq = int(li[5])
            except (IndexError, ValueError):
                numseq = 1
                pass
            try:
                numseq_this_user = numseq_user_dict[jobid]
            except KeyError:
                numseq_this_user = numseq
                pass
            # note that the priority is deducted by numseq so that for jobs
            # from the same user, jobs with fewer sequences are placed with
            # higher priority
            priority = myfunc.FloatDivision( myfunc.GetSuqPriority(numseq_this_user) - numseq, math.sqrt(numseq))

            if ip in g_params['blackiplist']:
                priority = priority/1000.0

            if email in g_params['vip_user_list']:
                numseq_this_user = 1
                priority = 999999999.0
                webcom.loginfo("email %s in vip_user_list"%(email), gen_logfile)

            li.append(numseq_this_user)
            li.append(priority)

    # sort the new_waitjob_list in descending order by priority
    new_waitjob_list = sorted(new_waitjob_list, key=lambda x: x[11], reverse=True)
    new_runjob_list = sorted(new_runjob_list, key=lambda x: x[11], reverse=True)

    # write to runjoblogfile
    li_str = []
    for joblist in [new_waitjob_list, new_runjob_list]:
        for li in joblist:
            li2 = li[:10]+[str(li[10]), str(li[11])]
            li_str.append("\t".join(li2))
#     print "write to", runjoblogfile
#     print "\n".join(li_str)
    if len(li_str) > 0:
        myfunc.WriteFile("\n".join(li_str)+"\n", runjoblogfile, "w", True)
    else:
        myfunc.WriteFile("", runjoblogfile, "w", True)

# }}}


@timeit
def SubmitJob(jobid, cntSubmitJobDict, numseq_this_user, g_params):  # {{{
    """Submit a job to the remote computational node
    """
# for each job rstdir, keep three log files,
# 1.seqs finished, finished_seq log keeps all information, finished_index_log
#   can be very compact to speed up reading, e.g.
#   1-5 7-9 etc
# 2.seqs queued remotely , format:
#       index node remote_jobid
# 3. format of the torun_idx_file
#    origIndex
    gen_logfile = g_params['gen_logfile']
    # gen_errfile = g_params['gen_errfile']
    name_server = g_params['name_server']

    webcom.loginfo("SubmitJob for %s, numseq_this_user=%d"%(jobid, numseq_this_user), gen_logfile)

    path_static = g_params['path_static']
    path_cache = g_params['path_cache']

    path_result = os.path.join(path_static, 'result')
    path_log = os.path.join(path_static, 'log')

    rstdir = "%s/%s"%(path_result, jobid)
    outpath_result = "%s/%s"%(rstdir, jobid)
    if not os.path.exists(outpath_result):
        os.mkdir(outpath_result)

    finished_idx_file = "%s/finished_seqindex.txt"%(rstdir)
    failed_idx_file = "%s/failed_seqindex.txt"%(rstdir)
    remotequeue_idx_file = "%s/remotequeue_seqindex.txt"%(rstdir)
    torun_idx_file = "%s/torun_seqindex.txt"%(rstdir) # ordered seq index to run
    cnttry_idx_file = "%s/cntsubmittry_seqindex.txt"%(rstdir)#index file to keep log of tries

    runjob_errfile = "%s/%s"%(rstdir, "runjob.err")
    runjob_logfile = "%s/%s"%(rstdir, "runjob.log")
    finished_seq_file = "%s/finished_seqs.txt"%(outpath_result)
    query_parafile = "%s/query.para.txt"%(rstdir)
    query_para = webcom.LoadJsonFromFile(query_parafile)
    tmpdir = "%s/tmpdir"%(rstdir)
    qdinittagfile = "%s/runjob.qdinit"%(rstdir)
    failedtagfile = "%s/%s"%(rstdir, "runjob.failed")
    starttagfile = "%s/%s"%(rstdir, "runjob.start")
    cache_process_finish_tagfile = "%s/cache_processed.finish"%(rstdir)
    fafile = "%s/query.fa"%(rstdir)
    split_seq_dir = "%s/splitaa"%(tmpdir)
    forceruntagfile = "%s/forcerun"%(rstdir)
    lastprocessed_cache_idx_file = "%s/lastprocessed_cache_idx.txt"%(rstdir)
    variant_file = "%s/variants.fa"%(rstdir)

    if os.path.exists(forceruntagfile):
        isForceRun = True
    else:
        isForceRun = False

    finished_idx_list = []
    failed_idx_list = []    # [origIndex]
    if os.path.exists(finished_idx_file):
        finished_idx_list = list(set(myfunc.ReadIDList(finished_idx_file)))
    if os.path.exists(failed_idx_file):
        failed_idx_list = list(set(myfunc.ReadIDList(failed_idx_file)))

    processed_idx_set = set(finished_idx_list) | set(failed_idx_list)

    jobinfofile = "%s/jobinfo"%(rstdir)
    jobinfo = ""
    if os.path.exists(jobinfofile):
        jobinfo = myfunc.ReadFile(jobinfofile).strip()
    jobinfolist = jobinfo.split("\t")
    email = ""
    if len(jobinfolist) >= 8:
        email = jobinfolist[6]
        method_submission = jobinfolist[7]

    # the first time when the this jobid is processed, do the following
    # 1. generate a file with sorted seqindex
    # 2. generate splitted sequence files named by the original seqindex
    if not os.path.exists(qdinittagfile): #initialization#{{{
        if not os.path.exists(tmpdir):
            os.mkdir(tmpdir)
        if isForceRun or os.path.exists(cache_process_finish_tagfile):
            isCacheProcessingFinished = True
        else:
            isCacheProcessingFinished = False

        # ==== 1.dealing with cached results 
        (seqIDList, seqAnnoList, seqList) = myfunc.ReadFasta(fafile)
        if len(seqIDList) <= 0:
            webcom.WriteDateTimeTagFile(failedtagfile, runjob_logfile, runjob_errfile)
            webcom.loginfo("Read query seq file failed. Zero sequence read in", runjob_errfile)
            return 1

        if 'DEBUG' in g_params and g_params['DEBUG']:
            msg = "jobid = %s, isCacheProcessingFinished=%s, MAX_CACHE_PROCESS=%d"%(
                    jobid, str(isCacheProcessingFinished), g_params['MAX_CACHE_PROCESS'])
            webcom.loginfo(msg, gen_logfile)

        if not isCacheProcessingFinished:
            finished_idx_set = set(finished_idx_list)

            lastprocessed_idx = -1
            if os.path.exists(lastprocessed_cache_idx_file):
                try:
                    lastprocessed_idx = int(myfunc.ReadFile(lastprocessed_cache_idx_file))
                except:
                    lastprocessed_idx = -1

            cnt_processed_cache = 0
            for i in range(lastprocessed_idx+1, len(seqIDList)):
                if i in finished_idx_set:
                    continue
                outpath_this_seq = "%s/%s"%(outpath_result, "seq_%d"%i)
                subfoldername_this_seq = "seq_%d"%(i)
                md5_key = hashlib.md5(seqList[i].encode('utf-8')).hexdigest()
                subfoldername = md5_key[:2]
                cachedir = "%s/%s/%s"%(path_cache, subfoldername, md5_key)
                zipfile_cache = cachedir + ".zip"

                if os.path.exists(cachedir) or os.path.exists(zipfile_cache):
                    if os.path.exists(cachedir):
                        try:
                            shutil.copytree(cachedir, outpath_this_seq)
                        except Exception as e:
                            msg = "Failed to copytree  %s -> %s"%(cachedir, outpath_this_seq)
                            webcom.loginfo("%s with errmsg=%s"%(msg, str(e)), runjob_errfile)
                    elif os.path.exists(zipfile_cache):
                        cmd = ["unzip", zipfile_cache, "-d", outpath_result]
                        webcom.RunCmd(cmd, runjob_logfile, runjob_errfile)
                        if os.path.exists(outpath_this_seq):
                            shutil.rmtree(outpath_this_seq)
                        shutil.move("%s/%s"%(outpath_result, md5_key), outpath_this_seq)

                    fafile_this_seq =  '%s/seq.fa'%(outpath_this_seq)
                    if os.path.exists(outpath_this_seq) and webcom.IsCheckPredictionPassed(outpath_this_seq, name_server):
                        myfunc.WriteFile('>%s\n%s\n'%(seqAnnoList[i], seqList[i]), fafile_this_seq, 'w', True)
                        if not os.path.exists(starttagfile): #write start tagfile
                            webcom.WriteDateTimeTagFile(starttagfile, runjob_logfile, runjob_errfile)

                        info_finish = webcom.GetInfoFinish(name_server, outpath_this_seq,
                                i, len(seqList[i]), seqAnnoList[i], source_result="cached", runtime=0.0)
                        myfunc.WriteFile("\t".join(info_finish)+"\n",
                                finished_seq_file, "a", isFlush=True)
                        myfunc.WriteFile("%d\n"%(i), finished_idx_file, "a", True)

                    if 'DEBUG' in g_params and g_params['DEBUG']:
                        webcom.loginfo("Get result from cache for seq_%d"%(i), gen_logfile)
                    if cnt_processed_cache+1 >= g_params['MAX_CACHE_PROCESS']:
                        myfunc.WriteFile(str(i), lastprocessed_cache_idx_file, "w", True)
                        return 0
                    cnt_processed_cache += 1

            webcom.WriteDateTimeTagFile(cache_process_finish_tagfile, runjob_logfile, runjob_errfile)

        # Regenerate toRunDict
        toRunDict = {}
        for i in range(len(seqIDList)):
            if not i in processed_idx_set:
                toRunDict[i] = [seqList[i], 0, seqAnnoList[i].replace('\t', ' ')]

        if name_server == "topcons2":
            webcom.ResetToRunDictByScampiSingle(toRunDict, g_params['script_scampi'], tmpdir, runjob_logfile, runjob_errfile)
        sortedlist = sorted(list(toRunDict.items()), key=lambda x:x[1][1], reverse=True)

        # Write splitted fasta file and write a torunlist.txt
        if not os.path.exists(split_seq_dir):
            os.mkdir(split_seq_dir)

        torun_index_str_list = [str(x[0]) for x in sortedlist]
        if len(torun_index_str_list)>0:
            myfunc.WriteFile("\n".join(torun_index_str_list)+"\n", torun_idx_file, "w", True)
        else:
            myfunc.WriteFile("", torun_idx_file, "w", True)

        # write cnttry file for each jobs to run
        cntTryDict = {}
        for idx in torun_index_str_list:
            cntTryDict[int(idx)] = 0
        json.dump(cntTryDict, open(cnttry_idx_file, "w"))

        for item in sortedlist:
            origIndex = item[0]
            seq = item[1][0]
            description = item[1][2]
            seqfile_this_seq = "%s/%s"%(split_seq_dir, "query_%d.fa"%(origIndex))
            seqcontent = ">%s\n%s\n"%(description, seq)
            myfunc.WriteFile(seqcontent, seqfile_this_seq, "w", True)
        # qdinit file is written at the end of initialization, to make sure
        # that initialization is either not started or completed
        webcom.WriteDateTimeTagFile(qdinittagfile, runjob_logfile, runjob_errfile)
#}}}


    # 3. try to submit the job 
    toRunIndexList = [] # index in str
    processedIndexSet = set([]) #seq index set that are already processed
    submitted_loginfo_list = []
    if os.path.exists(torun_idx_file):
        toRunIndexList = myfunc.ReadIDList(torun_idx_file)
        # unique the list but keep the order
        toRunIndexList = myfunc.uniquelist(toRunIndexList)
    if len(toRunIndexList) > 0:
        iToRun = 0
        numToRun = len(toRunIndexList)
        for node in cntSubmitJobDict:
            if "DEBUG" in g_params and g_params['DEBUG']:
                webcom.loginfo("Trying to submitjob to the node=%s\n"%(str(node)), gen_logfile)
            if iToRun >= numToRun:
                if "DEBUG" in g_params and g_params['DEBUG']:
                    webcom.loginfo("iToRun(%d) >= numToRun(%d). Stop SubmitJob for jobid=%s\n"%(iToRun, numToRun, jobid), gen_logfile)
                break
            wsdl_url = "http://%s/pred/api_submitseq/?wsdl"%(node)
            try:
                myclient = Client(wsdl_url, cache=None, timeout=30)
            except:
                webcom.loginfo("Failed to access %s"%(wsdl_url), gen_logfile)
                continue

            if "DEBUG" in g_params and g_params['DEBUG']:
                webcom.loginfo("iToRun=%d, numToRun=%d\n"%(iToRun, numToRun), gen_logfile)
            [cnt, maxnum, queue_method] = cntSubmitJobDict[node]
            cnttry = 0
            while cnt < maxnum and iToRun < numToRun:
                origIndex = int(toRunIndexList[iToRun])
                seqfile_this_seq = "%s/%s"%(split_seq_dir, "query_%d.fa"%(origIndex))
                # ignore already existing query seq, this is an ugly solution,
                # the generation of torunindexlist has a bug
                outpath_this_seq = "%s/%s"%(outpath_result, "seq_%d"%origIndex)
                if os.path.exists(outpath_this_seq):
                    iToRun += 1
                    continue

                if 'DEBUG' in g_params and g_params['DEBUG']:
                    webcom.loginfo("DEBUG: cnt (%d) < maxnum (%d) "\
                            "and iToRun(%d) < numToRun(%d)"%(cnt, maxnum, iToRun, numToRun), gen_logfile)
                fastaseq = ""
                seqid = ""
                seqanno = ""
                seq = ""
                if not os.path.exists(seqfile_this_seq):
                    all_seqfile = "%s/query.fa"%(rstdir)
                    try:
                        (allseqidlist, allannolist, allseqlist) = myfunc.ReadFasta(all_seqfile)
                        seqid = allseqidlist[origIndex]
                        seqanno = allannolist[origIndex]
                        seq = allseqlist[origIndex]
                        fastaseq = ">%s\n%s\n" % (seqanno, seq)
                    except KeyError:
                        pass
                else:
                    fastaseq = myfunc.ReadFile(seqfile_this_seq)#seq text in fasta format
                    (seqid, seqanno, seq) = myfunc.ReadSingleFasta(seqfile_this_seq)

                isSubmitSuccess = False
                if len(seq) > 0:
                    query_para['name_software'] = webcom.GetNameSoftware(name_server.lower(), queue_method)
                    query_para['queue_method'] = queue_method
                    if name_server.lower() == "pathopred":
                        variant_text = myfunc.ReadFile(variant_file)
                        query_para['variants'] = variant_text
                        # also include the identifier name as a query parameter
                        query_para['identifier_name'] = seqid

                    para_str = json.dumps(query_para, sort_keys=True)
                    jobname = ""
                    if email not in g_params['vip_user_list']:
                        useemail = ""
                    else:
                        useemail = email
                    try:
                        myfunc.WriteFile("\tSubmitting seq %4d "%(origIndex),
                                gen_logfile, "a", True)
                        rtValue = myclient.service.submitjob_remote(fastaseq, para_str,
                                jobname, useemail, str(numseq_this_user), str(isForceRun))
                    except Exception as e:
                        webcom.loginfo("Failed to run myclient.service.submitjob_remote with errmsg=%s"%(str(e)), gen_logfile)
                        rtValue = []
                        pass

                    cnttry += 1
                    if len(rtValue) >= 1:
                        strs = rtValue[0]
                        if len(strs) >=5:
                            remote_jobid = strs[0]
                            result_url = strs[1]
                            numseq_str = strs[2]
                            errinfo = strs[3]
                            warninfo = strs[4]
                            if remote_jobid != "None" and remote_jobid != "":
                                isSubmitSuccess = True
                                epochtime = time.time()
                                # 6 fields in the file remotequeue_idx_file
                                txt =  "%d\t%s\t%s\t%s\t%s\t%f"%( origIndex,
                                        node, remote_jobid, seqanno.replace('\t', ' '), seq,
                                        epochtime)
                                submitted_loginfo_list.append(txt)
                                cnttry = 0  #reset cnttry to zero
                        else:
                            webcom.loginfo("bad wsdl return value", gen_logfile)

                if isSubmitSuccess:
                    cnt += 1
                    myfunc.WriteFile(" succeeded on node %s\n"%(node), gen_logfile, "a", True)
                else:
                    myfunc.WriteFile(" failed on node %s\n"%(node), gen_logfile, "a", True)

                if isSubmitSuccess or cnttry >= g_params['MAX_SUBMIT_TRY']:
                    iToRun += 1
                    processedIndexSet.add(str(origIndex))
                    if 'DEBUG' in g_params and g_params['DEBUG']:
                        webcom.loginfo("DEBUG: jobid %s processedIndexSet.add(str(%d))\n"%(jobid, origIndex), gen_logfile)
            # update cntSubmitJobDict for this node
            cntSubmitJobDict[node][0] = cnt

    # finally, append submitted_loginfo_list to remotequeue_idx_file 
    if 'DEBUG' in g_params and g_params['DEBUG']:
        webcom.loginfo("DEBUG: len(submitted_loginfo_list)=%d\n"%(len(submitted_loginfo_list)), gen_logfile)
    if len(submitted_loginfo_list)>0:
        myfunc.WriteFile("\n".join(submitted_loginfo_list)+"\n", remotequeue_idx_file, "a", True)
    # update torun_idx_file
    newToRunIndexList = []
    for idx in toRunIndexList:
        if not idx in processedIndexSet:
            newToRunIndexList.append(idx)
    if 'DEBUG' in g_params and g_params['DEBUG']:
        webcom.loginfo("DEBUG: jobid %s, newToRunIndexList="%(jobid) + " ".join( newToRunIndexList), gen_logfile)

    if len(newToRunIndexList)>0:
        myfunc.WriteFile("\n".join(newToRunIndexList)+"\n", torun_idx_file, "w", True)
    else:
        myfunc.WriteFile("", torun_idx_file, "w", True)

    return 0
# }}}


@timeit
def GetResult(jobid, g_params):  # {{{
    """Get the result from the remote computational node for a job
    """
    # retrieving result from the remote server for this job
    gen_logfile = g_params['gen_logfile']
    gen_errfile = g_params['gen_errfile']

    webcom.loginfo(f"GetResult for {jobid}.\n", gen_logfile)

    path_static = g_params['path_static']
    path_result = os.path.join(path_static, 'result')
    path_cache = g_params['path_cache']
    finished_date_db = g_params['finished_date_db']
    name_server = g_params['name_server']

    rstdir = os.path.join(path_result, jobid)
    runjob_logfile = os.path.join(rstdir, "runjob.log")
    runjob_errfile = os.path.join(rstdir, "runjob.err")
    outpath_result = os.path.join(rstdir, jobid)
    if not os.path.exists(outpath_result):
        os.mkdir(outpath_result)

    remotequeue_idx_file = os.path.join(rstdir, "remotequeue_seqindex.txt")

    torun_idx_file = os.path.join(rstdir, "torun_seqindex.txt")
    finished_idx_file = os.path.join(rstdir, "finished_seqindex.txt")
    query_parafile = os.path.join(rstdir, "query.para.txt")

    query_para = {}
    if os.path.exists(query_parafile):
        content = myfunc.ReadFile(query_parafile)
        if content != "":
            try:
                query_para = json.loads(content)
            except ValueError:
                query_para = {}
    failed_idx_file = os.path.join(rstdir, "failed_seqindex.txt")

    starttagfile = os.path.join(rstdir, "runjob.start")
    cnttry_idx_file = os.path.join(rstdir, "cntsubmittry_seqindex.txt")  # index file to keep log of tries
    tmpdir = os.path.join(rstdir, "tmpdir")
    finished_seq_file = os.path.join(outpath_result, "finished_seqs.txt")

    if not os.path.exists(tmpdir):
        os.mkdir(tmpdir)

    finished_info_list = []  # [info for finished record]
    finished_idx_list = []  # [origIndex]
    failed_idx_list = []    # [origIndex]
    resubmit_idx_list = []  # [origIndex]
    keep_queueline_list = []  # [line] still in queue

    cntTryDict = {}
    if os.path.exists(cnttry_idx_file):
        with open(cnttry_idx_file, 'r') as fpin:
            try:
                cntTryDict = json.load(fpin)
            except Exception:
                cntTryDict = {}

    # in case of missing queries, if remotequeue_idx_file is empty  but the job
    # is still not finished, force recreating torun_idx_file
    if 'DEBUG' in g_params and g_params['DEBUG']:
        try:
            webcom.loginfo("DEBUG: %s: remotequeue_idx_file=%s, size(remotequeue_idx_file)=%d, content=\"%s\"\n" %(jobid, remotequeue_idx_file, os.path.getsize(remotequeue_idx_file), myfunc.ReadFile(remotequeue_idx_file)), gen_logfile)
        except Exception:
            pass
    if ((not os.path.exists(remotequeue_idx_file) or  # {{{
        os.path.getsize(remotequeue_idx_file) < 1)):
        idlist1 = []
        idlist2 = []
        if os.path.exists(finished_idx_file):
           idlist1 = myfunc.ReadIDList(finished_idx_file)
        if os.path.exists(failed_idx_file):
           idlist2 = myfunc.ReadIDList(failed_idx_file)

        completed_idx_set = set(idlist1 + idlist2)

        jobinfofile = os.path.join(rstdir, "jobinfo")
        jobinfo = myfunc.ReadFile(jobinfofile).strip()
        jobinfolist = jobinfo.split("\t")
        if len(jobinfolist) >= 8:
            numseq = int(jobinfolist[3])

        if 'DEBUG' in g_params and g_params['DEBUG']:
            webcom.loginfo("DEBUG: len(completed_idx_set)=%d+%d=%d, numseq=%d\n"%(len(idlist1), len(idlist2), len(completed_idx_set), numseq), gen_logfile)

        if len(completed_idx_set) < numseq:
            all_idx_list = [str(x) for x in range(numseq)]
            torun_idx_str_list = list(set(all_idx_list)-completed_idx_set)
            for idx in torun_idx_str_list:
                try:
                    cntTryDict[int(idx)] += 1
                except (ValueError, IndexError, KeyError):
                    cntTryDict[int(idx)] = 1
            myfunc.WriteFile("\n".join(torun_idx_str_list)+"\n", torun_idx_file, "w", True)

            if 'DEBUG' in g_params and g_params['DEBUG']:
                webcom.loginfo("recreate torun_idx_file: jobid = %s, numseq=%d, len(completed_idx_set)=%d, len(torun_idx_str_list)=%d\n"%(jobid, numseq, len(completed_idx_set), len(torun_idx_str_list)), gen_logfile)
        else:
            myfunc.WriteFile("", torun_idx_file, "w", True)
    else:
        if 'DEBUG' in g_params and g_params['DEBUG']:
            webcom.loginfo("DEBUG: %s: remotequeue_idx_file %s is not empty\n" %(jobid, remotequeue_idx_file), gen_logfile)
# }}}

    text = ""
    if os.path.exists(remotequeue_idx_file):
        text = myfunc.ReadFile(remotequeue_idx_file)
    if text == "":
        return 1
    lines = text.split("\n")

    nodeSet = set([])
    for i in range(len(lines)):
        line = lines[i]
        if not line or line[0] == "#":
            continue
        strs = line.split("\t")
        if len(strs) != 6:
            continue
        node = strs[1]
        nodeSet.add(node)

    myclientDict = {}
    for node in nodeSet:
        wsdl_url = f"http://{node}/pred/api_submitseq/?wsdl"
        try:
            myclient = Client(wsdl_url, cache=None, timeout=30)
            myclientDict[node] = myclient
        except Exception as e:
            webcom.loginfo(f"Failed to access {wsdl_url} with errmsg {e}", gen_logfile)
            pass

    for i in range(len(lines)):  # {{{
        line = lines[i]

        if 'DEBUG' in g_params and g_params['DEBUG']:
            myfunc.WriteFile(f"Process {line}\n", gen_logfile, "a", True)
        if not line or line[0] == "#":
            if 'DEBUG' in g_params and g_params['DEBUG']:
                webcom.loginfo("DEBUG: line empty or line[0] = '#', ignore", gen_logfile)
            continue
        strs = line.split("\t")
        if len(strs) != 6:
            if 'DEBUG' in g_params and g_params['DEBUG']:
                webcom.loginfo("DEBUG: len(strs)=%d (!=6), ignore\n"%(len(strs)), gen_logfile)
            continue
        origIndex = int(strs[0])
        node = strs[1]
        remote_jobid = strs[2]
        description = strs[3]
        seq = strs[4]
        submit_time_epoch = float(strs[5])
        subfoldername_this_seq = f"seq_{origIndex}"
        outpath_this_seq = os.path.join(outpath_result, subfoldername_this_seq)

        try:
            myclient = myclientDict[node]
        except KeyError:
            if 'DEBUG' in g_params and g_params['DEBUG']:
                webcom.loginfo("DEBUG: node (%s) not found in myclientDict, ignore"%(node), gen_logfile)
            keep_queueline_list.append(line)
            continue
        try:
            rtValue = myclient.service.checkjob(remote_jobid)
        except Exception as e:
            msg = "checkjob(%s) at node %s failed with errmsg %s"%(remote_jobid, node, str(e))
            webcom.loginfo(msg, gen_logfile)
            rtValue = []
            pass
        isSuccess = False
        isFinish_remote = False
        status = ""
        if len(rtValue) >= 1:
            ss2 = rtValue[0]
            if len(ss2) >= 3:
                status = ss2[0]
                result_url = ss2[1]
                errinfo = ss2[2]

                if errinfo and errinfo.find("does not exist") != -1:
                    if 'DEBUG' in g_params and g_params['DEBUG']:
                        msg = "Failed for remote_jobid %s with errmsg %s"%(remote_jobid, str(errinfo))
                        webcom.loginfo(msg, gen_logfile)

                    isFinish_remote = True

                if status == "Finished":  # {{{
                    isFinish_remote = True
                    outfile_zip = f"{tmpdir}/{remote_jobid}.zip"
                    isRetrieveSuccess = False
                    myfunc.WriteFile("\tFetching result for %s/seq_%d from %s " % (
                        jobid, origIndex, result_url), gen_logfile, "a", True)
                    if myfunc.IsURLExist(result_url, timeout=5):
                        try:
                            myfunc.urlretrieve(result_url, outfile_zip, timeout=10)
                            isRetrieveSuccess = True
                            myfunc.WriteFile(f" succeeded on node {node}\n", gen_logfile, "a", True)
                        except Exception as e:
                            myfunc.WriteFile(" failed with %s\n"%(str(e)), gen_logfile, "a", True)
                            pass
                    if os.path.exists(outfile_zip) and isRetrieveSuccess:
                        cmd = ["unzip", outfile_zip, "-d", tmpdir]
                        webcom.RunCmd(cmd, gen_logfile, gen_errfile)
                        rst_fetched = os.path.join(tmpdir, remote_jobid)
                        if name_server.lower() == "pconsc3":
                            rst_this_seq = rst_fetched
                        elif name_server.lower() == "boctopus2":
                            rst_this_seq = os.path.join(rst_fetched, "seq_0", "seq_0")
                            rst_this_seq_parent = os.path.join(rst_fetched, "seq_0")
                        else:
                            rst_this_seq = os.path.join(rst_fetched, "seq_0")

                        if os.path.islink(outpath_this_seq):
                            os.unlink(outpath_this_seq)
                        elif os.path.exists(outpath_this_seq):
                            shutil.rmtree(outpath_this_seq)

                        if os.path.exists(rst_this_seq) and not os.path.exists(outpath_this_seq):
                            cmd = ["mv", "-f", rst_this_seq, outpath_this_seq]
                            webcom.RunCmd(cmd, gen_logfile, gen_errfile)
                            if name_server.lower() == "boctopus2":
                                # move also seq.fa and time.txt for boctopus2
                                file1 = os.path.join(rst_this_seq_parent, "seq.fa")
                                file2 = os.path.join(rst_this_seq_parent, "time.txt")
                                for f in [file1, file2]:
                                    if os.path.exists(f):
                                        try:
                                            shutil.move(f, outpath_this_seq)
                                        except:
                                            pass

                            fafile_this_seq = os.path.join(outpath_this_seq, "seq.fa")
                            if webcom.IsCheckPredictionPassed(outpath_this_seq, name_server):
                                # relpace the seq.fa with original description
                                myfunc.WriteFile('>%s\n%s\n'%(description, seq), fafile_this_seq, 'w', True)
                                isSuccess = True

                            if isSuccess:
                                # delete the data on the remote server
                                try:
                                    rtValue2 = myclient.service.deletejob(remote_jobid)
                                except Exception as e:
                                    msg = "Failed to deletejob(%s) on node %s with errmsg %s"%(remote_jobid, node, str(e))
                                    webcom.loginfo(msg, gen_logfile)
                                    rtValue2 = []
                                    pass

                                logmsg = ""
                                if len(rtValue2) >= 1:
                                    ss2 = rtValue2[0]
                                    if len(ss2) >= 2:
                                        status = ss2[0]
                                        errmsg = ss2[1]
                                        if status == "Succeeded":
                                            logmsg = "Successfully deleted data on %s "\
                                                    "for %s"%(node, remote_jobid)
                                        else:
                                            logmsg = "Failed to delete data on %s for "\
                                                    "%s\nError message:\n%s\n"%(node, remote_jobid, errmsg)
                                else:
                                    logmsg = "Failed to call deletejob %s via WSDL on %s\n"%(remote_jobid, node)

                                # delete the downloaded temporary zip file and
                                # extracted file
                                if os.path.exists(outfile_zip):
                                    os.remove(outfile_zip)
                                if os.path.exists(rst_fetched):
                                    shutil.rmtree(rst_fetched)

                                # create or update the md5 cache
                                if name_server.lower() == "prodres" and query_para != {}:
                                    md5_key = hashlib.md5((seq+str(query_para)).encode('utf-8')).hexdigest()
                                else:
                                    md5_key = hashlib.md5(seq.encode('utf-8')).hexdigest()
                                subfoldername = md5_key[:2]
                                md5_subfolder = "%s/%s"%(path_cache, subfoldername)
                                cachedir = "%s/%s/%s"%(path_cache, subfoldername, md5_key)

                                # copy the zipped folder to the cache path
                                origpath = os.getcwd()
                                os.chdir(outpath_result)
                                shutil.copytree("seq_%d"%(origIndex), md5_key)
                                cmd = ["zip", "-rq", "%s.zip"%(md5_key), md5_key]
                                webcom.RunCmd(cmd, runjob_logfile, runjob_errfile)
                                if not os.path.exists(md5_subfolder):
                                    os.makedirs(md5_subfolder)
                                shutil.move("%s.zip"%(md5_key), "%s.zip"%(cachedir))
                                shutil.rmtree(md5_key) # delete the temp folder named as md5 hash
                                os.chdir(origpath)

                                # Add the finished date to the database
                                date_str = time.strftime(g_params['FORMAT_DATETIME'])
                                MAX_TRY_INSERT_DB = 3
                                cnttry = 0
                                while cnttry < MAX_TRY_INSERT_DB:
                                    t_rv = webcom.InsertFinishDateToDB(date_str, md5_key, seq, finished_date_db)
                                    if t_rv == 0:
                                        break
                                    cnttry += 1
                                    time.sleep(random.random()/1.0)

# }}}
                elif status in ["Failed", "None"]:
                    # the job is failed for this sequence, try to resubmit
                    isFinish_remote = True
                    if 'DEBUG' in g_params and g_params['DEBUG']:
                        webcom.loginfo("DEBUG: %s, status = %s\n"%(remote_jobid, status), gen_logfile)

                if status != "Wait" and not os.path.exists(starttagfile):
                    webcom.WriteDateTimeTagFile(starttagfile, runjob_logfile, runjob_errfile)

        if isSuccess:  # {{{
            time_now = time.time()
            runtime1 = time_now - submit_time_epoch  # in seconds
            timefile = os.path.join(outpath_this_seq, "time.txt")
            runtime = webcom.ReadRuntimeFromFile(timefile, default_runtime=runtime1)
            info_finish = webcom.GetInfoFinish(
                    name_server, outpath_this_seq,
                    origIndex, len(seq), description,
                    source_result="newrun", runtime=runtime)
            finished_info_list.append("\t".join(info_finish))
            finished_idx_list.append(str(origIndex))
            # }}}

        # if the job is finished on the remote but the prediction is failed,
        # try resubmit a few times and if all failed, add the origIndex to the
        # failed_idx_file
        if isFinish_remote and not isSuccess:
            cnttry = 1
            try:
                cnttry = cntTryDict[int(origIndex)]
            except KeyError:
                cnttry = 1
            if cnttry < g_params['MAX_RESUBMIT']:
                resubmit_idx_list.append(str(origIndex))
                cntTryDict[int(origIndex)] = cnttry+1
            else:
                failed_idx_list.append(str(origIndex))

        if not isFinish_remote:
            time_in_remote_queue = time.time() - submit_time_epoch
            # for jobs queued in the remote queue more than one day (but not
            # running) delete it and try to resubmit it. This solved the
            # problem of dead jobs in the remote server due to server
            # rebooting)
            if (
                    status != "Running"
                    and status != ""
                    and time_in_remote_queue > g_params['MAX_TIME_IN_REMOTE_QUEUE']):
                # delete the remote job on the remote server
                try:
                    rtValue2 = myclient.service.deletejob(remote_jobid)
                except Exception as e:
                    webcom.loginfo("Failed to run myclient.service.deletejob(%s) on node %s with msg %s"%(remote_jobid, node, str(e)), gen_logfile)
                    rtValue2 = []
                    pass
            else:
                keep_queueline_list.append(line)
# }}}
    # Finally, write log files
    finished_idx_list = list(set(finished_idx_list))
    failed_idx_list = list(set(failed_idx_list))
    resubmit_idx_list = list(set(resubmit_idx_list))

    if len(finished_info_list) > 0:
        myfunc.WriteFile("\n".join(finished_info_list)+"\n", finished_seq_file,
                         "a", True)
    if len(finished_idx_list) > 0:
        myfunc.WriteFile("\n".join(finished_idx_list)+"\n", finished_idx_file,
                         "a", True)
    if len(failed_idx_list) > 0:
        myfunc.WriteFile("\n".join(failed_idx_list)+"\n", failed_idx_file, "a",
                         True)
    if len(resubmit_idx_list) > 0:
        myfunc.WriteFile("\n".join(resubmit_idx_list)+"\n", torun_idx_file,
                         "a", True)

    if len(keep_queueline_list) > 0:
        keep_queueline_list = list(set(keep_queueline_list))
        myfunc.WriteFile("\n".join(keep_queueline_list)+"\n",
                         remotequeue_idx_file, "w", True)
    else:
        myfunc.WriteFile("", remotequeue_idx_file, "w", True)

    with open(cnttry_idx_file, 'w') as fpout:
        json.dump(cntTryDict, fpout)

    return 0
# }}}


@timeit
def CheckIfJobFinished(jobid, numseq, to_email, g_params):  # {{{
    """check if the job is finished and write tag files
    """
    path_result = os.path.join(g_params['path_static'], 'result')
    rstdir = os.path.join(path_result, jobid)
    gen_logfile = g_params['gen_logfile']
    gen_errfile = g_params['gen_errfile']
    name_server = g_params['name_server']
    g_params['jobid'] = jobid
    g_params['numseq'] = numseq
    g_params['to_email'] = to_email
    jsonfile = os.path.join(rstdir, "job_final_process.json")
    myfunc.WriteFile(json.dumps(g_params, sort_keys=True), jsonfile, "w")
    binpath_script = os.path.join(g_params['webserver_root'], "env", "bin")

    finished_idx_file = "%s/finished_seqindex.txt"%(rstdir)
    failed_idx_file = "%s/failed_seqindex.txt"%(rstdir)
    py_scriptfile = os.path.join(binpath_script, "job_final_process.py")
    finished_idx_list = []
    failed_idx_list = []
    if os.path.exists(finished_idx_file):
        finished_idx_list = list(set(myfunc.ReadIDList(finished_idx_file)))
    if os.path.exists(failed_idx_file):
        failed_idx_list = list(set(myfunc.ReadIDList(failed_idx_file)))


    lockname = "job_final_process.lock"
    lock_file = os.path.join(g_params['path_result'], g_params['jobid'], lockname)

    num_processed = len(finished_idx_list)+len(failed_idx_list)
    if num_processed >= numseq:# finished
        if ('THRESHOLD_NUMSEQ_CHECK_IF_JOB_FINISH' in g_params
                and numseq <= g_params['THRESHOLD_NUMSEQ_CHECK_IF_JOB_FINISH']):
            cmd = ["python", py_scriptfile, "-i", jsonfile]
            (isSubmitSuccess, t_runtime) = webcom.RunCmd(cmd, gen_logfile, gen_errfile)
        elif not os.path.exists(lock_file):
            bash_scriptfile = "%s/job_final_process,%s,%s.sh"%(rstdir, name_server, jobid)
            code_str_list = []
            code_str_list.append("#!/bin/bash")
            cmdline = "python %s -i %s"%(py_scriptfile, jsonfile)
            code_str_list.append(cmdline)
            code = "\n".join(code_str_list)
            myfunc.WriteFile(code, bash_scriptfile, mode="w", isFlush=True)
            os.chmod(bash_scriptfile, 0o755)
            os.chdir(rstdir)
            cmd = ['sbatch', bash_scriptfile]
            cmdline = " ".join(cmd)
            verbose = False
            if 'DEBUG' in g_params and g_params['DEBUG']:
                verbose = True
                webcom.loginfo("Run cmdline: %s"%(cmdline), gen_logfile)
            (isSubmitSuccess, t_runtime) = webcom.RunCmd(cmd, gen_logfile, gen_errfile, verbose)
            if 'DEBUG' in g_params and g_params['DEBUG']:
                webcom.loginfo("isSubmitSuccess: %s"%(str(isSubmitSuccess)), gen_logfile)
# }}}
