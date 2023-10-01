#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Description:
A collection of classes and functions for the qd_fe.py

Author: Nanjiang Shu (nanjiang.shu@scilifelab.se)

Address: Science for Life Laboratory Stockholm, Box 1031, 17121 Solna, Sweden
"""

import os
from . import myfunc
from . import webserver_common as webcom
import math
import random
import time
from datetime import datetime
# from pytz import timezone
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
    logfile = os.path.join(g_params["path_log"], f"{bsname}.log")
    errfile = os.path.join(g_params["path_log"], f"{bsname}.err")
    binpath_script = os.path.join(g_params['webserver_root'], "env", "bin")
    py_scriptfile = os.path.join(binpath_script, f"{bsname}.py")
    jsonfile = os.path.join(path_tmp, f"{bsname}.json")
    g_params['logfile'] = logfile
    g_params['errfile'] = errfile
    myfunc.WriteFile(json.dumps(g_params, sort_keys=True), jsonfile, "w")
    name_server = g_params['name_server']
    webcom.loginfo(f"Run server statistics..", g_params['gen_logfile'])
    if 'RUN_STATISTICS_IN_QD' in g_params and g_params['RUN_STATISTICS_IN_QD']:
        cmd = ["python", py_scriptfile, "-i", jsonfile]
        webcom.RunCmd(cmd, logfile, errfile)
    elif not os.path.exists(lock_file):
        bash_scriptfile = f"{path_tmp}/{bsname}-{name_server}.sh"
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
            webcom.loginfo(f"Run cmdline: {cmdline}", logfile)
        (isSubmitSuccess, t_runtime) = webcom.RunCmd(cmd,
                                                     logfile,
                                                     errfile,
                                                     verbose)
        if 'DEBUG' in g_params and g_params['DEBUG']:
            webcom.loginfo("isSubmitSuccess: {isSubmitSuccess}", logfile)
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

            if email in g_params['vip_user_list'] or ip in g_params['vip_user_list']:
                numseq_this_user = 1
                priority = 999999999.0
                webcom.loginfo("email/ip %s in vip_user_list"%(email), gen_logfile)

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
                        if os.path.getsize(zipfile_cache) == 0:
                            os.remove(zipfile_cache)  # remove empty archived result zip file
                        else:
                            cmd = ["unzip", zipfile_cache, "-d", outpath_result]
                            webcom.RunCmd(cmd, runjob_logfile, runjob_errfile)
                            if os.path.exists(outpath_this_seq):
                                shutil.rmtree(outpath_this_seq)
                            if os.path.exists(os.path.join(outpath_result, md5_key)):
                                shutil.move(os.path.join(outpath_result, md5_key), outpath_this_seq)

                    fafile_this_seq = '%s/seq.fa'%(outpath_this_seq)
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
        iNode = -1
        for node in cntSubmitJobDict:
            iNode += 1
            if "DEBUG" in g_params and g_params['DEBUG']:
                webcom.loginfo(f"Trying to submit job to the node {iNode}: {node}", gen_logfile)
                webcom.loginfo(f"cntSubmitJobDict={cntSubmitJobDict}", gen_logfile)
            if cntSubmitJobDict[node][3] == "OFF":
                webcom.loginfo(f"node {node} is offline, try again in the next loop", gen_logfile)
                continue
            if iToRun >= numToRun:
                if "DEBUG" in g_params and g_params['DEBUG']:
                    webcom.loginfo(f"iToRun({iToRun}) >= numToRun({numToRun}). Stop SubmitJob for jobid={jobid}", gen_logfile)
                break
            wsdl_url = "http://%s/pred/api_submitseq/?wsdl"%(node)
            try:
                myclient = Client(wsdl_url, cache=None, timeout=30)
            except Exception as e:
                webcom.loginfo(f"Failed to access {wsdl_url}, detailed error: {e}", gen_logfile)
                cntSubmitJobDict[node][3] = "OFF"
                continue

            if "DEBUG" in g_params and g_params['DEBUG']:
                webcom.loginfo(f"iToRun={iToRun}, numToRun={numToRun}", gen_logfile)
            [cnt, maxnum, queue_method, node_status] = cntSubmitJobDict[node]
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
                        webcom.loginfo(f"DEBUG: jobid {jobid} processedIndexSet.add({origIndex})", gen_logfile)
            # update cntSubmitJobDict for this node
            cntSubmitJobDict[node][0] = cnt

    # finally, append submitted_loginfo_list to remotequeue_idx_file 
    if 'DEBUG' in g_params and g_params['DEBUG']:
        webcom.loginfo(f"DEBUG: len(submitted_loginfo_list)={len(submitted_loginfo_list)}", gen_logfile)
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

    webcom.loginfo(f"GetResult for {jobid}.", gen_logfile)

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
            webcom.loginfo(f"DEBUG: {jobid}: remotequeue_idx_file={remotequeue_idx_file}, size(remotequeue_idx_file)={os.path.getsize(remotequeue_idx_file)}, content=\"{myfunc.ReadFile(remotequeue_idx_file)}\"", gen_logfile)
        except Exception as e:
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
            webcom.loginfo(f"DEBUG: len(completed_idx_set)={len(idlist1)}+{len(idlist2)}={len(completed_idx_set)}, numseq={numseq}", gen_logfile)

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
                webcom.loginfo(f"recreate torun_idx_file: jobid = {jobid}, numseq={numseq}, len(completed_idx_set)={len(completed_idx_set)}, len(torun_idx_str_list)={len(torun_idx_str_list)}", gen_logfile)
        else:
            myfunc.WriteFile("", torun_idx_file, "w", True)
    else:
        if 'DEBUG' in g_params and g_params['DEBUG']:
            webcom.loginfo(f"DEBUG: {jobid}: remotequeue_idx_file {remotequeue_idx_file} is not empty", gen_logfile)
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
                webcom.loginfo(f"DEBUG: len(strs)={len(strs)} (!=6), ignore", gen_logfile)
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
                                    msg = (f"Failed to delete the job {remote_jobid} on node {node}"
                                           f" with error: {str(e)}")
                                    webcom.loginfo(msg, gen_logfile)
                                    rtValue2 = []
                                    pass

                                logmsg = ""
                                if len(rtValue2) >= 1:
                                    ss2 = rtValue2[0]
                                    if len(ss2) >= 2:
                                        status_job_delete = ss2[0]
                                        errmsg = ss2[1]
                                        if status_job_delete == "Succeeded":
                                            logmsg = (f"Successfully deleted data on {node} "
                                                      f"for {remote_jobid}")
                                        else:
                                            logmsg = (f"Failed to delete data on {node} for "
                                                      f"{remote_jobid} with error: {errmsg}")
                                else:
                                    logmsg = f"Failed to call deletejob {remote_jobid} via WSDL on {node}\n"
                                webcom.loginfo(logmsg, gen_logfile)

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
                        webcom.loginfo(f"DEBUG: {remote_jobid}, status = {status}", gen_logfile)

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
            if 'DEBUG' in g_params and g_params['DEBUG']:
                if time_in_remote_queue > g_params['MAX_TIME_IN_REMOTE_QUEUE']:
                    webcom.loginfo(f"\ttime_in_remote_queue ({time_in_remote_queue}) >"
                                   f" MAX_TIME_IN_REMOTE_QUEUE ({g_params['MAX_TIME_IN_REMOTE_QUEUE']})"
                                   f" for remote_jobid ({remote_jobid})"
                                   f" with status ({status})", gen_logfile)
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
    bsname = "job_final_process"
    path_result = os.path.join(g_params['path_static'], 'result')
    rstdir = os.path.join(path_result, jobid)
    gen_logfile = g_params['gen_logfile']
    gen_errfile = g_params['gen_errfile']
    name_server = g_params['name_server']
    g_params['jobid'] = jobid
    g_params['numseq'] = numseq
    g_params['to_email'] = to_email
    jsonfile = os.path.join(rstdir, f"{bsname}.json")
    myfunc.WriteFile(json.dumps(g_params, sort_keys=True), jsonfile, "w")
    binpath_script = os.path.join(g_params['webserver_root'], "env", "bin")

    finished_idx_file = "%s/finished_seqindex.txt"%(rstdir)
    failed_idx_file = "%s/failed_seqindex.txt"%(rstdir)
    py_scriptfile = os.path.join(binpath_script, f"{bsname}.py")
    finished_idx_list = []
    failed_idx_list = []
    if os.path.exists(finished_idx_file):
        finished_idx_list = list(set(myfunc.ReadIDList(finished_idx_file)))
    if os.path.exists(failed_idx_file):
        failed_idx_list = list(set(myfunc.ReadIDList(failed_idx_file)))

    lockname = f"{bsname}.lock"
    lock_file = os.path.join(g_params['path_result'], g_params['jobid'],
                             lockname)

    num_processed = len(finished_idx_list)+len(failed_idx_list)
    if num_processed >= numseq:  # finished
        if ('THRESHOLD_NUMSEQ_CHECK_IF_JOB_FINISH' in g_params
                and numseq <= g_params['THRESHOLD_NUMSEQ_CHECK_IF_JOB_FINISH']):
            cmd = ["python", py_scriptfile, "-i", jsonfile]
            (isSubmitSuccess, t_runtime) = webcom.RunCmd(cmd, gen_logfile, gen_errfile)
        elif not os.path.exists(lock_file):
            bash_scriptfile = f"{rstdir}/{bsname},{name_server},{jobid}.sh"
            code_str_list = []
            code_str_list.append("#!/bin/bash")
            cmdline = f"python {py_scriptfile} -i {jsonfile}"
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


@timeit
def CleanCachedResult(g_params):  # {{{
    """Clean outdated cahced results on the server"""
    bsname = "clean_cached_result"
    gen_logfile = g_params['gen_logfile']
    gen_errfile = g_params['gen_errfile']
    path_tmp = os.path.join(g_params['path_static'], "tmp")
    name_server = g_params['name_server']
    if 'MAX_KEEP_DAYS_CACHE' in g_params:
        MAX_KEEP_DAYS_CACHE = g_params['MAX_KEEP_DAYS_CACHE']
    else:
        MAX_KEEP_DAYS_CACHE = 480
    binpath_script = os.path.join(g_params['webserver_root'], "env", "bin")
    py_scriptfile = os.path.join(binpath_script, f"{bsname}.py")
    jsonfile = os.path.join(path_tmp, f"{bsname}.json")
    myfunc.WriteFile(json.dumps(g_params, sort_keys=True), jsonfile, "w")
    lockname = f"{bsname}.lock"
    lock_file = os.path.join(g_params['path_log'], lockname)
    webcom.loginfo(f"Clean cached results older than {MAX_KEEP_DAYS_CACHE} days",
                   gen_logfile)
    cmd = ["python", py_scriptfile, "-i", jsonfile,
           "-max-keep-day", f"{MAX_KEEP_DAYS_CACHE}"]
    cmdline = " ".join(cmd)
    if ('CLEAN_CACHED_RESULT_IN_QD' in g_params
            and g_params['CLEAN_CACHED_RESULT_IN_QD']):
        webcom.RunCmd(cmd, gen_logfile, gen_errfile)
    elif not os.path.exists(lock_file):
        bash_scriptfile = f"{path_tmp}/{bsname}-{name_server}.sh"
        code_str_list = []
        code_str_list.append("#!/bin/bash")
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
        webcom.RunCmd(cmd, gen_logfile, gen_errfile, verbose)
# }}}
