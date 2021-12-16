#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Description:
#   A collection of classes and functions used by web-servers
#
# Author: Nanjiang Shu (nanjiang.shu@scilifelab.se)
#
# Address: Science for Life Laboratory Stockholm, Box 1031, 17121 Solna, Sweden

import os
import sys
import re
from . import myfunc
import time
from datetime import datetime
from dateutil import parser as dtparser
from pytz import timezone
import tabulate
import shutil
import logging
import subprocess
import sqlite3
import json
from geoip import geolite2
import pycountry
import requests
from .timeit import timeit

TZ = "Europe/Stockholm"
FORMAT_DATETIME = "%Y-%m-%d %H:%M:%S %Z"
ZB_SCORE_THRESHOLD = 0.45
chde_table = {
        'C':'CYS',
        'H': 'HIS',
        'D': 'ASP',
        'E': 'GLU',
        'CYS': 'C',
        'HIS': 'H',
        'ASP': 'D',
        'GLU': 'E'
        }
def IsCacheProcessingFinished(rstdir):# {{{
    """Check whether the jobdir is still under cache processing"""
    forceruntagfile = "%s/forcerun"%(rstdir)
    cache_process_finish_tagfile = "%s/cache_processed.finish"%(rstdir)
    if os.path.exists(forceruntagfile):
        isForceRun = True
    else:
        isForceRun = False
    if isForceRun or os.path.exists(cache_process_finish_tagfile):
        isCacheProcessingFinished = True
    else:
        isCacheProcessingFinished = False
    return isCacheProcessingFinished
# }}}
def IsHaveAvailNode(cntSubmitJobDict):#{{{
    """
    Check if there are available slots in any of the computational node
    format of cntSubmitJobDict {'node_ip': INT, 'node_ip': INT}  
    """
    for node in cntSubmitJobDict:
        num_queue_job = cntSubmitJobDict[node][0]
        max_allowed_job = cntSubmitJobDict[node][1]
        if num_queue_job < max_allowed_job:
            return True
    return False
#}}}
def get_job_status(jobid, numseq, path_result):#{{{
    """Get the status of a job submitted to the web-server
    """
    status = "";
    rstdir = "%s/%s"%(path_result, jobid)
    starttagfile = "%s/%s"%(rstdir, "runjob.start")
    finishtagfile = "%s/%s"%(rstdir, "runjob.finish")
    failedtagfile = "%s/%s"%(rstdir, "runjob.failed")
    remotequeue_idx_file = "%s/remotequeue_seqindex.txt"%(rstdir)
    torun_idx_file = "%s/torun_seqindex.txt"%(rstdir) # ordered seq index to run
    num_torun = len(myfunc.ReadIDList(torun_idx_file))
    if os.path.exists(failedtagfile):
        status = "Failed"
    elif os.path.exists(finishtagfile):
        status = "Finished"
    elif os.path.exists(starttagfile):
        if num_torun < numseq:
            status = "Running"
        else:
            status = "Wait"
    elif os.path.exists(rstdir):
        status = "Wait"
    return status
#}}}
def get_external_ip(timeout=5):# {{{
    """Return external IP of the host
    """
    try:
        ip = requests.get('https://api.ipify.org', timeout=timeout).text
        ip = ip.strip()
        return ip
    except:
        return ""
#}}}
def anonymize_ip_v4(ip):# {{{
    """Anonymize the IP address for protecting user privacy
    """
    strs = ip.split('.')
    for i in range(2, len(strs)):
        strs[i] = '*'
    return '.'.join(strs)
# }}}
def get_url_scheme(request):# {{{
    """get whether the url is http or https
    """
    try:
        if request.is_secure():
            return "https://"
        else:
            return "http://"
    except:
        return "http://"
# }}}
def CountNumPredZB(predfile, threshold=0.45):#{{{
    cntZB = 0
    cntHomo = 0
    hdl = myfunc.ReadLineByBlock(predfile)
    if not hdl.failure:
        lines = hdl.readlines()
        while lines != None:
            for line in lines:
                if not line:
                    continue
                if line[0] == "#":
                    strs = line.split()
                    if len(strs) >=6 and strs[0] == "#Homolog":
                        cntHomo += 1
                else:
                    strs = line.split("\t")
                    try:
                        score = float(strs[2])
                        if score >= threshold:
                            cntZB += 1
                    except:
                        pass
            lines = hdl.readlines()
        hdl.close()
        return (cntZB, cntHomo)
    else:
        return (cntZB, cntHomo)
#}}}

def ReadProQ3GlobalScore(infile):#{{{
    #return globalscore and itemList
    #itemList is the name of the items
    globalscore = {}
    keys = []
    values = []
    try:
        fpin = open(infile, "r")
        lines = fpin.read().split("\n")
        fpin.close()
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if line.lower().find("proq") != -1:
                keys = line.strip().split()
            elif myfunc.isnumeric(line.strip().split()[0]):
                values = line.split()
                try:
                    values = [float(x) for x in values]
                except:
                    values = []
        if len(keys) == len(values):
            for i in range(len(keys)):
                globalscore[keys[i]] = values[i]
    except IOError:
        pass
    return (globalscore, keys)
#}}}
def GetProQ3ScoreListFromGlobalScoreFile(globalscorefile):# {{{
    (globalscore, itemList) = ReadProQ3GlobalScore(globalscorefile)
    return itemList
# }}}
def GetProQ3Option(query_para):#{{{
    """Return the proq3opt in list
    """
    yes_or_no_opt = {}
    for item in ['isDeepLearning', 'isRepack', 'isKeepFiles']:
        if item in query_para and query_para[item]:
            yes_or_no_opt[item] = "yes"
        else:
            yes_or_no_opt[item] = "no"

    proq3opt = [
            "-r", yes_or_no_opt['isRepack'],
            "-deep", yes_or_no_opt['isDeepLearning'],
            "-k", yes_or_no_opt['isKeepFiles'],
            "-quality", query_para['method_quality'],
            "-output_pdbs", "yes"         #always output PDB file (with proq3 written at the B-factor column)
            ]
    if 'targetlength' in query_para:
        proq3opt += ["-t", str(query_para['targetlength'])]

    return proq3opt

#}}}

def ReadJobInfo(infile):# {{{
    """Read file jobinfo. return a dictionary
    """
    jobinfo = myfunc.ReadFile(jobinfofile).strip()
    dt = {}
    dt['submit_date_str'] = ""
    dt['jobid'] = ""
    dt['client_ip'] = ""
    dt['length_rawseq'] = 0
    dt['numseq'] = 1
    dt['jobname'] = ""
    dt['email'] = ""
    dt['method_submission'] = "web"
    dt['app_type'] = ""
    jobinfolist = jobinfo.split("\t")
    if len(jobinfolist) >= 8:
        dt['submit_date_str'] = jobinfolist[0]
        dt['jobid'] = jobinfolist[1]
        dt['client_ip'] = jobinfolist[2]
        dt['numseq'] = int(jobinfolist[3])
        dt['length_rawseq'] = int(jobinfolist[4])
        dt['jobname'] = jobinfolist[5]
        dt['email'] = jobinfolist[6]
        dt['method_submission'] = jobinfolist[7]
        if len(jobinfolist) == 9:
            dt['app_type'] = jobinfolist[8]
    return dt
# }}}

def WriteDumpedTextResultFile(name_server, outfile, outpath_result, maplist, runtime_in_sec, base_www_url, statfile=""):#{{{
    """Write the prediction result to a single text file. This function does not work for proq3
    """
    if name_server == "topcons2":
        WriteTOPCONSTextResultFile(outfile, outpath_result, maplist, runtime_in_sec, base_www_url, statfile)
    elif name_server == "subcons":
        WriteSubconsTextResultFile(outfile, outpath_result, maplist, runtime_in_sec, base_www_url, statfile)
    elif name_server == "boctopus2":
        WriteBoctopusTextResultFile(outfile, outpath_result, maplist, runtime_in_sec, base_www_url, statfile)
    elif name_server == "scampi2":
        WriteSCAMPI2MSATextResultFile(outfile, outpath_result, maplist, runtime_in_sec, base_www_url, statfile)
    elif name_server == "pconsc3":
        WritePconsC3TextResultFile(outfile, outpath_result, maplist, runtime_in_sec, base_www_url, statfile)
    elif name_server == "predzinc":
        WritePredZincTextResultFile(outfile, outpath_result, maplist, runtime_in_sec, base_www_url, statfile)
    elif name_server == "frag1d":
        WriteFrag1DTextResultFile(outfile, outpath_result, maplist, runtime_in_sec, base_www_url, statfile)

#}}}
def WritePconsC3TextResultFile(outfile, outpath_result, maplist, runtime_in_sec, base_www_url, statfile=""):#{{{
    try:
        fpout = open(outfile, "w")

        fpstat = None

        if statfile != "":
            fpstat = open(statfile, "w")

        date_str = time.strftime(FORMAT_DATETIME)
        print("##############################################################################", file=fpout)
        print("PconsC3 result file", file=fpout)
        print("Generated from %s at %s"%(base_www_url, date_str), file=fpout)
        print("Total request time: %.1f seconds."%(runtime_in_sec), file=fpout)
        print("##############################################################################", file=fpout)

        cnt = 0
        for line in maplist:
            strs = line.split('\t')
            subfoldername = strs[0]
            length = int(strs[1])
            desp = strs[2]
            seq = strs[3]
            outpath_this_seq = "%s/%s"%(outpath_result, subfoldername)
            predfile = "%s/query.fa.hhE0.pconsc3.out"%(outpath_this_seq)
            print("Sequence number: %d"%(cnt+1), file=fpout)
            print("Sequence name: %s"%(desp), file=fpout)
            print("Sequence length: %d aa."%(length), file=fpout)
            print("Sequence:\n%s\n\n"%(seq), file=fpout)
            print("Predicted contacts:", file=fpout)
            print("%-4s %4s %5s"%("Res1", "Res2", "Score"), file=fpout)

            if os.path.exists(predfile):
                content = myfunc.ReadFile(predfile)
                fpout.write("%s\n"%(content))
            else:
                print("***Contact prediction failed***", file=fpout)
            print("##############################################################################", file=fpout)
            cnt += 1

        fpout.close()
        if fpstat:
            fpstat.close()
    except IOError:
        print("Failed to write to file %s"%(outfile))
#}}}
def WriteProQ3TextResultFile(outfile, query_para, modelFileList, #{{{
        runtime_in_sec, base_www_url, proq3opt, statfile=""):
    try:
        fpout = open(outfile, "w")


        try:
            isDeepLearning = query_para['isDeepLearning']
        except KeyError:
            isDeepLearning = True

        if isDeepLearning:
            m_str = "proq3d"
        else:
            m_str = "proq3"

        try:
            method_quality = query_para['method_quality']
        except KeyError:
            method_quality = 'sscore'

        fpstat = None
        numTMPro = 0

        if statfile != "":
            fpstat = open(statfile, "w")
        numModel = len(modelFileList)

        date_str = time.strftime(FORMAT_DATETIME)
        print("##############################################################################", file=fpout)
        print("# ProQ3 result file", file=fpout)
        print("# Generated from %s at %s"%(base_www_url, date_str), file=fpout)
        print("# Options for Proq3: %s"%(str(proq3opt)), file=fpout)
        print("# Total request time: %.1f seconds."%(runtime_in_sec), file=fpout)
        print("# Number of finished models: %d"%(numModel), file=fpout)
        print("##############################################################################", file=fpout)
        print(file=fpout)
        print("# Global scores", file=fpout)
        fpout.write("# %10s"%("Model"))

        cnt = 0
        for i  in range(numModel):
            modelfile = modelFileList[i]
            globalscorefile = "%s.%s.%s.global"%(modelfile, m_str, method_quality)
            if not os.path.exists(globalscorefile):
                globalscorefile = "%s.proq3.%s.global"%(modelfile, method_quality)
                if not os.path.exists(globalscorefile):
                    globalscorefile = "%s.proq3.global"%(modelfile)
            (globalscore, itemList) = ReadProQ3GlobalScore(globalscorefile)
            if i == 0:
                for ss in itemList:
                    fpout.write(" %12s"%(ss))
                fpout.write("\n")

            try:
                if globalscore:
                    fpout.write("%2s %10s"%("", "model_%d"%(i)))
                    for jj in range(len(itemList)):
                        fpout.write(" %12f"%(globalscore[itemList[jj]]))
                    fpout.write("\n")
                else:
                    print("%2s %10s"%("", "model_%d"%(i)), file=fpout)
            except:
                pass

        print("\n# Local scores", file=fpout)
        for i  in range(numModel):
            modelfile = modelFileList[i]
            localscorefile = "%s.%s.%s.local"%(modelfile, m_str, method_quality)
            if not os.path.exists(localscorefile):
                localscorefile = "%s.proq3.%s.local"%(modelfile, method_quality)
                if not os.path.exists(localscorefile):
                    localscorefile = "%s.proq3.local"%(modelfile)
            print("\n# Model %d"%(i), file=fpout)
            content = myfunc.ReadFile(localscorefile)
            print(content, file=fpout)

    except IOError:
        print("Failed to write to file %s"%(outfile))
#}}}
def WriteBoctopusTextResultFile(outfile, outpath_result, maplist, runtime_in_sec, base_www_url, statfile=""):#{{{
    rstdir = os.path.realpath("%s/.."%(outpath_result))
    runjob_logfile = "%s/%s"%(rstdir, "runjob.log")
    runjob_errfile = "%s/%s"%(rstdir, "runjob.err")
    finishtagfile = "%s/%s"%(rstdir, "write_result_finish.tag")
    try:
        fpout = open(outfile, "w")
        fpstat = None
        numTMPro = 0

        if statfile != "":
            fpstat = open(statfile, "w")

        cnt = 0
        for line in maplist:
            strs = line.split('\t')
            subfoldername = strs[0]
            length = int(strs[1])
            desp = strs[2]
            seq = strs[3]
            isTMPro = False
            outpath_this_seq = "%s/%s"%(outpath_result, subfoldername)
            predfile = "%s/query_topologies.txt"%(outpath_this_seq)
            loginfo("predfile =  %s.\n"%(predfile), runjob_logfile)
            if not os.path.exists(predfile):
                loginfo("predfile %s does not exist\n"%(predfile), runjob_errfile)
            (seqid, seqanno, top) = myfunc.ReadSingleFasta(predfile)
            fpout.write(">%s\n%s\n"%(desp, top))
            numTM = myfunc.CountTM(top)
            if numTM >0:
                isTMPro = True
                numTMPro += 1

            cnt += 1

        if fpstat:
            out_str_list = ["numTMPro\t%d\n"%(numTMPro)]
            fpstat.write("%s"%("\n".join(out_str_list)))
            fpstat.close()
        WriteDateTimeTagFile(finishtagfile, runjob_logfile, runjob_errfile)
    except IOError:
        loginfo( "Failed to write to file %s"%(outfile), runjob_errfile)
#}}}
def WriteSCAMPI2MSATextResultFile(outfile, outpath_result, maplist, #{{{
        runtime_in_sec, base_www_url, statfile=""):
    finished_seq_file = "%s/finished_seqs.txt"%(outpath_result)
    TM_listfile = "%s/query.TM_list.txt"%(outpath_result)
    nonTM_listfile = "%s/query.nonTM_list.txt"%(outpath_result)
    finish_info_lines = myfunc.ReadFile(finished_seq_file).split('\n')
    str_TMlist = []
    str_nonTMlist = []
    try:
        fpout = open(outfile, "w")

        fpstat = None
        numTMPro = 0

        if statfile != "":
            fpstat = open(statfile, "w")

        cnt = 0
        for line in finish_info_lines:
            strs = line.split('\t')
            if len(strs) >= 8:
                numTM = int(strs[2])
                isTMPro = False
                desp = strs[5]
                top = strs[7]
                fpout.write(">%s\n%s\n"%(desp, top))
                numTM = myfunc.CountTM(top)
                if numTM >0:
                    str_TMlist.append(desp)
                    isTMPro = True
                    numTMPro += 1
                else:
                    str_nonTMlist.append(desp)

                cnt += 1

        if fpstat:
            out_str_list = ["numTMPro\t%d\n"%(numTMPro)]
            fpstat.write("%s"%("\n".join(out_str_list)))
            fpstat.close()

        myfunc.WriteFile("\n".join(str_TMlist), TM_listfile, "w")
        myfunc.WriteFile("\n".join(str_nonTMlist), nonTM_listfile, "w")

    except IOError:
        print("Failed to write to file %s"%(outfile))
#}}}
def WriteNiceResultPredZinc(predfile, fpout, threshold=0.45):#{{{
    is_ZB = False
    is_has_homo = False
    li_homolog = []
    li_ZB = []
    li_other = []
    hdl = myfunc.ReadLineByBlock(predfile)
    if not hdl.failure:
        lines = hdl.readlines()
        while lines != None:
            for line in lines:
                if not line:
                    continue
                if line[0] == "#":
                    strs = line.split()
                    if len(strs) >=6 and strs[0] == "#Homolog":
                        id_homo = strs[4]
                        score_homo = strs[5]
                        li_homolog.append([id_homo, score_homo])
                else:
                    strs = line.split("\t")
                    if len(strs)>=5:
                        key = strs[0]
                        ss2 = key.split(";")
                        if len(ss2)>=5:
                            res = ss2[3]
                            series = ss2[4]
                            try:
                                score = float(strs[2])
                            except:
                                score = -100.0
                            if score >= threshold:
                                li_ZB.append([res, series, score])
                            else:
                                li_other.append([res, series, score])
            lines = hdl.readlines()
        hdl.close()
        if len(li_ZB) > 0:
            is_ZB = True
        if len(li_homolog) > 0:
            is_has_homo = True

        try:
            if len(li_ZB) > 0:
                fpout.write("The following %d residues were predicted as zinc-binding (with score >= %g, sorted by scores\n\n"%(len(li_ZB), threshold))
                fpout.write("%-3s %8s %6s\n"%("Res","SerialNo","Score"))
                li_ZB = sorted(li_ZB, key=lambda x:x[2], reverse=True)
                for item in li_ZB:
                    fpout.write("%-3s %8s %6.3f\n"%(chde_table[item[0]],
                        item[1], item[2]))
            else:
                fpout.write("No residues were predicted as zinc-binding\n\n")

            if len(li_other) >0:
                fpout.write("\n\nPrediction scores for the rest %d CHDEs, sorted by scores\n\n"%(
                    len(li_other)))
                fpout.write("%-3s %8s %6s\n"%("Res","SerialNo","Score"))
                li_other = sorted(li_other, key=lambda x:x[2], reverse=True)
                for item in li_other:
                    fpout.write("%-3s %8s %6.3f\n"%(chde_table[item[0]],
                        item[1], item[2]))

            fpout.write("//\n\n") # write finishing tag
        except:
            pass

    else:
        pass

    return (is_ZB, is_has_homo)
#}}}
def WritePredZincTextResultFile(outfile, outpath_result, maplist, runtime_in_sec, base_www_url, statfile=""):#{{{
    try:
        fpout = open(outfile, "w")

        fpstat = None
        num_ZB = 0
        num_has_homo = 0

        if statfile != "":
            fpstat = open(statfile, "w")

        date_str = time.strftime(FORMAT_DATETIME)
        print("##############################################################################", file=fpout)
        print("PredZinc result file", file=fpout)
        print("Generated from %s at %s"%(base_www_url, date_str), file=fpout)
        print("Total request time: %.1f seconds."%(runtime_in_sec), file=fpout)
        print("##############################################################################", file=fpout)
        cnt = 0
        for line in maplist:
            strs = line.split('\t')
            subfoldername = strs[0]
            length = int(strs[1])
            desp = strs[2]
            seq = strs[3]
            print("Sequence number: %d"%(cnt+1), file=fpout)
            print("Sequence name: %s"%(desp), file=fpout)
            print("Sequence length: %d aa."%(length), file=fpout)
            print("Sequence:\n%s\n\n"%(seq), file=fpout)

            is_ZB = False
            is_has_homo = False
            outpath_this_seq = "%s/%s"%(outpath_result, subfoldername)
            predfile = "%s/query.predzinc.predict"%(outpath_this_seq)
            (is_ZB, is_has_homo ) = WriteNiceResultPredZinc(predfile, fpout,
                    threshold=ZB_SCORE_THRESHOLD) 

            if fpstat:
                num_ZB += is_ZB
                num_has_homo += is_has_homo

            cnt += 1

        if fpstat:
            out_str_list = []
            out_str_list.append("num_ZB %d"% num_ZB)
            out_str_list.append("num_has_homo %d"% num_has_homo)
            fpstat.write("%s"%("\n".join(out_str_list)))
            fpstat.close()
    except IOError:
        print("Failed to write to file %s"%(outfile))
#}}}
def WriteNiceResultFrag1D(predfile, fpout):#{{{
    hdl = myfunc.ReadLineByBlock(predfile)
    if not hdl.failure:
        lines = hdl.readlines()
        while lines != None:
            for line in lines:
                if not line or line[0] == "/":
                    continue
                if line[0] == "#":
                    if line.find("# Num AA Sec") == 0:
                        print(line, file=fpout)
                else:
                    print(line, file=fpout)
            lines = hdl.readlines()
        hdl.close()
        fpout.write("//\n\n") # write finishing tag
    else:
        pass
#}}}
def WriteFrag1DTextResultFile(outfile, outpath_result, maplist, runtime_in_sec, base_www_url, statfile=""):#{{{
    try:
        fpout = open(outfile, "w")
        fpstat = None
        if statfile != "":
            fpstat = open(statfile, "w")
        date_str = time.strftime(FORMAT_DATETIME)
        print("##############################################################################", file=fpout)
        print("Frag1D result file", file=fpout)
        print("Generated from %s at %s"%(base_www_url, date_str), file=fpout)
        print("Total request time: %.1f seconds."%(runtime_in_sec), file=fpout)
        print("##############################################################################", file=fpout)
        cnt = 0
        for line in maplist:
            strs = line.split('\t')
            subfoldername = strs[0]
            length = int(strs[1])
            desp = strs[2]
            seq = strs[3]
            print("Sequence number: %d"%(cnt+1), file=fpout)
            print("Sequence name: %s"%(desp), file=fpout)
            print("Sequence length: %d aa."%(length), file=fpout)
            print("Sequence:\n%s\n\n"%(seq), file=fpout)

            outpath_this_seq = "%s/%s"%(outpath_result, subfoldername)
            predfile = "%s/query.predfrag1d"%(outpath_this_seq)
            WriteNiceResultFrag1D(predfile, fpout)

            cnt += 1

        if fpstat:
            out_str_list = []
            fpstat.write("%s"%("\n".join(out_str_list)))
            fpstat.close()
    except IOError:
        print("Failed to write to file %s"%(outfile))
#}}}

@timeit
def WriteSubconsTextResultFile(outfile, outpath_result, maplist,#{{{
        runtime_in_sec, base_www_url, statfile=""):
    try:
        fpout = open(outfile, "w")
        if statfile != "":
            fpstat = open(statfile, "w")

        date_str = time.strftime(FORMAT_DATETIME)
        print("##############################################################################", file=fpout)
        print("Subcons result file", file=fpout)
        print("Generated from %s at %s"%(base_www_url, date_str), file=fpout)
        print("Total request time: %.1f seconds."%(runtime_in_sec), file=fpout)
        print("##############################################################################", file=fpout)
        cnt = 0
        for line in maplist:
            strs = line.split('\t')
            subfoldername = strs[0]
            length = int(strs[1])
            desp = strs[2]
            seq = strs[3]
            seqid = myfunc.GetSeqIDFromAnnotation(desp)
            print("Sequence number: %d"%(cnt+1), file=fpout)
            print("Sequence name: %s"%(desp), file=fpout)
            print("Sequence length: %d aa."%(length), file=fpout)
            print("Sequence:\n%s\n\n"%(seq), file=fpout)

            rstfile = "%s/%s/%s/query_0.csv"%(outpath_result, subfoldername, "plot")

            if os.path.exists(rstfile):
                content = myfunc.ReadFile(rstfile).strip()
                lines = content.split("\n")
                if len(lines) >= 6:
                    header_line = lines[0].split("\t")
                    if header_line[0].strip() == "":
                        header_line[0] = "Method"
                        header_line = [x.strip() for x in header_line]

                    data_line = []
                    for i in range(1, len(lines)):
                        strs1 = lines[i].split("\t")
                        strs1 = [x.strip() for x in strs1]
                        data_line.append(strs1)

                    content = tabulate.tabulate(data_line, header_line, 'plain')
            else:
                content = ""
            if content == "":
                content = "***No prediction could be produced with this method***"

            print("Prediction results:\n\n%s\n\n"%(content), file=fpout)

            print("##############################################################################", file=fpout)
            cnt += 1

    except IOError:
        print("Failed to write to file %s"%(outfile))
#}}}

@timeit
def WriteTOPCONSTextResultFile(outfile, outpath_result, maplist,#{{{
        runtime_in_sec, base_www_url, statfile=""):
    try:
        methodlist = ['TOPCONS', 'OCTOPUS', 'Philius', 'PolyPhobius', 'SCAMPI',
                'SPOCTOPUS', 'Homology']
        fpout = open(outfile, "w")

        fpstat = None
        num_TMPro_cons = 0
        num_TMPro_any = 0
        num_nonTMPro_cons = 0
        num_nonTMPro_any = 0
        num_SPPro_cons = 0
        num_SPPro_any = 0

        if statfile != "":
            fpstat = open(statfile, "w")

        date_str = time.strftime(FORMAT_DATETIME)
        print("##############################################################################", file=fpout)
        print("TOPCONS2 result file", file=fpout)
        print("Generated from %s at %s"%(base_www_url, date_str), file=fpout)
        print("Total request time: %.1f seconds."%(runtime_in_sec), file=fpout)
        print("##############################################################################", file=fpout)
        cnt = 0
        for line in maplist:
            strs = line.split('\t')
            subfoldername = strs[0]
            length = int(strs[1])
            desp = strs[2]
            seq = strs[3]
            print("Sequence number: %d"%(cnt+1), file=fpout)
            print("Sequence name: %s"%(desp), file=fpout)
            print("Sequence length: %d aa."%(length), file=fpout)
            print("Sequence:\n%s\n\n"%(seq), file=fpout)

            is_TM_cons = False
            is_TM_any = False
            is_nonTM_cons = True
            is_nonTM_any = True
            is_SP_cons = False
            is_SP_any = False

            for i in range(len(methodlist)):
                method = methodlist[i]
                seqid = ""
                seqanno = ""
                top = ""
                if method == "TOPCONS":
                    topfile = "%s/%s/%s/topcons.top"%(outpath_result, subfoldername, "Topcons")
                elif method == "Philius":
                    topfile = "%s/%s/%s/query.top"%(outpath_result, subfoldername, "philius")
                elif method == "SCAMPI":
                    topfile = "%s/%s/%s/query.top"%(outpath_result, subfoldername, method+"_MSA")
                else:
                    topfile = "%s/%s/%s/query.top"%(outpath_result, subfoldername, method)
                if os.path.exists(topfile):
                    (seqid, seqanno, top) = myfunc.ReadSingleFasta(topfile)
                else:
                    top = ""
                if top == "":
                    #top = "***No topology could be produced with this method topfile=%s***"%(topfile)
                    top = "***No topology could be produced with this method***"

                if fpstat != None:
                    if top.find('M') >= 0:
                        is_TM_any = True
                        is_nonTM_any = False
                        if method == "TOPCONS":
                            is_TM_cons = True
                            is_nonTM_cons = False
                    if top.find('S') >= 0:
                        is_SP_any = True
                        if method == "TOPCONS":
                            is_SP_cons = True

                if method == "Homology":
                    showtext_homo = method
                    if seqid != "":
                        showtext_homo = seqid
                    print("%s:\n%s\n\n"%(showtext_homo, top), file=fpout)
                else:
                    print("%s predicted topology:\n%s\n\n"%(method, top), file=fpout)


            if fpstat:
                num_TMPro_cons += is_TM_cons
                num_TMPro_any += is_TM_any
                num_nonTMPro_cons += is_nonTM_cons
                num_nonTMPro_any += is_nonTM_any
                num_SPPro_cons += is_SP_cons
                num_SPPro_any += is_SP_any

            dgfile = "%s/%s/dg.txt"%(outpath_result, subfoldername)
            dg_content = ""
            if os.path.exists(dgfile):
                dg_content = myfunc.ReadFile(dgfile)
            lines = dg_content.split("\n")
            dglines = []
            for line in lines:
                if line and line[0].isdigit():
                    dglines.append(line)
            if len(dglines)>0:
                print("\nPredicted Delta-G-values (kcal/mol) "\
                        "(left column=sequence position; right column=Delta-G)\n", file=fpout)
                print("\n".join(dglines), file=fpout)

            reliability_file = "%s/%s/Topcons/reliability.txt"%(outpath_result, subfoldername)
            reliability = ""
            if os.path.exists(reliability_file):
                reliability = myfunc.ReadFile(reliability_file)
            if reliability != "":
                print("\nPredicted TOPCONS reliability (left "\
                        "column=sequence position; right column=reliability)\n", file=fpout)
                print(reliability, file=fpout)
            print("##############################################################################", file=fpout)
            cnt += 1

        fpout.close()

        if fpstat:
            out_str_list = []
            out_str_list.append("num_TMPro_cons %d"% num_TMPro_cons)
            out_str_list.append("num_TMPro_any %d"% num_TMPro_any)
            out_str_list.append("num_nonTMPro_cons %d"% num_nonTMPro_cons)
            out_str_list.append("num_nonTMPro_any %d"% num_nonTMPro_any)
            out_str_list.append("num_SPPro_cons %d"% num_SPPro_cons)
            out_str_list.append("num_SPPro_any %d"% num_SPPro_any)
            fpstat.write("%s"%("\n".join(out_str_list)))

            fpstat.close()

        rstdir = os.path.realpath("%s/.."%(outpath_result))
        runjob_logfile = "%s/%s"%(rstdir, "runjob.log")
        runjob_errfile = "%s/%s"%(rstdir, "runjob.err")
        finishtagfile = "%s/%s"%(rstdir, "write_result_finish.tag")
        WriteDateTimeTagFile(finishtagfile, runjob_logfile, runjob_errfile)
    except IOError:
        print("Failed to write to file %s"%(outfile))
#}}}

def WriteHTMLHeader(title, fpout):#{{{
    exturl = "https://topcons.net/static"
    print("<HTML>", file=fpout)
    print("<head>", file=fpout)
    print("<title>%s</title>"%(title), file=fpout)
    print("<link rel=\"stylesheet\" href=\"%s/css/jquery.dataTables.css\" type=\"text/css\" />"%(exturl), file=fpout)
    print("<link rel=\"stylesheet\" href=\"%s/css/template_css.css\" type=\"text/css\" />"%(exturl), file=fpout)
    print("<script src=\"%s/js/sorttable.js\"></script>"%(exturl), file=fpout)
    print("<script src=\"%s/js/jquery.js\"></script>"%(exturl), file=fpout) 
    print("<script src=\"%s/js/jquery.dataTables.min.js\"></script>"%(exturl), file=fpout) 
    print("<script>", file=fpout)
    print("$(function(){", file=fpout)
    print("  $(\"#jobtable\").dataTable();", file=fpout)
    print("  })", file=fpout)
    print("</script>", file=fpout)
    print("</head>", file=fpout)
    print("<BODY>", file=fpout)
#}}}
def WriteHTMLTail(fpout):#{{{
    print("</BODY>", file=fpout)
    print("</HTML>", file=fpout)
#}}}
def WriteHTMLTableContent_TOPCONS(tablename, tabletitle, index_table_header,#{{{
        index_table_content_list, fpout):
    """Write the content of the html table for TOPCONS
    """
    print("<a name=\"%s\"></a><h4>%s</h4>"%(tablename,tabletitle), file=fpout)
    print("<table class=\"sortable\" id=\"jobtable\" border=1>", file=fpout)
    print("<thead>", file=fpout)
    print("<tr>", file=fpout)
    for item in index_table_header:
        print("<th>", file=fpout)
        print(item, file=fpout)
        print("</th>", file=fpout)
    print("</tr>", file=fpout)
    print("</thead>", file=fpout)

    print("<tbody>", file=fpout)

    for record in index_table_content_list:
        print("<tr>", file=fpout)
        for i in range(6):
            print("<td>%s</td>"%(record[i]), file=fpout)
        print("<td>", file=fpout)
        print("<a href=\"%s/Topcons/total_image.png\">Fig_all</a>"%(record[6]), file=fpout)
        print("<a href=\"%s/Topcons/topcons.png\">Fig_topcons</a><br>"%(record[6]), file=fpout)
        print("<a href=\"%s/query.result.txt\">Dumped prediction</a><br>"%(record[6]), file=fpout)
        print("<a href=\"%s/dg.txt\">deltaG</a><br>"%(record[6]), file=fpout)
        print("<a href=\"%s/nicetop.html\">Topology view</a><br>"%(record[6]), file=fpout)
        print("</td>", file=fpout)
        print("<td>%s</td>"%(record[7]), file=fpout)
        print("</tr>", file=fpout)

    print("</tbody>", file=fpout)
    print("</table>", file=fpout)
#}}}

@timeit
def WriteHTMLResultTable_TOPCONS(outfile, finished_seq_file):#{{{
    """Write html table for the results
    """
    try:
        fpout = open(outfile, "w")
    except OSError:
        print("Failed to write to file %s at%s"%(outfile,
                sys._getframe().f_code.co_name ), file=sys.stderr)
        return 1

    title="TOPCONS2 predictions"
    WriteHTMLHeader(title, fpout)
    print("<dir id=\"Content\">", file=fpout)
    tablename = 'table1'
    tabletitle = ""
    index_table_header = ["No.", "Length", "numTM",
            "SignalPeptide", "RunTime(s)", "SequenceName", "Prediction", "Source" ]
    index_table_content_list = []
    indexmap_content = myfunc.ReadFile(finished_seq_file).split("\n")
    cnt = 0
    for line in indexmap_content:
        strs = line.split("\t")
        if len(strs)>=7:
            subfolder = strs[0]
            length_str = strs[1]
            numTM_str = strs[2]
            isHasSP = "No"
            if strs[3] == "True":
                isHasSP = "Yes"
            source = strs[4]
            try:
                runtime_in_sec_str = "%.1f"%(float(strs[5]))
            except:
                runtime_in_sec_str = ""
            desp = strs[6]
            rank = "%d"%(cnt+1)
            index_table_content_list.append([rank, length_str, numTM_str,
                isHasSP, runtime_in_sec_str, desp, subfolder, source])
            cnt += 1
    WriteHTMLTableContent_TOPCONS(tablename, tabletitle, index_table_header,
            index_table_content_list, fpout)
    print("</dir>", file=fpout)

    WriteHTMLTail(fpout)
    fpout.close()

    rstdir = os.path.abspath(os.path.dirname(os.path.abspath(outfile))+'/../')
    runjob_logfile = "%s/%s"%(rstdir, "runjob.log")
    runjob_errfile = "%s/%s"%(rstdir, "runjob.err")
    finishtagfile = "%s/%s"%(rstdir, "write_htmlresult_finish.tag")
    WriteDateTimeTagFile(finishtagfile, runjob_logfile, runjob_errfile)
    return 0
#}}}

def ReplaceDescriptionSingleFastaFile(infile, new_desp):#{{{
    """Replace the description line of the fasta file by the new_desp
    """
    if os.path.exists(infile):
        (seqid, seqanno, seq) = myfunc.ReadSingleFasta(infile)
        if seqanno != new_desp:
            myfunc.WriteFile(">%s\n%s\n"%(new_desp, seq), infile)
        return 0
    else:
        sys.stderr.write("infile %s does not exists at %s\n"%(infile, sys._getframe().f_code.co_name))
        return 1
#}}}
def GetLocDef(predfile):#{{{
    """
    Read in LocDef and its corresponding score from the subcons prediction file
    """
    content = ""
    if os.path.exists(predfile):
        content = myfunc.ReadFile(predfile)

    loc_def = None
    loc_def_score = None
    if content != "":
        lines = content.split("\n")
        if len(lines)>=2:
            strs0 = lines[0].split("\t")
            strs1 = lines[1].split("\t")
            strs0 = [x.strip() for x in strs0]
            strs1 = [x.strip() for x in strs1]
            if len(strs0) == len(strs1) and len(strs0) > 2:
                if strs0[1] == "LOC_DEF":
                    loc_def = strs1[1]
                    dt_score = {}
                    for i in range(2, len(strs0)):
                        dt_score[strs0[i]] = strs1[i]
                    if loc_def in dt_score:
                        loc_def_score = dt_score[loc_def]

    return (loc_def, loc_def_score)
#}}}
def GetStatFrag1DPred(predfile):#{{{
    """analyze the frag1d prediction and return prediction parameters
    """
    para_pred = {}
    li_s3_seq = []
    li_sec_seq = []
    hdl = myfunc.ReadLineByBlock(predfile)
    if not hdl.failure:
        lines = hdl.readlines()
        while lines != None:
            for line in lines:
                if not line or line[0] == "#" or line[0]=="/":
                    continue
                strs = line.split()
                if len(strs)==8:
                    li_sec_seq.append(strs[2])
                    li_s3_seq.append(strs[6])
            lines = hdl.readlines()
        hdl.close()
    lenseq = len(li_sec_seq)
    cnt_sec_H = li_sec_seq.count("H")
    cnt_sec_S = li_sec_seq.count("S")
    cnt_sec_R = li_sec_seq.count("R")
    cnt_s3_H = li_s3_seq.count("H")
    cnt_s3_S = li_s3_seq.count("S")
    cnt_s3_T = li_s3_seq.count("T")

    per_sec_H = myfunc.FloatDivision(cnt_sec_H, lenseq)*100
    per_sec_S =  myfunc.FloatDivision(cnt_sec_S, lenseq)*100 
    per_sec_R =   myfunc.FloatDivision(cnt_sec_R, lenseq)*100 
    per_s3_H =   myfunc.FloatDivision(cnt_s3_H, lenseq)*100 
    per_s3_S =   myfunc.FloatDivision(cnt_s3_S, lenseq)*100  
    per_s3_T =  myfunc.FloatDivision(cnt_s3_T, lenseq)*100  
    para_pred['per_sec_H'] = per_sec_H
    para_pred['per_sec_R'] = per_sec_R
    para_pred['per_sec_S'] = per_sec_S
    para_pred['per_s3_H'] = per_s3_H
    para_pred['per_s3_S'] = per_s3_S
    para_pred['per_s3_T'] = per_s3_T
    return para_pred
#}}}
def datetime_str_to_epoch(date_str):# {{{
    """convert the date_time in string to epoch
    The string of date_time may with or without the zone info
    return the epoch time of the current time when conversion failed
    """
    try:
        return dtparser.parse(date_str).strftime("%s")
    except:
        return time.strftime('%s')
# }}}
def datetime_str_to_time(date_str, isSetDefault=True):# {{{
    """convert the date_time in string to datetime type
    The string of date_time may with or without the zone info
    return the the current time when conversion failed if isSetDefault is True
    otherwise return None when conversion failed
    """
    try:
        strs = date_str.split()
        if len(strs) == 2:
            date_str += " UTC"
        if len(strs) == 3 and strs[2] == "U":
            date_str = date_str.replace("U", "UTC")
        dt = dtparser.parse(date_str)
        return dt
    except:
        if isSetDefault:
            return datetime.now(timezone(TZ))
        else:
            return None
# }}}

def IsFrontEndNode(base_www_url):#{{{
    """
    check if the base_www_url is front-end node
    if base_www_url is ip address, then not the front-end
    otherwise yes
    """
    base_www_url = base_www_url.lstrip("http://").lstrip("https://").split("/")[0]
    if base_www_url == "":
        return False
    elif base_www_url.find("computenode") != -1:
        return False
    else:
        arr =  [x.isdigit() for x in base_www_url.split('.')]
        if all(arr):
            return False
        else:
            return True
#}}}
def GetAverageNewRunTime(finished_seq_file, window=100):#{{{
    """Get average running time of the newrun tasks for the last x number of
sequences
    """
    logger = logging.getLogger(__name__)
    avg_newrun_time = -1.0
    if not os.path.exists(finished_seq_file):
        return avg_newrun_time
    else:
        indexmap_content = myfunc.ReadFile(finished_seq_file).split("\n")
        indexmap_content = indexmap_content[::-1]
        cnt = 0
        sum_run_time = 0.0
        for line in indexmap_content:
            strs = line.split("\t")
            if len(strs)>=7:
                source = strs[4]
                if source == "newrun":
                    try:
                        sum_run_time += float(strs[5])
                        cnt += 1
                    except:
                        logger.debug("bad format in finished_seq_file (%s) with line \"%s\""%(finished_seq_file, line))
                        pass

                if cnt >= window:
                    break

        if cnt > 0:
            avg_newrun_time = sum_run_time/float(cnt)
        return avg_newrun_time


#}}}
def GetRunTimeFromTimeFile(timefile, keyword=""):# {{{
    runtime = 0.0
    if os.path.exists(timefile):
        lines = myfunc.ReadFile(timefile).split("\n")
        for line in lines:
            if keyword == "" or (keyword != "" and line.find(keyword) != -1):
                ss2 = line.split(";")
                try:
                    runtime = float(ss2[1])
                    if keyword == "":
                        break
                except:
                    runtime = 0.0
                    pass
    return runtime
# }}}

def IsCheckPredictionPassed(outpath_this_seq, name_server):# {{{
    """Check if the prediction is complete
    """
    name_server = name_server.lower()
    if name_server in ["subcons", "boctopus2", "pconsc3", "pathopred"]:
        if name_server == "subcons":
            checkfile = "%s/plot/query_0.png"%(outpath_this_seq)
        elif name_server == "boctopus2":
            checkfile = "%s/query.predict.png"%(outpath_this_seq)
        elif name_server == "pconsc3":
            checkfile = "%s/query.fa.hhE0.pconsc3.out"%(outpath_this_seq)
        elif name_server == "pathopred":
            checkfile = "%s/output_predictions"%(outpath_this_seq)
        elif name_server == "predzinc":
            checkfile = "%s/query.predzinc.report"%(outpath_this_seq)
        elif name_server == "frag1d":
            checkfile = "%s/query.predfrag1d"%(outpath_this_seq)
        if not os.path.exists(checkfile):
            return False
    return True
# }}}
def ValidateParameter_PRODRES(query_para):#{{{
    """Validate the input parameters for PRODRES
    query_para is a dictionary
    """
    is_valid = True
    if not 'errinfo' in query_para:
        query_para['errinfo'] = ""
    if query_para['pfamscan_evalue'] != "" and query_para['pfamscan_bitscore'] != "":
        query_para['errinfo'] += "Parameter setting error!"
        query_para['errinfo'] += "Both PfamScan E-value and PfamScan Bit-score "\
                "are set! One and only one of them should be set!"
        is_valid = False

    if query_para['jackhmmer_bitscore'] != "" and query_para['jackhmmer_evalue'] != "":
        query_para['errinfo'] += "Parameter setting error!"
        query_para['errinfo'] += "Both Jackhmmer E-value and Jackhmmer Bit-score "\
                "are set! One and only one of them should be set!"
        is_valid = False
    query_para['isValidSeq'] = is_valid
    return is_valid
#}}}
def ValidateQuery(request, query, g_params):#{{{
    query['errinfo_br'] = ""
    query['errinfo_content'] = ""
    query['warninfo'] = ""

    has_pasted_seq = False
    has_upload_file = False
    if query['rawseq'].strip() != "":
        has_pasted_seq = True
    if query['seqfile'] != "":
        has_upload_file = True

    if has_pasted_seq and has_upload_file:
        query['errinfo_br'] += "Confused input!"
        query['errinfo_content'] = "You should input your query by either "\
                "paste the sequence in the text area or upload a file."
        return False
    elif not has_pasted_seq and not has_upload_file:
        query['errinfo_br'] += "No input!"
        query['errinfo_content'] = "You should input your query by either "\
                "paste the sequence in the text area or upload a file "
        return False
    elif query['seqfile'] != "":
        try:
            fp = request.FILES['seqfile']
            fp.seek(0,2)
            filesize = fp.tell()
            if filesize > g_params['MAXSIZE_UPLOAD_FILE_IN_BYTE']:
                query['errinfo_br'] += "Size of uploaded file exceeds limit!"
                query['errinfo_content'] += "The file you uploaded exceeds "\
                        "the upper limit %g Mb. Please split your file and "\
                        "upload again."%(g_params['MAXSIZE_UPLOAD_FILE_IN_MB'])
                return False

            fp.seek(0,0)
            content = fp.read()
        except KeyError:
            query['errinfo_br'] += ""
            query['errinfo_content'] += """
            Failed to read uploaded file \"%s\"
            """%(query['seqfile'])
            return False
        query['rawseq'] = content.decode('utf-8')

    query['filtered_seq'] = ValidateSeq(query['rawseq'], query, g_params)
    if 'variants' in query:
        query['filtered_variants'] = ValidateVariants(query['variants'], query, g_params)
    is_valid = query['isValidSeq']
    return is_valid
#}}}
def ValidateSeq(rawseq, seqinfo, g_params):#{{{
# seq is the chunk of fasta file
# seqinfo is a dictionary
# return (filtered_seq)
    rawseq = re.sub(r'[^\x00-\x7f]',r' ',rawseq) # remove non-ASCII characters
    rawseq = re.sub(r'[\x00-\x09]',r' ',rawseq) # Filter non letter ASCII characters except CR (x13) LF (x10)
    rawseq = re.sub(r'[\x11-\x12]',r' ',rawseq) # 
    rawseq = re.sub(r'[\x13-\x1F]',r' ',rawseq) # 
    filtered_seq = ""
    # initialization
    for item in ['errinfo_br', 'errinfo', 'errinfo_content', 'warninfo']:
        if item not in seqinfo:
            seqinfo[item] = ""

    seqinfo['isValidSeq'] = True

    seqRecordList = []
    myfunc.ReadFastaFromBuffer(rawseq, seqRecordList, True, 0, 0)
# filter empty sequences and any sequeces shorter than MIN_LEN_SEQ or longer
# than MAX_LEN_SEQ
    newSeqRecordList = []
    li_warn_info = []
    isHasEmptySeq = False
    isHasShortSeq = False
    isHasLongSeq = False
    isHasDNASeq = False
    cnt = 0
    for rd in seqRecordList:
        seq = rd[2].strip()
        seqid = rd[0].strip()
        if len(seq) == 0:
            isHasEmptySeq = 1
            msg = "Empty sequence %s (SeqNo. %d) is removed."%(seqid, cnt+1)
            li_warn_info.append(msg)
        elif len(seq) < g_params['MIN_LEN_SEQ']:
            isHasShortSeq = 1
            msg = "Sequence %s (SeqNo. %d) is removed since its length is < %d."%(seqid, cnt+1, g_params['MIN_LEN_SEQ'])
            li_warn_info.append(msg)
        elif len(seq) > g_params['MAX_LEN_SEQ']:
            isHasLongSeq = True
            msg = "Sequence %s (SeqNo. %d) is removed since its length is > %d."%(seqid, cnt+1, g_params['MAX_LEN_SEQ'])
            li_warn_info.append(msg)
        elif myfunc.IsDNASeq(seq):
            isHasDNASeq = True
            msg = "Sequence %s (SeqNo. %d) is removed since it looks like a DNA sequence."%(seqid, cnt+1)
            li_warn_info.append(msg)
        else:
            newSeqRecordList.append(rd)
        cnt += 1
    seqRecordList = newSeqRecordList

    numseq = len(seqRecordList)

    if numseq < 1:
        seqinfo['errinfo_br'] += "Number of input sequences is 0!\n"
        t_rawseq = rawseq.lstrip()
        if t_rawseq and t_rawseq[0] != '>':
            seqinfo['errinfo_content'] += "Bad input format. The FASTA format should have an annotation line start with '>'.\n"
        if len(li_warn_info) >0:
            seqinfo['errinfo_content'] += "\n".join(li_warn_info) + "\n"
        if not isHasShortSeq and not isHasEmptySeq and not isHasLongSeq and not isHasDNASeq:
            seqinfo['errinfo_content'] += "Please input your sequence in FASTA format.\n"

        seqinfo['isValidSeq'] = False
    elif numseq > g_params['MAX_NUMSEQ_PER_JOB']:
        seqinfo['errinfo_br'] += "Number of input sequences exceeds the maximum (%d)!\n"%(
                g_params['MAX_NUMSEQ_PER_JOB'])
        seqinfo['errinfo_content'] += "Your query has %d sequences. "%(numseq)
        seqinfo['errinfo_content'] += "However, the maximal allowed sequences per job is %d. "%(
                g_params['MAX_NUMSEQ_PER_JOB'])
        seqinfo['errinfo_content'] += "Please split your query into smaller files and submit again.\n"
        seqinfo['isValidSeq'] = False
    else:
        li_badseq_info = []
        if 'isForceRun' in seqinfo and seqinfo['isForceRun'] and numseq > g_params['MAX_NUMSEQ_FOR_FORCE_RUN']:
            seqinfo['errinfo_br'] += "Invalid input!"
            seqinfo['errinfo_content'] += "You have chosen the \"Force Run\" mode. "\
                    "The maximum allowable number of sequences of a job is %d. "\
                    "However, your input has %d sequences."%(g_params['MAX_NUMSEQ_FOR_FORCE_RUN'], numseq)
            seqinfo['isValidSeq'] = False


# checking for bad sequences in the query

    if seqinfo['isValidSeq']:
        for i in range(numseq):
            seq = seqRecordList[i][2].strip()
            anno = seqRecordList[i][1].strip().replace('\t', ' ')
            seqid = seqRecordList[i][0].strip()
            seq = seq.upper()
            seq = re.sub("[\s\n\r\t]", '', seq)
            li1 = [m.start() for m in re.finditer("[^ABCDEFGHIKLMNPQRSTUVWYZX*-]", seq)]
            if len(li1) > 0:
                for j in range(len(li1)):
                    msg = "Bad letter for amino acid in sequence %s (SeqNo. %d) "\
                            "at position %d (letter: '%s')"%(seqid, i+1,
                                    li1[j]+1, seq[li1[j]])
                    li_badseq_info.append(msg)

        if len(li_badseq_info) > 0:
            seqinfo['errinfo_br'] += "There are bad letters for amino acids in your query!\n"
            seqinfo['errinfo_content'] = "\n".join(li_badseq_info) + "\n"
            seqinfo['isValidSeq'] = False

# convert some non-classical letters to the standard amino acid symbols
# Scheme:
#    out of these 26 letters in the alphabet, 
#    B, Z -> X
#    U -> C
#    *, - will be deleted
    if seqinfo['isValidSeq']:
        li_newseq = []
        for i in range(numseq):
            seq = seqRecordList[i][2].strip()
            anno = seqRecordList[i][1].strip()
            seqid = seqRecordList[i][0].strip()
            seq = seq.upper()
            seq = re.sub("[\s\n\r\t]", '', seq)
            anno = anno.replace('\t', ' ') #replace tab by whitespace


            li1 = [m.start() for m in re.finditer("[BZ]", seq)]
            if len(li1) > 0:
                for j in range(len(li1)):
                    msg = "Amino acid in sequence %s (SeqNo. %d) at position %d "\
                            "(letter: '%s') has been replaced by 'X'"%(seqid,
                                    i+1, li1[j]+1, seq[li1[j]])
                    li_warn_info.append(msg)
                seq = re.sub("[BZ]", "X", seq)

            li1 = [m.start() for m in re.finditer("[U]", seq)]
            if len(li1) > 0:
                for j in range(len(li1)):
                    msg = "Amino acid in sequence %s (SeqNo. %d) at position %d "\
                            "(letter: '%s') has been replaced by 'C'"%(seqid,
                                    i+1, li1[j]+1, seq[li1[j]])
                    li_warn_info.append(msg)
                seq = re.sub("[U]", "C", seq)

            li1 = [m.start() for m in re.finditer("[*]", seq)]
            if len(li1) > 0:
                for j in range(len(li1)):
                    msg = "Translational stop in sequence %s (SeqNo. %d) at position %d "\
                            "(letter: '%s') has been deleted"%(seqid,
                                    i+1, li1[j]+1, seq[li1[j]])
                    li_warn_info.append(msg)
                seq = re.sub("[*]", "", seq)

            li1 = [m.start() for m in re.finditer("[-]", seq)]
            if len(li1) > 0:
                for j in range(len(li1)):
                    msg = "Gap in sequence %s (SeqNo. %d) at position %d "\
                            "(letter: '%s') has been deleted"%(seqid,
                                    i+1, li1[j]+1, seq[li1[j]])
                    li_warn_info.append(msg)
                seq = re.sub("[-]", "", seq)

            # check the sequence length again after potential removal of
            # translation stop
            if len(seq) < g_params['MIN_LEN_SEQ']:
                isHasShortSeq = 1
                msg = "Sequence %s (SeqNo. %d) is removed since its length is < %d (after removal of translation stop)."%(seqid, i+1, g_params['MIN_LEN_SEQ'])
                li_warn_info.append(msg)
            else:
                li_newseq.append(">%s\n%s"%(anno, seq))

        filtered_seq = "\n".join(li_newseq) # seq content after validation
        seqinfo['numseq'] = len(li_newseq)
        seqinfo['warninfo'] = "\n".join(li_warn_info) + "\n"

    seqinfo['errinfo'] = seqinfo['errinfo_br'] + seqinfo['errinfo_content']
    return filtered_seq
#}}}
def ValidateVariants(rawvariants, seqinfo, g_params):#{{{
    # rawvariants is raw input from variants form
    # seqinfo is a dictionary
    # return (variants if valid)
    rawvariants = re.sub(r'[^\x00-\x7f]',r' ',rawvariants) # remove non-ASCII characters
    rawvariants = re.sub(r'[\x0b]',r' ',rawvariants) # filter invalid characters for XML
    filtered_variants = ""

    # initialization
    for item in ['errinfo_br', 'errinfo', 'errinfo_content', 'warninfo']:
        if item not in seqinfo:
            seqinfo[item] = ""

    seqinfo['isValidVariants'] = True
    valid_aa = "ABCDEFGHIKLMNPQRSTUVWYZX"
    li_err_info = []

    # identifiers, starting with >, can keep any name
    # we want to filter any variants not on the format AA,position,AA
    for var_line in rawvariants.split('\n'):
        stripped_line = re.sub("[\s\n\r\t]", '', var_line)
        if not stripped_line.startswith('>'):
            ref_aa = stripped_line[0].upper()
            alt_aa = stripped_line[-1].upper()
            position = stripped_line[1:-1]
            if not ref_aa in valid_aa:
                msg = "Bad letter for reference amino acid in variant %s (letter: '%s')"%(stripped_line, ref_aa)
                li_err_info.append(msg)
            if not alt_aa in valid_aa:
                msg = "Bad letter for altered amino acid in variant %s (letter: '%s')"%(stripped_line, alt_aa)
                li_err_info.append(msg)
            if not position.isdigit():
                msg = "Bad position value in variant %s (position: '%s')"%(stripped_line, position)
                li_err_info.append(msg)

    if len(li_err_info) > 0:
        seqinfo['errinfo_content'] += "\n".join(li_err_info) + "\n"
        seqinfo['isValidVariants'] = False
    else:
        filtered_variants = rawvariants

    seqinfo['errinfo'] = seqinfo['errinfo_br'] + seqinfo['errinfo_content']
    return filtered_variants
#}}}
def InsertFinishDateToDB(date_str, md5_key, seq, outdb):# {{{
    """ Insert the finish date to the sqlite3 database
    """
    tbname_content = "data"
    try:
        con = sqlite3.connect(outdb)
    except Exception as e:
        print(("Failed to connect to the database outdb %s"%(outdb)))
    with con:
        cur = con.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS %s
            (
                md5 TEXT PRIMARY KEY,
                seq TEXT,
                date_finish TEXT
            )"""%(tbname_content))
        cmd =  "INSERT OR REPLACE INTO %s(md5,  seq, date_finish) VALUES('%s', '%s','%s')"%(tbname_content, md5_key, seq, date_str)
        try:
            cur.execute(cmd)
            return 0
        except Exception as e:
            print("Exception %s"%(str(e)), file=sys.stderr)
            return 1

# }}}

def GetInfoFinish(name_server, outpath_this_seq, origIndex, seqLength, seqAnno, source_result="", runtime=0.0):# {{{
    """Get the list info_finish for finished prediction"""
    name_server = name_server.lower()
    if name_server == "topcons2":
        return GetInfoFinish_TOPCONS2(outpath_this_seq, origIndex, seqLength, seqAnno, source_result, runtime)
    elif name_server == "boctopus2":
        return GetInfoFinish_Boctopus2(outpath_this_seq, origIndex, seqLength, seqAnno, source_result, runtime)
    elif name_server == "subcons":
        return GetInfoFinish_Subcons(outpath_this_seq, origIndex, seqLength, seqAnno, source_result, runtime)
    elif name_server == "prodres":
        return GetInfoFinish_PRODRES(outpath_this_seq, origIndex, seqLength, seqAnno, source_result, runtime)
    elif name_server == "pconsc3":
        return GetInfoFinish_PconsC3(outpath_this_seq, origIndex, seqLength, seqAnno, source_result, runtime)
    elif name_server == "predzinc":
        return GetInfoFinish_PredZinc(outpath_this_seq, origIndex, seqLength, seqAnno, source_result, runtime)
    elif name_server == "frag1d":
        return GetInfoFinish_Frag1D(outpath_this_seq, origIndex, seqLength, seqAnno, source_result, runtime)
    else:
        return []
# }}}
def GetInfoFinish_Boctopus2(outpath_this_seq, origIndex, seqLength, seqAnno, source_result="", runtime=0.0):# {{{
    """Get the list info_finish for the method Boctopus2"""
    return GetInfoFinish_TOPCONS2(outpath_this_seq, origIndex, seqLength, seqAnno, source_result, runtime)
# }}}
def GetInfoFinish_Subcons(outpath_this_seq, origIndex, seqLength, seqAnno, source_result="", runtime=0.0):# {{{
    """Get the list info_finish for the method Subcons"""
    finalpredfile = "%s/%s/query_0.subcons-final-pred.csv"%(
            outpath_this_seq, "final-prediction")
    (loc_def, loc_def_score) = GetLocDef(finalpredfile)
    date_str = time.strftime(FORMAT_DATETIME)
    info_finish = [ "seq_%d"%origIndex,
            str(seqLength), str(loc_def), str(loc_def_score),
            source_result, str(runtime),
            seqAnno.replace('\t', ' '), date_str]
    return info_finish
# }}}
def GetInfoFinish_PredZinc(outpath_this_seq, origIndex, seqLength, seqAnno, source_result="", runtime=0.0):# {{{
    """Get the list info_finish for the method PredZinc"""
    predfile = "%s/query.predzinc.predict"%( outpath_this_seq)
    (numZB, cntHomo) = CountNumPredZB(predfile, threshold=ZB_SCORE_THRESHOLD)
    date_str = time.strftime(FORMAT_DATETIME)
    # info_finish has 8 items
    info_finish = [ "seq_%d"%origIndex,
            str(seqLength), str(numZB), str(cntHomo),
            source_result, str(runtime),
            seqAnno.replace('\t', ' '), date_str]
    return info_finish
# }}}
def GetInfoFinish_Frag1D(outpath_this_seq, origIndex, seqLength, seqAnno, source_result="", runtime=0.0):# {{{
    """Get the list info_finish for the method Frag1D"""
    predfile = "%s/query.predfrag1d"%( outpath_this_seq)
    para_pred = GetStatFrag1DPred(predfile)
    date_str = time.strftime(FORMAT_DATETIME)
    # info_finish has 12 items
    info_finish = [ "seq_%d"%origIndex,
            str(seqLength), 
            str(para_pred['per_sec_H']),
            str(para_pred['per_sec_S']),
            str(para_pred['per_sec_R']),
            str(para_pred['per_s3_H']),
            str(para_pred['per_s3_S']),
            str(para_pred['per_s3_T']),
            source_result, str(runtime),
            seqAnno.replace('\t', ' '), date_str]
    return info_finish
# }}}
def GetInfoFinish_TOPCONS2(outpath_this_seq, origIndex, seqLength, seqAnno, source_result="", runtime=0.0):# {{{
    """Get the list info_finish for the method TOPCONS2"""
    topfile = "%s/%s/topcons.top"%(
            outpath_this_seq, "Topcons")
    top = myfunc.ReadFile(topfile).strip()
    numTM = myfunc.CountTM(top)
    posSP = myfunc.GetSPPosition(top)
    if len(posSP) > 0:
        isHasSP = True
    else:
        isHasSP = False
    date_str = time.strftime(FORMAT_DATETIME)
    info_finish = [ "seq_%d"%origIndex,
            str(seqLength), str(numTM),
            str(isHasSP), source_result, str(runtime),
            seqAnno.replace('\t', ' '), date_str]
    return info_finish
# }}}
def GetInfoFinish_PRODRES(outpath_this_seq, origIndex, seqLength, seqAnno, source_result="", runtime=0.0):# {{{
    """Get the list info_finish for the method PRODRES"""
    date_str = time.strftime(FORMAT_DATETIME)
    info_finish = [ "seq_%d"%origIndex,
            str(seqLength), str(None),
            str(None), source_result, str(runtime),
            seqAnno.replace('\t', ' '), date_str]
    return info_finish
# }}}
def GetInfoFinish_PconsC3(outpath_this_seq, origIndex, seqLength, seqAnno, source_result="", runtime=0.0):# {{{
    """Get the list info_finish for the method PconsC3"""
    date_str = time.strftime(FORMAT_DATETIME)
    info_finish = [ "seq_%d"%origIndex,
            str(seqLength), str(None),
            str(None), source_result, str(runtime),
            seqAnno.replace('\t', ' '), date_str]
    return info_finish
# }}}

def GetNumSeqSameUserDict(joblist):#{{{
    """calculate the total number of sequences users with jobs either in queue or running
    joblist is a list of list with the data structure: 

    li = [jobid, status, jobname, ip, email, numseq_str,
    method_submission, submit_date_str, start_date_str,
    finish_date_str]

    the return value is a dictionary {'jobid': total_num_seq}
    """
    # Fixed error for getting numseq at 2015-04-11
    numseq_user_dict = {}
    for i in range(len(joblist)):
        li1 = joblist[i]
        jobid1 = li1[0]
        ip1 = li1[3]
        email1 = li1[4]
        try:
            numseq1 = int(li1[5])
        except:
            numseq1 = 123
            pass
        if not jobid1 in numseq_user_dict:
            numseq_user_dict[jobid1] = 0
        numseq_user_dict[jobid1] += numseq1
        if ip1 == "" and email1 == "":
            continue

        for j in range(len(joblist)):
            li2 = joblist[j]
            if i == j:
                continue

            jobid2 = li2[0]
            ip2 = li2[3]
            email2 = li2[4]
            try:
                numseq2 = int(li2[5])
            except:
                numseq2 = 123
                pass
            if ((ip2 != "" and ip2 == ip1) or
                    (email2 != "" and email2 == email1)):
                numseq_user_dict[jobid1] += numseq2
    return numseq_user_dict
#}}}
def GetRefreshInterval(queuetime_in_sec, runtime_in_sec, method_submission):# {{{
    """Get refresh_interval for the webpage"""
    refresh_interval = 2.0
    t =  queuetime_in_sec + runtime_in_sec
    if t < 10:
        if method_submission == "web":
            refresh_interval = 2.0
        else:
            refresh_interval = 5.0
    elif t >= 10 and t < 40:
        refresh_interval = t / 2.0
    else:
        refresh_interval = 20.0
    return refresh_interval

# }}}
def WriteDateTimeTagFile(outfile, logfile, errfile):# {{{
    if not os.path.exists(outfile):
        date_str = time.strftime(FORMAT_DATETIME)
        try:
            myfunc.WriteFile(date_str, outfile)
            msg = "Write tag file %s succeeded"%(outfile)
            myfunc.WriteFile("[%s] %s\n"%(date_str, msg),  logfile, "a", True)
        except Exception as e:
            msg = "Failed to write to file %s with message: \"%s\""%(outfile, str(e))
            myfunc.WriteFile("[%s] %s\n"%(date_str, msg),  errfile, "a", True)
# }}}
def RunCmd(cmd, logfile, errfile, verbose=False):# {{{
    """Input cmd in list
       Run the command and also output message to logs
    """
    begin_time = time.time()

    isCmdSuccess = False
    cmdline = " ".join(cmd)
    date_str = time.strftime(FORMAT_DATETIME)
    rmsg = ""
    try:
        rmsg = subprocess.check_output(cmd, encoding='UTF-8')
        if verbose:
            msg = "workflow: %s"%(cmdline)
            myfunc.WriteFile("[%s] %s\n"%(date_str, msg),  logfile, "a", True)
        isCmdSuccess = True
    except subprocess.CalledProcessError as e:
        msg = "cmdline: %s\nFailed with message \"%s\""%(cmdline, str(e))
        myfunc.WriteFile("[%s] %s\n"%(date_str, msg),  errfile, "a", True)
        isCmdSuccess = False
        pass

    end_time = time.time()
    runtime_in_sec = end_time - begin_time

    return (isCmdSuccess, runtime_in_sec)
# }}}
def SendEmail_on_finish(jobid, base_www_url, finish_status, name_server, from_email, to_email, contact_email, logfile="", errfile=""):# {{{
    """Send notification email to the user for the web-server, the name
    of the web-server is specified by the var 'name_server'
    """
    err_msg = ""
    if os.path.exists(errfile):
        err_msg = myfunc.ReadFile(errfile)

    subject = "Your result for %s JOBID=%s"%(name_server, jobid)
    if finish_status == "success":
        bodytext = """
Your result is ready at %s/pred/result/%s

Thanks for using %s

    """%(base_www_url, jobid, name_server)
    elif finish_status == "failed":
        bodytext="""
We are sorry that your job with jobid %s is failed.

Please contact %s if you have any questions.

Attached below is the error message:
%s
        """%(jobid, contact_email, err_msg)
    else:
        bodytext="""
Your result is ready at %s/pred/result/%s

We are sorry that %s failed to predict some sequences of your job.

Please re-submit the queries that have been failed.

If you have any further questions, please contact %s.

Attached below is the error message:
%s
        """%(base_www_url, jobid, name_server, contact_email, err_msg)

    date_str = time.strftime(FORMAT_DATETIME)
    msg =  "Sendmail %s -> %s, %s"%(from_email, to_email, subject)
    myfunc.WriteFile("[%s] %s\n"% (date_str, msg), logfile, "a", True)
    rtValue = myfunc.Sendmail(from_email, to_email, subject, bodytext)
    if rtValue != 0:
        msg =  "Sendmail to {} failed with status {}".format(to_email, rtValue)
        myfunc.WriteFile("[%s] %s\n"%(date_str, msg), errfile, "a", True)
        return 1
    else:
        return 0
# }}}
def GetJobCounter(info): #{{{
# get job counter for the client_ip
# get the table from runlog, 
# for queued or running jobs, if source=web and numseq=1, check again the tag file in
# each individual folder, since they are queued locally
    logfile_query = info['divided_logfile_query']
    logfile_finished_jobid = info['divided_logfile_finished_jobid']
    isSuperUser = info['isSuperUser']
    client_ip = info['client_ip']
    maxdaystoshow = info['MAX_DAYS_TO_SHOW']
    path_result = info['path_result']

    jobcounter = {}

    jobcounter['queued'] = 0
    jobcounter['running'] = 0
    jobcounter['finished'] = 0
    jobcounter['failed'] = 0
    jobcounter['nojobfolder'] = 0 #of which the folder jobid does not exist

    jobcounter['queued_idlist'] = []
    jobcounter['running_idlist'] = []
    jobcounter['finished_idlist'] = []
    jobcounter['failed_idlist'] = []
    jobcounter['nojobfolder_idlist'] = []

    hdl = myfunc.ReadLineByBlock(logfile_query)
    if hdl.failure:
        return jobcounter
    else:
        finished_job_dict = myfunc.ReadFinishedJobLog(logfile_finished_jobid)
        finished_jobid_set = set([])
        failed_jobid_set = set([])
        for jobid in finished_job_dict:
            status = finished_job_dict[jobid][0]
            rstdir = "%s/%s"%(path_result, jobid)
            if status == "Finished":
                finished_jobid_set.add(jobid)
            elif status == "Failed":
                failed_jobid_set.add(jobid)
        lines = hdl.readlines()
        while lines != None:
            for line in lines:
                strs = line.split("\t")
                if len(strs) < 7:
                    continue
                ip = strs[2]
                if not isSuperUser and ip != client_ip:
                    continue

                submit_date_str = strs[0]
                isValidSubmitDate = True
                try:
                    submit_date = datetime_str_to_time(submit_date_str)
                except ValueError:
                    isValidSubmitDate = False

                if not isValidSubmitDate:
                    continue

                current_time = datetime.now(submit_date.tzinfo)
                diff_date = current_time - submit_date
                if diff_date.days > maxdaystoshow:
                    continue
                jobid = strs[1]
                rstdir = "%s/%s"%(path_result, jobid)

                if jobid in finished_jobid_set:
                    jobcounter['finished'] += 1
                    jobcounter['finished_idlist'].append(jobid)
                elif jobid in failed_jobid_set:
                    jobcounter['failed'] += 1
                    jobcounter['failed_idlist'].append(jobid)
                else:
                    finishtagfile = "%s/%s"%(rstdir, "runjob.finish")
                    failtagfile = "%s/%s"%(rstdir, "runjob.failed")
                    starttagfile = "%s/%s"%(rstdir, "runjob.start")
                    if not os.path.exists(rstdir):
                        jobcounter['nojobfolder'] += 1
                        jobcounter['nojobfolder_idlist'].append(jobid)
                    elif os.path.exists(failtagfile):
                        jobcounter['failed'] += 1
                        jobcounter['failed_idlist'].append(jobid)
                    elif os.path.exists(finishtagfile):
                        jobcounter['finished'] += 1
                        jobcounter['finished_idlist'].append(jobid)
                    elif os.path.exists(starttagfile):
                        jobcounter['running'] += 1
                        jobcounter['running_idlist'].append(jobid)
                    else:
                        jobcounter['queued'] += 1
                        jobcounter['queued_idlist'].append(jobid)
            lines = hdl.readlines()
        hdl.close()
    return jobcounter
#}}}

def CleanJobFolder(rstdir, name_server):# {{{
    name_server = name_server.lower()
    if name_server == "boctopus2":
        CleanJobFolder_Boctopus2(rstdir)
    elif name_server == "scampi2":
        CleanJobFolder_Scampi(rstdir)
    elif name_server == "topcons2":
        CleanJobFolder_TOPCONS2(rstdir)
    elif name_server == "subcons":
        CleanJobFolder_Subcons(rstdir)
    elif name_server == "prodres":
        CleanJobFolder_PRODRES(rstdir)
    elif name_server == "pconsc3":
        CleanJobFolder_PconsC3(rstdir)
    else:
        CleanJobFolder_basic(rstdir)

# }}}

def CleanJobFolder_Boctopus2(rstdir):# {{{
    """Clean the jobfolder for TOPCONS2 after finishing"""
    flist =[
            "%s/remotequeue_seqindex.txt"%(rstdir),
            "%s/torun_seqindex.txt"%(rstdir)
            ]
    for f in flist:
        if os.path.exists(f):
            try:
                os.remove(f)
            except:
                pass
# }}}
def CleanJobFolder_Scampi(rstdir):# {{{
    """Clean the jobfolder for Scampi after finishing"""
    flist =[
            "%s/remotequeue_seqindex.txt"%(rstdir),
            "%s/torun_seqindex.txt"%(rstdir)
            ]
    for f in flist:
        if os.path.exists(f):
            try:
                os.remove(f)
            except:
                pass
# }}}
def CleanJobFolder_TOPCONS2(rstdir):# {{{
    """Clean the jobfolder for TOPCONS2 after finishing"""
    flist =[
            "%s/remotequeue_seqindex.txt"%(rstdir),
            "%s/torun_seqindex.txt"%(rstdir)
            ]
    for f in flist:
        if os.path.exists(f):
            try:
                os.remove(f)
            except:
                pass
# }}}
def CleanJobFolder_Subcons(rstdir):# {{{
    """Clean the jobfolder for Subcons after finishing"""
    flist =[
            "%s/remotequeue_seqindex.txt"%(rstdir),
            "%s/torun_seqindex.txt"%(rstdir)
            ]
    for f in flist:
        if os.path.exists(f):
            try:
                os.remove(f)
            except:
                pass
# }}}
def CleanJobFolder_PRODRES(rstdir):# {{{
    """Clean the jobfolder for PRODRES after finishing"""
    flist =[
            "%s/remotequeue_seqindex.txt"%(rstdir),
            "%s/torun_seqindex.txt"%(rstdir)
            ]
    for f in flist:
        if os.path.exists(f):
            try:
                os.remove(f)
            except:
                pass
# }}}
def CleanJobFolder_PconsC3(rstdir):# {{{
    """Clean the jobfolder for PconsC3 after finishing"""
    flist =[
            "%s/remotequeue_seqindex.txt"%(rstdir),
            "%s/torun_seqindex.txt"%(rstdir)
            ]
    for f in flist:
        if os.path.exists(f):
            try:
                os.remove(f)
            except:
                pass
# }}}
def CleanJobFolder_basic(rstdir):# {{{
    """Clean the jobfolder after finishing: the basic function"""
    flist =[
            "%s/remotequeue_seqindex.txt"%(rstdir),
            "%s/torun_seqindex.txt"%(rstdir)
            ]
    for f in flist:
        if os.path.exists(f):
            try:
                os.remove(f)
            except:
                pass
# }}}

def DeleteOldResult(path_result, path_log, logfile, MAX_KEEP_DAYS=180):#{{{
    """Delete jobdirs that are finished > MAX_KEEP_DAYS
    return True if therer is at least one result folder been deleted
    """
    finishedjoblogfile = "%s/finished_job.log"%(path_log)
    finished_job_dict = myfunc.ReadFinishedJobLog(finishedjoblogfile)
    isOldRstdirDeleted = False
    for jobid in finished_job_dict:
        li = finished_job_dict[jobid]
        try:
            finish_date_str = li[8]
        except IndexError:
            finish_date_str = ""
            pass
        if finish_date_str != "":
            isValidFinishDate = True
            try:
                finish_date = datetime_str_to_time(finish_date_str)
            except ValueError:
                isValidFinishDate = False

            if isValidFinishDate:
                current_time = datetime.now(finish_date.tzinfo)
                timeDiff = current_time - finish_date
                if timeDiff.days > MAX_KEEP_DAYS:
                    rstdir = "%s/%s"%(path_result, jobid)
                    msg = "\tjobid = %s finished %d days ago (>%d days), delete."%(jobid, timeDiff.days, MAX_KEEP_DAYS)
                    loginfo(msg, logfile)
                    try:
                        shutil.rmtree(rstdir)
                        isOldRstdirDeleted = True
                    except:
                        msg = "failed to delete rstdir %s"%(rstdir)
                        loginfo(msg, logfile)
    return isOldRstdirDeleted
#}}}
def loginfo(msg, outfile):# {{{
    """Write loginfo to outfile, appending current time"""
    date_str = time.strftime(FORMAT_DATETIME)
    myfunc.WriteFile("[%s] %s\n"%(date_str, msg), outfile, "a", True)
# }}}
@timeit
def CleanServerFile(path_static, logfile, errfile):#{{{
    """Clean old files on the server"""
# clean tmp files
    msg = "CleanServerFile, path_static=%s"%(path_static)
    loginfo(msg, logfile)
    cmd = ["clean_server_file.sh", path_static]
    RunCmd(cmd, logfile, errfile)
#}}}
@timeit
def CleanCachedResult(path_static, name_cachedir, logfile, errfile):#{{{
    """Clean outdated cahced results on the server"""
# clean tmp files
    msg = "Clean cached results..."
    date_str = time.strftime(FORMAT_DATETIME)
    myfunc.WriteFile("[%s] %s\n"%(date_str, msg), logfile, "a", True)
    cmd = ["clean_cached_result.py", "-path-static", path_static, '-name-cachedir', name_cachedir, "-max-keep-day", "480"]
    RunCmd(cmd, logfile, errfile)
#}}}

def ReadComputeNode(infile):# {{{
    """Read computenode file computenode.txt
    return a dict
    """
    try:
        fpin = open(infile,"r")
        lines = fpin.read().split('\n')
        fpin.close()
    except IOError:
        print("Failed to read computenodefile %s"%infile)
        return {}

    dt = {}
    for line in lines:
        line = line.strip()
        if not line or line[0] == '#':
            continue
        strs = line.split()
        node = strs[0]
        dt[node] = {}
        try:
            dt[node]['maxprocess'] = int(strs[1])
        except:
            dt[node]['maxprocess'] = 0
        try:
            dt[node]['queue_method'] = strs[2]
        except:
            dt[node]['queue_method'] = 'suq'

    return dt

# }}}
def ReadRuntimeFromFile(timefile, default_runtime=0.0):# {{{
    """Read runtime from timefile"""
    if os.path.exists(timefile):
        txt = myfunc.ReadFile(timefile).strip()
        ss2 = txt.split(";")
        try:
            runtime = float(ss2[1])
        except:
            runtime = default_runtime
    else:
        runtime = default_runtime
    return runtime
# }}}
def ArchiveLogFile(path_log, threshold_logfilesize=20*1024*1024, g_params={}):# {{{
    """Archive some of the log files if they are too big"""
    gen_logfile = "%s/qd_fe.py.log"%(path_log)
    if 'DEBUG_ARCHIVE' in g_params and g_params['DEBUG_ARCHIVE']:
        loginfo("Entering ArchiveLogFile", gen_logfile)
    gen_errfile = "%s/qd_fe.py.err"%(path_log)
    flist = [gen_logfile, gen_errfile,
            "%s/restart_qd_fe.cgi.log"%(path_log),
            "%s/debug.log"%(path_log),
            "%s/clean_cached_result.py.log"%(path_log)
            ]

    for f in flist:
        if os.path.exists(f):
            if 'DEBUG_ARCHIVE' in g_params and g_params['DEBUG_ARCHIVE']:
                filesize = os.path.getsize(f)
                if filesize > threshold_logfilesize:
                    loginfo("filesize(%s) = %d > %d, archive it"%(f, filesize, threshold_logfilesize), gen_logfile)
                else:
                    loginfo("filesize(%s) = %d, threshold_logfilesize=%d"%(f, filesize, threshold_logfilesize), gen_logfile)
            myfunc.ArchiveFile(f, threshold_logfilesize)
# }}}

def get_default_server_url(name_server):# {{{
    if name_server == "subcons":
        return "https://subcons.bioinfo.se"
    elif name_server == "prodres":
        return "https://prodres.bioinfo.se"
    elif name_server == "topcons2":
        return "https://topcons.net"
    elif name_server == "scampi2":
        return "https://scampi.bioinfo.se"
    elif name_server == "boctopus2":
        return "https://boctopus.bioinfo.se"
    elif name_server == "proq3":
        return "https://proq3.bioinfo.se"
    elif name_server == "pconsc3":
        return "https://pconsc3.bioinfo.se"
    elif name_server == "pathopred":
        return "https://pathopred.bioinfo.se"
    elif name_server == "predzinc":
        return "https://predzinc.bioshu.se"
    elif name_server == "frag1d":
        return "https://frag1d.bioshu.se"
# }}}
def GetNameSoftware(name_server, queue_method):# {{{
    """Determine name_software for each webserver
    """
    if name_server == "subcons":
        name_software = "docker_subcons"
        if queue_method == "slurm":
            name_software = "singularity_subcons"
    elif name_server == "prodres":
        name_software = "prodres"
    elif name_server == "topcons2":
        name_software = "docker_topcons2"
        if queue_method == "slurm":
            name_software = "singularity_topcons2"
    elif name_server == "proq3":
        name_software = "docker_proq3"
        if queue_method == "slurm":
            name_software = "singularity_proq3"
    elif name_server == "boctopus2":
        name_software = "docker_boctopus2"
        if queue_method == "slurm":
            name_software = "singularity_boctopus2"
    elif name_server == "pathopred":
        name_software = "docker_pathopred"
        if queue_method == "slurm":
            name_software = "singularity_pathopred"
    elif name_server == "scampi2":
        name_software = "scampi2-msa"
    elif name_server == "pconsc3":
        name_software = "pconsc3"
    elif name_server == "predzinc":
        name_software = "docker_predzinc"
    elif name_server == "frag1d":
        name_software = "docker_frag1d"

    return name_software
# }}}
def get_email_address_outsending(name_server):# {{{
    """determine the outsending email address for given name_server
    """
    name_server = name_server.lower()
    if name_server == "subcons":
        return "no-reply.SubCons@bioinfo.se"
    elif name_server == "topcons2":
        return "no-reply.TOPCONS@topcons.net"
    elif name_server == "scampi2":
        return "no-reply.SCAMPI@bioinfo.se"
    elif name_server == "boctopus2":
        return "no-reply.BOCTOPUS@bioinfo.se"
    elif name_server == "proq3":
        return "no-reply.PROQ3@bioinfo.se"
    elif name_server == "prodres":
        return "no-reply.PRODRES@bioinfo.se"
    elif name_server == "pconsc3":
        return "no-reply.PCONSC3@bioinfo.se"
    elif name_server == "predzinc":
        return "no-reply.predzinc@bioshu.se"
# }}}
def SubmitSlurmJob(datapath, outpath, scriptfile, debugfile):#{{{
    """Submit job to the Slurm queue
    """
    loginfo("Entering SubmitSlurmJob()", debugfile)
    rmsg = ""
    os.chdir(outpath)
    cmd = ['sbatch', scriptfile]
    cmdline = " ".join(cmd)
    loginfo("cmdline: %s\n\n"%(cmdline), debugfile)
    MAX_TRY = 2
    cnttry = 0
    isSubmitSuccess = False
    while cnttry < MAX_TRY:
        loginfo("run cmd: cnttry = %d, MAX_TRY=%d\n"%(cnttry,
            MAX_TRY), debugfile)
        (isSubmitSuccess, t_runtime) = RunCmd(cmd, debugfile, debugfile)
        if isSubmitSuccess:
            break
        cnttry += 1
        time.sleep(0.05+cnttry*0.03)
    if isSubmitSuccess:
        loginfo("Leaving SubmitSlurmJob() with success\n", debugfile)
        return 0
    else:
        loginfo("Leaving SubmitSlurmJob() with error\n\n", debugfile)
        return 1
#}}}
def SubmitSuqJob(suq_exec, suq_basedir, datapath, outpath, priority, scriptfile, logfile):#{{{
    loginfo("Entering SubmitSuqJob()", logfile)
    rmsg = ""
    cmd = [suq_exec,"-b", suq_basedir, "run", "-d", outpath, "-p", "%d"%(priority), scriptfile]
    cmdline = " ".join(cmd)
    loginfo("cmdline: %s\n\n"%(cmdline), logfile)
    MAX_TRY = 5
    cnttry = 0
    isSubmitSuccess = False
    while cnttry < MAX_TRY:
        loginfo("run cmd: cnttry = %d, MAX_TRY=%d\n"%(cnttry, MAX_TRY), logfile)
        (isSubmitSuccess, t_runtime) = RunCmd(cmd, logfile, logfile)
        if isSubmitSuccess:
            break
        cnttry += 1
        time.sleep(0.05+cnttry*0.03)
    if isSubmitSuccess:
        loginfo("Leaving SubmitSuqJob() with success\n", logfile)
        return 0
    else:
        loginfo("Leaving SubmitSuqJob() with error\n\n", logfile)
        return 1
#}}}
def get_queue_method_name():# {{{
    """Get the name of queue_method based on the hostname
    """
    import socket
    hostname = socket.gethostname()
    if hostname.find("shu") != -1 or hostname.find("pcons3") != -1:
        return "suq"
    else:
        return "slurm"
# }}}


# functions for views.py
def set_basic_config(request, info, g_params):# {{{
    """Set basic configurations for the template dict"""
    username = request.user.username
    client_ip = request.META['REMOTE_ADDR']
    path_static = g_params['path_static']
    path_log = "%s/log"%(path_static)
    path_result = "%s/result"%(path_static)
    if username in g_params['SUPER_USER_LIST']:
        isSuperUser = True
        divided_logfile_query =  "%s/%s"%(path_log, "submitted_seq.log")
        divided_logfile_finished_jobid =  "%s/%s"%(path_log, "failed_job.log")
    else:
        isSuperUser = False
        divided_logfile_query =  "%s/%s/%s"%(path_log, "divided", "%s_submitted_seq.log"%(client_ip))
        divided_logfile_finished_jobid =  "%s/%s/%s"%(path_log, "divided", "%s_failed_job.log"%(client_ip))

    if isSuperUser:
        info['MAX_DAYS_TO_SHOW'] = g_params['BIG_NUMBER']
    else:
        info['MAX_DAYS_TO_SHOW'] = g_params['MAX_DAYS_TO_SHOW']


    info['username'] = username
    info['isSuperUser'] = isSuperUser
    info['divided_logfile_query'] = divided_logfile_query
    info['divided_logfile_finished_jobid'] = divided_logfile_finished_jobid
    info['client_ip'] = client_ip
    info['BASEURL'] = g_params['BASEURL']
    info['STATIC_URL'] = g_params['STATIC_URL']
    info['path_result'] = path_result
# }}}
def SetColorStatus(status):#{{{
    if status == "Finished":
        return "green"
    elif status == "Failed":
        return "red"
    elif status == "Running":
        return "blue"
    else:
        return "black"
#}}}

def get_queue(request, g_params):#{{{
    info = {}
    path_result = "%s/result"%(g_params['path_static'])
    set_basic_config(request, info, g_params)

    status = "Queued"
    info['header'] = ["No.", "JobID","JobName", "NumSeq", "Email",
            "QueueTime", "RunTime", "Date", "Source"]
    if info['isSuperUser']:
        info['header'].insert(5, "Host")

    hdl = myfunc.ReadLineByBlock(info['divided_logfile_query'])
    if hdl.failure:
        info['errmsg'] = ""
        pass
    else:
        finished_jobid_list = []
        if os.path.exists(info['divided_logfile_finished_jobid']):
            finished_jobid_list = myfunc.ReadIDList2(info['divided_logfile_finished_jobid'], 0, None)
        finished_jobid_set = set(finished_jobid_list)
        jobRecordList = []
        lines = hdl.readlines()
        while lines != None:
            for line in lines:
                strs = line.split("\t")
                if len(strs) < 7:
                    continue
                ip = strs[2]
                if not info['isSuperUser'] and ip != info['client_ip']:
                    continue
                jobid = strs[1]
                if jobid in finished_jobid_set:
                    continue

                rstdir = "%s/%s"%(path_result, jobid)
                starttagfile = "%s/%s"%(rstdir, "runjob.start")
                failedtagfile = "%s/%s"%(rstdir, "runjob.failed")
                finishtagfile = "%s/%s"%(rstdir, "runjob.finish")
                if (os.path.exists(rstdir) and 
                        not os.path.exists(starttagfile) and
                        not os.path.exists(failedtagfile) and
                        not os.path.exists(finishtagfile)):
                    jobRecordList.append(jobid)
            lines = hdl.readlines()
        hdl.close()

        jobinfo_list = []
        rank = 0
        for jobid in jobRecordList:
            rank += 1
            ip =  ""
            jobname = ""
            email = ""
            method_submission = "web"
            numseq = 1
            rstdir = "%s/%s"%(path_result, jobid)

            submit_date_str = ""
            finish_date_str = ""
            start_date_str = ""

            jobinfofile = "%s/jobinfo"%(rstdir)
            jobinfo = myfunc.ReadFile(jobinfofile).strip()
            jobinfolist = jobinfo.split("\t")
            if len(jobinfolist) >= 8:
                submit_date_str = jobinfolist[0]
                ip = jobinfolist[2]
                numseq = int(jobinfolist[3])
                jobname = jobinfolist[5]
                email = jobinfolist[6]
                method_submission = jobinfolist[7]

            starttagfile = "%s/runjob.start"%(rstdir)
            queuetime = ""
            runtime = ""
            isValidSubmitDate = True
            try:
                submit_date = datetime_str_to_time(submit_date_str)
            except ValueError:
                isValidSubmitDate = False

            if isValidSubmitDate:
                current_time = datetime.now(submit_date.tzinfo)
                queuetime = myfunc.date_diff(submit_date, current_time)

            row_content = [rank, jobid, jobname[:20], numseq, email,
                    queuetime, runtime, submit_date_str, method_submission]
            if info['isSuperUser']:
                row_content.insert(5, ip)
            jobinfo_list.append(row_content)

        info['content'] = jobinfo_list

    info['jobcounter'] = GetJobCounter(info)
    #return render(request, 'pred/queue.html', info)
    return info
#}}}
def get_running(request, g_params):#{{{
    # Get running jobs
    info = {}
    path_result = "%s/result"%(g_params['path_static'])
    set_basic_config(request, info, g_params)

    status = "Running"
    info['header'] = ["No.", "JobID", "JobName", "NumSeq", "NumFinish", "Email",
            "QueueTime", "RunTime", "Date", "Source"]
    if info['isSuperUser']:
        info['header'].insert(6, "Host")

    hdl = myfunc.ReadLineByBlock(info['divided_logfile_query'])
    if hdl.failure:
        info['errmsg'] = ""
        pass
    else:
        finished_jobid_list = []
        if os.path.exists(info['divided_logfile_finished_jobid']):
            finished_jobid_list = myfunc.ReadIDList2(info['divided_logfile_finished_jobid'], 0, None)
        finished_jobid_set = set(finished_jobid_list)
        jobRecordList = []
        lines = hdl.readlines()
        while lines != None:
            for line in lines:
                strs = line.split("\t")
                if len(strs) < 7:
                    continue
                ip = strs[2]
                if not info['isSuperUser'] and ip != info['client_ip']:
                    continue
                jobid = strs[1]
                if jobid in finished_jobid_set:
                    continue
                rstdir = "%s/%s"%(path_result, jobid)
                starttagfile = "%s/%s"%(rstdir, "runjob.start")
                finishtagfile = "%s/%s"%(rstdir, "runjob.finish")
                failedtagfile = "%s/%s"%(rstdir, "runjob.failed")
                if (os.path.exists(rstdir) and os.path.exists(starttagfile) and (not
                    os.path.exists(finishtagfile) and not
                    os.path.exists(failedtagfile))):
                    jobRecordList.append(jobid)
            lines = hdl.readlines()
        hdl.close()

        jobinfo_list = []
        rank = 0
        for jobid in jobRecordList:
            rank += 1
            ip =  ""
            jobname = ""
            email = ""
            method_submission = "web"
            numseq = 1
            rstdir = "%s/%s"%(path_result, jobid)

            submit_date_str = ""
            finish_date_str = ""
            start_date_str = ""

            jobinfofile = "%s/jobinfo"%(rstdir)
            jobinfo = myfunc.ReadFile(jobinfofile).strip()
            jobinfolist = jobinfo.split("\t")
            if len(jobinfolist) >= 8:
                submit_date_str = jobinfolist[0]
                ip = jobinfolist[2]
                numseq = int(jobinfolist[3])
                jobname = jobinfolist[5]
                email = jobinfolist[6]
                method_submission = jobinfolist[7]

            finished_idx_file = "%s/finished_seqindex.txt"%(rstdir)
            numFinishedSeq = 0
            if os.path.exists(finished_idx_file):
                finished_idxlist = myfunc.ReadIDList(finished_idx_file)
                numFinishedSeq = len(set(finished_idxlist))

            starttagfile = "%s/runjob.start"%(rstdir)
            queuetime = ""
            runtime = ""
            isValidSubmitDate = True
            isValidStartDate = True
            try:
                submit_date = datetime_str_to_time(submit_date_str)
            except ValueError:
                isValidSubmitDate = False
            start_date_str = ""
            if os.path.exists(starttagfile):
                start_date_str = myfunc.ReadFile(starttagfile).strip()
            try:
                start_date = datetime_str_to_time(start_date_str)
            except ValueError:
                isValidStartDate = False
            if isValidStartDate:
                current_time = datetime.now(start_date.tzinfo)
                runtime = myfunc.date_diff(start_date, current_time)
            if isValidStartDate and isValidSubmitDate:
                queuetime = myfunc.date_diff(submit_date, start_date)

            row_content = [rank, jobid, jobname[:20], numseq, numFinishedSeq,
                    email, queuetime, runtime, submit_date_str,
                    method_submission]
            if info['isSuperUser']:
                row_content.insert(6, ip)
            jobinfo_list.append(row_content)

        info['content'] = jobinfo_list

    info['jobcounter'] = GetJobCounter(info)
    return info
#}}}
def get_finished_job(request, g_params):#{{{
    info = {}
    path_result = "%s/result"%(g_params['path_static'])
    set_basic_config(request, info, g_params)

    info['header'] = ["No.", "JobID","JobName", "NumSeq", "Email",
            "QueueTime","RunTime", "Date", "Source"]
    if info['isSuperUser']:
        info['header'].insert(5, "Host")

    hdl = myfunc.ReadLineByBlock(info['divided_logfile_query'])
    if hdl.failure:
        #info['errmsg'] = "Failed to retrieve finished job information!"
        info['errmsg'] = ""
        pass
    else:
        finished_job_dict = myfunc.ReadFinishedJobLog(info['divided_logfile_finished_jobid'])
        jobRecordList = []
        lines = hdl.readlines()
        while lines != None:
            for line in lines:
                strs = line.split("\t")
                if len(strs) < 7:
                    continue
                ip = strs[2]
                if not info['isSuperUser'] and ip != info['client_ip']:
                    continue

                submit_date_str = strs[0]
                isValidSubmitDate = True
                try:
                    submit_date = datetime_str_to_time(submit_date_str)
                except ValueError:
                    isValidSubmitDate = False
                if not isValidSubmitDate:
                    continue

                current_time = datetime.now(submit_date.tzinfo)
                diff_date = current_time - submit_date
                if diff_date.days > info['MAX_DAYS_TO_SHOW']:
                    continue
                jobid = strs[1]
                rstdir = "%s/%s"%(path_result, jobid)
                if jobid in finished_job_dict:
                    status = finished_job_dict[jobid][0]
                    if status == "Finished":
                        jobRecordList.append(jobid)
                else:
                    finishtagfile = "%s/%s"%(rstdir, "runjob.finish")
                    failedtagfile = "%s/%s"%(rstdir, "runjob.failed")
                    if (os.path.exists(rstdir) and  os.path.exists(finishtagfile) and
                            not os.path.exists(failedtagfile)):
                        jobRecordList.append(jobid)
            lines = hdl.readlines()
        hdl.close()

        jobinfo_list = []
        rank = 0
        for jobid in jobRecordList:
            rank += 1
            ip =  ""
            jobname = ""
            email = ""
            method_submission = "web"
            numseq = 1
            rstdir = "%s/%s"%(path_result, jobid)
            starttagfile = "%s/runjob.start"%(rstdir)
            finishtagfile = "%s/runjob.finish"%(rstdir)

            submit_date_str = ""
            finish_date_str = ""
            start_date_str = ""

            if jobid in finished_job_dict:
                status = finished_job_dict[jobid][0]
                jobname = finished_job_dict[jobid][1]
                ip = finished_job_dict[jobid][2]
                email = finished_job_dict[jobid][3]
                numseq = finished_job_dict[jobid][4]
                method_submission = finished_job_dict[jobid][5]
                submit_date_str = finished_job_dict[jobid][6]
                start_date_str = finished_job_dict[jobid][7]
                finish_date_str = finished_job_dict[jobid][8]
            else:
                jobinfofile = "%s/jobinfo"%(rstdir)
                jobinfo = myfunc.ReadFile(jobinfofile).strip()
                jobinfolist = jobinfo.split("\t")
                if len(jobinfolist) >= 8:
                    submit_date_str = jobinfolist[0]
                    numseq = int(jobinfolist[3])
                    jobname = jobinfolist[5]
                    email = jobinfolist[6]
                    method_submission = jobinfolist[7]

            isValidSubmitDate = True
            isValidStartDate = True
            isValidFinishDate = True
            try:
                submit_date = datetime_str_to_time(submit_date_str)
            except ValueError:
                isValidSubmitDate = False
            start_date_str = ""
            if os.path.exists(starttagfile):
                start_date_str = myfunc.ReadFile(starttagfile).strip()
            try:
                start_date = datetime_str_to_time(start_date_str)
            except ValueError:
                isValidStartDate = False
            finish_date_str = myfunc.ReadFile(finishtagfile).strip()
            try:
                finish_date = datetime_str_to_time(finish_date_str)
            except ValueError:
                isValidFinishDate = False

            queuetime = ""
            runtime = ""

            if isValidStartDate and isValidFinishDate:
                runtime = myfunc.date_diff(start_date, finish_date)
            if isValidSubmitDate and isValidStartDate:
                queuetime = myfunc.date_diff(submit_date, start_date)

            row_content = [rank, jobid, jobname[:20], str(numseq), email,
                    queuetime, runtime, submit_date_str, method_submission]
            if info['isSuperUser']:
                row_content.insert(5, ip)
            jobinfo_list.append(row_content)


        info['content'] = jobinfo_list

    info['jobcounter'] = GetJobCounter(info)
    return info
#}}}
def get_failed_job(request, g_params):#{{{
    info = {}
    path_result = "%s/result"%(g_params['path_static'])
    set_basic_config(request, info, g_params)
    info['header'] = ["No.", "JobID","JobName", "NumSeq", "Email",
            "QueueTime","RunTime", "Date", "Source"]
    if info['isSuperUser']:
        info['header'].insert(5, "Host")

    hdl = myfunc.ReadLineByBlock(info['divided_logfile_query'])
    if hdl.failure:
#         info['errmsg'] = "Failed to retrieve finished job information!"
        info['errmsg'] = ""
        pass
    else:
        finished_job_dict = myfunc.ReadFinishedJobLog(info['divided_logfile_finished_jobid'])
        jobRecordList = []
        lines = hdl.readlines()
        while lines != None:
            for line in lines:
                strs = line.split("\t")
                if len(strs) < 7:
                    continue
                ip = strs[2]
                if not info['isSuperUser'] and ip != info['client_ip']:
                    continue

                submit_date_str = strs[0]
                submit_date = datetime_str_to_time(submit_date_str)
                current_time = datetime.now(submit_date.tzinfo)
                diff_date = current_time - submit_date
                if diff_date.days > info['MAX_DAYS_TO_SHOW']:
                    continue
                jobid = strs[1]
                rstdir = "%s/%s"%(path_result, jobid)

                if jobid in finished_job_dict:
                    status = finished_job_dict[jobid][0]
                    if status == "Failed":
                        jobRecordList.append(jobid)
                else:
                    failedtagfile = "%s/%s"%(rstdir, "runjob.failed")
                    if os.path.exists(rstdir) and os.path.exists(failedtagfile):
                        jobRecordList.append(jobid)
            lines = hdl.readlines()
        hdl.close()


        jobinfo_list = []
        rank = 0
        for jobid in jobRecordList:
            rank += 1

            ip = ""
            jobname = ""
            email = ""
            method_submission = ""
            numseq = 1
            submit_date_str = ""

            rstdir = "%s/%s"%(path_result, jobid)
            starttagfile = "%s/runjob.start"%(rstdir)
            failedtagfile = "%s/runjob.failed"%(rstdir)

            if jobid in finished_job_dict:
                submit_date_str = finished_job_dict[jobid][0]
                jobname = finished_job_dict[jobid][1]
                ip = finished_job_dict[jobid][2]
                email = finished_job_dict[jobid][3]
                numseq = finished_job_dict[jobid][4]
                method_submission = finished_job_dict[jobid][5]
                submit_date_str = finished_job_dict[jobid][6]
                start_date_str = finished_job_dict[jobid][ 7]
                finish_date_str = finished_job_dict[jobid][8]
            else:
                jobinfofile = "%s/jobinfo"%(rstdir)
                jobinfo = myfunc.ReadFile(jobinfofile).strip()
                jobinfolist = jobinfo.split("\t")
                if len(jobinfolist) >= 8:
                    submit_date_str = jobinfolist[0]
                    numseq = int(jobinfolist[3])
                    jobname = jobinfolist[5]
                    email = jobinfolist[6]
                    method_submission = jobinfolist[7]

            isValidStartDate = True
            isValidFailedDate = True
            isValidSubmitDate = True

            try:
                submit_date = datetime_str_to_time(submit_date_str)
            except ValueError:
                isValidSubmitDate = False

            start_date_str = ""
            if os.path.exists(starttagfile):
                start_date_str = myfunc.ReadFile(starttagfile).strip()
            try:
                start_date = datetime_str_to_time(start_date_str)
            except ValueError:
                isValidStartDate = False
            failed_date_str = myfunc.ReadFile(failedtagfile).strip()
            try:
                failed_date = datetime_str_to_time(failed_date_str)
            except ValueError:
                isValidFailedDate = False

            queuetime = ""
            runtime = ""

            if isValidStartDate and isValidFailedDate:
                runtime = myfunc.date_diff(start_date, failed_date)
            if isValidSubmitDate and isValidStartDate:
                queuetime = myfunc.date_diff(submit_date, start_date)

            row_content = [rank, jobid, jobname[:20], str(numseq), email,
                    queuetime, runtime, submit_date_str, method_submission]
            if info['isSuperUser']:
                row_content.insert(5, ip)
            jobinfo_list.append(row_content)

        info['content'] = jobinfo_list

    info['jobcounter'] = GetJobCounter(info)
    return info
#}}}

def get_countjob_country(request, g_params):#{{{
    info = {}
    set_basic_config(request, info, g_params)

    path_stat = "%s/log/stat"%(g_params['path_static'])

    countjob_by_country = "%s/countjob_by_country.txt"%(path_stat)
    lines = myfunc.ReadFile(countjob_by_country).split("\n")
    li_countjob_country = []
    for line in lines: 
        if not line or line[0]=="#":
            continue
        strs = line.split("\t")
        if len(strs) >= 4:
            country = strs[0]
            try:
                numseq = int(strs[1])
            except:
                numseq = 0
            try:
                numjob = int(strs[2])
            except:
                numjob = 0
            try:
                numip = int(strs[3])
            except:
                numip = 0
            li_countjob_country.append([country, numseq, numjob, numip])
    li_countjob_country_header = ["Country", "Numseq", "Numjob", "NumIP"]

    info['li_countjob_country'] = li_countjob_country
    info['li_countjob_country_header'] = li_countjob_country_header

    info['jobcounter'] = GetJobCounter(info)
    return info
#}}}
def get_help(request, g_params):#{{{
    info = {}
    set_basic_config(request, info, g_params)
    configfile = "%s/config/config.json"%(g_params['SITE_ROOT'])
    config = {}
    if os.path.exists(configfile):
        text = myfunc.ReadFile(configfile)
        try:
            config = json.loads(text)
        except:
            config = {}
    try:
        MAX_KEEP_DAYS = config['qd_fe']['MAX_KEEP_DAYS']
    except KeyError:
        MAX_KEEP_DAYS = 30
        pass
    info['MAX_KEEP_DAYS'] = MAX_KEEP_DAYS
    info['jobcounter'] = GetJobCounter(info)
    return info
#}}}
def get_news(request, g_params):#{{{
    info = {}
    set_basic_config(request, info, g_params)

    newsfile = "%s/%s/%s"%(g_params['SITE_ROOT'], "static/doc", "news.txt")
    newsList = []
    if os.path.exists(newsfile):
        newsList = myfunc.ReadNews(newsfile)
    info['newsList'] = newsList
    info['newsfile'] = newsfile
    info['jobcounter'] = GetJobCounter(info)
    return info
#}}}
def help_wsdl_api(request, g_params):#{{{
    info = {}
    set_basic_config(request, info, g_params)
    api_script_rtname =  g_params['api_script_rtname']
    extlist = [".py"]
    api_script_lang_list = ["Python"]
    api_script_info_list = []

    for i in range(len(extlist)):
        ext = extlist[i]
        api_script_file = "%s/%s/%s"%(g_params['SITE_ROOT'],
                "static/download/script", "%s%s"%(api_script_rtname,
                    ext))
        api_script_basename = os.path.basename(api_script_file)
        if not os.path.exists(api_script_file):
            continue
        cmd = [api_script_file, "-h"]
        try:
            usage = subprocess.check_output(cmd, encoding='UTF-8')
        except subprocess.CalledProcessError as e:
            usage = ""
        api_script_info_list.append([api_script_lang_list[i], api_script_basename, usage])

    info['api_script_info_list'] = api_script_info_list
    info['jobcounter'] = GetJobCounter(info)
    return info
#}}}
def get_serverstatus(request, g_params):#{{{
    info = {}
    set_basic_config(request, info, g_params)
    path_log = os.path.join(g_params['SITE_ROOT'], 'static/log')
    path_result = os.path.join(g_params['SITE_ROOT'], 'static/result')
    path_stat = os.path.join(path_log, "stat")

    logfile_finished =  os.path.join(path_log, "finished_job.log")
    logfile_runjob =  os.path.join(path_log, "runjob_log.log")
    logfile_country_job = os.path.join(path_log, "stat", "country_job_numseq.txt")


# get jobs queued locally (at the front end)
    num_seq_in_local_queue = 0

    if 'isShowLocalQueue' in g_params and g_params['isShowLocalQueue']:
        cmd = [g_params['suq_exec'], "-b", g_params['suq_basedir'], "ls"]
        cmdline = " ".join(cmd)
        try:
            suq_ls_content =  subprocess.check_output(cmd, encoding='UTF-8', stderr=subprocess.STDOUT)
            lines = suq_ls_content.split('\n')
            cntjob = 0
            for line in lines:
                if line.find("runjob") != -1:
                    cntjob += 1
            num_seq_in_local_queue = cntjob
        except subprocess.CalledProcessError as e:
            loginfo("Run '%s' exit with error message: %s"%(cmdline, str(e)), g_params['gen_errfile'])

# get jobs queued remotely ()
    runjob_dict = {}
    if os.path.exists(logfile_runjob):
        runjob_dict = myfunc.ReadRunJobLog(logfile_runjob)
    cntseq_in_remote_queue = 0
    for jobid in runjob_dict:
        li = runjob_dict[jobid]
        numseq = li[4]
        rstdir = "%s/%s"%(path_result, jobid)
        finished_idx_file = "%s/finished_seqindex.txt"%(rstdir)
        if os.path.exists(finished_idx_file):
            num_finished = len(myfunc.ReadIDList(finished_idx_file))
        else:
            num_finished = 0

        cntseq_in_remote_queue += (numseq - num_finished)


# get number of finished seqs
    allfinishedjoblogfile = os.path.join(path_log, "all_finished_job.log")
    allfinished_job_dict = {}
    user_dict = {} # by IP
    if os.path.exists(allfinishedjoblogfile):
        allfinished_job_dict = myfunc.ReadFinishedJobLog(allfinishedjoblogfile)
    total_num_finished_seq = 0
    numjob_wed = 0
    numjob_wsdl = 0
    startdate = ""
    submitdatelist = []
    iplist = []
    countrylist = []
    for jobid in allfinished_job_dict:
        li = allfinished_job_dict[jobid]
        try:
            numseq = int(li[4])
        except:
            numseq = 1
        try:
            submitdatelist.append(li[6])
        except:
            pass
        try:
            method_submission = li[5]
        except:
            method_submission = ""
        try:
            iplist.append(li[2])
        except:
            pass
        ip = ""
        try:
            ip = li[2]
        except:
            pass


        if method_submission == "web":
            numjob_wed += 1
        elif method_submission == "wsdl":
            numjob_wsdl += 1

        if ip != "" and ip != "All" and ip != "127.0.0.1":

            if not ip in user_dict:
                user_dict[ip] = [0,0] #[num_job, num_seq]
            user_dict[ip][0] += 1
            user_dict[ip][1] += numseq

        total_num_finished_seq += numseq

    submitdatelist = sorted(submitdatelist, reverse=False)
    if len(submitdatelist)>0:
        startdate = submitdatelist[0].split()[0]

    uniq_iplist = list(set(iplist))

    countjob_by_country = "%s/countjob_by_country.txt"%(path_stat)
    lines = myfunc.ReadFile(countjob_by_country, mode='r', encoding='utf-8').split("\n")
    li_countjob_country = []
    countrylist = []
    for line in lines: 
        if not line or line[0]=="#":
            continue
        strs = line.split("\t")
        if len(strs) >= 4:
            country = strs[0]
            try:
                numseq = int(strs[1])
            except:
                numseq = 0
            try:
                numjob = int(strs[2])
            except:
                numjob = 0
            try:
                numip = int(strs[3])
            except:
                numip = 0
            li_countjob_country.append([country, numseq, numjob, numip])
            countrylist.append(country)
    uniq_countrylist = list(set(countrylist))

    li_countjob_country_header = ["Country", "Numseq", "Numjob", "NumIP"]

    # get most active users by num_job
    activeuserli_njob_header = ["IP", "Country", "NumJob", "NumSeq"]
    activeuserli_njob = []
    rawlist = sorted(list(user_dict.items()), key=lambda x:x[1][0], reverse=True)
    cnt = 0
    for i in range(len(rawlist)):
        cnt += 1
        ip = rawlist[i][0]
        njob = rawlist[i][1][0]
        nseq = rawlist[i][1][1]
        country = "N/A"
        try:
            match = geolite2.lookup(ip)
            country = pycountry.countries.get(alpha_2=match.country).name
        except:
            pass
        activeuserli_njob.append([anonymize_ip_v4(ip), country, njob, nseq])
        if cnt >= g_params['MAX_ACTIVE_USER']:
            break

    # get most active users by num_seq
    activeuserli_nseq_header = ["IP", "Country", "NumJob", "NumSeq"]
    activeuserli_nseq = []
    rawlist = sorted(list(user_dict.items()), key=lambda x:x[1][1], reverse=True)
    cnt = 0
    for i in range(len(rawlist)):
        cnt += 1
        ip = rawlist[i][0]
        njob = rawlist[i][1][0]
        nseq = rawlist[i][1][1]
        country = "N/A"
        try:
            match = geolite2.lookup(ip)
            country = pycountry.countries.get(alpha_2=match.country).name
        except:
            pass
        activeuserli_nseq.append([anonymize_ip_v4(ip), country, njob, nseq])
        if cnt >= g_params['MAX_ACTIVE_USER']:
            break

# get longest predicted seq
# get query with most TM helics
# get query takes the longest time
    extreme_runtimelogfile = "%s/stat/extreme_jobruntime.log"%(path_log)
    runtimelogfile = "%s/jobruntime.log"%(path_log)
    infile_runtime = runtimelogfile
    if os.path.exists(extreme_runtimelogfile) and os.path.getsize(extreme_runtimelogfile):
        infile_runtime = extreme_runtimelogfile

    li_longestseq = []
    li_mostTM = []
    li_longestruntime = []
    longestlength = -1
    mostTM = -1
    longestruntime = -1.0

    hdl = myfunc.ReadLineByBlock(infile_runtime)
    if not hdl.failure:
        lines = hdl.readlines()
        while lines != None:
            for line in lines:
                strs = line.split()
                if len(strs) < 8:
                    continue
                runtime = -1.0
                jobid = strs[0]
                seqidx = strs[1]
                try:
                    runtime = float(strs[3])
                except:
                    pass
                numTM = -1
                try:
                    numTM = int(strs[6])
                except:
                    pass
                mtd_profile = strs[4]
                lengthseq = -1
                try:
                    lengthseq = int(strs[5])
                except:
                    pass
                if runtime > longestruntime:
                    li_longestruntime = [jobid, seqidx, runtime, lengthseq, numTM]
                    longestruntime = runtime
                if lengthseq > longestlength:
                    li_longestseq = [jobid, seqidx, runtime, lengthseq, numTM]
                    longestlength = lengthseq
                if numTM > mostTM:
                    mostTM = numTM
                    li_mostTM = [jobid, seqidx, runtime, lengthseq, numTM]
            lines = hdl.readlines()
        hdl.close()

    info['longestruntime_str'] = myfunc.second_to_human(int(longestruntime+0.5))
    info['mostTM_str'] = str(mostTM)
    info['longestlength_str'] = str(longestlength)
    info['total_num_finished_seq'] = total_num_finished_seq
    info['total_num_finished_job'] = len(allfinished_job_dict)
    info['num_unique_ip'] = len(uniq_iplist)
    info['num_unique_country'] = len(uniq_countrylist)
    info['num_finished_seqs_str'] = str(info['total_num_finished_seq'])
    info['num_finished_jobs_str'] = str(info['total_num_finished_job'])
    info['num_finished_jobs_web_str'] = str(numjob_wed)
    info['num_finished_jobs_wsdl_str'] = str(numjob_wsdl)
    info['num_unique_ip_str'] = str(info['num_unique_ip'])
    info['num_unique_country_str'] = str(info['num_unique_country'])
    info['num_seq_in_local_queue'] = num_seq_in_local_queue
    info['num_seq_in_remote_queue'] = cntseq_in_remote_queue
    info['activeuserli_nseq_header'] = activeuserli_nseq_header
    info['activeuserli_njob_header'] = activeuserli_njob_header
    info['li_countjob_country_header'] = li_countjob_country_header
    info['li_countjob_country'] = li_countjob_country
    info['activeuserli_njob_header'] = activeuserli_njob_header
    info['activeuserli_nseq'] = activeuserli_nseq
    info['activeuserli_njob'] = activeuserli_njob
    info['li_longestruntime'] = li_longestruntime
    info['li_longestseq'] = li_longestseq
    info['li_mostTM'] = li_mostTM

    info['startdate'] = startdate
    info['jobcounter'] = GetJobCounter(info)
    return info
#}}}
def get_results_eachseq(request, name_resultfile, name_nicetopfile, jobid, seqindex, g_params):#{{{
    """base function for get_results_eachseq
    """
    resultdict = {}
    set_basic_config(request, resultdict, g_params)

    rstdir = "%s/%s"%(g_params['path_result'], jobid)
    outpathname = jobid

    jobinfofile = "%s/jobinfo"%(rstdir)
    jobinfo = myfunc.ReadFile(jobinfofile).strip()
    jobinfolist = jobinfo.split("\t")
    if len(jobinfolist) >= 8:
        submit_date_str = jobinfolist[0]
        numseq = int(jobinfolist[3])
        jobname = jobinfolist[5]
        email = jobinfolist[6]
        method_submission = jobinfolist[7]
    else:
        submit_date_str = ""
        numseq = 1
        jobname = ""
        email = ""
        method_submission = "web"

    status = ""

    resultdict['jobid'] = jobid
    resultdict['jobname'] = jobname
    resultdict['outpathname'] = os.path.basename(outpathname)
    resultdict['BASEURL'] = g_params['BASEURL']
    resultdict['status'] = status
    resultdict['numseq'] = numseq
    base_www_url = get_url_scheme(request) + request.META['HTTP_HOST']

    resultfile = "%s/%s/%s/%s"%(rstdir, outpathname, seqindex, name_resultfile)
    if os.path.exists(resultfile):
        resultdict['resultfile'] = os.path.basename(resultfile)
    else:
        resultdict['resultfile'] = ""

    # get prediction results for the first seq
    topfolder_seq0 = "%s/%s/%s"%(rstdir, jobid, seqindex)
    subdirname = seqindex
    resultdict['subdirname'] = subdirname
    nicetopfile = "%s/%s"%(topfolder_seq0, name_nicetopfile)
    if os.path.exists(nicetopfile):
        resultdict['nicetopfile'] = "%s/%s/%s/%s/%s"%(
                "result", jobid, jobid, subdirname,
                os.path.basename(nicetopfile))
    else:
        resultdict['nicetopfile'] = ""
    resultdict['isResultFolderExist'] = False
    if os.path.exists(topfolder_seq0):
        resultdict['isResultFolderExist'] = True

    resultdict['jobcounter'] = GetJobCounter(resultdict)
    return resultdict
#}}}
