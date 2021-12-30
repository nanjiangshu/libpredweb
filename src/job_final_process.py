#!/usr/bin/env python

import sys
import os
import shutil
import argparse

import time
from libpredweb import myfunc
from libpredweb import webserver_common as webcom

progname=os.path.basename(sys.argv[0])
rootname_progname = os.path.splitext(progname)[0]
lockname = os.path.realpath(__file__).replace(" ", "").replace("/", "-")
import fcntl

def JobFinalProcess(g_params):#{{{
    """Run final process of a job
    """
    jobid = g_params['jobid']
    numseq = g_params['numseq']
    to_email = g_params['to_email']
    gen_logfile = g_params['gen_logfile']
    gen_errfile = g_params['gen_errfile']
    name_server = g_params['name_server']

    webcom.loginfo("JobFinalProcess for %s.\n" %(jobid), gen_logfile)

    path_static = g_params['path_static']
    path_result = os.path.join(path_static, 'result')
    path_log = os.path.join(path_static, 'log')
    path_cache = g_params['path_cache']
    contact_email = g_params['contact_email']

    rstdir = "%s/%s"%(path_result, jobid)
    runjob_errfile = "%s/%s"%(rstdir, "runjob.err")
    runjob_logfile = "%s/%s"%(rstdir, "runjob.log")
    tmpdir = "%s/tmpdir"%(rstdir)
    outpath_result = "%s/%s"%(rstdir, jobid)
    finished_idx_file = "%s/finished_seqindex.txt"%(rstdir)
    failed_idx_file = "%s/failed_seqindex.txt"%(rstdir)
    seqfile = "%s/query.fa"%(rstdir)

    base_www_url_file = "%s/base_www_url.txt"%(path_log)
    base_www_url = ""

    finished_idx_list = []
    failed_idx_list = []
    if os.path.exists(finished_idx_file):
        finished_idx_list = list(set(myfunc.ReadIDList(finished_idx_file)))
    if os.path.exists(failed_idx_file):
        failed_idx_list = list(set(myfunc.ReadIDList(failed_idx_file)))

    finishtagfile = "%s/%s"%(rstdir, "runjob.finish")
    failedtagfile = "%s/%s"%(rstdir, "runjob.failed")
    starttagfile = "%s/%s"%(rstdir, "runjob.start")

    num_processed = len(finished_idx_list)+len(failed_idx_list)
    finish_status = "" #["success", "failed", "partly_failed"]
    if num_processed >= numseq:# finished
        if len(failed_idx_list) == 0:
            finish_status = "success"
        elif len(failed_idx_list) >= numseq:
            finish_status = "failed"
        else:
            finish_status = "partly_failed"

        if os.path.exists(base_www_url_file):
            base_www_url = myfunc.ReadFile(base_www_url_file).strip()
        if base_www_url == "":
            base_www_url = webcom.get_default_server_url(name_server.lower())

        date_str_epoch_now = time.time()

        # Now write the text output to a single file
        statfile = "%s/%s"%(outpath_result, "stat.txt")
        resultfile_text = "%s/%s"%(outpath_result, "query.result.txt")
        resultfile_html = "%s/%s"%(outpath_result, "query.result.html")
        if name_server.lower() == 'pconsc3':
            resultfile_text = os.path.join(outpath_result, "query.pconsc3.txt")
        elif name_server.lower() == "boctopus2":
            resultfile_text = os.path.join(outpath_result, "query.top")
        elif name_server.lower() == "predzinc":
            resultfile_text = os.path.join(outpath_result, "query.predzinc.txt")
        elif name_server.lower() == "frag1d":
            resultfile_text = os.path.join(outpath_result, "query.frag1d.txt")

        (seqIDList, seqAnnoList, seqList) = myfunc.ReadFasta(seqfile)
        maplist = []
        for i in range(len(seqIDList)):
            maplist.append("%s\t%d\t%s\t%s"%("seq_%d"%i, len(seqList[i]),
                seqAnnoList[i].replace('\t', ' '), seqList[i]))
        start_date_str = myfunc.ReadFile(starttagfile).strip()
        start_date_epoch = webcom.datetime_str_to_epoch(start_date_str)
        all_runtime_in_sec = float(date_str_epoch_now) - float(start_date_epoch)

        finishtagfile_result = "%s/%s"%(rstdir, "write_result_finish.tag")
        if not os.path.exists(finishtagfile_result):
            msg =  "Dump result to file %s ..."%(resultfile_text)
            webcom.loginfo(msg, gen_logfile)
            webcom.WriteDumpedTextResultFile(name_server.lower(), resultfile_text, outpath_result, maplist,
                    all_runtime_in_sec, base_www_url, statfile)

        if name_server.lower() == "topcons2":
            finishtagfile_resulthtml = "%s/%s"%(rstdir, "write_htmlresult_finish.tag")
            if not os.path.exists(finishtagfile_resulthtml):
                webcom.loginfo("Write HTML table to %s ..."%(resultfile_html), gen_logfile)
                webcom.WriteHTMLResultTable_TOPCONS(resultfile_html, finished_seq_file)

        # note that zip rq will zip the real data for symbolic links
        zipfile = "%s.zip"%(jobid)
        zipfile_fullpath = "%s/%s"%(rstdir, zipfile)
        os.chdir(rstdir)
        is_zip_success = True
        cmd = ["zip", "-rq", zipfile, jobid]

        finishtagfile_zipfile = "%s/%s"%(rstdir, "write_zipfile_finish.tag")
        if not os.path.exists(finishtagfile_zipfile):
            (is_zip_success, t_runtime) = webcom.RunCmd(cmd, runjob_logfile, runjob_errfile)
            if is_zip_success:
                webcom.WriteDateTimeTagFile(finishtagfile_zipfile, runjob_logfile, runjob_errfile)

        if len(failed_idx_list)>0:
            webcom.WriteDateTimeTagFile(failedtagfile, runjob_logfile, runjob_errfile)

        if is_zip_success:
            webcom.WriteDateTimeTagFile(finishtagfile, runjob_logfile, runjob_errfile)

        if finish_status == "success":
            shutil.rmtree(tmpdir)

        # send the result to to_email
        from_email = webcom.get_email_address_outsending(name_server.lower())
        if webcom.IsFrontEndNode(base_www_url) and myfunc.IsValidEmailAddress(to_email):
            webcom.SendEmail_on_finish(jobid, base_www_url,
                    finish_status, name_server=name_server, from_email=from_email,
                    to_email=to_email, contact_email=contact_email,
                    logfile=runjob_logfile, errfile=runjob_errfile)
        webcom.CleanJobFolder(rstdir, name_server.lower())

#}}}

def main(g_params):# {{{
    parser = argparse.ArgumentParser(
            description='Run final process of a given job',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog='''\
Created 2021-12-28, updated 2021-12-29, Nanjiang Shu

Examples:
    %s -i job_final_process.json
'''%(sys.argv[0]))
    parser.add_argument('-i' , metavar='JSONFILE', dest='jsonfile',
            type=str, required=True,
            help='Provide the Json file with all parameters')
    parser.add_argument('-v', dest='verbose', nargs='?', type=int, default=0, const=1, 
            help='show verbose information, (default: 0)')

    args = parser.parse_args()

    jsonfile = args.jsonfile
    verbose = args.verbose

    if not os.path.exists(jsonfile):
        print("Jsonfile %s does not exist. Exit %s!"%(jsonfile, progname), file=sys.stderr);
        return 1

    g_params.update(webcom.LoadJsonFromFile(jsonfile))

    lockname = "job_final_process.lock"
    lock_file = os.path.join(g_params['path_result'], g_params['jobid'], lockname)
    g_params['lockfile'] = lock_file
    fp = open(lock_file, 'w')
    try:
        fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        webcom.loginfo("Another instance of %s is running"%(progname), g_params['gen_logfile'])
        return 1

    if 'DEBUG_LOCK_FILE' in g_params and g_params['DEBUG_LOCK_FILE']:
        time.sleep(g_params['SLEEP_INTERVAL']*6)
    return JobFinalProcess(g_params)
# }}}

def InitGlobalParameter():#{{{
    g_params = {}
    g_params['lockfile'] = ""
    return g_params
#}}}
if __name__ == '__main__' :
    g_params = InitGlobalParameter()
    status = main(g_params)
    if os.path.exists(g_params['lockfile']):
        try:
            os.remove(g_params['lockfile'])
        except:
            webcom.loginfo("Failed to delete lockfile %s\n"%(g_params['lockfile']), g_params['gen_logfile'])
    sys.exit(status)
