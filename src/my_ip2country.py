#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Description: For Python 3+
import os
import sys

from geoip import geolite2
import pycountry
from libpredweb import myfunc


progname =  os.path.basename(sys.argv[0])
wspace = ''.join([" "]*len(progname))

usage_short="""
Usage: %s IP [IP ...] [-o OUTFILE]
"""%(progname)

usage_ext="""
Description:

OPTIONS:
  -o OUTFILE    Output the result to OUTFILE
  -l LISTFILE   Set the listfile with IPs
  -show-eu      Show whether it is a european country
  -q            Quiet mode
  -h, --help    Print this help message and exit

Created 2016-01-28, updated 2020-07-11, Nanjiang Shu 
"""
usage_exp="""
Examples:
"""

eu_country_list = [#{{{
"Austria",
"Belgium",
"Bulgaria",
"Croatia",
"Cyprus",
"Czech Republic",
"Denmark",
"Estonia",
"Finland",
"France",
"Germany",
"Greece",
"Hungary",
"Ireland",
"Italy",
"Latvia",
"Lithuania",
"Luxembourg",
"Malta",
"Netherlands",
"Poland",
"Portugal",
"Romania",
"Slovakia",
"Slovenia",
"Spain",
"Sweden"
]
#}}}
all_european_country_list = [#{{{
"Russia",
"Germany",
"France",
"United Kingdom",
"Italy",
"Spain",
"Ukraine",
"Poland",
"Romania",
"Netherlands",
"Belgium",
"Greece",
"Portugal",
"Czech Republic",
"Hungary",
"Sweden",
"Belarus",
"Austria",
"Switzerland",
"Bulgaria",
"Serbia",
"Denmark",
"Finland",
"Slovakia",
"Norway",
"Ireland",
"Croatia",
"Bosnia and Herzegovina",
"Moldova",
"Lithuania",
"Albania",
"Macedonia",
"Slovenia",
"Latvia",
"Kosovo",
"Estonia",
"Montenegro",
"Luxembourg",
"Malta",
"Iceland",
"Jersey",
"Isle of Man",
"Andorra",
"Guernsey",
"Faroe Islands",
"Liechtenstein",
"Monaco",
"San Marino",
"Gibraltar",
"Aland Islands",
"Svalbard and Jan Mayen",
"Vatican City"
        ]#}}}
eu_country_set = set(eu_country_list)
all_european_country_set = set(all_european_country_list)

def PrintHelp(fpout=sys.stdout):#{{{
    print(usage_short, file=fpout)
    print(usage_ext, file=fpout)
    print(usage_exp, file=fpout)#}}}

def IP2Country(ipList, fpout):#{{{
    for ip in ipList:
        country = "N/A"
        try:
            match = geolite2.lookup(ip)
            country = pycountry.countries.get(alpha_2=match.country).name
        except:
            pass
        fpout.write("%s\t%s"%(ip, country.encode('utf-8').decode()))
        if g_params['isShowEU']:
            if country in all_european_country_set:
                fpout.write("\tEU")
            else:
                fpout.write("\tnon-EU")

        fpout.write("\n")
#}}}
def main(g_params):#{{{
    argv = sys.argv
    numArgv = len(argv)
    if numArgv < 2:
        PrintHelp()
        return 1

    outpath = "./"
    outfile = ""
    ipListFile = ""
    ipList = []

    i = 1
    isNonOptionArg=False
    while i < numArgv:
        if isNonOptionArg == True:
            ipList.append(argv[i])
            isNonOptionArg = False
            i += 1
        elif argv[i] == "--":
            isNonOptionArg = True
            i += 1
        elif argv[i][0] == "-":
            if argv[i] in ["-h", "--help"]:
                PrintHelp()
                return 1
            elif argv[i] in ["-o", "--o", "-outfile"]:
                (outfile, i) = myfunc.my_getopt_str(argv, i)
            elif argv[i] in ["-outpath", "--outpath"]:
                (outpath, i) = myfunc.my_getopt_str(argv, i)
            elif argv[i] in ["-l", "--l"] :
                (ipListFile, i) = myfunc.my_getopt_str(argv, i)
            elif argv[i] in ["-q", "--q"]:
                g_params['isQuiet'] = True
                i += 1
            elif argv[i] in ["-show-eu", "--show-eu"]:
                g_params['isShowEU'] = True
                i += 1
            else:
                print("Error! Wrong argument:", argv[i], file=sys.stderr)
                return 1
        else:
            ipList.append(argv[i])
            i += 1


    if ipListFile != "":
        ipList += myfunc.ReadIDList(ipListFile)
    fpout = myfunc.myopen(outfile, sys.stdout, "w", False)

    IP2Country(ipList, fpout)

    myfunc.myclose(fpout)

#}}}

def InitGlobalParameter():#{{{
    g_params = {}
    g_params['isQuiet'] = True
    g_params['isShowEU'] = False
    return g_params
#}}}
if __name__ == '__main__' :
    g_params = InitGlobalParameter()
    sys.exit(main(g_params))
