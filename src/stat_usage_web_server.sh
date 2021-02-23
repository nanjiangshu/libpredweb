#!/usr/bin/env bash

# Filename: 
# Description:  usage analysis of web-servers
# Author: Nanjiang Shu (nanjiang.shu@scilifelab.se)

set -e
#set -x
progname=`basename $0`
size_progname=${#progname}
wspace=`printf "%*s" $size_progname ""`
usage="
Usage:  $progname method [method ...]
Options:
  -o       OUTFILE  Set output file
  -start-date STR   Set the start date for analysis, in the format 2015-01-01
  -end-date   STR   Set the end date for analysis
  -onlydata         Show only data, no comments
  -special-country  Show number of users for specified countries
  -out-numuser-country FILE
                    Output number of users per country
  -h, --help        Print this help message and exit

Created 2017-05-03, updated 2021-02-23, Nanjiang Shu
"

declare -A ratioEGI=( ["topcons2"]="0.5" ["scampi2"]="0.2" ["proq3"]="0.3" ["pconsc3"]="1.0" ["boctopus2"]="0.25" ["subcons"]="0.25" ["prodres"]="1.0" ) 

UsageAna(){  #{{{
    local method=$1
    local startdate=$2
    local enddate=$3
    case $method in 
        topcons2|proq3|pconsc3|subcons|prodres|scampi2|boctopus2)
            #infile1=/var/www/html/$method/proj/pred/static/log/all_submitted_seq.log
            #infile2=/var/www/html/$method/proj/pred/static/log/submitted_seq.log
            infile1=/big/server/var/www/web_$method/proj/pred/static/log/all_submitted_seq.log
            infile2=/big/server/var/www/web_$method/proj/pred/static/log/submitted_seq.log
            if [ -f $infile1 ]; then
                infile=$infile1
            else
                infile=$infile2
            fi
            uniqiplistfile=$tmpdir/uniqiplist.$method.txt
            anafile=$tmpdir/uniqidlist.$method.ana.txt
            numseqfile=$tmpdir/$method.numseq.txt
            awk -F "\t" -v d1=$startdate -v d2=$enddate '{ip=$3; split($1,ss," "); date=ss[1]; gsub(/-/, "", date); if(date>=d1 && date<=d2) {print ip}}' $infile | sort -u > $uniqiplistfile

            awk -F "\t" -v d1=$startdate -v d2=$enddate '{ip=$4; split($1,ss," "); date=ss[1]; gsub(/-/, "", date); if(date>=d1 && date<=d2) {print ip}}' $infile > $numseqfile

            my_ip2country.py -l  $uniqiplistfile -show-eu > $anafile


            numJob=$(cat $numseqfile |wc -l)
            numSeq=$(cat $numseqfile | awk 'BEGIN{sum=0}{sum+=$1}END{print sum}')
            numUser=$(awk -F "\t" '{print $1}' $anafile | sort -u |wc -l )
            numCountry=$(awk -F "\t" '{print $2}' $anafile | sort -u |wc -l )
            numUserEU=$(awk -F "\t" '{if ($3=="EU") print $2}' $anafile | wc -l )
            #echo -e "#Method\tNumUser\tNumCountry\tNumUser_EU\tPercentEU"
            if [ $numUser -eq 0 ];then
                percentEU=0
            else
                percentEU=$(python -c "print (float($numUserEU)/$numUser*100)")
            fi

            printf "%-9s %8d %10d %10d %10.1f" $method $numUser $numCountry $numUserEU $percentEU

            for ((ic=0; ic<numSpecialCountry; ic++));do
                country=${specialCountryList[$ic]}
                numUserCountry=$(awk -v country=${country} -F "\t" '{if ($2==country) print $2}' $anafile | wc -l )
                printf " %15d" $numUserCountry
            done
            printf " %10d %10d %6s\n" $numJob $numSeq ${ratioEGI[$method]}

            # output number of users (counted as unique IP) 
            if [ "$outfile_numuser_country" != "" ]; then
                awk -F"\t" '{print $2}' $anafile | sort | uniq -c | sort -nr | awk -v method=$method '{ss=$2; for (i=3; i<=NF; i++) {ss=ss" "$i}; printf("%s\t%s\t%s\n", method, ss, $1)}' >> $outfile_numuser_country

            fi
            ;;
    esac
} #}}}

if [ $# -lt 1 ]; then
    echo "$usage"
    exit
fi

methodList=()
startdate=19000000
enddate=22000000
isShowOnlyData=0
isAnaPerMonth=0
specialCountryList=()
outfile_numuser_country=

tmpdir=$(mktemp -d /tmp/tmpdir.stat_usage_web_server.XXXXXXXXX) || { echo "Failed to create temp dir" >&2; exit 1; }

trap 'rm -rf "$tmpdir"' INT TERM EXIT


isNonOptionArg=0
while [ "$1" != "" ]; do
    if [ $isNonOptionArg -eq 1 ]; then 
        methodList+=("$1")
        isNonOptionArg=0
    elif [ "$1" == "--" ]; then
        isNonOptionArg=true
    elif [ "${1:0:1}" == "-" ]; then
        case $1 in
            -h | --help) echo "$usage"; exit;;
            -start-date|--start-date) startdate=$2;shift;;
            -end-date|--end-date) enddate=$2;shift;;
            -onlydata|--onlydata) isShowOnlyData=1;;
            -permonth|--permonth) isAnaPerMonth=1;;
            -spc|--special-country) specialCountryList+=("$2");shift;;
            -out-numuser-country) outfile_numuser_country=$2;shift;;
            -q|-quiet|--quiet) isQuiet=1;;
            -*) echo Error! Wrong argument: $1 >&2; exit;;
        esac
    else
        methodList+=("$1")
    fi
    shift
done



numMethod=${#methodList[@]}
numSpecialCountry=${#specialCountryList[@]}
if [ $numMethod -eq 0  ]; then
    echo Input not set! Exit. >&2
    exit 1
fi

if [ $isShowOnlyData -eq 0 ];then
    echo -e "Web server usage statistics for the period $startdate to $enddate \n"
fi

if [ "$outfile_numuser_country" != "" ];then
    cat /dev/null > $outfile_numuser_country
fi

if [ $isAnaPerMonth -eq 0 ];then
    startdate=${startdate//-/}
    enddate=${enddate//-/}
    printf "%-9s %8s %10s %10s %10s" "#Method" "NumUser" "NumCountry" "NumUserEU" "PercentEU" 
    for ((ic=0; ic< numSpecialCountry; ic++));do
        country=${specialCountryList[$ic]}
        printf " %15s" "NUser$country"
    done
    printf " %10s %10s %6s\n" "NumJob" "NumSeq" "RatioEGI"
    for ((im=0;im<numMethod;im++));do
        method=${methodList[$im]}
        UsageAna "$method" $startdate $enddate
    done
else
    : #permonth analysis not implemented
fi

if [ $isShowOnlyData -eq 0 ];then
    echo """
#==========================================================================================
NumUser     NumUser is calculated as the unique IP address the job is submitted from
NumCountry  Number of countries the users are from
NumUserEU   Number of users from the European countries
PercentEU   Percentage of users that are from the European countries out of the whole world
NumJob      Number of jobs submitted to the server
NumSeq      Number of sequences (queries) submitted to the server, one job can have multiple sequences.
"""
fi
