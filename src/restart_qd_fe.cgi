#!/usr/bin/perl -w
# get set suq ntask
#Created 2015-03-20, updated 2015-03-20, Nanjiang Shu
use CGI qw(:standard);
use CGI qw(:cgi-lib);
use CGI qw(:upload);

use Cwd 'abs_path';
use File::Basename;
my $rundir = dirname(abs_path(__FILE__));
my $python = abs_path("$rundir/python");
# at proj
my $basedir = abs_path("$rundir/../../proj/pred");
my $auth_ip_file = "$basedir/config/auth_iplist.txt";#ip address which allows to run cgi script
my $name_targetprog = "qd_fe.py";
my $path_targetprog = "$basedir/app/$name_targetprog";
$path_targetprog = abs_path($path_targetprog);
my $progname = basename(__FILE__);

print header();
print start_html(-title => "restart $name_targetprog",
    -author => "nanjiang.shu\@scilifelab.se",
    -meta   => {'keywords'=>''});

if(!param())
{
    my $remote_host = $ENV{'REMOTE_ADDR'};
    my @auth_iplist = ();
    if (open(IN, "<", $auth_ip_file)){
        while(<IN>) {
            chomp;
            push @auth_iplist, $_;
        }
    } else {
        close IN;
    }

    print "Host IP: $remote_host\n\n";
    if (grep { $_ eq $remote_host } @auth_iplist) {
        print "<pre>";
        print "Host IP: $remote_host\n\n";
        print "Already running daemons:\n";
        my $already_running=`ps aux | grep  "$path_targetprog" | grep -v grep | grep -v archive_logfile | grep -v vim ` ;
        my $num_already_running = `echo "$already_running" | grep "$path_targetprog" | wc -l`;
        chomp($num_already_running);
        print $already_running;
        print "num_already_running=$num_already_running\n";
        if ($num_already_running > 0){
            my $ps_info = `ps aux  | grep "$path_targetprog" | grep -v grep | grep -v vim | grep -v archive_logfile`;
            my @lines = split('\n', $ps_info);
            my @pidlist = ();
            foreach my $line  (@lines){
                chomp($line);
                my @fields = split(/\s+/, $line);
                if (scalar @fields > 2 && $fields[1] =~ /[0-9]+/){
                    push (@pidlist, $fields[1]);
                }
            }
            print "\n\nkilling....";
            foreach my $pid (@pidlist){
                print "kill -9 $pid\n";
                system("kill -9 $pid");
            }
        }
        print "\n\nStarting up...";
        my $logfile = "$basedir/static/log/$progname.log";
        system("$python $path_targetprog >> $logfile 2>&1 &");

        $already_running=`ps aux | grep  "$path_targetprog" | grep -v vim | grep -v grep | grep -v archive_logfile `;
        print "\n\nupdated running daemons:\n";
        print $already_running;
        print "\n$path_targetprog restarted\n";

        print "</pre>";
    }else{
        print "Permission denied for the host $remote_host!\n";
    }

    print '<br>';
    print end_html();
}

