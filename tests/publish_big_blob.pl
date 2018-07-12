#!/usr/bin/perl
use 5.16.1;
use utf8;
binmode $_, ':utf8' for *STDOUT, *STDERR;
#use open qw(:utf8 :std);
use String::CRC32;
use Digest::MD5 qw(md5_hex);
use constant DFLT_JSON_FILE => 'data/cache2.json';
#use lib 'lib1';
use constant DFLT_COUNT_SUBS => 8;
use Redis::BCStation;
use Time::HiRes qw(time);
use File::Map qw(map_file);
use File::Slurp qw(read_file);
use Tag::DeCoder;
use Log4perl::KISS;
use Mojo::IOLoop;
use JSON::XS qw(decode_json);
use Data::Dumper;
use Getopt::Std;

getopts('n:tf:j' => \my %opt);
my $cntSubscribers = $opt{'n'} // DFLT_COUNT_SUBS;
my $pthJSONFile = $opt{'f'} // $ARGV[0] // DFLT_JSON_FILE;

debug_ 'LETS BEGIN!';

my $keep_alive=defined($ARGV[0]) ? eval $ARGV[0] : [1,2];
my @rS=map {
    Redis::BCStation->new('test_blob', 'redis'=>'redis://127.0.0.1:6379', 'fast_but_binary_unsafe'=>0, 'keep_alive'=>$keep_alive)
} 0..($cntSubscribers-1);
my $rP=Redis::BCStation->new('test_blob', 'redis'=>'redis://127.0.0.1:6379', 'fast_but_binary_unsafe'=>0);

utf8::decode(my $jsonData1 = read_file($pthJSONFile));
printf "%s\n", md5_hex($jsonData1);
exit;

my $tsBeforePub;
Mojo::IOLoop->delay(
    sub {
        my ($delay) = @_;
        $_->subscribe('data' => \&on_receive, $delay->begin) for @rS
    },
    sub {
        my ($delay) = @_;
        $tsBeforePub=time();
        $rP->publish('data' => my $data2snd=encodeByTag('JS' => decodeByTag('{JS}' . $jsonData1)));
        say 'DATA2SND is ' . (utf8::is_utf8($data2snd) ? '' : 'NOT ') . 'utf8';
        printf "<<%s...>> CRC32=%s\n", substr($data2snd, 0, 65), crc32($data2snd);
#        printf "PUB LEN=%s CRC32=%s MD5=%s\n", length($data2snd), crc32($data2snd), md5_hex($data2snd);
    }
);

sub on_receive {
        state $cnt=0;
        my $msg = $_[0];
#        utf8::decode($msg) unless utf8::is_utf8($msg);
#        debug { 'SUB LEN=%s MD5=%s' } length($msg), md5_hex($msg);
        say 'MSG is ' . (utf8::is_utf8($msg) ? '' : 'NOT ') . 'utf8';
        printf "SUB LEN=%s CRC32=%s\n", length($msg), crc32($msg);
        my $data = decodeByTag($msg);
        debug { 'TIME: %s', +(time - $tsBeforePub) };
        ++$cnt == $cntSubscribers and exit;
}


Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
