#!/usr/bin/perl
use strict;
use warnings;
use constant {
    ENC_TAG => 'JS',
};
#use lib 'lib1';

use 5.16.1;
use EV;
use AnyEvent;
use Mojo::IOLoop;
use Log4perl::KISS;
use JSON::XS;
use Tag::DeCoder;
use Log::Log4perl;

use FindBin qw($Bin);
use lib "$Bin/../lib";
use Redis::BCStation;

Log::Log4perl->init(\(<<'EOLOG'));
log4perl.rootLogger           =      DEBUG, Screen

log4perl.appender.Screen                 =      Log::Log4perl::Appender::Screen
log4perl.appender.Screen.stderr          =      1
log4perl.appender.Screen.layout          =      Log::Log4perl::Layout::PatternLayout
log4perl.appender.Screen.layout.ConversionPattern = %d{HH:mm:ss} | %d{dd.MM.yyyy} | %P | %C | %l | %p | %m%n
EOLOG
debug { 'Here we go' };

my $r = Redis::BCStation->new('Test',
    'keep_alive' => [0, 5],
    'reconnect_after' => 0,
);
#$r->keepAlive(1);
#$r->logger(Log::Log4perl->get_logger());
my $json = JSON::XS->new->pretty;
$r->subscribe('cmn', sub {
    debug_ 'Received new message:', $json->encode(decodeByTag($_[0]));
});

my $cv=AnyEvent->condvar;
my %aeh;
my $n=1;

$aeh{'pub_time'}=AnyEvent->timer('after'=>1,'interval'=>1,'cb'=>sub {
    $r->publish('cmn', encodeByTag(ENC_TAG() => {'time'=>scalar(localtime),'rand'=>int(rand(1000)),'count'=>$n++}))
});
$aeh{'reconnect'}=AnyEvent->timer('after'=>2,'interval'=>5,'cb'=>sub {
    $r->reconnect;
});

Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
