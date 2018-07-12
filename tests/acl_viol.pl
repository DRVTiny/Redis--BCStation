#!/usr/bin/perl
use strict;
use warnings;
use 5.16.1;
use Redis::BCStation;
use Log::Log4perl;
Log::Log4perl->init(\(<<'EOLOG'));
log4perl.rootLogger           =      ERROR, Screen

log4perl.appender.Screen                 =      Log::Log4perl::Appender::Screen
log4perl.appender.Screen.stderr          =      1
log4perl.appender.Screen.layout          =      Log::Log4perl::Layout::PatternLayout
log4perl.appender.Screen.layout.ConversionPattern = %d{HH:mm:ss} | %d{dd.MM.yyyy} | %P | %C | %p | %m%n
EOLOG

my $r=Redis::BCStation->new('Test');
$r->logger(Log::Log4perl->get_logger());
$r->publish('cmn','some');
$r->logger('aaaa');

