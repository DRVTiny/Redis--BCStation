#!/usr/bin/perl
use strict;
use warnings;
use 5.16.1;
use FindBin;
use lib $FindBin::Bin . '/../lib';
use Redis::BCStation;
use Log::Log4perl;
use Test::More tests => 4;
use Test::Exception;
Log::Log4perl->init(\(<<'EOLOG'));
log4perl.rootLogger           =      ERROR, Screen

log4perl.appender.Screen                 =      Log::Log4perl::Appender::Screen
log4perl.appender.Screen.stderr          =      1
log4perl.appender.Screen.layout          =      Log::Log4perl::Layout::PatternLayout
log4perl.appender.Screen.layout.ConversionPattern = %d{HH:mm:ss} | %d{dd.MM.yyyy} | %P | %C | %p | %m%n
EOLOG

my $bcst = Redis::BCStation->new('Test');
lives_ok { $bcst->publish('cmn' =>'some') } 'check that we can do something usual (message publishing)';
lives_ok { $bcst->logger(Log::Log4perl->get_logger()) } 'check that we can pass valid Log4perl logger to the appropriate method';
close STDERR;
throws_ok { $bcst->logger('aaaa')  } qr/^Incorrect value passed to method/, 'check that we cant pass some trash as logger object'; 
throws_ok { $bcst->reconot('bbbb') } qr/^Access control violation while calling <<reconot>>/, 'check that acl violation will lead to exception as well';
