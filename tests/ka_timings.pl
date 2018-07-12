#!/usr/bin/perl
use 5.16.1;
use Redis::BCStation;
use Mojo::IOLoop;

my $r=Redis::BCStation->new('testka', 'keep_alive'=>defined($ARGV[0]) ? eval $ARGV[0] : [1,2]);

Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
