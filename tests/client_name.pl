#!/usr/bin/perl
use Redis::BCStation;
use Data::Dumper; 
my $r=Redis::BCStation->new("cache2up"); 
$r->subscribe("fullReload", sub {print "hello\n"} ); 
print Dumper {
    'client_name_default'=>$r->client(),
};

$r->client('NewClient');

print Dumper {
    'client_name_after_set'=>$r->client(),
};
