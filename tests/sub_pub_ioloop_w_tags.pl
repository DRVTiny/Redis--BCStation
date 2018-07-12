#!/usr/bin/perl
use 5.16.1;
use Redis::BCStation;
use Mojo::IOLoop::Delay;
use Tag::DeCoder;
use JSON::XS qw(encode_json);
use Data::Dumper;

my $r=Redis::BCStation->new('testcm');

Mojo::IOLoop::Delay->new()->steps(
    sub {
        $r->subscribe(
            'full_reload',
            sub { say "Received message: ",encode_json(decodeByTag($_[0])) },
            $_[0]->begin(0)
        )
    },
    sub {
        my ($delay,$err,$res)=@_;
#        say Dumper \@_;
        die "SUBS ERROR: $err" if $err;
        say "SUBS result: ", encode_json($res);
        $r->publish('full_reload' => encodeByTag(($ARGV[0]?'CB':'JS') => {"A"=>[{"B"=>{"C"=>1.05}}]}));
        Mojo::IOLoop->timer(1, $delay->begin)
    },
    sub { say '1s delay is out' },
)->catch(sub { say 'CATCHED: ', $_[1] })->wait;

#Mojo::IOLoop->start
#    unless Mojo::IOLoop->is_running;
