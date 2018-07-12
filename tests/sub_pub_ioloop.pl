#!/usr/bin/perl
use 5.16.1;
use Redis::BCStation;
use Mojo::IOLoop::Delay;
use JSON::XS qw(encode_json);
use Data::Dumper;

my $r=Redis::BCStation->new('Test');


Mojo::IOLoop::Delay->new()->steps(
    sub {
        $r->subscribe(
            'common', 
            sub { say "Received message: ",$_[0] },
            $_[0]->begin(0)
        )
    },
    sub {
        my ($delay, $err, $res) = @_;
#        say Dumper \@_;
        die "SUBS ERROR: $err" if $err;
        say "SUBS result: ", encode_json($res);
        $r->publish('common' => 'Some information', $delay->begin(0));
#        Mojo::IOLoop->timer(1, $delay->begin)
    },
    sub {
        my ($delay, $err, $msg) = @_;       
        $err and die 'PUBL ERROR: '.$err;
        printf "PUBL %s bytes OK\n", length($msg);
        Mojo::IOLoop->timer(1 => $delay->begin);
    },
    sub {
        say '1s delay is out'
    },
)->catch(sub { say 'CATCHED: ', $_[1] })->wait;

#Mojo::IOLoop->start
#    unless Mojo::IOLoop->is_running;
