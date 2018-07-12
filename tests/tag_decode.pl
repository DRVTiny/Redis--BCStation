#!/usr/bin/perl
use constant { 
    Station => 'testcm001',
    Channel => 'full_reload',
    PUB_INTERVAL => 2
};
use 5.16.1;
use warnings;
use EV;
use Time::HiRes qw(time);
use lib 'lib1';
use Redis::BCStation;
use Data::Dumper;
use Tag::DeCoder;
use Mojo::IOLoop;
use Mojo::Redis2;
use JSON::XS qw(encode_json);

my ($mr, $r);

if ($ARGV[0]) {
    if ($ARGV[0] eq 'test') {
        $mr=Mojo::Redis2->new;

        Mojo::IOLoop->delay(
            sub {
                say 'HERE';
                $mr->subscribe([join('' => Station(),'<<',Channel(),'>>')] => $_[0]->begin)
            },
            sub {
                say Dumper [ @_[2..$#_] ];
            },
        );
        
        $mr->on('error'=>sub {
            say 'ON_ERROR'
        });
        $mr->on('message' => sub {
            say 'ON_MESSAGE';
            say Dumper \@_;
        });        
    } else {
        $r=Redis::BCStation->new(Station(), 'keep_alive'=>1);
        $r->subscribe(Channel(), sub {
            say "/// HERE $_[0] ///";
#            print substr(encode_json(decodeByTag($_[0])),0,1000);
        });
    }
} else {
    $r=Redis::BCStation->new(Station(), 'keep_alive'=>0);
    Mojo::IOLoop->recurring(PUB_INTERVAL() => sub {
        $r->publish('full_reload', encodeByTag('JS' => {"A"=>[{"B"=>{"C"=>time()}}]}));
    });
}

Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
say 'WOW';
