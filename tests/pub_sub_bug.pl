#!/usr/bin/perl
use 5.16.1;
use strict;
use warnings;
use Data::Dumper;
use Scalar::Util qw(refaddr);
use Mojo::IOLoop;
use Mojo::Redis2;
use Log4perl::KISS;

my ($rp, $rs) = map Mojo::Redis2->new(), 1, 2;

my $np = 1;
Mojo::IOLoop->recurring(1 => sub {
    $rp->publish('tst<<cmn>>', $np++, sub {
        print '(pub) ' . Dumper(\@_)
    })
});

sub resubscr {
    $rs->subscribe(['tst<<cmn>>']);
    $rs->on('message' => sub {
        my ($r, $msg) = @_;
        debug_ 'by #%s received: %s', refaddr($r), $msg;
    });
}

resubscr();

Mojo::IOLoop->recurring(3 => sub {
    undef($rs);
    $rs = Mojo::Redis2->new;
    resubscr;
});

Mojo::IOLoop->start;
