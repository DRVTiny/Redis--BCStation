#!/usr/bin/perl
use 5.16.1;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Log4perl::KISS;
use Redis::BCStation;
debug { 'Lets BEGIN!' };

my $bcst_p = Redis::BCStation->new('tst_casting', 'keep_alive' => 1, 'reconnect_every' => 0.3, 	'reconnect_after' => 0.1);
my $bcst_s = Redis::BCStation->new('tst_casting', 'keep_alive' => 1, 'reconnect_every' => 0.05, 'reconnect_after' => 0);

Mojo::IOLoop->recurring(8 => sub {
    system('sudo systemctl stop redis');
    Mojo::IOLoop->timer(2 => sub {
        system('sudo systemctl start redis');
    });
});

Mojo::IOLoop->recurring(1 => sub {
    say STDERR sprintf('~<//<< %s >>\\>~', time);
    $bcst_p->publish('now', time);
});

$bcst_s->subscribe('now' => sub {
    debug_ "$_[0] received";
});

Mojo::IOLoop->start;
