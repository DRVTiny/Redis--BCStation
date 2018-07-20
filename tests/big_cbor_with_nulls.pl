#!/usr/bin/perl
use 5.16.1;
use utf8;
use warnings;
use constant ENC_TAGS => 'CB,Z';
use Tag::DeCoder;
use Log4perl::KISS;
use Digest::MD5 qw(md5_hex);
use File::Slurp qw(read_file);
use JSON::XS;

use FindBin;
use lib $FindBin::Bin . '/../lib';
use Redis::BCStation;

my $data_pub = encodeByTag(ENC_TAGS() => JSON::XS->new->decode(read_file(shift // die 'You must specify file path')));
my ($l_data_pub, $md5_data_pub) = (length($data_pub), md5_hex($data_pub));
#say("$l_data_pub, $md5_data_pub"), exit;
log_level('TRACE');
info_ 'Lets start our game';
my $bcst = Redis::BCStation->new('galaxyNews', 'keep_alive' => 1, 'fast_but_binary_unsafe' => 0);

$bcst->subscribe('warNeverChanges' =>
sub {
    my ($l_data_rcvd, $md5_data_rcvd) = ( length($_[0]), md5_hex($_[0]) );
    info_ "Received data with %scorrect length=%d and %svalid md5sum=%s\n", $l_data_rcvd == $l_data_pub ? '' : 'in', $l_data_rcvd, $md5_data_rcvd eq $md5_data_pub ? '' : 'in', $md5_data_rcvd;
    error_ 'Length is not the same as was published' unless $l_data_rcvd == $l_data_pub;
    error_ 'md5sum is not the same as for the data that was published' unless $md5_data_rcvd eq $md5_data_pub;
    Mojo::IOLoop->reset;
}, 
sub {
    die $_[0] if $_[0];
    debug_('OK, subscribed');
    Mojo::IOLoop->timer(1 => sub {
        $bcst->publish('warNeverChanges', $data_pub => sub {
            info_ 'Sended data with length=%s and md5sum=%s', $l_data_pub, $md5_data_pub;
        });
    });
});

Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
