# Redis::BCStation

BCStation is a broadcasting radio station... oh no, it is simply a package to subscribe 
on and publish to channels of the some "virtual broadcasting station"
implemented on top of the widely-known Redis pub/sub technology.

Redis::BCStation uses Mojo::Redis2 as its backend but provides more stable
interface - with keep alives, disconnect detection and auto-restoring of subscriptions
on reconnection.

With this package, you can listen on channel with the same name, but on
different "stations" - data published on those channels will be different
too:

```perl
my $bcstTokyo = Redis::BCStation('tokyo');
my $bcstMoscow = Redis::BCStation('moscow');
$bcstTokyo->subscribe('weather', sub { say @_ } );
$bcstMoscow->subscribe('weather', sub { say @_ } );
```

...it is obvious that the weather forecast in Tokyo and Moscow will be  different :)

## Logging

Internaly Redis::BCStation strongly requires logging to be initialized - or it will
initialize Log::Log4perl by herself and in this case it will log all that you may 
want to know - to STDERR. Initialize Log4perl early to write that messages
to the appropriate/desired place.


To be continued :)