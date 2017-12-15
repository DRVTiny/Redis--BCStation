== WHAT IS IT ==

Redis::BCStation is a broadcasting radio station... oh no, it is a package to subscribe 
on and publish to channels of the some "virtual broadcasting station"
implemented with the Redis pub/sub widely-used technology.

Redis::BCStation uses Mojo::Redis2 as its backend but provides more stable
interface - with keep alives, disconnect detection and auto-restoring of subscriptions
on reconnection.

With this package, you can listen on channel with the same name, but on
different "stations" - data published on those channels will be different
too:

my $bcstTokyo = Redis::BCStation('tokyo');
my $bcstMoscow = Redis::BCStation('moscow');
$bcstTokyo->subscribe('weather', sub { say @_ } );
$bcstMoscow->subscribe('weather', sub { say @_ } );

...it is obvious that the weather forecast in Tokyo and Moscow will be  different :)


To be continued :)