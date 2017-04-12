package Redis::BCStation;
use 5.16.1;
use Carp qw(confess croak);
use Mojo::Redis2;
use Log::Log4perl;
use Log::Log4perl::Level;
use Log::Dispatch;
use Scalar::Util qw(refaddr blessed);
use Data::Dumper;

sub __redcon {
    my $conpar=shift;
    ($conpar && eval {{
                       'HASH'  => sub { Mojo::Redis2->new(%{$_[0]}) },
                       'Mojo::Redis2' => sub { $_[0] },
                       ''      => sub { Mojo::Redis2->new('url'=>lc(substr $_[0],0,6) eq 'redis:'?$_[0]:'redis://'.$_[0]) },
                    }->{ref $conpar}->($conpar)}
    ) || Mojo::Redis2->new();
}

sub __xtopic() {
    my ($slf, $topic)=@_;
    return sprintf($slf->('topic_format'), $slf->('name'), $topic)
}

sub new {
    my $class=shift;
    my ($stationName,%pars)=(undef,());
    
    if (scalar(@_)>1) {
        if ( scalar(@_) & 1 ) {
            $stationName=shift;
            %pars=@_
        } else {
            %pars=@_;
            $stationName=$pars{'name'}
        }
    } else {
        $stationName=shift;
    }
    confess('You must specify BCStation name') unless $stationName and ! ref($stationName);
    confess('BCStation name must not contain symbol ":"') if index($stationName,':')>=0;
    my $logger=sub {
        my $L=shift;
        ($L and ref($L) and blessed($L) and !(grep !$L->can($_), qw/debug info warn error fatal/))
            ? $L
            : Log::Log4perl->initialized()
                ? Log::Log4perl::get_logger(__PACKAGE__)
                : do {
                    say STDERR __PACKAGE__.': Your logger is not suitable for me, RTFM please :)' if $L;
                    my %LOGCONF=('category'=>__PACKAGE__=~s/::/./gr);
                    Log::Log4perl::init(\(<<'EOLOGCONF'=~s/\{\{([^}]+)\}\}/$LOGCONF{lc($1)}/gr))
log4perl.category.{{CATEGORY}}           =      DEBUG, Screen

log4perl.appender.Screen                 =      Log::Log4perl::Appender::Screen
log4perl.appender.Screen.stderr          =      1
log4perl.appender.Screen.layout          =      Log::Log4perl::Layout::PatternLayout
log4perl.appender.Screen.layout.ConversionPattern = %d{HH:mm:ss} | %d{dd.MM.yyyy} | %P | %p | %m%n
EOLOGCONF
                        ? Log::Log4perl->get_logger(__PACKAGE__)
                        : Log::Dispatch->new('outputs'=>[['Screen','min_level' => 'debug', 'newline' => 1, 'stderr' => 1]])
                };
    }->($pars{'logger'});
    my %props;
    %props=(
        'name'=>{'val'=>$stationName},
        'topic_format'=>{'val'=>'%s<<%s>>'},
        'redc'=>{
            'val'=>__redcon($pars{'redis'}),
            'visible'=>'private'
        },
        'debug'=>{'val'=>$pars{'debug'}?1:0},
        'subscribers'=>{'val'=>{}},
        'logger'=>{'val'=>$logger},
        'hasMethod'=>{
            'val'=>sub {
                shift if ref $_[0];
                return unless $_[0] and !ref($_[0]);
                return $props{$_[0]}?1:0
            }
        },
        '_dumper'=>{'val'=>sub { $_[0]->('logger')->debug(Dumper(\%props)) }},
    );
    my $redCastObj;
    $redCastObj=bless sub {
        return unless my $method=shift;
        
        confess("No such method: $method") unless my $methodProps=$props{$method};
        
        return ref $methodProps->{'val'} eq 'CODE'
                ? $methodProps->{'val'}->($redCastObj,@_)
                : $_[0]?$props{$method}{'val'}=shift:$props{$method}{'val'};
    }, ( ref($class) || $class );
}

sub publish {
    my ($slf, $topic, $msg)=@_;
    do { $msg=$topic; $topic='other' } unless $msg;
    my $xtopic=$slf->__xtopic($topic);
    $slf->logger->debug(sprintf q(BCStation publishes: {topic: "%s", message: "%s"}), $xtopic, $msg);
    $slf->('redc')->publish($xtopic, $msg);
}

sub subscribe {
    my ($slf, $topic, $handler)=@_;
    my $xtopic=$slf->__xtopic($topic);
    my $log=$slf->logger;
    $log->debug('Requested subscribe for '.$xtopic);;
    my $redc=$slf->('redc');
    my $subs=$slf->('subscribers');
    $redc->subscribe([$xtopic], sub { 
        my ($slf, $err)=@_;
        confess(__PACKAGE__.': ERROR when subscribing: '.$err) if $err;
        $log->debug('Succesfully subscribed to channel '.$topic);
        return 1
    }) unless $subs->{$xtopic};
    unless (%{$subs}) {
        $subs->{$xtopic}{refaddr $handler}=$handler;
        $redc->on('message'=>sub {
            my ($r, $msg, $chan)=@_;
            my $chanSubs=$subs->{$chan};
            return unless ref($chanSubs) eq 'HASH' and %{$chanSubs};
            $_->($msg,$chan) for values %{$chanSubs};
        });        
    } elsif ($subs->{$xtopic}{refaddr $handler}) {
        $log->error('Passed callback already subscribed to << '.$topic.' >>');
    } else {
        $subs->{$xtopic}{refaddr $handler}=$handler;
    }
}

sub AUTOLOAD {
    our $AUTOLOAD;
    my $slf=$_[0];
#    say '>>>> ',ref($slf),' <<<< ', $AUTOLOAD;
    return unless my ($method)=$AUTOLOAD=~/::(\w+)$/;
    return unless $slf->('hasMethod'=>$method);
    {
        no strict 'refs';
        *{$AUTOLOAD}=sub {
            $_[0]->($method, @_[1..$#_])
        }        
    }
    goto &{$AUTOLOAD};
}

1;
