package Redis::BCStation;
use 5.16.1;
use Carp qw(confess croak);
use EV;
use AnyEvent;
use Mojo::Redis2;
use Mojo::IOLoop;
use Log::Log4perl;
use Log::Log4perl::Level;
use Log::Dispatch;
use Scalar::Util qw(refaddr blessed);
#use JSON::XS;
use Data::Dumper;

my $callerLvl=0;

sub __redcon {
    my $conpar=shift;
    ($conpar && eval {{
                       'HASH'  => sub { Mojo::Redis2->new(%{$_[0]}) },
                       'Mojo::Redis2' => sub { $_[0] },
                       ''      => sub { Mojo::Redis2->new('url'=>lc(substr $_[0],0,6) eq 'redis:'?$_[0]:'redis://'.$_[0]) },
                    }->{ref $conpar}->($conpar)}
    ) || Mojo::Redis2->new();
}

sub __xtopic {
    my ($slf, $topic)=@_;
    return sprintf($slf->('topic_format'), $slf->('name'), $topic)
}

sub __check_logger {
    shift while ref($_[0]) eq __PACKAGE__;
    my $L=shift;
    return ($L and ref($L) and blessed($L) and !(grep !$L->can($_), qw/debug info warn error fatal logdie/))
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
        __check_logger($L)
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
log4perl.appender.Screen.layout.ConversionPattern = %d{HH:mm:ss} | %d{dd.MM.yyyy} | %P | %C | %p | %m%n
EOLOGCONF
                        ? Log::Log4perl->get_logger(__PACKAGE__)
                        : Log::Dispatch->new('outputs'=>[['Screen','min_level' => 'debug', 'newline' => 1, 'stderr' => 1]])
                };
    }->($pars{'logger'});
    my (%props,$redCastObj);
    my $redc=__redcon($pars{'redis'}) or $logger->logdie('Connection to Redis with Mojo::Redis2 failed');
    $redc->on('error'=>sub {
        my ($R, $err)=@_;
        $logger->logdie('Very-Big-Trouble: Mojo::Redis2 operational error: ',$err);
    });
    # AnyEvent handlers
    my %aeh;
    %props=(
        'name'=>{		'val'=>$stationName, 		'acl'=>'r'  },
        'topic_format'=>{ 	'val'=>'%s<<%s>>', 		'acl'=>'r'  },
        'redc'=>{		'val'=>$redc, 			'acl'=>'-'  },
        'redis'=>{		'val'=>$pars{'redis'}, 		'acl'=>'r'  },
        'keepAlive'=>{
            'val'=>sub {
                state $flKeepAlive=0;
                return $flKeepAlive unless defined(my $newVal=shift);
                return if (my $newVal=shift)==$flKeepAlive;
                do {
                    undef $aeh{'keep_alive_timer'}; return 1
                } unless $newVal;
                $aeh{'keep_alive_timer'}=AnyEvent->timer(
                    'after'=>30,
                    'interval'=>10,
                    'cb'=>sub {
                        $redCastObj->('redc')->ping(sub {
                           my ($redO,$err,$res)=@_;
#                           $logger->debug(join(', '=>map $_.'='.do{ my $t=eval("\$$_"); ref($t)?JSON::XS->new->encode($t):$t }, qw/err res/));
                           if ($err || $res ne 'PONG') {
                               $redCastObj->reconnect('Failed to re-animate Redis connection after ping lost');
                           } else {
                               $logger->debug('Mojo::Redis2 PING returned status: OK');
                           }
                        });
                    }
                );
            },
            'acl'=>'rw'
        },
        'debug'=>{		'val'=>$pars{'debug'}?1:0, 	'acl'=>'rw' },
        'subscribers'=>{	'val'=>{},			'acl'=>'-'  },
        'logger'=>{
            'val'=>$logger,
            'chk'=>\&__check_logger,
            'acl'=>'w'
        },
        'hasMethod'=>{
            'val'=>sub {
                shift if ref $_[0];
                return unless $_[0] and !ref($_[0]);
                return $props{$_[0]}?1:0
            },
            'acl'=>'rw'
        },
        '_dumper'=>{
            'val'=>sub { 
                $_[0]->('logger')->debug(Dumper(\%props))
            }, 
            'acl'=>'-',
        },
        'on_message'=>{
            'val'=>{'hndl'=>sub {
                my ($r, $msg, $chan)=@_;
                my $chanSubs=$redCastObj->subscribers->{$chan};
                return unless ref($chanSubs) eq 'HASH' and %{$chanSubs};
                $_->($msg,$chan) for values %{$chanSubs};
            }},
            'acl'=>'-'
        },
    );
    $redCastObj=bless sub {
        return unless my $method=shift;        
        $logger->logdie('No such method: ',$method) unless my $methodProps=$props{$method};
        my $callerPkg=scalar(caller($callerLvl==1?1:0));
        my $acl=$methodProps->{'acl'} || '-';
        unless ( (($callerPkg eq __PACKAGE__) || (index($callerPkg,__PACKAGE__.'::')==0)) || (index($acl,@_?'w':'r')>=0) ) {
            $logger->logdie('Access control violation while calling <<',$method,'>> method');
        }
        my $errMsg;
        return (ref($methodProps->{'val'}) eq 'CODE')
                ? $methodProps->{'val'}->($redCastObj,@_)
                : $_[0]
                    ? ($methodProps->{'check'} and !$methodProps->{'check'}->($_[0],$errMsg))
                        ? $logger->logdie('Incorrect value passed to method ',$method,$errMsg?(': ',$errMsg):())
                        : do { $methodProps->{'val'}=shift }
                    : $methodProps->{'val'};
    }, ( ref($class) || $class );
    $redCastObj->($_, $pars{$_}) for grep exists($props{$_}), keys %pars;
    return $redCastObj
}

sub reconnect {
    state $flReconInProgress;
    my $slf=shift;
    my $log=$slf->logger;
    
    if ($flReconInProgress++) {
        $log->debug('Cant reconnect: reconnection already is in progress');
        return
    }

    $slf->redc->DESTROY;
    my $redc=$slf->redc(__redcon($slf->('redis')) || $log->logdie($_[0] || 'Failed to re-establish my connection to Redis server'));
    
    do {
        $log->debug('No subscriptions defined yet, so we dont need to restore anything. Its a friday-evening, Luke!');
        return 1
    } unless my %subs=%{$slf->subscribers};
    
    my @chans=keys %subs;
    my $reconDelay=Mojo::IOLoop->delay(
        sub {
            $redc->ping($_[0]->begin)
        },
        sub {
            my ($delay,$err,$res)=@_;
            $log->logdie('(resub) Redis ping error: ',$err) if $err;
            $log->debug('(resub) Redis ping result: ', $res);
            $redc->subscribe([@chans], $delay->begin);
        },
        sub {
            my ($delay,$err,$res)=@_;
            my $sChans='( '.join(', '=>@chans).' )';
            if ($err) {
                $log->logdie(sprintf '%s: ERROR when resubscribing to channels %s on reconnect: %s', __PACKAGE__, $sChans, $err);
                return
            }
            $log->info(sprintf 'Succesfully resubscribed to channels %s', $sChans);
            $redc->on('message'=>$slf->('on_message')->{'hndl'});
        }
    );
    my $doFinally=sub { $flReconInProgress=0 }; 
    $reconDelay->on('error'=>$doFinally);
    $reconDelay->on('finish'=>$doFinally);
}

sub publish {
    my ($slf, $topic, $msg)=@_;
    do { $msg=$topic; $topic='other' } unless $msg;
    my $xtopic=$slf->__xtopic($topic);
    $slf->logger->debug(sprintf q(BCStation publishes: {topic: "%s", message: "%s"}), $xtopic, $msg);
    $slf->('redc')->publish($xtopic, $msg);
}

sub subscribe {
    my ($slf, $topic, $hndl_on_msg, $opt_hndl_on_subs_status, $fl_opt_hndl_is_delbeg)=@_;
    
    my $xtopic=$slf->__xtopic($topic);
    my $log=$slf->logger;
    $log->logdie('You must pass sub {} as a second parameter for subscirbe method!')
        unless ref($hndl_on_msg) eq 'CODE';
#    { open my $fh, '>>/tmp/redis_sub.log'; printf $fh "[%s] %s\n", $$, ref($log) }
    $log->info('Requested subscribe for ',$xtopic);;
    my $redc=$slf->('redc');
    my $subs=$slf->('subscribers');
    unless ( ref($subs) eq 'HASH' and %{$subs} and $subs->{$xtopic} ) {
        Mojo::IOLoop->delay(
            sub {
                $redc->ping($_[0]->begin)
            },
            sub {
                my ($delay,$err,$res)=@_;
                $log->logdie('Redis ping error: ',$err) if $err;
                $log->debug('Redis ping result: ', $res);
                # In Mojo::Redis2->subscribe() we are only declaring that we need to receive messages on channel $xtopic
                #  handler for new incoming messages will be defined later (search for << $redc->on('message'=>sub { >>)
                # Mojo::Redis2->subscribe() needs to be called once for every $xtopic subscription
                $redc->subscribe([$xtopic], $delay->begin);
            },
            sub {
                my ($delay,$err,$res)=@_;
                if ($opt_hndl_on_subs_status and ref($opt_hndl_on_subs_status) eq 'CODE') {                    
                    return $opt_hndl_on_subs_status->($fl_opt_hndl_is_delbeg?(undef):(), $err, $res);
                } else {
                    if ($err) {
                        $log->logdie(sprintf '%s: ERROR when subscribing to %s: %s', __PACKAGE__, $xtopic, $err);
                        $log->debug('How can you log this if you are dead?');
                        return
                    }
                    $log->info('Succesfully subscribed to channel ', $topic);
                    return 1
                }
            }
        );
    }
    unless (%{$subs}) {
        $subs->{$xtopic}{refaddr $hndl_on_msg}=$hndl_on_msg;
        $redc->on('message'=>$slf->('on_message')->{'hndl'})
    } elsif ($subs->{$xtopic}{refaddr $hndl_on_msg}) {
        $log->error('Passed callback already subscribed to << ',$topic,' >>');
    } else {
        $subs->{$xtopic}{refaddr $hndl_on_msg}=$hndl_on_msg;
    }
}

sub AUTOLOAD {
    our $AUTOLOAD;
    my $slf=$_[0];
    
    return unless my ($method)=$AUTOLOAD=~/::(\w+)$/;
    return unless $slf->('hasMethod'=>$method);
    {
        no strict 'refs';
        *{$AUTOLOAD}=sub {
            $callerLvl=1;
            my $rslt=$_[0]->($method, @_[1..$#_]);
            $callerLvl=0;
            return $rslt
        }        
    }
    goto &{$AUTOLOAD};
}

sub DESTROY {
    my $slf=shift;
    $callerLvl=0;
    $slf->('redc')->DESTROY();
}

1;
