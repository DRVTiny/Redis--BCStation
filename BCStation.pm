package Redis::BCStation;

use Carp qw(confess croak cluck);
BEGIN {
    $SIG{__WARN__}=\&Carp::cluck;
}

use 5.16.1;

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
use constant {
    KEEP_ALIVE_SCHED_RUN_AFTER=>30,
    KEEP_ALIVE_SCHED_INTERVAL=>10,
    DFLT_MAX_PUB_RETRIES=>20,
    MAX_MSG_LENGTH_TO_SHOW=>128,
};

my $callerLvl=0;
my $flReconInProgress;
my $cntQueUnPubNxtId=0;
my %queUnPub;

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
        'client' =>{
            'val'=>sub { 
                state $clientName;
                shift if ref $_[0] eq __PACKAGE__;
                if (!defined($clientName) or (defined($_[0]) and ! ref($_[0]) and ($clientName ne $_[0]))) {
                    $redc->client->name(                        
                        $clientName=(defined($_[0]) and ! ref($_[0]))?$_[0]:($pars{'client'} // $$),
                        (@_ and ref($_[$#_]) eq 'CODE')?($_[$#_]):()
                    )
                }
                $clientName
            }, 
            'acl'=>'rw' 
        },
        'topic_format'=>{ 	'val'=>'%s<<%s>>', 		'acl'=>'r'  },
        'redc'=>{		'val'=>$redc, 			'acl'=>'-'  },
        'redis'=>{		'val'=>$pars{'redis'}, 		'acl'=>'r'  },
        'keep_alive'=>{
            'val'=>sub {
                state $flKeepAlive=0;
                shift if ref $_[0] eq __PACKAGE__;
                return $flKeepAlive unless @_;
                $logger->logdie('Keep-alive flag value must be simple scalar, not reference') if ref $_[0];
                # Nothing changes - nothing to do - return old (still actual) value.
                return $flKeepAlive if (my $newVal=shift)==$flKeepAlive;
                # Remove periodically scheduller if keep-alive value is "falsish" (i.e. 0 or undef)
                undef($aeh{'keep_alive_timer'}), return($newVal) unless $newVal;
                $aeh{'keep_alive_timer'}=AnyEvent->timer(
                    'after'=>KEEP_ALIVE_SCHED_RUN_AFTER,
                    'interval'=>KEEP_ALIVE_SCHED_INTERVAL,
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
        'max_pub_retries'=>{
            'val'=>sub {
                my $slf=shift;
                state $nMaxRetries=DFLT_MAX_PUB_RETRIES;
                return $nMaxRetries unless @_;
                my $n=shift;
                confess "max_pub_retries ($n) is invalid" unless ! ref($n) and defined($n) and length($n) and $n !~ m/[^\d]/;
                $nMaxRetries=$n
            },
            'acl'=>'rw',
        },
        'check_unpub'=>{
            'val'=>sub {
                return $aeh{'check_unpub'} if $aeh{'check_unpub'};
                Mojo::IOLoop->timer(0.5=>sub {
                    $aeh{'check_unpub'}=Mojo::IOLoop->recurring(0.1 =>
                    sub {
                        return if $flReconInProgress;
                        my $log=$redCastObj->('logger');
                        my $redc=$redCastObj->('redc');
                        my $retries=$redCastObj->('max_pub_retries');
                        my $reconDelay=Mojo::IOLoop->delay(
                            sub {
                                $redc->ping($_[0]->begin)
                            },
                            sub {
                                my ($delay,$err,$res)=@_;
                                $log->warn('(unpub) Redis ping error: ',$err), return if $err;
                                $log->trace('(unpub) Redis ping OK');
                                for my $umi (sort keys %queUnPub) {
                                    $redc->publish(@{$queUnPub{$umi}}[1,2] => sub {
                                        unless ($_[1]) {
                                            my $x=delete($queUnPub{$umi});
                                            $log->debug(sprintf '(unpub) Succesfully published message <<%s>> on channel [%s] with UMI=%d', $x->[2], $x->[1], $umi);
                                            return
                                        }
                                        my $n=++$queUnPub{$umi}[0];
                                        if ($retries and $n>$retries) {
                                            $log->error(sprintf 'Cant publish message #%s after %d retries, purging it from the queue', $umi, $n);
                                            delete $queUnPub{$umi}
                                        }
                                    })
                                }  
                            },
                        );   
                    }) # <- recurring ("interval")
                }); # <- timer ("after")
                    
            },
            'acl'=>'-',
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
    $pars{'client'}//=$$;
    $redCastObj->($_, $pars{$_}) for grep exists($props{$_}), keys %pars;
    $redCastObj->('check_unpub');
    return $redCastObj
}

sub reconnect {
#    state $flReconInProgress;
    my $slf=shift;
    my $log=$slf->logger;
    
    if ($flReconInProgress++) {
        $log->debug('Cant reconnect: reconnection already is in progress');
        return
    }

    $slf->redc->DESTROY;
    my $redc=$slf->redc(__redcon($slf->('redis')) || $log->logdie($_[0] || 'Failed to re-establish my connection to Redis server'));
    
    do {
        $log->debug(q|No subscriptions defined yet, so we dont need to restore anything. It's a friday-evening, Luke!|);
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
    $reconDelay->on('error'=>sub { $log->error('Error in Mojo::IOLoop->delay while resubscribing to Redis channels'); $doFinally->() });
    $reconDelay->on('finish'=>$doFinally);
}

sub __add2unpub {
    $queUnPub{my $umi=$cntQueUnPubNxtId++}=[1,$_[0],$_[1]];
    $umi;
}

sub __cut {
    length($_[0])<=MAX_MSG_LENGTH_TO_SHOW ? $_[0] : substr($_[0],0,MAX_MSG_LENGTH_TO_SHOW - 3).'...'
}

sub publish {
    my ($slf, $topic, $msg)=@_;
    do { $msg=$topic; $topic='other' } unless $msg;
    my $xtopic=$slf->__xtopic($topic);
    
     if ($flReconInProgress) {
         my $umi=__add2unpub($xtopic => $msg);
         $slf->logger->debug(sprintf 'Reconnection is in progress, we have to drop message <<%s>> to the "unpub" queue as #%d', __cut($msg), $umi);
         return;
    }
    
    $slf->('redc')->publish($xtopic, $msg, sub {
        if (my $err=$_[1]) {
            my $umi=__add2unpub($xtopic => $msg);
            $slf->logger->warn(sprintf 'Fail to publish message on channel [%s]. Reason: <<%s>>. Queued to "unpub" as %d', $xtopic, __cut($err), $umi);
        } else {
            $slf->logger->debug(sprintf q(BCStation publishes: {topic:"%s",message:{length:%d,data:"%s"}), $xtopic, length($msg), __cut($msg));
        }
        
    } );
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
