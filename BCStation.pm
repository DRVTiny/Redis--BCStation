package Redis::BCStation;

use Carp qw(confess croak cluck);
BEGIN {
    $SIG{__WARN__}=\&Carp::cluck;
}

use 5.16.1;

use EV;
use Mojo::Redis2;
use Mojo::IOLoop;
use Log::Log4perl;
use Log::Log4perl::Level;
use Log::Dispatch;
use Scalar::Util qw(refaddr blessed);
use Data::Dumper;

use constant {
    KEEP_ALIVE_SCHED_RUN_AFTER	=>	3, 	# sec.
    KEEP_ALIVE_SCHED_INTERVAL	=>	4, 	# sec.
    FIRST_UNPUB_CHECK_AFTER	=>	0.1, 	# sec.
    CHECK_UNPUB_EVERY		=>	0.2,	# sec.
    DFLT_MAX_PUB_RETRIES	=>	20,
    MAX_MSG_LENGTH_TO_SHOW	=>	128,
    UPUB_XTOPIC_I    		=>	1,
    UPUB_MSG_I			=>	2,
};

my $callerLvl=0;

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
    # Event handlers
    my %aeh;
    my %queUnPub;
    my $cntQueUnPubNxtId=0;
    my $flReconInProgress;    
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
                my $log=$redCastObj->('logger');
                $log->logdie('Keep-alive flag value must be simple scalar, not reference') if ref $_[0];
                # Nothing changes - nothing to do - return old (still actual) value.
                return $flKeepAlive if (my $newVal=shift)==$flKeepAlive;
                # Remove periodically scheduller if keep-alive value is "falsish" (i.e. 0 or undef)
                undef($aeh{'keep_alive_timer'}), return($newVal) unless $newVal;
                Mojo::IOLoop->delay(
                    sub {
                        Mojo::IOLoop->timer(KEEP_ALIVE_SCHED_RUN_AFTER() => $_[0]->begin);
                    }, # <- set one-time alarm
                    sub {
                        $aeh{'keep_alive_timer'}=
                        Mojo::IOLoop->recurring(KEEP_ALIVE_SCHED_INTERVAL() => sub {
                            $redCastObj->('redc')->ping(sub {
                               my ($redO,$err,$res)=@_;
                               if ($err || $res ne 'PONG') {
                                   $redCastObj->reconnect('(keep_alive) failed to re-animate Redis connection after ping lost');
                               } else {
                                   $log->debug('(keep_alive) PING returned status: OK');
                               }
                            });
                        });
                    }, # <- set recurring timer
                ); # <- $loop->delay()
            },
            'acl'=>'rw'
        },
        'reconnect'=>{
            'val'=>sub {
                my $slf=shift;
                my $log=$slf->logger;
                # We use localized variable here (instead of normal object method) because veriable can guarantee atomicity in simple increment operation (avoiding possible race conditions)
                if ($flReconInProgress++) {
                    $log->debug('Cant reconnect: reconnection already is in progress');
                    return
                }

                $slf->redc->DESTROY;
                my $redc=$slf->redc(__redcon($slf->('redis')) || $log->logdie($_[0] || 'Failed to re-establish connection to Redis server'));
                my %subs=eval { %{$slf->subscribers} }
                    or $log->debug('No subscriptions defined yet, so we dont need to restore anything. Its friday-evening, Luke!'), return(1);
                
                my @chans=keys %subs;
                my $reconDelay=
                Mojo::IOLoop->delay(
                    sub {
                        $redc->ping($_[0]->begin)
                    },
                    sub {
                        my ($delay, $err, $res)=@_;
                        $log->logdie('(resub) Redis ping error: ',$err) if $err;
                        $log->debug('(resub) Redis ping result: ', $res);
                        $redc->subscribe(\@chans, $delay->begin);
                    },
                    sub {
                        my ($delay, $err, $res)=@_;
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
            },
            'acl'=>'r',
        }, # <- reconnect()
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
        'add_unpub'=>{
            'val'=>sub{
                my $flWasEmpty=! %queUnPub;
                $queUnPub{my $umi=$cntQueUnPubNxtId++}=[1,$_[1],$_[2]];
                return $umi unless $flWasEmpty;
                my $slf=$_[0];
                $slf->logger->debug('Unpublished queue is not empty (again). Setting "publishing" watcher');
                Mojo::IOLoop->delay(
                    sub {
                        $aeh{'check_unpub'}=Mojo::IOLoop->timer(FIRST_UNPUB_CHECK_AFTER() => $_[0]->begin)
                    }, # <- one-time alarm ("after" in AE)
                    sub {
                        sub {
                            state $cntRun=0;
                            $cntRun++ or 
                                $aeh{'check_unpub'}=
                                Mojo::IOLoop->recurring(CHECK_UNPUB_EVERY() =>__SUB__);
                            my $log=$slf->('logger');
                            $flReconInProgress and $log->debug('Cant check "unpublished" queue: reconnection is in progress'), return;
                            my $redc=$slf->('redc');
                            my $retries=$slf->('max_pub_retries');
                            Mojo::IOLoop->delay(
                                sub {
                                    $redc->ping($_[0]->begin)
                                },
                                sub {
                                    my ($delay,$err,$res)=@_;
                                    $log->warn('(unpub) Redis ping error: ',join(', '=>$err,$res)), return if $err or $res ne 'PONG';
                                    $log->trace('(unpub) Redis ping OK');
                                    for my $umi (sort keys %queUnPub) {
                                        $redc->publish(@{$queUnPub{$umi}}[UPUB_XTOPIC_I , UPUB_MSG_I] => sub {
                                            # If message from unpub queue was published (no errors occured in $_[1])...
                                            unless ($_[1]) {
                                                my $x=$slf->del_unpub($umi);
                                                $log->debug(sprintf '(unpub) Succesfully published message <<%s>> on channel [%s] with UMI=%d', $x->[2], $x->[1], $umi);
                                                return
                                            }
                                            my $n=++$queUnPub{$umi}[0];
                                            if ($retries and $n>$retries) {
                                                $log->error(sprintf 'Cant publish message #%s after %d retries, purging it from the queue', $umi, $n);
                                                $slf->del_unpub($umi);
                                            }
                                        }) # <-  redc->publish
                                    } # <- for every unpublished message (TODO: what if we cant publish first message? do we really must to attempt publish rest messages? hmm...)
                                }, 
                            );  # <- loop->delay
                        }->()
                    }, # <- recurring timer ("interval" in AE)
                ); # <- ioloop->delay
                return $umi
            },
            'acl'=>'-',
        }, # <- add_unpub()
        'del_unpub'=>{
            'val'=>sub {
                my $unpubElement=delete $queUnPub{$_[1]};
                Mojo::IOLoop->remove(delete $aeh{'check_unpub'}) unless %queUnPub or !$aeh{'check_unpub'};
                return $unpubElement;
            },
            'acl'=>'-',
        }, # <- del_unpub()
        'debug'=>{		'val'=>$pars{'debug'}?1:0, 	'acl'=>'rw' },
        'subscribers'=>{	'val'=>{},			'acl'=>'-'  },
        'logger'=>{
            'val'=>$logger,
            'chk'=>\&__check_logger,
            'acl'=>'rw'
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
                my ($r, $message, $xtopic)=@_;
                $_->($message => $xtopic) for values do {
                    ($_=eval { $redCastObj->subscribers->{$xtopic} } and ref($_) eq 'HASH' and %{$_} and $_) or {}
                };
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
    return $redCastObj
} # <- constructor aka NEW

sub publish {
    my ($slf, $topic, $msg)=@_;
    do { $msg=$topic; $topic='other' } unless $msg;
    my $xtopic=$slf->__xtopic($topic);
    
     if ($flReconInProgress) {
         my $umi=$slf->add_unpub($xtopic => $msg);
         $slf->logger->debug(sprintf 'Reconnection is in progress, we have to drop message <<%s>> to the "unpub" queue as #%d', __cut($msg), $umi);
         return;
    }
    
    $slf->('redc')->publish($xtopic, $msg, sub {
        if (my $err=$_[1]) {
            my $umi=$slf->add_unpub($xtopic => $msg);
            $slf->logger->warn(sprintf 'Fail to publish message on channel [%s]. Reason: <<%s>>. Queued to "unpub" as %d', $xtopic, __cut($err), $umi);
        } else {
            $slf->logger->debug(sprintf 'BCStation publishes: {topic:"%s",message:{length:%d,data:"%s"}', $xtopic, length($msg), __cut($msg));
        }
        
    } );
} # <- publish()

sub subscribe {
    my ($slf, $topic, $hndl_on_msg, $opt_hndl_on_subs_status, $fl_opt_hndl_is_delbeg)=@_;
    
    my $xtopic=$slf->__xtopic($topic);
    my $log=$slf->logger;
    $log->logdie('You must pass sub {} as a second parameter for subscirbe method!')
        unless $hndl_on_msg and ref($hndl_on_msg) eq 'CODE';
    $log->info('Requested subscribe for ',$xtopic);;
    my $redc=$slf->('redc');
    my $listeners=$slf->('subscribers');
    my $flHasListeners=ref($listeners) eq 'HASH' && %{$listeners};
    unless ( $flHasListeners and $listeners->{$xtopic} ) {
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
    my $psubHndlOnMsg=refaddr($hndl_on_msg);
    
    $log->error('Passed callback already subscribed to << ',$topic,' >>'), return 
        if $flHasListeners and $listeners->{$xtopic}{$psubHndlOnMsg};
    
    $listeners->{$xtopic}{$psubHndlOnMsg}=$hndl_on_msg;
    $redc->on('message'=>$slf->('on_message')->{'hndl'}) unless $flHasListeners;
} # <- subscribe()

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

###################################### FOOTER #############################################
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

sub __cut {
    length($_[0])<=MAX_MSG_LENGTH_TO_SHOW ? $_[0] : substr($_[0],0,MAX_MSG_LENGTH_TO_SHOW - 3).'...'
}

1;
