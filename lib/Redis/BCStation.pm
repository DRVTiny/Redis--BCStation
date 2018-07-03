package Redis::BCStation;
use Carp qw(confess croak cluck);
#BEGIN {
#    $SIG{__WARN__}=\&Carp::cluck;
#}

use 5.16.1;
use English;
use subs qw/__get_timer_settings __not_empty_arref __cut/;
use Ref::Util qw(is_plain_coderef is_plain_arrayref is_plain_hashref);
use EV;
use Try::Tiny;
use Net::Domain qw(hostfqdn);
use Mojo::Redis2;
use Mojo::IOLoop;
use Log::Log4perl qw(:easy);
use Log::Log4perl::Level;
use Log::Dispatch;
use Scalar::Util qw(refaddr blessed looks_like_number);
use JSON::XS;
use Data::Dumper;
use constant {
    KEEP_ALIVE_SCHED_RUN_AFTER	=>	 3, 	# sec.
    KEEP_ALIVE_SCHED_INTERVAL	=>	 4, 	# sec.
    FIRST_UNPUB_CHECK_AFTER	=>	 0.1, 	# sec.
    CHECK_UNPUB_EVERY		=>	 0.2,	# sec.
    DFLT_MAX_PUB_RETRIES	=>	 20,
    MAX_MSG_LENGTH_TO_SHOW	=>	 128,
    UPUB_FAILCNT_I		=>	 0,
    UPUB_XTOPIC_I    		=>	 1,
    UPUB_MSG_I			=>	 2,
    TIMER_OPT_AFTER		=>	 0,
    TIMER_OPT_INTERVAL		=>	 1,
    DFLT_TOPIC			=>	'other',
    DFLT_RECON_RETRIES_COUNT	=>	600,
    DFLT_RECON_INTERVAL		=>	0.1,
    DFLT_RECON_AFTER		=>	0.05,
    TRUE			=>	1,
    FALSE			=>	undef,
    DONE			=>      1,
};

BEGIN {
    for my $log_level (qw/trace debug info warn error fatal logdie/) {
        no strict 'refs';
        *{__PACKAGE__ . '::log_' . $log_level} = eval(<<'EOCODE' =~ s%LOG_LEVEL%${log_level}%gr)
        sub {
            my $slf = shift;
            $slf->('logger')->LOG_LEVEL(sprintf('<%s> | ', $slf->clientid), @_);
        }
EOCODE
    }
}

my $callerLvl = 0;
sub new {
    my $class = shift;
    my ($stationName, %pars) = (undef,());
    
    if (scalar(@_) > 1) {
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
    # because "gladiolous"? :)
    confess('BCStation name must not contain symbol ":"') if index($stationName, ':') >= 0;
    my $hostName = $ENV{'HOSTNAME'} // hostfqdn;
    my $logger = sub {
        my $L=shift;
        __check_logger($L)
            ? $L
            : Log::Log4perl->initialized()
                ? Log::Log4perl::get_logger(__PACKAGE__)
                : do {
                    say STDERR __PACKAGE__.': Your logger is not suitable for me, RTFM please :)' if $L;
                    my %LOGCONF=('category'=>__PACKAGE__=~s/::/./gr);
                    Log::Log4perl->easy_init({'level' => $DEBUG, 'layout' => '%d{HH:mm:ss} | %d{dd.MM.yyyy} | %P | %C | %p | %m%n'})
                        ? Log::Log4perl->get_logger(__PACKAGE__)
                        : Log::Dispatch->new('outputs'=>[['Screen','min_level' => 'debug', 'newline' => 1, 'stderr' => 1]])
                };
    }->($pars{'logger'});
    my (%props, $redCastObj);
    my (%aeh, $flReconInProgress);
    my $redc = __redcon($pars{'redis'}) or $logger->logdie('Failed to establish connection to Redis');
    
    # Event handlers
    my ($cntQueUnPubNxtId, %queUnPub) = (0);
    my @queAfterRecon;
    %props = (
        'name'			 => { 'val' => $stationName, 	'acl' => 'r'  	},
        'fast_but_binary_unsafe' => { 'val' => sub { 
                                          state $flFastButUnsafe = FALSE;
                                          return $flFastButUnsafe unless $#_ > 0;
                                          my $slf = shift;
                                          __is_boolean($_[0], \my $flNewFastButUnsafe)
                                              or $slf->log_logdie('fast-but-binary-unsafe option value is incorrect');
                                          if ( defined($flNewFastButUnsafe) xor defined($flFastButUnsafe) ) {
                                              $flFastButUnsafe = $flNewFastButUnsafe;
                                              $slf->redc->protocol_class('Protocol::Redis' . ($flNewFastButUnsafe ? '::XS' : ''))
                                          }
                                          return DONE
                                      },
                                      'acl' => 'rw'
                                    },
        'reconnecting'  => {	'val' => sub { $flReconInProgress }, 	'acl' => '-' 	},
        'clientid' 	=> {
            'val' => sub {
                state $clientName;
                my $slf = shift;
                
                if (!defined($clientName) or (defined($_[0]) and ! ref($_[0]) and ($clientName ne $_[0]))) {
                    $redc->client->name( 
                        $clientName = 
                            defined($_[0]) && ! ref($_[0])
                                ? $_[0]
                                : ($pars{'client'} // join('/' => $hostName, $PID, refaddr($slf))),
                        (@_ and is_plain_coderef($_[$#_])) ? ($_[$#_]) : ()
                    )
                }
                $clientName
            },
            'acl'=>'rw' 
        },
        'topic_format'	=> {	'val'=>'%s<<%s>>',	 'acl'=>'r'  },
        'redc'		=> {	'val' => $redc, 	 'acl'=>'-'  },
        'redis' 	=> {	'val' => $pars{'redis'}, 'acl'=>'r'  },
        'keep_alive' 	=> {
            'val' => sub {
                state $keepAliveTimings;
                my $slf = shift;
                
                return wantarray ? @{$keepAliveTimings} : [@{$keepAliveTimings}] unless @_;
                
                unless (defined($_[0]) and $_[0]) {
                    Mojo::IOLoop->remove(delete $aeh{'keep_alive_timer'}) if defined $aeh{'keep_alive_timer'};
                    $slf->log_debug('keep_alive was disabled');
                    return
                }
                
                my $dfltTimings = $keepAliveTimings // [KEEP_ALIVE_SCHED_RUN_AFTER, KEEP_ALIVE_SCHED_INTERVAL];
                my @oldTimings = $keepAliveTimings ? @{$keepAliveTimings} : ();
                $keepAliveTimings =
                    ( ! ref($_[0]) and (looks_like_number($_[0]) ? $_[0] : ($_[0] =~ m/^(?:true|on|y)/i) ) )
                        ? $dfltTimings
                        : __get_timer_settings($_[0], $dfltTimings)
                            || $slf->log_logdie('Keep-alive settings must be "[after:]interval" string or ref to the list containing maximum 2 numeric elements');
                            
                return TRUE if $aeh{'keep_alive_timer'} and @oldTimings and $oldTimings[0] == $keepAliveTimings->[0] and $oldTimings[1] == $keepAliveTimings->[1];
                
                Mojo::IOLoop->remove(delete $aeh{'keep_alive_timer'}) if $aeh{'keep_alive_timer'};
                __set_timer(
                    \$aeh{'keep_alive_timer'},
                    'after'	=> $keepAliveTimings->[TIMER_OPT_AFTER],
                    'interval'	=> $keepAliveTimings->[TIMER_OPT_INTERVAL],
                    'cb'	=> sub {
                        $slf->redc->ping(sub {
                           my ($redO, $err, $res)=@_;
                           if ($err || $res ne 'PONG') {
                               $slf->log_error(sprintf '(keep_alive) PING FAILED <<%s>>, lets try to reconnect immediately', join('', grep defined($_), $err, $res));
                               $slf->reconnect
                           } else {
                               $slf->log_debug('(keep_alive) PING returned status: OK')
                           }
                        })
                    }
                ); # <- __set_timer()
            },
            'acl' => 'rw'
        }, # <- keep_alive()
        'reconnect_after' => { 'val' => DFLT_RECON_AFTER, 	'acl' => 'rw', 	'chk' => \&__is_pos_number	},
        'reconnect_every' => { 'val' => DFLT_RECON_INTERVAL, 	'acl' => 'rw', 	'chk' => \&__is_pos_number	},
        'reconnect' => {
            'val' => sub {
                my ($slf, %opt) = @_;
                push @queAfterRecon, __not_empty_arref( $opt{'on_success'} );
                # We use localized variable here (instead of normal object method) because veriable can guarantee atomicity in simple increment operation (avoiding possible race conditions)
                if ($flReconInProgress++) {
                    $slf->log_debug('Cant reconnect: reconnection already is in progress');
                    return
                }
                my $afterDelay = $opt{'afterDelay'} // $slf->reconnect_after;
                my $retryEvery = $opt{'retryEvery'} // $slf->reconnect_every;
                $slf->redc->DESTROY;
                my $nReconRetries = 1;
                my $doReconnect = sub {
                    my $redc =
                    try {
                        $slf->redc( __redcon( $pars{'redis'} ) ) or die;
                    } catch {
                        $slf->log_warn(sprintf 'Reconnection failed after %d retr%s', $nReconRetries, ($nReconRetries == 1 ? 'y' : 'ies'));
                        if ( $nReconRetries++ > DFLT_RECON_RETRIES_COUNT ) {
                            $aeh{'try2recon'} and Mojo::IOLoop->remove(delete $aeh{'try2recon'});
                            $flReconInProgress = FALSE;
                            $slf->log_logdie(sprintf 'Failed to re-establish connection to Redis server: reconnection retries count exceeds limit (%d tries)', $nReconRetries);
                        }
                        undef
                    };
                    unless ( $redc ) {
                        $aeh{'try2recon'} //= Mojo::IOLoop->recurring($retryEvery => __SUB__);
                        return
                    }
                    $aeh{'try2recon'} and Mojo::IOLoop->remove(delete $aeh{'try2recon'});
                    $redc->on('error' => $slf->on_error->{'hndl'});
                    $slf->resubscribe(sub {
                        if ( @queAfterRecon ) {
                            my %alreadyDone;
                            while (defined(my $doAfterRecon = shift @queAfterRecon)) {
                                next if $alreadyDone{refaddr $doAfterRecon->[0]}++;
                                $doAfterRecon->[0]->(@{$doAfterRecon}[1..$#{$doAfterRecon}])
                            }
                            @queAfterRecon = ();

                        }
                        $flReconInProgress = FALSE;
                        $slf->log_info('Reconnection succesful');
                        
                    });
                };
                defined($afterDelay) && looks_like_number($afterDelay) && ($afterDelay > 0)
                    ? Mojo::IOLoop->timer($afterDelay => $doReconnect)
                    : $doReconnect->();
            },
            'acl' => 'r',
        },
        'resubscribe' => {
            'val' => sub {
                my ($slf, $doFinally) = @_;
                
                my %subs = eval { %{$slf->subscribers} }
                    or do {
                        $slf->log_debug('No subscriptions defined yet, so we dont need to restore anything. Its friday-evening, Luke!');
                        $doFinally->() if $doFinally;
                        return DONE;
                    };
                my @chans = keys %subs;
                
                my $resubDelay = $slf->__ping_and_exec(
                    'name' => q<resubscribing to Redis channels>,
                    'exec' 	 => sub {
                        $slf->redc->subscribe(\@chans, $_[1])
                    },
                    'on_success' => sub {
                        $slf->redc->on('message' => $slf->('on_message')->{'hndl'});
                        $slf->log_info(sprintf 'Succesfully resubscribed to channels: %s', join(', ' => @chans));
                    },
                    'on_error'  => sub {
                        $slf->log_error(sprintf 'Failed to resubscribe to channels <<%s>>. Reason: %s', join(', ' => @chans), ${$_[1]});
                    },
                    is_plain_coderef( $doFinally ) ? ('on_finish' => $doFinally) : (),
                );
            },
            'acl' => 'r',
        }, # <- resubscribe()
        'max_pub_retries'=>{
            'val'=>sub {
                state $nMaxRetries = DFLT_MAX_PUB_RETRIES;
                return $nMaxRetries unless $#_ > 0;
                my ($slf, $n) = @_;
                (! ref($n) and defined($n) and length($n) and $n !~ m/[^\d]/ ) 
                    or $slf->log_logdie(sprintf 'max_pub_retries (%s) is invalid', $n) unless ! ref($n) and defined($n) and length($n) and $n !~ m/[^\d]/;
                $nMaxRetries = $n
            },
            'acl'=>'rw',
        },
        'next_umi' => {
            'val' => sub {
                state $nextUMI = 0;
                $nextUMI++
            },
            'acl' => '-'
        },
        'add_unpub'=>{
            'val' => sub {
                my $flWasEmpty = ! %queUnPub;
                my ($slf, $umi) = @_[0, 1];
                @{$queUnPub{$umi}}[0, UPUB_XTOPIC_I, UPUB_MSG_I] = (1, @_[2, 3]);
                return $umi unless $flWasEmpty;
                $slf->log_debug('Unpublished queue is not empty (again). Setting up "republisher" schedulling');
                my $mutexRepub = 0;
                __set_timer(\$aeh{'check_unpub'},
                    'after' 	=> FIRST_UNPUB_CHECK_AFTER,
                    'interval'	=> CHECK_UNPUB_EVERY,
                    'cb'	=> sub {
                        try { # to be removed
                            if ( $mutexRepub++ ) {
                                $mutexRepub--;
                                $slf->log_warn('(unpub) republisher already running? ', $mutexRepub);
                                return
                            }
                            
                            if (! %queUnPub and $aeh{'check_unpub'}) {
                                Mojo::IOLoop->remove(delete $aeh{'check_unpub'});
                                $slf->log_debug('(unpub) unpublished queue is empty, republishing task was removed from evloop');
                                return $mutexRepub--
                            }
                            
                            if ( $flReconInProgress or !defined($slf->redc) ) {
                                $slf->log_debug('(unpub) cant check "unpublished" queue: reconnection is in progress');
                                return $mutexRepub--
                            }
#                            $slf->log_debug('(unpub) queUnPub=', Dumper(\%queUnPub));
                            
                            for my $umi (sort keys %queUnPub) {
                                my $pubpack = delete $queUnPub{$umi};
                                $slf->redc->publish( $pubpack->[UPUB_XTOPIC_I] => ${$pubpack->[UPUB_MSG_I]},
                                sub {
                                    # If message from unpub queue was published (error message in $_[1] is absent)...
                                    unless ( $_[1] ) {
                                        $slf->log_debug(sprintf '(unpub) succesfully published message <<%s>> (%d bytes) delayed as UMI=%s on channel [%s]', __cut(${$pubpack->[UPUB_MSG_I]}), length(${$pubpack->[UPUB_MSG_I]}), $umi, $pubpack->[UPUB_XTOPIC_I]);
                                        return
                                    }
                                    $slf->log_error('(unpub) error when publishing delayed msg#%s <<%s>>: %s', $umi, __cut(${$pubpack->[UPUB_MSG_I]}), $_[1]);
                                    my $n = ++($queUnPub{$umi} = $pubpack)->[UPUB_FAILCNT_I];
                                    my $nMaxPubRetries = $slf->('max_pub_retries');
                                    if ($nMaxPubRetries and $n > $nMaxPubRetries) {
                                        $slf->log_error(sprintf '(unpub) cant publish delayed msg#%s after %d retries, so we have to wipe it out from the queue', $umi, $n);
                                        $slf->del_unpub( $umi );
                                    }
                                }) # <-  redc->publish
                                
                            } # <- for every unpublished message (TODO: what if we cant publish first message? do we really must to attempt publish rest messages? hmm...)
                            $mutexRepub--;
                        } catch { # to be removed
                            say STDERR "************************************ ( $_ ) **************************************";
                        }; # to be removed
                    });
                return $umi
            },
            'acl'=>'-',
        }, # <- add_unpub()
        'del_unpub'=>{
            'val'=>sub {
                my $unpubElement = delete $queUnPub{$_[1]};
                Mojo::IOLoop->remove(delete $aeh{'check_unpub'}) unless %queUnPub or !$aeh{'check_unpub'};
                return $unpubElement
            },
            'acl'=>'-',
        }, # <- del_unpub()
        'debug'=>{		'val'=>$pars{'debug'}?1:0, 	'acl'=>'rw' },
        'subscribers'=>{	'val'=>{},			'acl'=>'-'  },
        'logger'=>{
            'val'	=>	$logger,
            'chk'	=>	\&__check_logger,
            'acl'	=>	'rw'
        },
        'hasMethod'=>{
            'val'=>sub {
                shift if ref $_[0];
                return unless $_[0] and !ref($_[0]);
                return $props{$_[0]}?1:0
            },
            'acl'=>'rw'
        },
        '_dumper' => {
            'val'=>sub { 
                $_[0]->log_debug(Dumper \%props)
            }, 
            'acl'=>'-',
        },
        'on_message' => {
            'val' => {
                'hndl' => sub {
                    my ($r, $message, $xtopic)=@_;
                    $_->($message => $xtopic) for values do {
                        ($_=eval { $redCastObj->subscribers->{$xtopic} } and is_plain_hashref($_) and %{$_} and $_) or {}
                    };
                }
            },
            'acl' => '-'
        },
        'on_error' => {
            'val' => {
                'hndl' => sub {
                    my ($redc, $err) = @_;
                    if ( $err =~ m/connection/i ) {
                        $redCastObj->log_error(sprintf 'Redis connection error detected, lets try to reconnect after %s sec.', $redCastObj->reconnect_after);
                        $redCastObj->reconnect
                    } else {
                        $redCastObj->log_logdie(sprintf 'Very-Big-Trouble: Redis connector %s operational error: %s', ref($redc), $err)
                    }
                }
            },
            'acl' => '-'        
        },
    );
    
    $redCastObj = bless sub {
        return unless my $method = shift;
        $logger->logdie('No such method: ',$method) unless my $methodProps = $props{$method};
        my $callerPkg = scalar(caller($callerLvl == 1 ? 1 : 0));
        my $acl = $methodProps->{'acl'} // '-';
        unless ( ($callerPkg eq __PACKAGE__ or index($callerPkg, __PACKAGE__ . '::') == 0) or index($acl, @_ ? 'w' : 'r') >= 0 ) {
            $logger->logdie(sprintf 'Access control violation while calling <<%s>> method', $method);
        }
        my $errMsg;
        return is_plain_coderef($methodProps->{'val'})
                ? $methodProps->{'val'}->($redCastObj, @_)
                : ($#_ >= 0)
                    ? ($methodProps->{'check'} and !$methodProps->{'check'}->($_[0], $errMsg))
                        ? $logger->logdie('Incorrect value passed to method ', $method, $errMsg ? (': ', $errMsg) : () )
                        : do { $methodProps->{'val'} = shift }
                    : $methodProps->{'val'};
    }, ( ref($class) || $class );
    $redCastObj->($_, $pars{$_}) for grep exists($props{$_}), keys %pars;
    $redCastObj->redc->on('error' => $redCastObj->('on_error')->{'hndl'});
    $redCastObj->logger->debug(__PACKAGE__.' instance id=#'.refaddr($redCastObj).' is ready to use');
    return $redCastObj
} # <- constructor aka NEW

sub publish {
    my $slf = shift;
    # P.D.K. reference :)
    $slf->log_logdie('"What to publish?" - BCStation said') unless defined($_[0]) or defined($_[1]);
    my $umi = $slf->next_umi;
    my ($topic, $refMsg) =
      (defined($_[0]) && !defined($_[1]))
        ? (undef, \$_[0])
        : ($_[0], \$_[1]);
    my $hndl_on_pub = $_[2];
    
    $topic or $slf->log_warn('Target channel was not defined, so publishing to "' . ($topic=DFLT_TOPIC()) . '"');    
    my $xtopic = $slf->__xtopic($topic);
    
    my $doAdd2UnPub = sub {
        my $umi = $slf->add_unpub($umi, $xtopic, $refMsg);
        $slf->log_warn(sprintf 'Failed to publish message <<%s>>{%s} on channel [%s]. It was appended to the deferred queue as UMI=%s. Reason of failure: <<%s>>', __cut(${$refMsg}), refaddr($refMsg), $xtopic, $umi, __cut(${$_[0]}));                    
    };    
    
    if ($slf->reconnecting) {
        $doAdd2UnPub->(\'Reconnection is in progress');
        return
    }
    
    my $flAlreadyFired = 0;
    $slf->__ping_and_exec(
        'exec' => sub {
            $slf->redc->publish($xtopic => ${$refMsg}, $_[1])
        },
        'on_ping_error' => sub {
            $slf->log_error('(pub) Redis ping error, will try to reconnect');
            $doAdd2UnPub->($_[1]);
            $slf->reconnect;
        },
        'on_exec_error' => sub {
            return if $flAlreadyFired++;
            $doAdd2UnPub->($_[1]);
            is_plain_coderef($hndl_on_pub) and $hndl_on_pub->(undef, ${$_[1]})
        },
        'on_success' => sub {
            return if $flAlreadyFired++;
            $slf->log_debug(sprintf '(pub) BCStation published message <<%s>> on channel: [%s], message_length: %d', __cut(${$refMsg}), $xtopic, length ${$refMsg});
            is_plain_coderef($hndl_on_pub) and $hndl_on_pub->($refMsg);
        },
    );
} # <- publish()

sub subscribe {
# $fl_opt_hndl_is_delbeg means "handle is delay->begin sub {}" :)
    my ($slf, $topic, $hndl_on_msg, $opt_hndl_on_subs_status, $maybe_mojo_delay)=@_;
    my $xtopic=$slf->__xtopic($topic);
    my $log = $slf->logger;
    ( $hndl_on_msg and is_plain_coderef($hndl_on_msg) )
        or $slf->log_logdie('You must pass on_message callback as a second parameter for subscirbe() method!');
    $slf->log_info('Requested subscribe for ', $xtopic);
    my $redc = $slf->redc;
    my $listeners = $slf->subscribers;
    my $flHasListeners = is_plain_hashref($listeners) && %{$listeners};
    unless ( $flHasListeners and $listeners->{$xtopic} ) {
        $slf->__ping_and_exec(
            'name' => 'subscribe to ' . $topic,
            'exec' => sub {
                # In Mojo::Redis2->subscribe() we are only declaring that we need to receive messages on channel $xtopic
                #  handler for new incoming messages will be defined later (search for << $redc->on('message'=>sub { >>)
                # Mojo::Redis2->subscribe() needs to be called once for every $xtopic subscription            
                $slf->redc->subscribe([$xtopic], $_[1])
            },
            'handle_exec_result' => sub {
                my ($delay, $err, $res) = @_;
                if ($opt_hndl_on_subs_status and is_plain_coderef($opt_hndl_on_subs_status)) {
                    return $opt_hndl_on_subs_status->($maybe_mojo_delay ? ($maybe_mojo_delay) : (), $err, $res)
                } else {
                    if ($err) {
                        $slf->log_logdie(sprintf '%s: ERROR when subscribing to %s: %s', __PACKAGE__, $xtopic, $err);
                        $slf->log_debug('How can you log this if you are dead?');
                        return
                    }
                    $slf->log_info('Succesfully subscribed to channel ', $topic);
                    return DONE
                }
            },
        );
    }
    my $psubHndlOnMsg = refaddr($hndl_on_msg);
    if ( $flHasListeners and $listeners->{$xtopic}{$psubHndlOnMsg} ) {
        $slf->log_error(sprintf 'Passed callback already subscribed to <<%s>>', $topic);
        return 
    }
    
    $listeners->{$xtopic}{$psubHndlOnMsg} = $hndl_on_msg;
    $redc->on('message' => $slf->('on_message')->{'hndl'}) unless $flHasListeners;
    return DONE
} # <- subscribe()

sub AUTOLOAD {
    our $AUTOLOAD;
    my $slf=$_[0];
    
    return unless my ($method)=$AUTOLOAD=~/::(\w+)$/;
    
    no strict 'refs';
    *{$AUTOLOAD} = do {
      $slf->('hasMethod'=>$method)
        ? sub {
            $callerLvl = 1;
            my $rslt = $_[0]->($method, @_[1..$#_]);
            $callerLvl = 0;
            return $rslt
          }
        : do {
            if ( my $cr = $slf->('redc')->can($method) ) {
                sub { 
                    $cr->( $_[0]->('redc'), @_[1..$#_] )
                }
            } else {
                $slf->log_logdie(sprintf 'Method %s is not implemented by %s', $method, __PACKAGE__)
            }
          }
    };
    
    goto &{$AUTOLOAD};
} # <- AUTOLOAD()

sub DESTROY {
    my $slf = shift;
    $callerLvl = 0;
    $slf->('redc')->DESTROY();
} # <- DESTROY()

###################################### FOOTER #############################################
sub __redcon {
    my $conpar = $_[0];
    my $redc = ($conpar && eval {{
                       'HASH'  => sub { Mojo::Redis2->new(%{$_[0]}) },
                       'Mojo::Redis2' => sub { $_[0] },
                       ''      => sub { Mojo::Redis2->new('url'=>lc(substr $_[0],0,6) eq 'redis:'?$_[0]:'redis://'.$_[0]) },
                    }->{ref $conpar}->($conpar)}
    ) || Mojo::Redis2->new();
    return unless $redc->ping eq 'PONG';
    $redc
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

sub __cut($) {
    length($_[0]) <= MAX_MSG_LENGTH_TO_SHOW
        ? $_[0]
        : substr($_[0], 0, MAX_MSG_LENGTH_TO_SHOW - 3) . '...'
}

sub __get_timer_settings {
    my $r = ref($_[0]);
    my $dflt = $_[1];
    return unless 
        my @afterANDperiod =
        ($r eq 'ARRAY')
            ? @{$_[0]}
            : $r
                ? (return)
                : split /:/ => $_[0];
    return unless @afterANDperiod and scalar(@afterANDperiod) <= 2;
    for my $c (0,1) {
        for ($afterANDperiod[$c]) {
            return if defined and $_ and (!looks_like_number($_) or $_ < 0);
            ( (!(defined and length) or ($c and !$_) ) and $_ = $dflt->[$c] ) or $_ += 0
        }
    }
    \@afterANDperiod;    
}

sub __set_timer {
    my ($ptrEvID, %options)=@_;
    my ($secAfter, $secInterval, $callback) = @options{qw/after interval cb/};
    return unless $secAfter or $secInterval;
    $$ptrEvID=
        $secAfter 
            ? Mojo::IOLoop->timer($secAfter => 
                $secInterval 
                    ? sub { $callback->(); $$ptrEvID=Mojo::IOLoop->recurring($secInterval => $callback) }
                    : $callback
              )
            : Mojo::IOLoop->recurring($secInterval => $callback);
    return DONE
}

sub __ping_and_exec {
    my ($slf, %opt) = @_;
    my $delayObj = Mojo::IOLoop->delay(
        sub {
            $slf->redc->ping($_[0]->begin)
        },
        sub {
            my ($delay, $err, $res) = @_;
            if ( $err or $res ne 'PONG' ) {
                $opt{'on_ping_error'}
                    ? $opt{'on_ping_error'}->($slf, \$err)
                    : do { $slf->reconnect( 'on_success' => [$opt{'exec'}, $slf, $delay->begin] ); 
                           return
                         }
            }
            $opt{'exec'}->($slf, $delay->begin);
        },
        is_plain_coderef( $opt{'handle_exec_result'} )
            ? $opt{'handle_exec_result'}
            : sub {
                my ($delay, $err, $res) = @_;
                if ( $err ) {
                    is_plain_coderef($opt{'on_error'} //= $opt{'on_exec_error'})
                        ? $opt{'on_error'}->($slf, \$err)
                        : $slf->log_error('Unhandled delayed execution error: ', $err);
                } elsif ( is_plain_coderef $opt{'on_success'} ) {
                    $opt{'on_success'}->($slf)
                }
              }
    );
    $delayObj->on(
        'error' => sub {
            $slf->log_error(sprintf 'Error in evloop<<%s>>: %s', ($opt{'name'} // 'UNNAMED'), $_[1]);
            $_->($slf, $_[1]) for grep is_plain_coderef($_), @opt{qw/on_ioloop_error on_finish/};
        },
        (is_plain_coderef($opt{'on_finish'}) ? ('finish' => $opt{'on_finish'}) : () ),
    );
    $delayObj
}

sub __not_empty_arref {
    is_plain_arrayref($_[0]) && $#{$_[0]} >= 0 ? $_[0] : ()
}

sub __is_pos_number($) {
    looks_like_number($_[0]) && $_[0] > 0
}

sub __is_boolean {
    return unless !defined($_[0]) or (!ref($_[0]) and length($_[0]) and $_[0] =~ m/^(?:(?<TRUE>[+-]?[1-9][0-9]*|true|y(?:es)?|on)|(?<FALSE>0|false|no?|off))$/i);
    
    ${$_[1]} = 
        defined($_[0])
            ? defined($+{'TRUE'})
                ? TRUE
                : FALSE
            : FALSE
}

1;
