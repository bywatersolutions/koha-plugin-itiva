package Koha::Plugin::Com::ByWaterSolutions::MessageBee;

## It's good practice to use Modern::Perl
use Modern::Perl;

## Required for all plugins
use base qw(Koha::Plugins::Base);

## We will also need to include any Koha libraries we want to access
use C4::Auth;
use C4::Context;
use Koha::DateUtils qw(dt_from_string);

use DateTime;
use File::Path qw(make_path);
use File::Slurp qw(write_file);
use File::Temp qw(tempdir);
use Mojo::JSON qw(encode_json decode_json);
use Net::SFTP::Foreign;
use POSIX;
use Try::Tiny;
use YAML::XS qw(Load);

## Here we set our plugin version
our $VERSION         = "{VERSION}";
our $MINIMUM_VERSION = "{MINIMUM_VERSION}";

our $metadata = {
    name            => 'Unique Management Services - MessageBee',
    author          => 'Kyle M Hall',
    date_authored   => '2021-09-20',
    date_updated    => "1900-01-01",
    minimum_version => $MINIMUM_VERSION,
    maximum_version => undef,
    version         => $VERSION,
    description     =>
      'Plugin to forward messages to MessageBee for processing and sending',
};

=head3 new

=cut

sub new {
    my ( $class, $args ) = @_;

    ## We need to add our metadata here so our base class can access it
    $args->{'metadata'} = $metadata;
    $args->{'metadata'}->{'class'} = $class;

    ## Here, we call the 'new' method for our base class
    ## This runs some additional magic and checking
    ## and returns our actual $self
    my $self = $class->SUPER::new($args);

    return $self;
}

=head3 configure

=cut

sub configure {
    my ( $self, $args ) = @_;
    my $cgi = $self->{'cgi'};

    unless ( $cgi->param('save') ) {
        my $template = $self->get_template( { file => 'configure.tt' } );

        ## Grab the values we already have for our settings, if any exist
        $template->param(
            host     => $self->retrieve_data('host'),
            username => $self->retrieve_data('username'),
            password => $self->retrieve_data('password'),
        );

        $self->output_html( $template->output() );
    }
    else {
        $self->store_data(
            {
                host     => $cgi->param('host'),
                username => $cgi->param('username'),
                password => $cgi->param('password'),
            }
        );
        $self->go_home();
    }
}

=head3 install

This is the 'install' method. Any database tables or other setup that should
be done when the plugin if first installed should be executed in this method.
The installation method should always return true if the installation succeeded
or false if it failed.

=cut

sub install() {
    my ( $self, $args ) = @_;

    return 1;
}

=head3 upgrade

This is the 'upgrade' method. It will be triggered when a newer version of a
plugin is installed over an existing older version of a plugin

=cut

sub upgrade {
    my ( $self, $args ) = @_;

    return 1;
}

=head3 uninstall

This method will be run just before the plugin files are deleted
when a plugin is uninstalled. It is good practice to clean up
after ourselves!

=cut

sub uninstall() {
    my ( $self, $args ) = @_;

    return 1;
}

=head3 before_send_messages

Plugin hook that runs right before the message queue is processed
in process_message_queue.pl

=cut

sub before_send_messages {
    my ( $self, $params ) = @_;

    my $archive_dir = $ENV{MESSAGEBEE_ARCHIVE_PATH};
    my $test_mode = $ENV{MESSAGEBEE_TEST_MODE};
    my $verbose = $ENV{MESSAGEBEE_VERBOSE};

    if ($archive_dir) {
        if ( -d $archive_dir ) {
            my $dt = dt_from_string();
            $dt->subtract( days => 30 );
            my $age_threshold = $dt->datetime;
            opendir my $dir, $archive_dir or die "Cannot open directory: $!";
            my @files = readdir $dir;
            closedir $dir;

            foreach my $f (@files) {
                next unless $f =~ /json$/;
                if ( $f lt $age_threshold ) {
                    unlink( $archive_dir . "/" . $f );
                }
            }
        }
        else {
            make_path $archive_dir or die "Failed to create path: $archive_dir";
        }
    }

    say "MESSAGE BEE TEST MODE" if $test_mode;

    my $messages = Koha::Notice::Messages->search(
        {
            status => 'pending',
            content => { -like => '%messagebee: yes%' },
        }
    );

    my @message_data;
    my $messages_seen = {};
    my $messages_generated = 0;
    while ( my $m = $messages->next ) {

        $m->status('sent')->update() unless $test_mode;

        try {

            say "WORKING ON MESSAGE " . $m->id if $verbose;
            say "CONTENT:\n" . $m->content     if $verbose > 2;
            my $content = $m->content();

            my $patron;

            my @yaml;
            try {
                @yaml = Load $content;
            }
            catch {
                say "LOADING YAML FAILED!:\n" . $m->content;
                @yaml = undef;
            };

            foreach my $yaml (@yaml) {

                next unless $yaml;
                next unless ref $yaml eq 'HASH';
                next unless $yaml->{messagebee};
                next unless $yaml->{messagebee} eq 'yes';

                $messages_seen->{ $m->message_id } = 1;

                my $data;
                $data->{message} = $m->unblessed;

                ## Handle 'checkout' / 'old_checkout'
                my $checkout;
                if ( $yaml->{checkout} ) {
                    $checkout = Koha::Checkouts->find( $yaml->{checkout} );
                }
                if ( $yaml->{old_checkout} ) {
                    $checkout =
                      Koha::Old::Checkouts->find( $yaml->{old_checkout} );
                }
                if ($checkout) {
                    $patron          = $checkout->patron;
                    $data->{patron}  = $patron->unblessed;
                    $data->{library} = $checkout->library->unblessed;

                    my $subdata;
                    my $item = $checkout->item;
                    $subdata->{checkout}   = $checkout->unblessed;
                    $subdata->{item}       = $item->unblessed;
                    $subdata->{biblio}     = $item->biblio->unblessed;
                    $subdata->{biblioitem} = $item->biblioitem->unblessed;

                    $data->{checkouts} = [$subdata];
                }

                ## Handle 'checkouts'
                if ( $yaml->{checkouts} ) {
                    my @checkouts = split( /,/, $yaml->{checkouts} );

                    foreach my $id (@checkouts) {
                        my $checkout = Koha::Checkouts->find($id);
                        next unless $checkout;

                        $patron //= $checkout->patron;
                        $data->{patron} //= $patron->unblessed;

                        my $subdata;
                        my $item = $checkout->item;
                        $subdata->{checkout}   = $checkout->unblessed;
                        $subdata->{library}    = $checkout->library->unblessed;
                        $subdata->{item}       = $item->unblessed;
                        $subdata->{biblio}     = $item->biblio->unblessed;
                        $subdata->{biblioitem} = $item->biblioitem->unblessed;

                        $data->{checkouts} //= [];
                        push( @{ $data->{checkouts} }, $subdata );
                    }
                }

                ## Handle 'hold'
                if ( $yaml->{hold} ) {
                    my $hold = Koha::Holds->find( $yaml->{hold} );
                    next unless $hold;

                    my $biblio = $hold->biblio;
                    next unless $biblio;

                    my $biblioitem = $biblio->biblioitem;
                    next unless $biblioitem;

                    $patron //= $hold->patron;
                    $data->{patron} //= $patron->unblessed;

                    my $subdata;
                    $subdata->{hold}           = $hold->unblessed;
                    $subdata->{pickup_library} = $hold->branch->unblessed;
                    $subdata->{biblio}         = $biblio->unblessed;
                    $subdata->{biblioitem}     = $biblioitem->unblessed;

                    if ( my $item = $hold->item ) {
                        $subdata->{item} = $item->unblessed;
                    }

                    $data->{holds} = [$subdata];
                }

                ## Handle 'old_hold'
                if ( $yaml->{old_hold} ) {
                    my $hold = Koha::Old::Holds->find( $yaml->{old_hold} );
                    next unless $hold;

                    my $biblio = Koha::Biblios->find( $hold->biblionumber );
                    next unless $biblio;

                    my $biblioitem = $biblio->biblioitem;
                    next unless $biblioitem;

                    $patron //= $hold->patron;
                    $data->{patron} //= $patron->unblessed;

                    my $subdata;
                    $subdata->{hold}           = $hold->unblessed;
                    $subdata->{pickup_library} = $hold->branch->unblessed;
                    $subdata->{biblio}         = $biblio->unblessed;
                    $subdata->{biblioitem}     = $biblioitem->unblessed;

                    if ( my $item = $hold->item ) {
                        $subdata->{item} = $item->unblessed;
                    }

                    $data->{holds} = [$subdata];
                }

                ## Handle 'holds'
                if ( $yaml->{holds} ) {
                    my @holds = split( /,/, $yaml->{holds} );

                    foreach my $id (@holds) {
                        my $hold = Koha::Holds->find($id);
                        next unless $hold;

                        $patron //= $hold->patron;
                        $data->{patron} //= $patron->unblessed;

                        my $subdata;
                        my $item = $hold->item;
                        $subdata->{hold}    = $hold->unblessed;
                        $subdata->{library} = $hold->branch->unblessed;
                        $subdata->{item}    = $item ? $item->unblessed : undef;
                        $subdata->{biblio} =
                          $item ? $item->biblio->unblessed : undef;
                        $subdata->{biblioitem} =
                          $item ? $item->biblioitem->unblessed : undef;

                        $data->{holds} //= [];
                        push( @{ $data->{holds} }, $subdata );
                    }
                }

                ## Handle misc key/value pairs
                try {
                    $data->{library} ||=
                      Koha::Libraries->find( $yaml->{library} )->unblessed
                      if $yaml->{library};
                };
                try {
                    $patron //= Koha::Patrons->find( $yaml->{patron} )
                      if $yaml->{patron};
                    $data->{patron} //= $patron->unblessed;
                };
                try {
                    $data->{item} ||=
                      Koha::Items->find( $yaml->{item} )->unblessed
                      if $yaml->{item};
                };
                try {
                    $data->{biblio} ||=
                      Koha::Biblios->find( $yaml->{biblio} )->unblessed
                      if $yaml->{biblio};
                };
                try {
                    $data->{biblioitem} ||=
                      Koha::Biblioitems->find( $yaml->{biblioitem} )->unblessed
                      if $yaml->{biblioitem};
                };

                $data->{patron}->{account_balance} = $patron->account->balance
                  if $patron;

                # UMS has requested the abstract field to be removed
                # it greatly increases the file size and is not needed
                delete $data->{biblio}->{abstract} if $data->{biblio};

                if ( keys %$data ) {
                    $messages_generated++;
                    push( @message_data, $data );
                    say "MESSAGE DATA: " . Data::Dumper::Dumper($data)
                      if $verbose > 1;
                }
                else {
                    $m->status('failed')->update() unless $test_mode;
                }
            }

        }
        catch {
            $m->status('deleted')->update() unless $test_mode;
        }
    }

    if (@message_data) {
        my $dev_version = '{' . 'VERSION' . '}'; # Prevents substitution
        my $v = $VERSION eq $dev_version ? "DEVELOPMENT VERSION" : $VERSION;
        my $json = encode_json( { messagebee_plugin_version => $v, messages => \@message_data } );

        my $library_name = C4::Context->preference('LibraryName');
        $library_name =~ s/ /_/g;
        my $dir      = tempdir( CLEANUP => 0 );
        my $ts       = strftime( "%Y-%m-%dT%H-%M-%S", gmtime( time() ) );
        my $filename = "$ts-Notices-$library_name.json";
        my $realpath = "$dir/$filename";

        if ( $archive_dir ) {
            my $archive_path = $archive_dir . "/$filename";
            write_file( $archive_path, $json );
            say "FILE WRITTEN TO $archive_path";
        }

        unless ( $test_mode ) {
            write_file( $realpath, $json );
            say "FILE WRITTEN TO $realpath";

            my $host      = $self->retrieve_data('host');
            my $username  = $self->retrieve_data('username');
            my $password  = $self->retrieve_data('password');
            my $directory = 'cust2unique';

            my $sftp = Net::SFTP::Foreign->new(
                host     => $host,
                user     => $username,
                port     => 22,
                password => $password
            );
            $sftp->die_on_error("Unable to establish SFTP connection");
            $sftp->setcwd($directory)
              or die "unable to change cwd: " . $sftp->error;
            $sftp->put( $realpath, $filename )
              or die "put failed: " . $sftp->error;
        }
    }
}

sub api_routes {
    my ( $self, $args ) = @_;

    my $spec_str = $self->mbf_read('openapi.json');
    my $spec     = decode_json($spec_str);

    return $spec;
}

sub api_namespace {
    my ($self) = @_;

    return 'messagebee';
}

1;
