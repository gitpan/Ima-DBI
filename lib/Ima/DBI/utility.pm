package Ima::DBI::utility;

require Exporter;
use Carp;
use Carp::Assert;

use base qw(Exporter);

use vars qw($VERSION);
BEGIN { $VERSION = 0.03; }

@EXPORT = qw(SUCCESS FAILURE TRUE FALSE YES NO ERROR UNUSED
             _taint_check _unimplemented _taint_these _taint_this);

use constant SUCCESS => 1;
use constant FAILURE => 0;
use constant TRUE    => 1;
use constant FALSE   => 0;
use constant YES     => TRUE;       # functions which answer a question (is_blah)
use constant NO      => FALSE;      # should use these.
use constant ERROR   => -1;         # an error occured
use constant UNUSED  => undef;  # the return value of is unused.

# NOT rigourous enough.
# Mostly unncessary due to DBI's new Taint syntax.
sub _taint_check {
    unless(eval { () = join('',@_), kill 0; 1; }) {
        croak "Insecure dependency";
    }
    return UNUSED;
}

sub _unimplemented {
    carp 'This function is unimplemented at this time.';
    return UNUSED;
}


sub _taint_these {
    map { _taint_this($_) } @_;
}


# Unncessary due to DBI's new Taint syntax.
# Needs much work.  Needs to recurse into data structures.
my $Evil = $0;
$Evil =~ s/.*//;
sub _taint_this ($) {
    my($data) = shift;

    local $@ = $Evil;
    if( ref $data ) {
        eval { die $data };
        $data = $@;
    }
    else {
        $data .= $Evil;
    }

    return $data;
}






