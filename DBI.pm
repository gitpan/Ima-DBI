package Ima::DBI;


=head1 NAME

Ima::DBI - Makes an object be the database connection

=head1 SYNOPSIS

  package Foo;

  use Ima::DBI;
  @ISA = qw(Ima::DBI);

  # Set up the database connections used, but don't actually connect, yet.
  Foo->setConnection('Users', 'dbi:Oracle:STAR', 'UserAdmin', 'b4u3');
  Foo->setConnection('Customers', 'dbi:Oracle:STAR', 'Staff', 'r3d0dd');

  Foo->setStatement('FindUser', 
					'SELECT * FROM USERS WHERE name = ?','Users');
  Foo->setStatement('AddUser', $AddUserSQL, 'Users');
  Foo->setStatement('AlterCustomer', 
					'UPDATE CUSTOMERS SET language = ? 
                     WHERE country = ?', 'Customers');


  sub new {
    # the usual stuff
  }

  
  package main;

  $obj = Foo->new;
  
  eval {
    $obj->sql_FindUser->execute(['Butch'], [\($name)]);  # bind and execute.
    while( $obj->sql_FindUser->fetch ) {  # fetch
      last if $name eq 'Cassidy';
    }
  
    # Only does $sth->bind_cols, execute and $sth->fetchall_array, since 
    # we're already connected, and prepared...
    $obj->sql_FindUser->execute(['Sundance']);
    @row = $obj->sql_FindUser->fetchall;
    
    # binds and executes the UPDATE statement.
    $rowsAltered = $obj->sql_AlterCustomer->execute(['es', 'mx']);
  };
  unless ($@) {
    # commit will figure out which DBH's to commit.
    $obj->commit('FindUser', 'AlterCustomer');
  }
  else {
    $obj->rollback('FindUser', 'AlterCustomer');
    warn "We've failed you!:  $@";
  }

  # $obj will disconnect its db's automatically upon destruction.


=cut

use strict;
use DBI;
use Carp;

use vars qw($VERSION);


BEGIN {
  $VERSION = 0.03;
}


my %Connections;    # All available DBH connections.
my %Statements;     # All available Statements (but not handles)

# So it works something like this...
# - All dbh's are named and held in %Connections.  This will eventually
#   be a pool of pre-connected $dbh's, checked in, checked out, etc... a
#   la the mod_perl & DBI article in TPJ #9.
# - All statements are named and hold the original SQL statement, the name
#   of the DBH this is associated with, in %Statements.


=head1 PUBLIC METHODS

=over 4

=item B<setConnection>

  Module::Name->setConnection($connectionName, $dataSource, 
		                      $username, $password) ||
    warn "We already have a connection with the name $connectionName";

Tells how to connect to a database.  This connection will be refered to by
its $connectionName.  The rest is passed to DBI/connect, eventually.

It will return undef if there is already a connection with this
$connectionName, a true value otherwise.

Note:  This does -not- actually connect to the database.  Connections are 
defered until an sql_*() method is called.  Eventually this will do
something more clever, like the dbh checkin/checkout system described in
the new TPJ.

Eventually it will use the DBI->connect_cached that's been promised for a 
while.

=cut

#'# cperl bug


# This doesn't simply override an existing connection because this would be
# potentially disasterous for existing statement handles, and I -really-
# don't feel like thinking about that.
#
# In the future, this may also create a _dbh_* closure.
#
# Shit, this isn't safe across packages!
sub setConnection {
  my($self, $dbName, $dataSource, $username, $password) = @_;

  unless (exists $Connections{$dbName}) {
    $Connections{$dbName} = {'Data Source' => $dataSource,
			     Username   => $username,
			     Password   => $password,
			     InUse      => 1
			    };
    return 'My aloe plant would like to say a few words.';  # Suckcess!
  }
  else {
    $Connections{$dbName}{InUse}++;
    return undef;  # name clash.
  }
}

=pod

=item B<setStatement>

  Module::Name->setStatement($statementName, $sql, $connectionName) ||
     warn "We couldn't make the statement $statementName";

Sets up an SQL statement and access method on the database specified by 
$connectionName, which was set up by setConnection().  The access
method is named "sql_$statementName()".  Returns undef if a statement with
this name has already been created, true otherwise.

ie.  Module::Name->setStatement('Bubba', 'select * from table', 'Data');  creates a method called sql_Bubba().


=item B<sql_*>

  $sth = $obj->sql_name;

Access method to an SQL statement, set eariler with setStatement().
Preperation of the statement is handled for you.  

Returns a statement handle().  While you can use this handle directly, the 
prefered method of using sql_* is not to do this.  Use it as
"$obj->sql_name->sth_method" where sth_method is one of the methods outlined
in L<statement handle>.

=cut


#'# cperl bug

# If you're squeamish about perl magick, don't look at this.  It involves
# closures and symbol table manipulation.
#
# <\mjd> sub setstatement  { my ($self, $name, @vars) = @_; my $pack = ref
#  $self; *{$pack .'::'. $name} = sub { .... } }
sub setStatement {
  my($self, $name, $sql, $database) = @_;

  my $package = ref $self;

  unless ( exists $Statements{$name} ||
	   !exists $Connections{$database}) 
  {
    
    $Statements{$name} = { 
			  sql      => $sql,
			  database => $database,
			 };


    # PROBLEM - How to have an access method which returns a unique
    # statement handle instance for each Ima::DBI instance?

    # SOLUTION -
    # <Roderick> sub method { my $self = shift; my $closure = 
    #  ($closure_map{$self} ||= generate_closure); $closure->(@_) }

    # Here we create our sql_* methods as closures.
    # I wonder if I can pull this out of the code somehow, put it in a more
    # logical location rather than sitting here in the middle of nowhere.
    # ------------------ sql_* CLOSURE --------------------------------------
    no strict 'refs';  # We need a symbolic reference here.
    *{$package."::sql_$name"} = 
      sub {
		use strict 'refs';
		my($self) = @_;

		my $sth = $self->_connect($database)->prepare_cached($sql);

		# This isn't the most pleasent thing in the universe to do.
		# The hard-coded class is baaaaaaad.  Need to think of something
		# more creative.
		bless $sth, 'Ima::DBI::st';   # Praise Bob, it is Born Again!!!
	
		return $sth;
      };
    #-------------- END sql_* CLOSURE --------------------------------------


    return 'Oh, I wish I were an Oscar Meyer Weiner';  # Suckcess!
  }
  else {
    return undef;  # name clash.
  }
}


=pod

=item B<commit>

  $obj->commit;
  $obj->commit(@connectionNames);

Wrapper around DBI/commit.

Ima::DBI shuts off AutoCommit by default.  If you want to turn it back on...
well, there's no good way to do that just yet.  Deal.  So you must commit your
changes.  

If called with no args, it commits all open database handles
available to it, via DBI/commit (this may be a Bad Thing).
If given arguments, it will commit
only on those @connectionNames (as created with setConnection).

commit will die if it is given an invalid connection name.  It should 
probably do something more pleasent.

=cut

#'# cperl bug

sub commit {
  my ($self, @connectionNames) = @_;
  
  if (@connectionNames) {
    # Commit each given connection name.
    foreach my $name (@connectionNames) {
      $self->_getDBH($name)->commit;
    }
  }
  else {
    # Commit every open connection.
    foreach my $dbh ($self->_getAllDBHs) {
      $dbh->commit;
    }
  }

  return 'Truth is Light.';  # Suckcess!
}


=pod

=item B<rollback>

  $obj->rollback;
  $obj->rollback(@connectionNames);

Wrapper around DBI/rollback.

Acts exactly like L</commit>.

=back

=cut

#'# cperl bug

sub rollback {
  my ($self, @connectionNames) = @_;
  
  if (@connectionNames) {
    # Commit each given connection name.
    foreach my $name (@connectionNames) {
      my $dbh = $self->_getDBH($name);
      $dbh->rollback if defined $dbh;
    }
  }
  else {
    # Commit every open connection.
    foreach my $dbh ($self->_getAllDBHs) {
      $dbh->rollback;
    }
  }

  return q|Boy, that\'s quite a head of hair on those legs.|;  # Suckcess!  
}



# PRIVATE METHODS

# $dbh = $obj->_connect(connection_name);
# You really should never have to call this.  sql_* handles it.
#
# Raise error is -on-.  PrintError is -off-.  Autocommit is -off-.  Fuck ODBC.
sub _connect {
  my( $self, $name ) = @_;

  my $dbh;
  
  # For clarity and speed, store our connection info seperately.
  my $conn = $Connections{$name};

  
  # Check if we've already opened this connection.
  # Can dbh's timeout?  Or fail?  Gotta think of a way to check against
  # that.  Should I use Active() or ping()?
  unless ( defined $conn->{'dbh'} ) {
    $dbh = DBI->connect(@{$conn}{'Data Source', 'Username', 'Password'},
			{
			 RaiseError => 1,
			 AutoCommit => 0,
			 PrintError => 0
			}) ||
      carp("Can't DBI connect with '", 
	   join(', ', @{$conn}{'Data Source', 'Username', 'Password'}), 
	        "'.  ", $DBI::errstr);
    $conn->{'dbh'} = $dbh;
  }
  else {  # Guess its already opened.
    $dbh = $conn->{'dbh'};    
  }
  
  return $dbh;
}


# Get the database handle, by name.
sub _getDBH {
  my($self, $name) = @_;

  return $Connections{$name}{'dbh'};
}


# Get all connected database handles.
sub _getDBHs {
  my @dbhs;
  foreach (keys %Connections) { 
    push @dbhs, $Connections{$_}{'dbh'} if defined $Connections{$_}{'dbh'};
  }

  return @dbhs;
}


# This needs to be set up to decrement its $dbh and $sth connections, and
# in the future, check-in its $dbh's.
# DESTTTRRRRRRROOOOOOYYYYY!!
#sub DESTROY {
#  my $self = @_;
#
#  $self->removeStatements;  # Finish and delete all $sth's owned by
#                            # this object.
#  $self->disconnect;  # inform the connection pool that I'll no longer be
#                      # needing them.
#}


return 2112;  # We are the Priests


###########################  Ima::DBI::st ###############################
########################### DBI::st subclass #############################
package Ima::DBI::st;

use vars qw(@ISA $VERSION);
BEGIN {
  $VERSION = 0.01;
  @ISA = qw(DBI::st);
}

=head1 Ima::DBI::st - DBI statement handle subclass.

sql_*() returns a special subclass of the normal DBI/statement handle
which works very much like a normal DBI/statement handle with a few
differences outlined here.

You should avoid using the statement handle directly, but should instead
access it through the sql_*() method.

=head2 Modified Statement Handle Methods

Unless otherwise specified here, all methods available to a normal DBI
Statement Handle operate normally.

The below examples all use "sql_name", but any sql_*() method can be used.

=over 4

=item B<execute>

  $obj->sql_name->execute(\@bind_params, \@bind_columns);

Execute performs all the functions of DBI/bind_param, DBI/bind_col and 
DBI/execute without you worrying your pretty little head about such things.

@bind_params is fed to DBI/bind_param one at a time, roughly equivalent
to...

  for (1..@bind_params) { 
    $sth->bind_param($_, $bind_params[$_]);
  }

or simply

  $sth->execute(@bind_values);

If @bind_params is formated as a list of lists, then it is used as follows...

  for(1..@bind_params) { 
    $sth->bind_param($_, $bind_params[$_][0], $bind_params[$_][1]);
  }

First element is the bind value, the seconds would be a hash reference for
the D<DBI/\%attr>.

@bind_columns is used in a manner similar to @bind_params, but it feeds to
L<DBI/bind_col>.  In the future it may feed to L<DBI/bind_columns>, but I
need to figure out the differences between the use of \%attr between the two.

=cut

#'# cperl bug

sub execute {
  my($sth, $bindParams, $bindCols) = @_;
  
  # Do we have any parameters to bind?
  if ( defined $bindParams ) {
    for my $col (1..@$bindParams) {
      my $value = $bindParams->[$col];
      unless ( ref $value ) {
	$sth->SUPER::bind_param($col, $value);
      }
      elsif ( ref $value eq 'ARRAY' ) {
	$sth->SUPER::bind_param($col, @$value);
      }
      else {
	die "First argument to execute() must be a list reference, or reference to a list of lists.";
      }
    }
  }
  
  # Do we have any variables to bind?
  if ( defined $bindCols ) {
    for my $col (1..@$bindCols) {
      my $value = $bindCols->[$col];
      if ( ref $value eq 'SCALAR' ) {
	$sth->SUPER::bind_col($col, $value);
      }
      elsif ( ref $value eq 'ARRAY' ) {
	$sth->SUPER::bind_col($col, @$value);
      }
      else {
	die "Second argument to execute() must be a list reference, or reference to a list of lists.";
      }
    }
  }

  # Toss the rv back.
  return $sth->SUPER::execute;
}


=pod

=item B<fetch>

  $row_ref = $obj->sql_name->fetch;
  @row     = $obj->sql_name->fetch;

This implements a context sensitive version of L<DBI/fetch>.  If used in
scalar context it will act as L<DBI/fetchrow_arrayref>.  If used in list
context it will act as L<DBI/fetchrow_array>.

Makes more sense to me.

=cut

#'# cperl bug

sub fetch {
  my($sth) = @_;
  
  return wantarray ? $sth->SUPER::fetchrow_array :
                     $sth->SUPER::fetchrow_arrayref;
}


=item B<fetchall>

  $tbl_ref = $obj->sql_name->fetchall;
  @tbl     = $obj->sql_name->fetchall;

If used in scalar context it will act as L<DBI/fetchall_arrayref>.  If used 
in list context it will return an array of rows fetched.  This is, of course
horribly inefficient and is provided more for convenience and orthigality 
with L</fetch> than anything else.

=cut

#'#cperl bug

sub fetchall {
  my($sth) = @_;
  my $tbl = $sth->fetchall_arrayref;
  
  return wantarray ? @$tbl : $tbl;
}


=pod

=item B<fetchall_hashref>

  $tbl_ref = $obj->sql_name->fetchall_hash;
  @tbl     = $obj->sql_name->fetchall_hash;

This method acts very much like fetchrow_hashref(), except that it
slurps down the every row of the fetch, as fetchall does, but into an
array of hashes, rather than an array of arrays.

Like fetchall(), it is sensitive to the context, so if used in list
context it returns an array of hashes.  In scalar context it returns a
reference to an array of hashes (I don't know how much that really
buys you.)

B<CAVET SCRIPTOR!> fetchrow_hashref is bad enough as a potential
memory/performance hog, fetchall_hashref has the potential to -really-
hose things if used on large fetches.  It's ment as a convenience,
don't abuse it too badly.

=cut

# Looks like there's some code to do this in DBI->fetchall_arrayref,
# but its undocumented.
sub fetchall_hashref {
    my $sth = shift;
    my (@rows, $row);
    push @rows, $row while ($row = $sth->fetchrow_hashref);
    return wantarray ? @rows : \@rows;
} 

# DESTRRRRRRROOOOOOOOOYYYY!!
sub DESTROY {
  my($sth) = @_;
  $sth->finish;  # I don't think this is necessary, but I'll toss it in
                 # for kicks.
}

=pod

=back

=head1 TODO

=over 3

=item Unstable interface.

I haven't decided if I like the way this module works, so expect the worst.
The interface will change.

=item Checkin/checkout system for $dbh's.

DBI::dh needs to be subclassed.  connect(), disconect() to work with the
checkin/out system.

=item Clean up unused $sth's.

=back

=head1 OO EVILNESS

Ima::DBI contains no OO Evilness.  No new items are added to your object's
hash... in fact, it doesn't even assume that your object will be modeled as
a hash.

Please examine Ima::DBI::setStatement()'s code and let me know if you have
a better way to accomplish this.

=head1 DEPENDENCIES

DBI

=head1 AUTHOR

Michael G. Schwern <schwern@starmedia.net> for Starmedia Networks

=cut 

return 1001001;  # SOS
