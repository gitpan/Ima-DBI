package Ima::DBI;

use strict;
use DBI;
use Carp;
use Carp::Assert;
use Ima::DBI::utility;

use vars qw($VERSION);

BEGIN {
    $VERSION = '0.18';
}


=pod

=head1 NAME

Ima::DBI - Database connection caching and organization


=head1 SYNOPSIS

    # Class-wide methods.
    __PACKAGE__->set_db($db_name, $data_source, $user, $password);
    __PACKAGE__->set_db($db_name, $data_source, $user, $password, \%attr);
    
    __PACKAGE__->set_sql($sql_name, $statement, $db_name);

    
    # Object methods.
    $dbh = $obj->db_*;      # Where * is the name of the db connection.
    $sth = $obj->sql_*;     # Where * is the name of the sql statement.
    $sth = $obj->sql_*(@sql_pieces);

    $obj->DBIwarn($what, $doing);


    # Modified statement handle methods.
    $rv = $sth->execute;
    $rv = $sth->execute(@bind_values);
    $rv = $sth->execute(\@bind_values, \@bind_cols);

    # In addition to the normal DBI sth methods...
    $row_ref = $sth->fetch;
    @row     = $sth->fetch;
    
    $row_ref = $sth->fetch_hash;
    %row     = $sth->fetch_hash;
    
    $rows_ref = $sth->fetchall;
    @rows     = $sth->fetchall;

    $rows_ref = $sth->fetchall_hash;
    @tbl      = $sth->fetchall_hash;

    $rc = $obj->commit;             #UNIMPLEMENTED
    $rc = $obj->commit(@db_names);  #UNIMPLEMENTED
    
    $rc = $obj->rollback;            #UNIMPLEMENTED
    $rc = $obj->rollback(@db_names); #UNIMPLEMENTED

    $sth->clear_cache;  #UNIMPLEMENTED
    
    $obj->clear_db_cache;            #UNIMPLEMENTED
    $obj->clear_db_cache(@db_names); #UNIMPLEMENTED
    
    $obj->clear_sql_cache;             #UNIMPLMENTED
    $obj->clear_sql_cache(@sql_names); #UNIMPLMENTED


=head1 DESCRIPTION

Ima::DBI attempts to organize and facilitate caching and more
efficient use of database connections and statement handles by
storing DBI and SQL information with your class (instead of as
seperate objects).  This allows you to pass around just one object
without worrying about a trail of DBI handles behind it.

One of the things I always found annoying about writing large programs
with DBI was making sure that I didn't have duplicate database handles
open.  I was also annoyed by the somewhat wasteful nature of the
prepare/execute/finish route I'd tend to go through in my subroutines.
The new DBI->connect_cached and DBI->prepare_cached helped alot, but I
still had to throw around global datasource, username and password
information.

So, after a while I grew a small library of DBI helper routines and
techniques.  Ima::DBI is the culmination of all this, put into a
nice(?), clean(?) class to be inherited from.


=head2 Why should I use this thing?

Ima::DBI is a little odd, and it's kinda hard to explain.  So lemme
explain why you'd want to use this thing...

=over 4

=item * Consolidation of all SQL statements and database information

No matter what, embedding one language into another is messy.  DBI
alleviates this somewhat, but I've found a tendency to have that
scatter the SQL around inside the Perl code.  Ima::DBI allows you to
easily group the SQL statements in one place where they are easier to
maintain (especially if one developer is writing the SQL, another
writing the Perl).  Alternatively, you can place your SQL statement
alongside the code which uses it.  Whatever floats your boat.

Database connection information (data source, username, password,
atrributes, etc...) can also be consolidated together and tracked.

Both the SQL and the connection info are probably going to change
alot, so having them well organized and easy to find in the code is a
Big Help.

=item * Holds off opening a database connection until necessary.

While Ima::DBI is informed of all your database connections and SQL
statements at compile-time, it will not connect to the database until
you actually prepare a statement on that connection.

This is obviously very good for programs that sometimes never touch
the database.  It's also good for code that has lots of possible
connections and statements, but which typically only use a few.  Kinda
like an autoloader.

=item * Easy integration of the DBI handles into your class

Ima::DBI causes each database handle to be associated with your class,
allowing you to pull handles from an instance of your object, as well
as making many oft-used DBI methods available directly from your
instance.

This gives you a cleaner OO design, since you can now just throw
around the object as usual and it will carry its associated DBI
baggage with it.

=item * Honors taint mode

It always struck me as a design deficiency that tainted SQL statements 
could be passed to $sth->prepare().  For example:

    # $user is from an untrusted source and is tainted.
    $user = get_user_data_from_the_outside_world;
    $sth = $dbh->prepare('DELETE FROM Users WHERE User = $user');

Looks innocent enough... but what if $user was the string "1 OR User
LIKE %".  You just blew away all your users, hope you have backups.

Ima::DBI turns on the DBI->connect Taint attribute so that all DBI
methods (except execute()) will no longer accept tainted data.
L<DBI/Taint> for details.

=item * Taints returned data

Databases should be like any other system call.  Its the scary Outside
World, thus it should be tainted.  Simp.  Ima::DBI turns on DBI's
Taint attribute on each connection.  This feature is overridable by
passing your own Taint attribute to set_db as normal for DBI.
L<DBI/Taint> for details.

=item * Encapsulation of some of the more repetative bits of everyday DBI usage

I get lazy alot and I forget to do things I really should, like using
bind_cols(), or rigorous error checking.  Ima::DBI does some of this
stuff automaticly, other times it just makes it more convenient.

=item * Encapsulation of DBI's cache system

DBI's automatic handle caching system is relatively new, some people
aren't aware of its use.  Ima::DBI uses it automatically, so you don't
have to worry your pretty little head about it.  (It even makes it a bit
more efficient)

=item * Sharing of database and sql information amongst inherited classes

Any SQL statements and connections created by a class is available to
its children via normal method inheritance.

=item * Convenience and orthoganality amongst statement handle methods

It always struck me odd that DBI didn't take much advantage of Perl's
context sensitivity.  Ima::DBI redefines some of the various fetch
methods to fix this oversight; it also adds a few new methods for
convenience (though not necessarily efficiency).

=item * Guarantees one connection per program.

One program, one database connection (per database user).  One
program, one prepared statement handle (per statement, per database
user).  That's what Ima::DBI enforces.  Extremely handy in persistant
environments (servers, daemons, mod_perl, FastCGI, etc...)

=item * Encourages use of bind parameters and columns

Bind parameters are safer and more efficient than embedding the column
information straight into the SQL statement.  Bind columns are more
efficient than normal fetching.  Ima::DBI pretty much requires the
usage of the former, and eases the use of the latter.

=back

=head2 Why shouldn't I use this thing.

=over 4

=item * It's all about OO

Although it is possible to use Ima::DBI as a stand-alone module as
part of a function-oriented design, its generally not to be used
unless integrated into an object-oriented design.

=item * Overkill for small programs

=item * Overkill for programs with only one or two SQL statements

Its up to you whether the trouble of setting up a class and jumping
through the necessary Ima::DBI hoops is worth it for small programs.
To me, it takes just as much time to set up an Ima::DBI subclass as it
would to access DBI without it... but then again I wrote the module.
YMMV.

=item * Overkill for programs that only use their SQL statements once

Ima::DBI's caching might prove to be an unecessary performance hog if
you never use the same SQL statement twice (soon caching will become
optional).  Not sure, I haven't looked into it.

=back


=head1 USAGE

The basic steps to "DBIing" a class are:

=over 4

=item 1 

Inherit from Ima::DBI

=item 2 

Set up and name all your database connections via set_db()

=item 3 

Set up and name all your SQL statements via set_sql()

=item 4 

Use sql_* to retrieve your statement handles ($sth) as needed and db_*
to retreive database handles ($dbh).


=back

Have a look at the L<EXAMPLE> below.


=head1 TAINTING

Ima::DBI, by default, uses DBI's Taint flag on all connections.

This means that no Ima::DBI method will accept tainted data and all
data fetched from the database will be tainted.  This may be different
from the DBI behavior you're used to.  L<DBI/Taint> for details.


=head1 METHODS

=head2 Class methods

=over 4

=item B<set_db>

    __PACKAGE__->set_db($db_name, $data_source, $user, $password);
    __PACKAGE__->set_db($db_name, $data_source, $user, $password, \%attr);

This method is used in place of DBI->connect to create your database handles.

Sets up a new DBI database handle associated to $db_name.  All other
arguments are passed through to DBI->connect_cached.

A new method is created for each db you setup.  This new method is
db_$db_name... so, for example, __PACKAGE__->set_db("foo", ...) will
create a method called db_foo().

If no %attr is supplied (RaiseError => 1, AutoCommit => 0, PrintError
=> 0) is assumed.  This is a better default IMHO, however it does give
databases without transactions (such as MySQL) a hard time.  Be sure
to turn AutoCommit back on if your database does not support
transactions.

The actual database handle creation (and thus the database connection)
is held off until a prepare is attempted with this handle.

Spaces in $db_name will be translated into underscores ('_')

=cut

#'#
sub set_db {
    my($class, $db_name, $data_source, $user, $password, $attr) = @_;
    
    # The rest will be delt with by DBI better than I can.
    _taint_check($class, $db_name);
    
    assert(@_ >= 5 || @_ <= 6) if DEBUG;
    assert(!defined $attr or ref $attr eq 'HASH') if DEBUG;
    
    # Join the user's %attr with our defaults.
    $attr = {} unless defined $attr;
    $attr = { RaiseError => 1, AutoCommit => 0, PrintError => 0, Taint => 1, 
              %$attr };

    $db_name =~ s/\s/_/g;
    
    no strict 'refs';
    *{$class."::db_$db_name"} = 
      $class->_mk_db_closure($data_source, $user, $password, $attr);
    
    return SUCCESS;
}

sub _mk_db_closure {
    my($class, @connection) = @_;

    my $dbh;
    return sub {
          unless( $dbh && $dbh->FETCH('Active') && $dbh->ping ) {
	      $dbh = DBI->connect_cached(@connection);
	      bless $dbh, 'Ima::DBI::db';
	  }
            
	  return $dbh;
    };
}


=pod

=item B<set_sql>

    __PACKAGE__->set_sql($sql_name, $statement, $db_name);

This method is used in place of DBI->prepare to create your statement
handles.

Sets up a new statement handle using associated to $sql_name using the
database connection associated with $db_name.  $statement is passed
through to DBI->prepare_cached to create the statement handle.

A new method is created for each statement you set up.  This new
method is sql_$sql_name... so, as with set_db,
__PACKAGE__->set_sql("bar", ..., "foo"); will create a method called
sql_bar() which uses the database connection from db_foo().

The actual statement handle creation is held off until sql_* is first
called on this name.

Spaces in $sql_name will be translated into underscores ('_')

To make up for the limitations of bind parameters, $statement can
contain sprintf() style formatting (ie. %s and such) to allow
dynamically generated SQL statements.  See sql_* below for more
details.

=cut

sub set_sql {
    my($class, $sql_name, $statement, $db_name) = @_;
    
    # DBI will take care of the rest better than I can.
    _taint_check($class, $sql_name, $db_name);
    
    # ------------------------- sql_* closure ----------------------- #
    my $db_meth = $db_name;
    $db_meth =~ s/\s/_/g;
    $db_meth = "db_$db_meth";
    $class->can($db_meth) or
      die "There is no database connection named '$db_name' ".
	  "defined in $class";

    my $sql_meth = $sql_name;
    $sql_meth =~ s/\s/_/g;
    $sql_meth = "sql_$sql_name";
    
    no strict 'refs';
    *{$class."::$sql_meth"} = 
      $class->_mk_sql_closure($sql_name, $statement, $db_meth);

    return SUCCESS;
}


sub _mk_sql_closure {
    my($class, $sql_name, $statement, $db_meth) = @_;

    my $sth;
    return sub {
        my $class = shift;

        # Must allow subclasses to override database connections.
        my $dbh = $class->$db_meth();

        # Calling prepare_cached over and over again is also expensive.
        # Again, we co-opt some of prepare_cached's functionality.
        if ( !$sth ) {  # No $sth defined yet.
            # Maybe I can do this at compile-time.
            my $sql = '';
            if( @_ ) {
                # Make sure we got something that looks like a 
                # sprintf() string.
                assert( do { my $count = 0;
                             $count = () = $statement =~ m/(?<!%)%[^%]/g;
                             $count == @_ } ) if DEBUG;
                
                $sql = sprintf($statement, @_);
            }
            else {
                $sql = $statement;
            }

            # Need to make caching an option.
            unless( @_) {
                $sth = $dbh->prepare_cached($sql);
            }
            else {
                $sth = $dbh->prepare($sql);
            }

            bless $sth, 'Ima::DBI::st';
        }
        else {          # $sth defined.
            # Check to see if the handle is active.
            if( $sth->FETCH('Active') ) {
                Carp::croak("'$sql_name' statement handle is still ".
                            "active!  Finishing for you.");
                $sth->finish;
            }
        }
        
        return $sth;
    };
}

=pod

=back

=head2 Object methods

=over 4

=item B<db_*>

    $dbh = $obj->db_*;
    
This is how you directly access a database handle you set up with set_db.

The actual particular method name is derived from what you told set_db.

db_* will handle all the issues of making sure you're already
connected to the database.

=item B<sql_*>

    $sth = $obj->sql_*;
    $sth = $obj->sql_*(@sql_pieces);

sql_*() is a catch-all name for the methods you set up with set_sql().
For instance, if you did:

    __PACKAGE__->set_sql('GetAllFoo', 'Select * From Foo', 'SomeDb');

you'd run that statement with sql_GetAllFoo().

sql_* will handle all the issues of making sure the database is
already connected, and the statement handle is prepared.  It returns a
prepared statement handle for you to use.  (You're expected to
execute() it)

If sql_*() is given a list of @sql_pieces it will use them to fill in
your statement, assuming you have sprintf() formatting tags in your
statement.  For example:

    __PACKAGE__->set_sql('GetTable', 'Select * From %s', 'Things');
    
    # Assuming we have created an object... this will prepare the
    # statement 'Select * From Bar'
    $sth = $obj->sql_Search('Bar');

Be B<very careful> with what you feed this function.  It cannot
do any quoting or escaping for you, so it is totally up to you
to take care of that.  Fortunately if you have tainting on you
will be spared the worst.

It is recommended you only use this in cases where bind parameters
will not work.

=item B<clear_db_cache>     *UNIMPLEMENTED*

    $obj->clear_db_cache;
    $obj->clear_db_cache(@db_names);

Ima::DBI uses the DBI->connect_cached to cache open database handles.
For whatever reason you might want to clear this cache out and start
over again.

A call to clear_db_cache with no arguments deletes all database
handles out of the cache and all associated statement handles.
Otherwise it only deletes those handles listed in @db_names (and their
associated statement handles).

Note that clearing from the cache does not necessarily destroy the
database handle.  Something else might have a reference to it.

Alternatively, you may do:  $obj->db_Name->clear_cache;

=cut

sub clear_db_cache {
#    _taint_check(@_);
    _unimplemented;
}

=pod

=item B<clear_sql_cache>    *UNIMPLEMENTED*

    $obj->clear_sql_cache;
    $obj->clear_sql_cache(@sql_names);

Does the same thing as clear_db_cache, except it does it in relation
to statement handles.

Alternatively, you may do:  $obj->sql_Name->clear_cache;

=cut

sub clear_sql_cache {
#    _taint_check(@_);
    _unimplemented;
}

=pod

=item B<DBIwarn>    *UNIMPLEMENTED*

    $obj->DBIwarn($what, $doing);
    
Produces a useful error for exceptions with DBI.

B<I'm not particularly happy with this interface>

Most useful like this:

    eval {
        $self->sql_Something->execute($self->{ID}, @stuff);
    };
    if($@) {
        $self->DBIwarn($self->{ID}, 'Something');
                return;
    }


=cut

sub DBIwarn {
    my($self, $thing, $doing) = @_;
    my $errstr = "Failure while doing '$doing' with '$thing'\n";
    $errstr .= $@ if $@;

    require Carp;
    Carp::carp $errstr;

    return SUCCESS;
}

=pod

=back


=head2 Modified database handle methods

Ima::DBI makes some of the methods available to your object that are
normally only available via the database handle.  In addition, it
spices up the API a bit.

=cut

################################ Ima::DBI::db #############################
###################### DBI database handle subclass #######################

package Ima::DBI::db;

use Ima::DBI::utility;
use Carp::Assert;
use Carp;

use base qw(DBI::db);  # Uhh, I think that's right.

=pod

=over 4

=item B<prepare>

   $sth = $dbh->prepare(...);

Acts just like DBI's prepare except that it returns an Ima::DBI::st
object.  Eventually, all database handle methods which return a
statement handle will return an Ima::DBI::st.

=cut

sub prepare {
   my ($self) = shift;
   my $sth = $self->SUPER::prepare(@_);
   bless $sth, "Ima::DBI::st";
}

=pod

=item B<commit>         *UNIMPLEMENTED*

    $rc = $obj->commit;
    $rc = $obj->commit(@db_names);

Derived from $dbh->commit() and basically does the same thing.

If called with no arguments, it causes commit() to be called on all
database handles associated with $obj.  Otherwise it commits all
database handles whose names are listed in @db_names.

Alternatively, you may like to do:  $rc = $obj->db_Name->commit;

=cut

sub commit {
#    _taint_check(@_);
    _unimplemented;
}

=pod

=item B<rollback>       *UNIMPLEMENTED*

    $rc = $obj->rollback;
    $rc = $obj->rollback(@db_names);

Derived from $dbj->rollback, it acts just like Ima::DBI->commit,
except that it calls rollback().

Alternatively, you may like to do:  $rc = $obj->db_Name->rollback;

=cut

sub rollback {
#    _taint_check(@_);
    _unimplemented;
}

=pod

=item B<clear_cache>    *UNIMPLEMENTED*

    $dbh->clear_cache;
    
Provides a mechanism to clear a given database handle from the cache.


=back

=head2 Modified statement handle methods

Ima::DBI overrides the normal DBI statement handle with its own,
slightly modified, version.  Don't worry, it inherits from DBI::st, so
anything not explicitly mentioned here will work just like in normal
DBI.

=cut

############################## Ima::DBI::st ##################################
###################### DBI statement handle subclass #########################

package Ima::DBI::st;

use base qw(DBI::st);

use Ima::DBI::utility;
use Carp::Assert;
use Carp;

=pod

=over 4

=item B<execute>

    $rv = $sth->execute;
    $rv = $sth->execute(@bind_values);
    $rv = $sth->execute(\@bind_values, \@bind_cols);
    
DBI::st->execute is overridden to enhance execute() a bit.

If called with no arguments, or with a simple list, execute() operates
normally.  When when called with two array references, it performs the
functions of bind_param, execute and bind_columns similar to the
following:

    $sth->execute(@bind_values);
    $sth->bind_columns(undef, @bind_cols);

In addition, execute I<will accept tainted @bind_values>.  I
personally found it annoying to have to detaint everything I passed to
execute() and tended to shut off taint mode rather than go through the
trouble.  I also can't think of what a malicious user could do with a
tainted bind value (in the general case.  Your application may vary.)

Thus a typical idiom would be:

    $sth->execute([$this, $that], [\($foo, $bar)]);

Of course, this method provides no way of passing bind attributes
through to bind_param or bind_columns.  If that is necessary, then you
must perform the bind_param, execute, bind_col sequence yourself.

=cut

sub execute {
    my($sth) = shift;
    
    my $rv;

    my $orig_taint = $sth->{Taint};

    # Allow $sth->execute(\@param, \@cols) and 
    # $sth->execute(undef, \@cols) syntax.
    if( @_ == 2 and 
        (!defined $_[0] || ref $_[0] eq 'ARRAY') and
        ref $_[1] eq 'ARRAY' ) 
    {
        my($bind_params, $bind_cols) = @_;

	# We're going to shut off tainting for execute() because I
	# can't think of a good reason why a tainted bind param would
	# be dangerous (in general) and its really obnoxious to have
	# to detaint -all- your bind params.
	$sth->{Taint} = 0;
        $rv = $sth->SUPER::execute(@$bind_params);
	$sth->{Taint} = $orig_taint;
        $sth->SUPER::bind_columns(@$bind_cols);
    }
    else {
        # There should be no references
        assert(!grep { ref $_ } @_) if DEBUG;
	# Same as above.
	$sth->{Taint} = 0;
        $rv = $sth->SUPER::execute(@_);
	$sth->{Taint} = $orig_taint;
    }
    
    return $rv;
}

=pod

=item B<clear_cache>    *UNIMPLEMENTED*

    $sth->clear_cache;
    
Provides a mechanism to clear a given statement handle from the cache.

=cut

sub clear_cache {
    _taint_check(@_);
    _unimplemented;
}

=pod

=back

=head2 fetching

The following are modifications or expansions on DBI's various fetch
methods.  Most are simply context sensitive implementations.  Some
just have shorter names.

Remember that most of the list context versions of the fetch methods
tend to use more memory and be slower.  Same with the fetchall
methods.  Use with care.

=over 4

=item B<fetch>

    $row_ref = $sth->fetch;
    @row     = $sth->fetch;

A context sensitive version of fetch().  When in scalar context, it
will act as fetchrow_arrayref.  In list context it will use
fetchrow_array.

=cut

#'#
sub fetch {
    my($sth) = shift;
    return wantarray ? $sth->SUPER::fetchrow_array
                     : $sth->SUPER::fetchrow_arrayref;
}

=pod

=item B<fetch_hash>

    $row_ref = $sth->fetch_hash;
    %row     = $sth->fetch_hash;

A modification on fetchrow_hashref.  When in scalar context, it acts
just as fetchrow_hashref() does.  In list context it returns the
complete hash.

=cut

sub fetch_hash {
    my($sth) = shift;
    my $row = $sth->SUPER::fetchrow_hashref;
    return unless defined $row;
    return wantarray ? %$row
                     : $row;
}

=pod

=item B<fetchall>

    $rows_ref = $sth->fetchall;
    @rows     = $sth->fetchall;

A modification on fetchall_arrayref.  In scalar context it acts as
fetchall_arrayref.  In list it returns an array of references to rows
fetched.

=cut

sub fetchall {
    my($sth) = shift;
    my $rows = $sth->SUPER::fetchall_arrayref;
    return wantarray ? @$rows
                     : $rows;
}

=pod

=item B<fetchall_hash>

    $rows_ref = $sth->fetchall_hash;
    @rows     = $sth->fetchall_hash;

A mating of fetchall_arrayref() with fetchrow_hashref().  It gets all
rows from the hash, each as hash references.  In scalar context it
returns a reference to an array of hash references.  In list context
it returns a list of hash references.

=cut

# There may be some code in DBI->fetchall_arrayref, but its undocumented.
sub fetchall_hash {
    my($sth) = shift;
    my(@rows, $row);
    push @rows, $row while ($row = $sth->SUPER::fetchrow_hashref);
    return wantarray ? @rows : \@rows;
}

=pod

=back

=head1 EXAMPLE

    package Foo;
    use base qw(Ima::DBI);
    
    # Set up database connections (but don't connect yet)
    __PACKAGE__->set_db('Users', 'dbi:Oracle:Foo', 'admin', 'passwd');
    __PACKAGE__->set_db('Customers', 'dbi:Oracle:Foo', 'Staff', 'passwd');
    
    # Set up SQL statements to be used through out the program.
    __PACKAGE__->set_sql('FindUser', <<"SQL", 'Users');
        SELECT  *
        FROM    Users
        WHERE   Name LIKE ?
    SQL
    
    __PACKAGE__->set_sql('ChangeLanguage', <<"SQL", 'Customers');
        UPDATE  Customers
        SET     Language = ?
        WHERE   Country = ?
    SQL
    
    
    # rest of the class as usual.
    
    
    package main:
    
    $obj = Foo->new;
    
    eval {
        # Does connect & prepare
        my $sth = $obj->sql_FindUser;
        # bind_params, execute & bind_columns
        $sth->execute(['Likmi%'], [\($name)]);
        while( $sth->fetch ) {
            print $name;
        }
        
        # Uses cached database and statement handles
        $sth = $obj->sql_FindUser;
        # bind_params & execute.
        $sth->execute('%Hock');
        @names = $sth->fetchall;
        
        # connects, prepares
        $rows_altered = $obj->sql_ChangeLanguage->execute(qw(es_MX mx));
    };
    unless ($@) {
        # Everything went okay, commit the changes to the customers.
        $obj->commit('Customers');
    }
    else {
        $obj->rollback('Customers');
        warn "DBI failure:  $@";    
    }


=head1 TODO, Caveat, BUGS, etc....

=over 4

=item Ima::DBI::st has to remplace DBI::st where applicable.

   $sth = $obj->db_foo->prepare(...);
   @rows = $sth->fetchall;

This won't work because $sth is, surprisingly, not an Ima::DBI::st object.
I need to make sure things like this don't happen.  For now I'll do it on
a case-by-case basis.

I could do some autoloading tricks to make this work in all cases, but
I don't know what performance impact this will have.

=item Unstable Interface

I haven't totally decided if I'm satisfied with the way this module
works, so expect the worst, the interface will change.

=item execute() extensions questionable

I'm not really sure the additional functionality added to execute() is
all that useful.

=item tainting may be too broad

Having Ima::DBI not accept any tainted data at all is probably too
general, but I'd rather be too strict to start than be too lax and try
to restrict later.  In the future, certain methods may accept tainted
data.

This is now a joint issue between DBI and Ima::DBI (well, more like
a master/slave issue.)

=item db_* should take arguments

But what?

=item set_sql() and/or sql_* needs an optional caching flag.

Right now static SQL (those without sprintf flags) is always prepared
with prepare_cached() and dynamic with just prepare().  It would be
nice if there was a way to explicitly set the caching behavior.

=item I seriously doubt its thread safe.

You can bet cupcackes to sno-cones that much havoc will be wrought if
Ima::DBI is used in a threaded Perl.  I don't think DBI is even
thread-safe.

=item Should make use of private_* handle method to store information

=item Having difficulty storing a list of dbh and sth names.

Storing the association between names and handles is fine, via the
closures (and thus, the symbol table), but trying to store a complete
list of all names available to a given object (and thus, inheritable)
is difficult.  Many minor methods are unimplemented until I figure out
this problem.

=item The docs stink.

The docs were originally written when I didn't have a good handle on
the module and how it will be used in practical cases.  I need to
rewrite the docs from the ground up.

=back


=head1 AUTHOR

Michael G Schwern <schwern@pobox.com>


=head1 COPYRIGHT

This module is Copyright (c) 1998-2000 Michael G Schwern.
USA.  All rights reserved.

This module is free software.  You may distribute under the same terms
as Perl itself.  IT COMES WITHOUT WARRANTY OF ANY KIND.


=head1 THANKS MUCHLY

    Tim Bunce, for enduring all my DBI questions and adding Taint,
    prepare_cached and connect_cached methods to DBI.  It simplified
    my job greatly!

    Arena Networks, for effectively paying for me to finish writing
    this module.


=head1 SEE ALSO

DBI

=cut



return 1001001;
