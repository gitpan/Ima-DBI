# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl test.pl'

######################### We start with some black magic to print on failure.

# Change 1..1 below to 1..last_test_to_print .
# (It may become useful if the test is moved to ./t subdirectory.)

END {print "not ok 1\n" unless $loaded;}
use Ima::DBI 0.16;
$loaded = 1;
print "ok 1\n";

######################### End of black magic.



package My::DBI;

use base qw(Ima::DBI);
use strict;

sub new { return bless {}; }

my $test_num = 2;
sub ok {
    my($test, $name) = @_;
    print "not " unless $test;
    print "ok $test_num";
    print " - $name" if defined $name;
    print "\n";
    $test_num++;
}

# Test set_db
__PACKAGE__->set_db('test1', 'dbi:ExampleP:', '', '', {AutoCommit => 1});
__PACKAGE__->set_db('test2', 'dbi:ExampleP:', '', '', {AutoCommit => 1,foo=>1});
ok(__PACKAGE__->can('db_test1'));
ok(__PACKAGE__->can('db_test2'));

# Test set_sql
__PACKAGE__->set_sql('test1', 'select foo from bar where yar = ?', 'test1');
__PACKAGE__->set_sql('test2', 'select mode,size,name from ?', 'test2');
__PACKAGE__->set_sql('test3', 'select %s from ?', 'test1');
ok(__PACKAGE__->can('sql_test1'));
ok(__PACKAGE__->can('sql_test2'));
ok(__PACKAGE__->can('sql_test3'));

my $obj = My::DBI->new;

# Test sql_*
my $sth = $obj->sql_test2;
ok($sth->isa('Ima::DBI::st'));

# Test execute & fetch
use Cwd;
my $dir = cwd();
my($col0, $col1, $col2);
$sth->execute([$dir], [\($col0, $col1, $col2)]);
my(@row_a) = $sth->fetch;
ok($row_a[0] eq $col0);
ok($row_a[1] eq $col1);
ok($row_a[2] eq $col2);
$sth->finish;

# Test fetch_hash
$sth = $obj->sql_test2;
$sth->execute($dir);
my %row_hash;
%row_hash = $sth->fetch_hash;
ok(keys %row_hash == 3);

eval {
    while( my %row = $sth->fetch_hash ) { }
};
ok( !$@ ); # Make sure fetch_hash() doesn't blow up at the end of its fetching
        
    

# Test dynamic SQL generation.
$sth = $obj->sql_test3(join ',', qw(mode size name));
ok( $sth->isa('Ima::DBI::st') );

# Same as before.
# Test execute & fetch
use Cwd;
my $dir = cwd();
my($col0, $col1, $col2);
$sth->execute([$dir], [\($col0, $col1, $col2)]);
my(@row_a) = $sth->fetch;
ok($row_a[0] eq $col0);
ok($row_a[1] eq $col1);
ok($row_a[2] eq $col2);
$sth->finish;


BEGIN {
    use vars qw($tests);  
    $tests = 16;
    print "1..$tests\n";
}
