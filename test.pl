# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl test.pl'

######################### We start with some black magic to print on failure.

# Change 1..1 below to 1..last_test_to_print .
# (It may become useful if the test is moved to ./t subdirectory.)

END {print "not ok 1\n" unless $loaded;}
use Ima::DBI 0.04;
$loaded = 1;
print "ok 1\n";

######################### End of black magic.



package My::DBI;

use base qw(Ima::DBI);
use strict;

sub new { return bless {}; }

my $t = 1;
sub ok ($$) {
    my($n, $ok) = @_;
    ++$t;
    die "sequence error, expected $n but actually $t"
	if $n and $n != $t;
    ($ok) ? print "ok $t\n" : print "not ok $t\n";
    warn "# failed test $t at line ".(caller)[2]."\n" unless $ok;
}

# Test set_db
__PACKAGE__->set_db('test1', 'dbi:ExampleP:', '', '', {AutoCommit => 1});
__PACKAGE__->set_db('test2', 'dbi:ExampleP:', '', '', {AutoCommit => 1,foo=>1});
ok(0, __PACKAGE__->can('db_test1') && __PACKAGE__->can('db_test2'));

# Test set_dql
__PACKAGE__->set_sql('test1', 'select foo from bar where yar = ?', 'test1');
__PACKAGE__->set_sql('test2', 'select mode,size,name from ?', 'test2');
ok(0, __PACKAGE__->can('sql_test1') && __PACKAGE__->can('sql_test2'));

my $obj = My::DBI->new;

# Test sql_*
my $sth = $obj->sql_test2;
ok(0, ref $sth eq 'Ima::DBI::st');

# Test execute & fetch
use Cwd;
my $dir = cwd();
my($col0, $col1, $col2);
$sth->execute([$dir], [\($col0, $col1, $col2)]);
my(@row_a) = $sth->fetch;
ok(0, $row_a[0] eq $col0);
ok(0, $row_a[1] eq $col1);
ok(0, $row_a[2] eq $col2);
$sth->finish;

# Test fetch_hash
$sth = $obj->sql_test2;
$sth->execute($dir);
my %row_hash = $sth->fetch_hash;
ok(0, keys %row_hash == 3);

BEGIN {
    use vars qw($tests);  
    $tests = 8;
    print "1..$tests\n";
}








