use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
eval {
    require Ima::DBI;
};

my $LAST_API_CHANGE = 0.04;
unless ($@) { # Make sure we did find the module.
    print <<"CHANGE_WARN" if $Ima::DBI::VERSION < $LAST_API_CHANGE;

NOTE: There have been API changes between this version and any older
than version $LAST_API_CHANGE!  Please read the Changes file if you
are upgrading from a version of Ima::DBI older than $LAST_API_CHANGE.

CHANGE_WARN
}

WriteMakefile(
          'NAME'        => 'Ima::DBI',
          'VERSION_FROM'  => 'DBI.pm', # finds $VERSION
          'PREREQ_PM'  => { DBI => 1.07,
                            Carp::Assert => 0.05,
                            Carp => 0,
                          },
          'dist'      =>   {COMPRESS => 'gzip -9',
                            SUFFIX   => '.gz',
                            DIST_DEFAULT => 'all tardist',
                           },
         );
