use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
	      'NAME'	    => 'Ima::DBI',
	      'VERSION_FROM'  => 'DBI.pm', # finds $VERSION
	      'PREREQ_PM'  => { DBI => 0
						  },
	      'dist'      =>   {COMPRESS => 'gzip -9',
							SUFFIX   => '.gz',
							DIST_DEFAULT => 'all tardist',
						   },
	     );
