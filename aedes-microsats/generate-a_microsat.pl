#!/usr/bin/env perl
#  -*- mode: CPerl -*-

#
# usage ./generate-a_microsat.pl [ -inputfile filename ] > a_microsat.txt
#
#
#

use strict;
use warnings;
use feature "switch";

use Text::CSV::Hashify;
use Getopt::Long;
use Scalar::Util qw(looks_like_number);
use DateTime::Format::Strptime;
use Geo::Coordinates::UTM;

my %parser_defaults = (binary => 1, eol => $/, sep_char => "\t");

my $inputfile = '/home/sakelly/source-data/aedes-microsat/Gloria Soria.txt.txt';

GetOptions(
	   "inputfile=s"=>\$inputfile,
	  );

# edit the :A :B out of these and make unique
# e.g. AC1 AC2 AC4 etc
my @loci = qw/AC1 AC2 AC4 AC5 CT2 AG1 AG2 AG5 A1 A9 B2 B3/;



#
# INPUT TABULAR DATA
#
#


# Read in the tab delimited data into 
my $lines_aoh = Text::CSV::Hashify->new( {
					   file   => $inputfile,
					   format => 'aoh',
					   %parser_defaults,
					  } )->all;


# print the headers - separated by \t
print "Sample Name\t Assay Name\t Protocol REF\t Performer\t Date\t Comment[note]\t Raw Data File\n";

# this loop processes every line in the file
foreach my $row_ref (@$lines_aoh) {
  my $sample_id = $row_ref->{"Sample ID"};
  if (defined $sample_id) {

    # now do every allele
    foreach my $locus (@loci) {

      # printf prints a formatted 'template' string
      # the variable values follow
      printf "%s\t%s.%s\tMICRO_PCR\t\t\t\tg_microsat.txt\n", $sample_id, $sample_id, $locus

    }
  } else {
    print "problem reading row\n";
  }
}


#
# the following unused function writes "proper" CSV/TSV but
# we don't need it for this simple task
#
sub write_table {
  my ($filename, $arrayref) = @_;
  my $handle;
  open($handle, ">", $filename) || die "problem opening $filename for writing\n";
  my $tsv_writer = Text::CSV->new ( \%parser_defaults );
  foreach my $row (@{$arrayref}) {
    $tsv_writer->print($handle, $row);
  }
  close($handle);
  warn "sucessfully wrote $filename\n";
}

