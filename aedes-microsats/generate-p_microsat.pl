#!/usr/bin/env perl
#  -*- mode: CPerl -*-

#
# usage ./generate-isa-tab.pl [ -outdir dirname ] [ -inputfile filename ]
#
# actually this will only generate the a_microsat.txt and g_microsat.txt files
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
my $outdir = 'temp-isa-tab';

my $inputfile = '/home/sakelly/source-data/aedes-microsat/Gloria Soria.txt.txt';

GetOptions(
	   "outdir=s"=>\$outdir,
	   "inputfile=s"=>\$inputfile,
	  );

mkdir $outdir unless (-e $outdir);
die "can't make output directory: $outdir\n" unless (-d $outdir);


my @alleles = qw/AC1:A   AC1:B   AC2:A   AC2:B   AC4:A   AC4:B   AC5:A   AC5:B   CT2:A   CT2:B   AG1:A   AG1:B   AG2:A   AG2:B   AG5:A   AG5:B   A1:A    A1:B    A9:A    A9:B    B2:A    B2:B    B3:A    B3:B/;



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


foreach my $row_ref (@$lines_aoh) {
  my $sample_id = $row_ref->{"Sample ID"};
  if (defined $sample_id) {
    foreach my $allele (@alleles) {
      my $length = $row_ref->{$allele};
      print "I read in the line for $sample_id it as $length for $allele\n";
    }
  } else {
    print "problem reading row\n";
  }
}


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

