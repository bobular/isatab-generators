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


# print the headers - separated by \t
print "THIS WOULD BE A GOOD PLACE TO PRINT THE HEADERS\n";

# this loop processes every line in the file
foreach my $row_ref (@$lines_aoh) {
  my $sample_id = $row_ref->{"Sample ID"};
  if (defined $sample_id) {

    # now do every allele
    foreach my $allele (@alleles) {
      my $length = $row_ref->{$allele};

      # get the AC1 part out of AC1:A
      my ($locus, $a_or_b) = split /:/, $allele;

      # printf prints a formatted 'template' string
      # the variable values follow it
      printf "Sample=%s\tLocus=%s\tAllele=%s\tLength=%d\n",
	$sample_id, $locus, $allele, $length;
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

