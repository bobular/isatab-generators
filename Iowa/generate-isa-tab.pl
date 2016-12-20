#!/usr/bin/env perl
# -*- mode: Cperl -*-
#
# usage: ./generate-isa-tab.pl [raw data files...] -o isa-tab-dir
#
#


use warnings;
use strict;

use Text::CSV::Hashify;
use Getopt::Long;

my $locations_file = 'private-data/site-lookups-DL/sorted-unique.txt';
my $outdir;

GetOptions(
	   "locations=s" => \$locations_file,
	   "outdir=s" => \@outdir,
	  );

my (@raw_data_file) = @ARGV;

my %tsv_parser_defaults = (binary => 1, eol => $/, sep_char => "\t");
my %csv_parser_defaults = (binary => 1, eol => $/, sep_char => ",");


# get the locations
# NO HEADERS - NEED TO READ DIFFERENTLY


# NEED TO LOOP OVER INPUT FILES AND DETECT WHICH COLUMNS CONTAIN LOCATION
#
my $raw_data_aoh = load_tsv_file($raw_data_file);
my @raw_data_locations;
foreach my $row (@$raw_data_aoh) {
  my $location = join '-', map { $row->{$_} } @raw_column_headers;

  $location =~ s/([A-Z][A-Z])(?=-)/IowaCountyCodes::abbrev2full($1) || die "countycode lookup failure"/e;

  push @raw_data_locations, $location;
}




sub load_tsv_file {
  my ($filename) = @_;
  return Text::CSV::Hashify->new( {
				   file   => $filename,
				   format => 'aoh',
				   %parser_defaults,
				  } )->all;
}
