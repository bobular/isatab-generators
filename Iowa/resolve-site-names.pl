#!/usr/bin/env perl
#
# usage: ./resolve-site-names -col County -col Site raw_data_file.txt
#
# collection site location filenames are hardcoded
#
#

use warnings;
use strict;

use Text::Fuzzy;
use Text::CSV::Hashify;
use Getopt::Long;

my $NJLT_locations_file = 'private-data/tsv/NJLT locations.txt';
my $CDC_locations_file = 'private-data/tsv/CDC locations.txt';
my $gravid_locations_file = 'private-data/tsv/gravid locations.txt';

my @raw_column_headers;

GetOptions("columns=s@" => \@raw_column_headers);

my ($raw_data_file) = @ARGV;


my %parser_defaults = (binary => 1, eol => $/, sep_char => "\t");

# get the NJLT locations from County and Site columns
my $NJLT_locations_aoh = load_tsv_file($NJLT_locations_file);
my @NJLT_locations; # "County-Site"
foreach my $row (@$NJLT_locations_aoh) {
  push @NJLT_locations, "$row->{County}-$row->{Site}";
  push @NJLT_locations, $row->{Site};
}

# get the CDC locations
my $CDC_locations_aoh = load_tsv_file($CDC_locations_file);
my @CDC_locations; # "County-Site"
foreach my $row (@$CDC_locations_aoh) {
  push @CDC_locations, "$row->{County}-$row->{Site}";
  push @CDC_locations, $row->{Site};
}

# get the gravid trap locations
my $gravid_locations_aoh = load_tsv_file($gravid_locations_file);
my @gravid_locations; # "County-Site"
foreach my $row (@$gravid_locations_aoh) {
  push @gravid_locations, "$row->{County}-$row->{Site}";
  push @gravid_locations, $row->{Site};
}


#get the sites from the raw data
my $raw_data_aoh = load_tsv_file($raw_data_file);
my @raw_data_locations;
foreach my $row (@$raw_data_aoh) {
  push @raw_data_locations, join '-', map { $row->{$_} } @raw_column_headers;
}


my @all_locations = (@NJLT_locations, @CDC_locations, @gravid_locations);
my %seen_location;

print join("\t", join('-', @raw_column_headers), "Best match overall", "Source of best overall match", "Best match in NJLT locations", "Best match in CDC locations", "Best match in gravid locations")."\n";
foreach my $raw_location (@raw_data_locations) {
  next if ($seen_location{$raw_location}++); # only do each site once!

  my $fuzzy = Text::Fuzzy->new($raw_location);

  my $nearest_all_location = $fuzzy->nearestv(\@all_locations);
  my $nearest_NJLT_location = $fuzzy->nearestv(\@NJLT_locations);
  my $nearest_CDC_location = $fuzzy->nearestv(\@CDC_locations);
  my $nearest_gravid_location = $fuzzy->nearestv(\@gravid_locations);
  my $best_hit_source;
  if ($nearest_all_location eq $nearest_NJLT_location) {
    $best_hit_source = 'NJLT locations';
  } elsif ($nearest_all_location eq $nearest_CDC_location) {
    $best_hit_source = 'CDC locations';
  } elsif ($nearest_all_location eq $nearest_gravid_location) {
    $best_hit_source = 'gravid locations';
  }

  print join("\t", $raw_location, $nearest_all_location, $best_hit_source, $nearest_NJLT_location, $nearest_CDC_location, $nearest_gravid_location)."\n";
}


sub load_tsv_file {
  my ($filename) = @_;
  return Text::CSV::Hashify->new( {
				   file   => $filename,
				   format => 'aoh',
				   %parser_defaults,
				  } )->all;
}
