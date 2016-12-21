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
	   "outdir=s" => \$outdir,
	  );

my (@raw_data_files) = @ARGV;

my %tsv_parser_defaults = (binary => 1, eol => $/, sep_char => "\t");
my %csv_parser_defaults = (binary => 1, eol => $/, sep_char => ",");


# get the locations
my %site2location; # raw_site_name => { name => 'Cedar Falls', county => 'Black Hawk', latitidue => 123.456, longitude => 34.56 }
my %skipped_site; # raw_site_key => 1

# where raw_site_key is the location field in NJLT ('BK-Evansdale')
# or hyphen-joined county-site ('Lee-Ivanhoe Park')

# no headers - need to read with vanilla Text::CSV
my $csv = Text::CSV->new( \%csv_parser_defaults );
open my $locations_fh, "<", $locations_file or die "can't open '$locations_file'\n";
while ( my $row = $csv->getline( $locations_fh ) ) {
  my ($col1, $col2, $official_site, $latitude, $longitude) = @$row;

  my ($raw_site, $county, $raw_site_key);
  if ($col1 =~ /^[A-Z][A-Z]-/) {
    ($raw_site, $county) = ($col1, $col2);
    $raw_site_key = $raw_site;
  } else {
    ($raw_site, $county) = ($col2, $col1);
    $raw_site_key = join('-', $county, $raw_site);
  }

  unless (defined $latitude && defined $longitude) {
    warn "acknowledging dodgy site: '$county' '$raw_site' => '$official_site'\n";
    $skipped_site{$raw_site_key} = 1;
    next;
  }


  $site2location{$raw_site_key} = { name => $official_site,
				    county => $county,
				    latitude => $latitude,
				    longitude => $longitude,
				  };
}



#
# loop over input files
#
foreach my $raw_data_file (@raw_data_files) {
  my $raw_data_aoh = load_tsv_file($raw_data_file);
  foreach my $row (@$raw_data_aoh) {
    # figure out if we have 'location' or 'county','site' headings
    my $raw_site_key;
    if (defined $row->{location}) {
      next if ($row->{location} =~ /this file was cached/); # edge case last line of NJLT files
      $raw_site_key = $row->{location};
    } elsif (defined $row->{County} && defined $row->{Site}) {
      $raw_site_key = join('-', $row->{County}, $row->{Site});
    } else {
      die "cannot determine site name from a raw data row";
    }

    $raw_site_key =~ s/\s+$//;
    $raw_site_key =~ s/^\s+//;

    if ($skipped_site{$raw_site_key}) {
      warn "skipping data for known problem '$raw_site_key'\n";
      next;
    }

    # find the true location
    my $location = $site2location{$raw_site_key};
    warn "missing location data for '$raw_site_key'\n" unless defined $location;

  #  print "OK for $location->{name} from $location->{county}\n";

  }
}







sub load_tsv_file {
  my ($filename) = @_;
  return Text::CSV::Hashify->new( {
				   file   => $filename,
				   format => 'aoh',
				   %tsv_parser_defaults,
				  } )->all;
}
