#!/usr/bin/env perl

use strict;
use warnings;

use Text::CSV::Hashify;


my %parser_defaults = (binary => 1, eol => $/, sep_char => "\t");

# get arrays of hashes of the input data

# Counts.txt
# not sure yet which columns are unique, so can't use hash of hashes
my $counts_aoh = Text::CSV::Hashify->new( {
					   file   => 'mock-data/Counts.txt',
					   format => 'aoh',
					   %parser_defaults,
					  } );

# make a hash of hashes using Household ID:Collection date
my $counts = hashify_by_multiple_keys($counts_aoh->all, ':', 'HH ID', 'Date');

# Anophelines.txt
# can use unique IDs in first column
my $anophelines = Text::CSV::Hashify->new( {
					    file   => 'mock-data/Anophelines.txt',
					    format => 'hoh',
					    key => 'ID',
					    %parser_defaults,
					   } );
# Household data.txt
my $hh_aoh = Text::CSV::Hashify->new( {
				       file   => 'mock-data/Household data.txt',
				       format => 'aoh',
				       %parser_defaults,
				      } );
my $households = hashify_by_multiple_keys($hh_aoh->all, ':', 'HH ID', 'Date');



foreach my $hh_date (keys %$households) {
  printf "got a household %s with trap ID %s - count trap ID %s\n", $hh_date, $households->{$hh_date}{'Trap ID'}, $counts->{$hh_date}{'Trap ID'};
}







#
# usage $hashref = hashify_by_multiple_keys($arrayref, ':', 'HH ID', 'Collection Date')
#
# builds and returns a new hashref by iterating through the hashref elements of arrayref
# using the provided keys, it joins their values using the delimiter and uses
# the result as the key in the new hashref - which points to the arrayref rows
# make sense??
#
# it's as if Text::CSV::Hashify had a multiple keys option

sub hashify_by_multiple_keys {
  my ($arrayref, $delimiter, @keys) = @_;
  my $hashref = {};

  foreach my $row (@$arrayref) {
    my $newkey = join $delimiter, @$row{@keys};
    die "non-unique multiple key (@keys): >$newkey<\n" if exists $hashref->{$newkey};
    $hashref->{$newkey} = $row;
  }
  
  return $hashref;
}
