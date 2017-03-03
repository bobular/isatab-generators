#!/usr/bin/env perl
#  -*- mode: CPerl -*-

#
# usage ./generate-isa-tab.pl -indir dir-with-tsv -outdir isa-tab-dir
#
#
# edit investigation sheet manually in Google Spreadsheets and download as TSV
# into the output directory before loading into Chado
#


use strict;
use warnings;
use feature "switch";
use utf8::all;

use Text::CSV::Hashify;
use Getopt::Long;
use Scalar::Util qw(looks_like_number);
use DateTime::Format::Strptime;
use Geo::Coordinates::DecimalDegrees;

my %parser_defaults = (binary => 1, eol => $/, sep_char => "\t");
my $indir;
my $outdir;

GetOptions(
	   "indir=s"=>\$indir,
	   "outdir=s"=>\$outdir,
	  );

# check mandatory command line options given
unless (defined $indir && defined $outdir) {
  die "must give -indir AND -outdir options on command line\n";
}

# check input dir exists (with some nasty magic)
if (not -d $indir) {
  die "indir does not exist\n";
}

mkdir $outdir unless (-e $outdir);
die "can't make output directory: $outdir\n" unless (-d $outdir);



# #
# Set up headers for all the output sheets.
#
# each sheet will be an array of rows.
# first row contains the headers
#
#


my @a_blood_species = ( [ 'Sample Name', 'Assay Name', 'Protocol REF', 'Characteristics [sample size (VBcv:0000983)]', 'Raw Data File' ] );
my @p_blood_species = ( [ 'Assay Name', 'Phenotype Name', 'Observable', 'Term Source Ref', 'Term Accession Number', 'Attribute', 'Term Source Ref', 'Term Accession Number', 'Comment [note]', 'Value', 'Term Source Ref', 'Term Accession Number' ] );




#
# INPUT TABULAR DATA
#
#

my @bloodmeal_headings = qw/Human	Cow	Pig	Dog	Goat	Galliformes	Rat	Rodentia	Monkey	Didelphis/;


#
# expect any number of tab delimited text files in the input dir
#



# do a wildcard file "search" - results are a list of filenames that match
foreach my $filename (glob "$indir/*.{txt,tsv}") {
  warn "Reading data from '$filename'\n";

  # read in the whole file into an (reference to an) array of hashes
  # where the keys in the hash are the column names (from the input file)
  # and the values are the values from 
  my $lines_aoh = Text::CSV::Hashify->new( {
					    file   => $filename,
					    format => 'aoh',
					    %parser_defaults,
					   } )->all;


  foreach my $row (@$lines_aoh) {
    next unless ($row->{CODE});
    # This is the main loop for generating ISA-Tab data
    # Will need headings for at least 2013 seperately from the other years
    # Do I need to make headings for the other sheets if the rest are the same?

    my $sample_name = $row->{CODE};
    if ($filename =~ /LUP2015|CAH2015/) {
      $sample_name .= ".2015";
    }
    # now we loop through each blood meal type and create an entry in the sample sheet
    # each mossie needs a Sample Name

    foreach my $bm_heading (@bloodmeal_headings) {
      my $assay_result = $row->{$bm_heading} // '-';

      my $protocol_ref = "BM_".uc($bm_heading);
      my $assay_name = "$sample_name.$protocol_ref";

      # do the assay line
      push @a_blood_species, [ $sample_name, $assay_name, $protocol_ref, 'Characteristics [sample size (VBcv:0000983]', 'p_blood_species.txt' ];

      # do the phenotype line
      if (lc($assay_result) eq 'positiv') {
	push @p_blood_species, [ $assay_name, "$bm_heading blood meal", 'blood meal', 'VBcv', '0001003', bloodmeal_species_term($bm_heading), 'IP', 'present', 'PATO', '0000467' ];
      } elsif ($assay_result =~ /^\s*-\s*$/) {
	push @p_blood_species, [ $assay_name, "$bm_heading blood meal not detected", 'blood meal', 'VBcv', '0001003', bloodmeal_species_term($bm_heading),'IP', 'absent', 'PATO', '0000462' ];
      } else {
	die "unexpected value '$assay_result' in input spreadsheet '$filename'\n";
      }
    }
  }
}


  write_table("$outdir/a_blood_species.txt", \@a_blood_species);
  write_table("$outdir/p_blood_species.txt", \@p_blood_species);
#
#
# LOOKUP SUBS
#
# return lists, often (term_name, ontology, accession number)
#
#

 sub bloodmeal_species_term {
  my $input = shift;
  given ($input) {
    when (/^Human$/) {
      return ('Human', 'VBsp', '0001357');
    }
    when (/^Cow$/) {
      return ('Cow', 'VBsp', '0001925');
    }
    when (/^Pig$/) {
      return ('Pig', 'VBsp', '0000184');
    }
    when (/^Dog$/) {
      return ('Dog', 'VBsp', '0000645');
    }
    when (/^Goat$/) {
      return ('Goat', 'VBsp', '0001547');
    }
    when (/^Galliformes$/) {
      return ('Galliformes', 'VBsp', '0001400');
    }
    when (/^Rat$/) {
      return ('Rat', 'VBsp', '0003234');
    }
    when (/^Rodentia$/) {
      return ('Rodentia', 'VBsp', '0003233');
    }
    when (/^Monkey$/) {
      return ('Monkey', 'VBsp', '0003236');
    }
    when (/^Didelphis$/) {
      return ('Didelphis', 'VBsp', '0003237');
    }
    default {
      die "fatal error: unknown sex_term >$input<\n";
    }
  }
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
