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

use Text::CSV::Hashify;
use Getopt::Long;
use Scalar::Util qw(looks_like_number);
use DateTime::Format::Strptime;
use Geo::Coordinates::UTM;

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



#
# INPUT TABULAR DATA
#
#

#
# expect any number of tab delimited text files in the input dir
#


# an array containing one row per collection (the lat/long on different input lines is merged)
# array of hashes (colname=>value)
my @combined_input_rows; # empty to begin with


# do a wildcard file "search" - results are a list of filenames that match
foreach my $filename (glob "$indir/*.txt") {
  print "Reading data from '$filename'\n";

  # read in the whole file into an (reference to an) array of hashes
  # where the keys in the hash are the column names (from the input file)
  # and the values are the values from 
  my $lines_aoh = Text::CSV::Hashify->new( {
					     file   => $filename,
					     format => 'aoh',
					     %parser_defaults,
					    } )->all;

  # now loop through each line of this file and fill up @combined_input_rows array.
  my $last_line_hash;
  foreach my $line_hash (@$lines_aoh) {

    if ($line_hash->{'Type of Dwelling'}) {
      # if the dwelling cell has a value we're in the "main" row
      push @combined_input_rows, $line_hash;

    } elsif (defined $last_line_hash && $line_hash->{GPS}) {
      # else we're in the row with only the Easting GPS coord
      # and we'll copy the GPS value into the previous line (that was already saved in @combined_input_rows)
      $last_line_hash->{GPS2} = $line_hash->{GPS};
    }

    $last_line_hash = $line_hash;
  }

}



#
# now quickly check what we read in
#

foreach my $row (@combined_input_rows) {

  printf "%s\t%s\t%s\t\%s\t%s\t%d\n",
    $row->{Month}, $row->{GPS}, ($row->{GPS2} // 'N/A'),
      $row->{Village}, $row->{Species},
	$row->{'Total number of mosquitoes'};
}

# Convert northing and easting to WGS84
# Can I take the row GPS2 and make this my @??
use Coordinate; # is this is the same as using the one at the top of the script?

my @collection_site = Coordinate->new();
$position->northing(%s);
$position->easthing(%s);
$position->datum("WGS84")

printf ("Starting position(deg,min): %f %f/n",
	$position->northing(),
	$postiion->easting() );

$position->degminsec_to_WGS84();

printf ("Starting position(deg,min): %s %s/n",
	$position->latitude(),
	$position->longitude() );


#
# SAMPLES, SPECIES and COLLECTIONS
#

# each row in the Anophelines table has its own ID, and is an individual mosquito

# headers
my @s_samples = ( ['Source Name', 'Sample Name', 'Description', 'Material Type', 'Term Source Ref', 'Term Accession Number', 'Characteristics [sex (EFO:0000695)]', 'Term Source Ref', 'Term Accession Number', 'Characteristics [developmental stage (EFO:0000399)]', 'Term Source Ref', 'Term Accession Number', 'Characteristics [combined feeding and gonotrophic status of insect (VSMO:0002038)]', 'Term Source Ref', 'Term Accession Number'] );

my @a_species = ( [ 'Sample Name', 'Assay Name', 'Description', 'Protocol REF', 'Date', 'Characteristics [species assay result (VBcv:0000961)]', 'Term Source Ref', 'Term Accession Number' ] );

my @a_collection = ( [ 'Sample Name', 'Assay Name', 'Description', 'Protocol REF', 'Date', 'Comment [Household ID]', 'Comment [Hse]', 'Comment [Room]', 'Comment [Trap ID]', 'Comment [Trap location]', 'Characteristics [building roof (ENVO:01000472)]', 'Term Source Ref', 'Term Accession Number', 'Comment [House eave]', 'Comment [Fire burn last night]', 'Comment [number ITN]', 'Comment [number people sleeping]', 'Comment [number people sleeping under ITN]', 'Comment [data comment]', 'Characteristics [Collection site (VBcv:0000831)]', 'Term Source Ref', 'Term Accession Number', 'Characteristics [Collection site latitude (VBcv:0000817)]', 'Characteristics [Collection site longitude (VBcv:0000816)]', 'Characteristics [Collection site altitude (VBcv:0000832)]', 'Comment [UTM coordinates]' ] ); # 'Characteristics [Collection site location (VBcv:0000698)]', 'Characteristics [Collection site village (VBcv:0000829)]', 'Characteristics [Collection site locality (VBcv:0000697)]', 'Characteristics [Collection site suburb (VBcv:0000845)]', 'Characteristics [Collection site city (VBcv:0000844)]', 'Characteristics [Collection site county (VBcv:0000828)]', 'Characteristics [Collection site district (VBcv:0000699)]', 'Characteristics [Collection site province (VBcv:0000700)]', 'Characteristics [Collection site country (VBcv:0000701)]' ] );

my @a_blood_species = ( [ 'Sample Name', 'Assay Name', 'Protocol REF', 'Comment [note]', 'Raw Data File' ] );
my @p_blood_species = ( [ 'Assay Name', 'Phenotype Name', 'Observable', 'Term Source Ref', 'Term Accession Number', 'Attribute', 'Term Source Ref', 'Term Accession Number', 'Value', 'Term Source Ref', 'Term Accession Number', 'Unit', 'Term Source Ref', 'Term Accession Number' ] );

my @a_elisa_pf = ( [ 'Sample Name', 'Assay Name', 'Protocol REF', 'Raw Data File' ] );
my @p_elisa_pf = ( [ 'Assay Name', 'Phenotype Name', 'Observable', 'Term Source Ref', 'Term Accession Number', 'Attribute', 'Term Source Ref', 'Term Accession Number', 'Value', 'Term Source Ref', 'Term Accession Number', 'Unit', 'Term Source Ref', 'Term Accession Number' ] );


# write_table("$outdir/s_samples.txt", \@s_samples);
# write_table("$outdir/a_species.txt", \@a_species);
# write_table("$outdir/a_collection.txt", \@a_collection);
#
# write_table("$outdir/a_blood_species.txt", \@a_blood_species);
# write_table("$outdir/p_blood_species.txt", \@p_blood_species);
#


#
# LOOKUP SUBS
#
# return lists, often (term_name, ontology, accession number)
#
#

sub sex_term {
  my $input = shift;
  given ($input) {
    when (/^F$/) {
      return ('female', 'PATO', '0000383');
    }
    when (/^M$/) {
      return ('male', 'PATO', '0000384');
    }
    default {
      die "fatal error: unknown sex_term >$input<\n";
    }
  }
}

sub feeding_status_term {
  my $input = shift;
  given ($input) {
    when (/^N$/) {
      return ('unfed female insect', 'VSMO', '0000210')
    }
    when (/^Y$/) {
      return ('fed female insect', 'VSMO', '0000218');
    }
    default {
      die "fatal error: unknown feeding_status_term >$input<\n";
    }
  }
}


sub building_roof_term {
  my $input = shift;
  given ($input) {
    when (/^metal$/) {
      return ('sheet-iron building roof', 'ENVO', '01000510')
    }
    when (/^thatch$/) {
      return ('thatched building roof', 'ENVO', '01000511');
    }
    default {
      die "fatal error: unknown building_roof_term >$input<\n";
    }
  }
}

sub morpho_species_term {
  my $input = shift;
  given ($input) {
    when (/^An\. funestus$/) {
      return ('Anopheles funestus sensu lato', 'VBsp', '0003478')
    }
    when (/^An\. gambiae$/) {
      return ('Anopheles gambiae sensu lato', 'VBsp', '0003480')
    }
    when (/^Culicine$/) {
      return ('Culicini', 'VBsp', '0003820')
    }
    default {
      die "fatal error: unknown morpho_species_term >$input<\n";
    }
  }
}

sub pcr_species_term {
  my $input = shift;
  given ($input) {
    when (/^An\. funestus$/) {
      return ('Anopheles funestus', 'VBsp', '0003834')
    }
    when (/^An\. leesoni$/) {
      return ('Anopheles leesoni', 'VBsp', '0003509')
    }
    when (/^An\. gambiae s\.s\.$/) {
      return ('Anopheles gambiae', 'VBsp', '0003829')
    }
    default {
      die "fatal error: unknown pcr_species_term >$input<\n";
    }
  }
}



sub positive_negative_term {
  my $input = shift;
  given ($input) {
    when (/^Posi?tive$/) {
      return ('present', 'PATO', '0000467');
    }
    when (/^Negative$/) {
      return ('absent', 'PATO', '0000462');
    }
    default {
      die "fatal error: unknown positive_negative_term >$input<\n";
    }
  }
}



#
# makes the row of data for a_collections, given the sample id (first arg) and the relevant hashref 'row' from the households hash (second arg)
#
# Here are the columns again, as a reminder
# Sample Name
# Assay Name
# Description
# Protocol REF
# Date
# Comment [household ID]
# Comment [Hse]
# Comment [Room]
# Comment [Trap ID]
# Comment [Trap location]
# Characteristics [building roof (ENVO:01000472)]
# Term Source Ref
# Term Accession Number
# Comment [House eave]
# Comment [Fire burn last night]
# Comment [number ITN]
# Comment [number people sleeping]
# Comment [number people sleeping under ITN]
# Comment [data comment]
# Characteristics [Collection site (VBcv:0000831)]
# Term Source Ref
# Term Accession Number
# Characteristics [Collection site latitude (VBcv:0000817)]
# Characteristics [Collection site longitude (VBcv:0000816)]
# Characteristics [Collection site altitude (VBcv:0000832)]
# Comment [UTM coordinates]
### NOT USED YET
# Characteristics [Collection site location (VBcv:0000698)]
# Characteristics [Collection site village (VBcv:0000829)]
# Characteristics [Collection site locality (VBcv:0000697)]
# Characteristics [Collection site suburb (VBcv:0000845)]
# Characteristics [Collection site city (VBcv:0000844)]
# Characteristics [Collection site county (VBcv:0000828)]
# Characteristics [Collection site district (VBcv:0000699)]
# Characteristics [Collection site province (VBcv:0000700)]
# Characteristics [Collection site country (VBcv:0000701)]

sub collection_row {
  my ($sample_id, $data) = @_;
  use DateTime::Format::Strptime;

  my $date_parser = DateTime::Format::Strptime->new(
						    pattern   => '%d-%b-%y',
						    locale    => 'en_US',
						    time_zone => 'Europe/London'
						   );
  my $dt = $date_parser->parse_datetime($data->{Date});

  my $iso_ish_date = sprintf "%d-%02d-%02d", $dt->year, $dt->month, $dt->day;

  return [
	  $sample_id,
	  'collection.'.$data->{Date}.'.'.$data->{'HH ID'},
	  '',
	  $data->{'Coll Method'},
	  $iso_ish_date,
	  $data->{'HH ID'},
	  $data->{'Hse'},
	  $data->{'Room'},
	  $data->{'Trap ID'},
	  $data->{'Trap location'},
	  building_roof_term($data->{'Roof Type'}),
	  $data->{'House eave'},
	  $data->{'Fire burn last night'},
	  $data->{'# ITN'},
	  $data->{'#p Slept '},
	  $data->{'#p under ITN '},
	  $data->{'data comments'},
	  ('Zambia', 'GAZ', '00001107'),
	  utm_to_latlon('WGS-84', '35C', $data->{'UTM X'}, $data->{'UTM Y'}),
	  '',
	  join(' ', $data->{Grid}, $data->{'UTM X'}, $data->{'UTM Y'}),
	 ];
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

