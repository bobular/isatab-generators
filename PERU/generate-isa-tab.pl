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



#
# INPUT TABULAR DATA
#
#

#
# expect any number of tab delimited text files in the input dir
#


# an array containing one row per collection (the lat/long on different input lines is merged)
# array of hashes (colname=>value)
#my @combined_input_rows; # empty to begin with


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



# #
# Set up headers for all the output sheets.
#
# each sheet will be an array of rows.
# first row contains the headers
#
#


my @a_blood_species = ( [ 'Sample Name', 'Assay Name', 'Protocol REF','Characteristics [sample size (VBcv:0000983)]', 'Raw Data File' ] );
my @p_blood_species = ( [ 'Assay Name', 'Phenotype Name', 'Observable', 'Term Source Ref', 'Term Accession Number', 'Attribute', 'Term Source Ref', 'Term Accession Number', 'Comment [note]', 'Value', 'Term Source Ref', 'Term Accession Number' ] );



#
# This is the main loop for generating ISA-Tab data
# Will need headings for at least 2013 seperately from the other years
# Do I need to make headings for the other sheets if the rest are the same?


  my @culicifacies_bm_headings = qw/BCE:BM:Bovine	BCE:BM:Human	BCE:BM:B+H BCE:BM:Unfed	BCE:BM:Others AD:BM:Bovine AD:BM:Human AD:BM:B+H AD:BM:Unfed AD:BM:Others/;
  my @fluviatilis_bm_headings = qw/T:BM:Bovine	T:BM:Human	T:BM:B+H	T:BM:Unfed	T:BM:Others	S:BM:Bovine	S:BM:Human	S:BM:B+H	S:BM:Unfed	S:BM:Others/;

  # sibling species count headings
  my @culicifacies_ss_headings = qw/SS:BCE SS:AD/;
  my @fluviatilis_ss_headings = qw/SS:S SS:T/;

  # sanity check for mosquito numbers
  my $sum_bloodmeal = 0;
  foreach my $bm_heading (@culicifacies_bm_headings, @fluviatilis_bm_headings) {
    $sum_bloodmeal += ($row->{$bm_heading} || 0);
  }
  if ($sum_bloodmeal != $total_mossies) {
    warn "<<<<< mosquito numbers (sum is $sum_bloodmeal, total expected is $total_mossies) need checking for $row->{Month} $row->{Species} >>>>>\n";
    next; # go straight to next row, do not collect...
  }

  #
  # TO DO: sanity check that SS counts add up to Total number of ...
  #
  my %subspecies_counts; # $subspecies_counts{BCE} = 123; etc for AD, S and T


  my ($clean_species) = morpho_species_term($row->{Species});

  # now we loop through each blood meal type and create an entry in the sample sheet
  # each mossie needs a Sample Name

  foreach my $bm_heading (@culicifacies_bm_headings, @fluviatilis_bm_headings) {
    # get the subspecies code from the heading
    my ($subspecies, $always_bm, $bm_type) = split /:/, $bm_heading;

    my $num_mossies = $row->{$bm_heading};
    if ($num_mossies && $num_mossies>0) {
      my $sample_name = sprintf "%s %s %04d", $clean_species, $subspecies, ++$sample_number{$clean_species}{$subspecies};
      # replace non alphanumeric with underscore
      $sample_name =~ s/\W+/_/g;

      # for sanity check later
      $subspecies_counts{$subspecies} += $num_mossies;

      #push a reference to an array of row data into s_samples

      my @feeding_status = $bm_type eq 'Unfed' ? ('unfed female insect', 'VSMO', '0000210') : ('fed female insect', 'VSMO', '0000218');
      my $sample_description = sprintf "%d %s%s", $num_mossies, $feeding_status[0], $num_mossies > 1 ? "s" : "";

##################################################################
	if ($bm_type eq 'Bovine') {
	  push @p_blood_species, [ "$sample_name.BM_BOVINE", "bovine blood meal", 'blood meal', 'VBcv', '0001003', 'bovine', 'VBsp', '0001401', 'PRESENT', 'PATO', '0000467' ];
	  push @p_blood_species, [ "$sample_name.BM_HUMAN", 'human blood meal not detected', 'blood meal', 'VBcv', '0001003', 'human', 'VBsp', '0001357', 'ABSENT', 'PATO', '0000462' ];
	  $sample_description .= " with bovine blood source detected";
	} elsif ($bm_type eq 'Human') {
	   push @p_blood_species, [ "$sample_name.BM_BOVINE", 'bovine blood meal not detected', 'blood meal', 'VBcv', '0001003', 'bovine', 'VBsp', '0001401', 'ABSENT', 'PATO', '0000462' ];
	  push @p_blood_species, [ "$sample_name.BM_HUMAN", 'human blood meal', 'blood meal', 'VBcv', '0001003', 'human', 'VBsp', '0001357', 'PRESENT', 'PATO', '0000467' ];
	   $sample_description .= " with human blood source detected";
	} elsif ($bm_type eq 'B+H') {
	  push @p_blood_species, [ "$sample_name.BM_BOVINE", 'bovine blood meal', 'blood meal', 'VBcv', '0001003', 'bovine', 'VBsp', '0001401', 'PRESENT', 'PATO', '0000467' ];
	  push @p_blood_species, [ "$sample_name.BM_HUMAN", 'human blood meal', 'blood meal', 'VBcv', '0001003', 'human', 'VBsp', '0001357', 'PRESENT', 'PATO', '0000467' ];
	    $sample_description .= " with bovine and human blood sources detected";
	} elsif ($bm_type eq 'Others') {
          push @p_blood_species, [ "$sample_name.BM_BOVINE", 'bovine blood meal not detected', 'blood meal', 'VBcv', '0001003', 'bovine', 'VBsp', '0001401', 'ABSENT', 'PATO', '0000462' ];
          push @p_blood_species, [ "$sample_name.BM_HUMAN", 'human blood meal not detected', 'blood meal', 'VBcv', '0001003', 'human', 'VBsp', '0001357', 'ABSENT', 'PATO', '0000462' ];
	    $sample_description .= " with non-bovine and non-human blood sources detected";
        }
      }
      #	print STDOUT "created '$sample_name' from $row->{Village} that dined on $bm_type\n";
    }
  }


  # do the SS sanity check
  foreach my $subspecies (keys %subspecies_counts) {
    die "subspecies count error... <insert useful info here>\n" unless ($subspecies_counts{$subspecies} == $row->{"SS:$subspecies"});
  }

  #sanity checking output
#printf "%s\t%s\t%s\t\%s\t%s\t%d\t%d\t%s\n",
#  $row->{Month},
#    (looks_like_number($lat_decimal) ? sprintf("%.4f", $lat_decimal) : $lat_decimal ),
#	(looks_like_number($long_decimal) ? sprintf("%.4f", $long_decimal) : $long_decimal ),
#	  $row->{Village}, $row->{Species},
#	    $total_mossies,
#	      $sum_bloodmeal,
#		( $total_mossies == $sum_bloodmeal ? 'OK' : '<<<<NOT EQUAL>>>>' );

 #printf "Sample Name\tAssay Name\tProtocolREF\tComment [note]\tCharacteristics [sample size (VBcv0000983)]


}# end of foreach $row in combined rows



# printing an array with tab separators
# print join("\t", @headings)."\n";




#write_table("$outdir/s_samples.txt", \@s_samples);

#write_table("$outdir/a_species.txt", \@a_species);
#write_table("$outdir/a_collection.txt", \@a_collection);
#
write_table("$outdir/a_blood_species.txt", \@a_blood_species);
write_table("$outdir/p_blood_species.txt", \@p_blood_species);
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
    when (/^An\. culicifacies$/) {
      return ('Anopheles culicifacies', 'VBsp', '0002255')
    }
    when (/^An\.? fluviatilis$/) {
      return ('Anopheles fluviatilis', 'VBsp', '0003475')
    }
    default {
      die "fatal error: unknown morpho_species_term >$input<\n";
    }
  }
}

sub pcr_species_term {
  my $input = shift;
  given ($input) {
    when (/^BCE$/) {
      return ('Anopheles culicifacies BCE subgroup', 'VBsp', '0000645')
    }
    when (/^AD$/) {
      return ('Anopheles culicifacies AD subgroup', 'VBsp', '0000471')
    }
    when (/^S$/) {
      return ('Anopheles fluviatilis S', 'VBsp', '0000647')
    }
    when (/^T$/) {
      return ('Anopheles fluviatilis T', 'VBsp', '0000650')
    }
    default {
      die "fatal error: unknown pcr_species_term >$input<\n";
    }
  }
}

sub collection_protocol_ref {
  my $input = shift;
  given ($input) {
    when (/^HD$/) {
      return ('COLL_HOUSE')
    }
    when (/^CS$/) {
      return ('COLL_CATTLE')
    }
    default {
      die "fatal error: unknown protocol_ref >$input<\n";
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

