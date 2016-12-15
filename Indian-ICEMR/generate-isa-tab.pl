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
my @combined_input_rows; # empty to begin with


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

# #
# Set up headers for all the output sheets.
#
# each sheet will be an array of rows.
# first row contains the headers
#
#

my @s_samples = ( ['Source Name', 'Sample Name', 'Material Type', 'Term Source Ref', 'Term Accession Number', 'Characteristics [sex (EFO:0000695)]', 'Term Source Ref', 'Term Accession Number', 'Characteristics [developmental stage (EFO:0000399)]', 'Term Source Ref', 'Term Accession Number', 'Characteristics [age (EFO:0000246)]', 'Unit', 'Term Source Ref', 'Term Accession Number' ] );

my @a_species = ( [ 'Sample Name', 'Assay Name', 'Description', 'Protocol REF', 'Characteristics [species assay result (VBcv:0000961)]', 'Term Source Ref', 'Term Accession Number' ] );

my @a_collection = ( [ 'Sample Name', 'Assay Name', 'Description', 'Protocol REF', 'Performer', 'Date', 'Characteristics [Collection site (VBcv:0000831)]', 'Term Source Ref', 'Term Accession Number', 'Characteristics [Collection site latitude (VBcv:0000817)]', 'Characteristics [Collection site longitude (VBcv:0000816)]', 'Comment [collection site coordinates]', 'Characteristics [Collection site village (VBcv:0000829)]', 'Characteristics [Collection site country (VBcv:0000701)]' ] ); # 'Characteristics [Collection site location (VBcv:0000698)]', 'Characteristics [Collection site village (VBcv:0000829)]', 'Characteristics [Collection site locality (VBcv:0000697)]', 'Characteristics [Collection site suburb (VBcv:0000845)]', 'Characteristics [Collection site city (VBcv:0000844)]', 'Characteristics [Collection site county (VBcv:0000828)]', 'Characteristics [Collection site district (VBcv:0000699)]', 'Characteristics [Collection site province (VBcv:0000700)]', 'Characteristics [Collection site country (VBcv:0000701)]' ] );

my @a_blood_species = ( [ 'Sample Name', 'Assay Name', 'Protocol REF', 'Comment [note]', 'Characteristics [sample size (VBcv:0000983)]', 'Raw Data File' ] );
my @p_blood_species = ( [ 'Assay Name', 'Phenotype Name', 'Observable', 'Term Source Ref', 'Term Accession Number', 'Attribute', 'Term Source Ref', 'Term Accession Number', 'Value', 'Unit', 'Term Source Ref', 'Term Accession Number', 'Characteristics [organism (OBI:0100026)]' ] );



#
# This is the main loop for generating ISA-Tab data
#

# sample number counters (for Sample Name) for each sib species
my %sample_number; # first level key is Species (raw from spreadsheet as there are prob no typos)
                   # second level key is sibling code also from spreadsheet, BCE, AD, T and S
                   # value stored in the two-level hash is the number
                   #
                   # $sample_number{'An. culicifacies'}{BCE} = 123

# serial number counter used in a_collection Assay Name
my $collection_counter = 0;

# then use a four (!) level hash to remember which Assay Name to use for each combination of Village, Date and Location
my %collection_name; # $collection_name{VILLAGE}{YYYY-MM}{LATITUDE}{LONGITUDE} = "Village_YYYY-MM_C042"


foreach my $row (@combined_input_rows) {


  # Convert northing and easting to WGS84
  # Can I take the row GPS2 and make this my @??
  # is this is the same as using the one at the top of the script?


  my $lat_deg_min = $row->{GPS};
  my $long_deg_min = $row->{GPS2}; # this is undefined for "No GPS record" lines

  my $lat_decimal = ''; # we will write this empty string to a_collection if coord parsing fails
  my $long_decimal = ''; # ditto

  if (defined $lat_deg_min) {
    my ($lat_deg, $lat_min) = $lat_deg_min =~ /N (\d+) \D (\d+ (?: \.\d+ )?)/x;
    if (defined $lat_deg) {
      $lat_decimal = dm2decimal($lat_deg, $lat_min);
    } else {
      warn "'$lat_deg_min' was not expected latitude deg/min format\n";
    }
  } else {
    die "Unexpected empty latitude 'GPS' column in row\n";
  }

  if (defined $long_deg_min) {
    my ($long_deg, $long_min) = $long_deg_min =~ /E (\d+) \D (\d+ (?: \.\d+ )?)/x;
    if (defined $long_deg) {
      $long_decimal = dm2decimal ($long_deg, $long_min);
    } else {
      warn "'$long_deg_min' was not expected longitude deg/min format\n";
    }
  }

  # check that total moz = sum of bloodmeals
  my $total_mossies = $row->{'Total number of mosquitoes'};

  my @culicifacies_bm_headings = qw/BCE:BM:Bovine	BCE:BM:Human	BCE:BM:B+H BCE:BM:Unfed	BCE:BM:Others AD:BM:Bovine AD:BM:Human AD:BM:B+H AD:BM:Unfed AD:BM:Others/;
  my @fluviatilis_bm_headings = qw/T:BM:Bovine	T:BM:Human	T:BM:B+H	T:BM:Unfed	T:BM:Others	S:BM:Bovine	S:BM:Human	S:BM:B+H	S:BM:Unfed	S:BM:Others/;

  # sanity check for mosquito numbers
  my $sum_bloodmeal = 0;
  foreach my $bm_heading (@culicifacies_bm_headings, @fluviatilis_bm_headings) {
    $sum_bloodmeal += ($row->{$bm_heading} || 0);
  }
  if ($sum_bloodmeal != $total_mossies) {
    warn "<<<<< mosquito numbers (sum is $sum_bloodmeal, total expected is $total_mossies) need checking for $row->{Month} $row->{Species} >>>>>\n";
    next; # go straight to next row, do not collect...
  }

  # now we loop through each blood meal type and create an entry in the sample sheet
  # each mossie needs a Sample Name

  foreach my $bm_heading (@culicifacies_bm_headings, @fluviatilis_bm_headings) {
    # get the subspecies code from the heading
    my ($subspecies, $always_bm, $bm_type) = split /:/, $bm_heading;

    my $num_mossies = $row->{$bm_heading};
    if ($num_mossies) {
      for (1 .. $num_mossies) {
	# MAIN LOOP PER MOSQUITO #
	my $sample_name = sprintf "%s %s %04d", $row->{Species}, $subspecies, ++$sample_number{$row->{Species}}{$subspecies};
	# replace non alphanumeric with underscore
	$sample_name =~ s/\W+/_/g;
	
	#for sib species
	my $species_protocol_ref = $row->{Species} =~ /culicifacies/ ? 'SPECIES_SIB_CUL' : 'SPECIES_SIB_FLUV';
	#To make option for sib species
	my $protocol_ref = $row->{'Type of Dwelling'} =~ /HD/ ? 'COLL_HOUSE' : 'COLL_CATTLE';

	#To make my assay names for collection
	my $a_collection_assay_name = $collection_name{$row->{Village}}{$row->{Month}}{$lat_decimal}{$long_decimal} //= sprintf "%s_%s_C%03d", $row->{Village}, $row->{Month}, ++$collection_counter;

	#push a reference to an array of row data into s_samples

	push @s_samples, [ '2016-indian-icemr', $sample_name, 'individual', 'EFO', '0000542', 'female', 'PATO', '0000383', 'adult', 'IDOMAL', '0000655' ];
	push @a_species, [ $sample_name, '$sample_name.SPECIES', '', 'SPECIES', morpho_species_term($row->{Species}) ];
	push @a_species, [ $sample_name, "$sample_name.$species_protocol_ref" , '', $species_protocol_ref, pcr_species_term($subspecies) ];
	push @a_collection, [ $sample_name, $a_collection_assay_name, '', protocol_ref($row->{'Type of Dwelling'}), '', $row->{Month}, 'India', 'GAZ', '00002839', $lat_decimal, $long_decimal, 'IA', $row->{Village}, 'India' ]

#	print STDOUT "created '$sample_name' from $row->{Village} that dined on $bm_type\n";
      }
    }

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




write_table("$outdir/s_samples.txt", \@s_samples);

write_table("$outdir/a_species.txt", \@a_species);
write_table("$outdir/a_collection.txt", \@a_collection);
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

sub protocol_ref {
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

