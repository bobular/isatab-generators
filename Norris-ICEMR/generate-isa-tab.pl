#!/usr/bin/env perl
#  -*- mode: CPerl -*-

#
# usage ./generate-isa-tab.pl [ -outdir dirname ]
#
# everything is hardcoded on purpose!
# currently expects files to be in mock-data directory (will prob add an option)
#
# edit investigation sheet manually in Google Spreadsheets and download as TSV
# into the output directory before loading into Chado
#

#
# Notes, Issues, and TO DOs
#
# 'Missing Cx data' in Counts sheet is not handled correctly - it is treated the same as zero
# mosquito counts.  Possible solution is to add a comment to the relevant collection record?
#
# Morpho species ID: An. funestus -> An. funestus s.l.
# PCR species ID:    An. funestus -> An. funestus s.s.  THIS NEEDS CONFIRMATION from authors.
#
# metal roof = sheet-iron building roof ENVO:01000510 ?
#
# open/closed eaves potentially awaiting ENVO terms? see https://confluence.vectorbase.org/display/DB/New+ontology+terms
#
# UTM conversion is iffy, to say the least - awaiting author input
#
# Blood PCR species ID - is 'no fragment' always from an actual assay? there are males with 'no fragment'
# Blood PCR results not ontologised (do we want to import whole of NCBITaxon for other purposes (e.g. metagenomics)
#
# ELISA Postive typo allowed
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
my $culicine_id_format = 'IcNc13-C%04d';
my $culicine_id_start = 1;

GetOptions("outdir=s"=>\$outdir);

mkdir $outdir unless (-e $outdir);
die "can't make output directory: $outdir\n" unless (-d $outdir);



#
# INPUT TABULAR DATA
#
# into hashes of hashes, e.g.
#
# $table->{$row_id}{$col_name}
#

## Counts.txt
# not sure yet which columns are unique, so can't use hash of hashes
my $counts_aoh = Text::CSV::Hashify->new( {
					   file   => 'mock-data/Counts.txt',
					   format => 'aoh',
					   %parser_defaults,
					  } );
# make a hash of hashes using Household ID:Collection date
my $counts = hashify_by_multiple_keys($counts_aoh->all, ':', 'HH ID', 'Date');


## Anophelines.txt
# can use unique IDs in first column
my $anophelines_hoh = Text::CSV::Hashify->new( {
					    file   => 'mock-data/Anophelines.txt',
					    format => 'hoh',
					    key => 'ID',
					    %parser_defaults,
					   } );
my $anophelines = $anophelines_hoh->all;

## Household data.txt
my $hh_aoh = Text::CSV::Hashify->new( {
				       file   => 'mock-data/Household data.txt',
				       format => 'aoh',
				       %parser_defaults,
				      } );
my $households = hashify_by_multiple_keys($hh_aoh->all, ':', 'HH ID', 'Date');




#
# SAMPLES, SPECIES and COLLECTIONS
#

# each row in the Anophelines table has its own ID, and is an individual mosquito

my $culicine_id_num = $culicine_id_start;

# headers
my @s_samples = ( ['Source Name', 'Sample Name', 'Description', 'Material Type', 'Term Source Ref', 'Term Accession Number', 'Characteristics [sex (EFO:0000695)]', 'Term Source Ref', 'Term Accession Number', 'Characteristics [developmental stage (EFO:0000399)]', 'Term Source Ref', 'Term Accession Number', 'Characteristics [combined feeding and gonotrophic status of insect (VSMO:0002038)]', 'Term Source Ref', 'Term Accession Number'] );

my @a_species = ( [ 'Sample Name', 'Assay Name', 'Description', 'Protocol REF', 'Date', 'Characteristics [species assay result (VBcv:0000961)]', 'Term Source Ref', 'Term Accession Number' ] );

my @a_collection = ( [ 'Sample Name', 'Assay Name', 'Description', 'Protocol REF', 'Date', 'Comment [Household ID]', 'Comment [Hse]', 'Comment [Room]', 'Comment [Trap ID]', 'Comment [Trap location]', 'Characteristics [building roof (ENVO:01000472)]', 'Term Source Ref', 'Term Accession Number', 'Comment [House eave]', 'Comment [Fire burn last night]', 'Comment [number ITN]', 'Comment [number people sleeping]', 'Comment [number people sleeping under ITN]', 'Comment [data comment]', 'Characteristics [Collection site (VBcv:0000831)]', 'Term Source Ref', 'Term Accession Number', 'Characteristics [Collection site latitude (VBcv:0000817)]', 'Characteristics [Collection site longitude (VBcv:0000816)]', 'Characteristics [Collection site altitude (VBcv:0000832)]', 'Comment [UTM coordinates]' ] ); # 'Characteristics [Collection site location (VBcv:0000698)]', 'Characteristics [Collection site village (VBcv:0000829)]', 'Characteristics [Collection site locality (VBcv:0000697)]', 'Characteristics [Collection site suburb (VBcv:0000845)]', 'Characteristics [Collection site city (VBcv:0000844)]', 'Characteristics [Collection site county (VBcv:0000828)]', 'Characteristics [Collection site district (VBcv:0000699)]', 'Characteristics [Collection site province (VBcv:0000700)]', 'Characteristics [Collection site country (VBcv:0000701)]' ] );

my @a_blood_species = ( [ 'Sample Name', 'Assay Name', 'Protocol REF', 'Comment [note]', 'Raw Data File' ] );
my @p_blood_species = ( [ 'Assay Name', 'Phenotype Name', 'Observable', 'Term Source Ref', 'Term Accession Number', 'Attribute', 'Term Source Ref', 'Term Accession Number', 'Value', 'Term Source Ref', 'Term Accession Number', 'Unit', 'Term Source Ref', 'Term Accession Number' ] );

my @a_elisa_pf = ( [ 'Sample Name', 'Assay Name', 'Protocol REF', 'Raw Data File' ] );
my @p_elisa_pf = ( [ 'Assay Name', 'Phenotype Name', 'Observable', 'Term Source Ref', 'Term Accession Number', 'Attribute', 'Term Source Ref', 'Term Accession Number', 'Value', 'Term Source Ref', 'Term Accession Number', 'Unit', 'Term Source Ref', 'Term Accession Number' ] );

# actual data rows for Anopheline individuals
foreach my $id (keys %{$anophelines}) {
  my $anopheline = $anophelines->{$id};
  my $hh_date = $anopheline->{'HH ID'}.':'.$anopheline->{'Collection Date'};
  unless ($households->{$hh_date}) {
    warn "skipping sample $id because no household/collection data for hh:date >$hh_date<\n";
    next;
  }
  push @s_samples,
    ['Norris ICEMR',
     $id,
     'Anopheline',
     qw/individual EFO 0000542/,
     sex_term($anopheline->{Sex}),
     qw/adult IDOMAL 0000655/,
     # males can't have a blood feeding status
     ( $anopheline->{Sex} eq 'M' ? ('','','') : feeding_status_term($anopheline->{Blooded}) ),
    ];
  push @a_species,
    [
     $id, $id.'.species.morpho', '', 'SPECIES_MORPHO', '',
     morpho_species_term($anopheline->{'Morph species'}),
    ];
  push @a_species,
    [
     $id, $id.'.species.pcr', '', 'SPECIES_PCR', '',
     pcr_species_term($anopheline->{'PCR species'}),
    ];
  push @a_collection, collection_row($id, $households->{$hh_date});


  # blood meal identification
  my $blood_PCR = $anopheline->{'Blood PCR'};
  if ($blood_PCR) {
    my (@blood_values, $comment, $phenotype_name);
    if ($blood_PCR eq 'no fragment') {
      @blood_values = ('record of missing knowledge', 'OBI', '0000852');
      $comment = 'no amplified fragment';
      $phenotype_name = 'unidentified blood meal';
    } else {
      @blood_values = ($blood_PCR, '', '');
      $comment = '';
      $phenotype_name = "$blood_PCR blood meal";
    }

    my $assay_name = "$id.blood_PCR";
    push @a_blood_species, [ $id, $assay_name, "BLOOD_PCR", $comment, 'p_blood_species.txt' ];
    push @p_blood_species,
      [
       $assay_name, $phenotype_name,
       'identification of source of blood meal in arthropod', 'VSMO', '0000174',
       'organism', 'OBI', '0100026',
       @blood_values,
       '', '', '' # no units
      ];
  } # else no blood meal assay if this cell is empty

  # plasmodium ELISA
  my $ELISA_Pf = $anopheline->{'ELISA Pf'};
  if ($ELISA_Pf) {
    my $assay_name = "$id.Pf_ELISA";
    push @a_elisa_pf, [ $id, $assay_name, "ELISA", 'p_elisa_pf.txt' ];
    push @p_elisa_pf,
      [
       $assay_name,
       "$ELISA_Pf Plasmodium falciparum ELISA test",
       'arthropod infection status', 'VSMO', '0000009', 'test result', 'EFO', '0000720',
       positive_negative_term($ELISA_Pf),
       '', '', '' # no units
      ];
  } # else no assay if cell is empty




}

# now process the counts sheet to add culicines
foreach my $hh_date (keys %{$counts}) {
  unless ($households->{$hh_date}) {
    warn "skipping counts row for hh:date >$hh_date< because no household/collection data\n";
    next;
  }
  my $count = $counts->{$hh_date};
  if (looks_like_number($count->{'#fem Culicines'})) {
    for (my $i=0; $i<$count->{'#fem Culicines'}; $i++) {
      my $id = sprintf $culicine_id_format, $culicine_id_num++;
      push @s_samples,
	[
	 'Norris ICEMR',
	 $id,
	 'Culicine',
	 qw/individual EFO 0000542/,
	 sex_term('F'),
	 qw/adult IDOMAL 0000655/,
	 ('','','')
	];

      push @a_species,
	[
	 $id, $id.'.species.morpho', '', 'SPECIES_MORPHO', '',
	 morpho_species_term('Culicine'),
	];

      push @a_collection, collection_row($id, $households->{$hh_date});
    }
  }
  if (looks_like_number($count->{'#male Culicines'})) {
    for (my $i=0; $i<$count->{'#male Culicines'}; $i++) {
      my $id = sprintf $culicine_id_format, $culicine_id_num++;
      push @s_samples,
	[
	 'Norris ICEMR',
	 $id,
	 'Culicine',
	 qw/individual EFO 0000542/,
	 sex_term('M'),
	 qw/adult IDOMAL 0000655/,
	 ('','','')
	];

      push @a_species,
	[
	 $id, $id.'.species.morpho', '', 'SPECIES_MORPHO', '',
	 morpho_species_term('Culicine'),
	];

      push @a_collection, collection_row($id, $households->{$hh_date});
    }
  }
}



write_table("$outdir/s_samples.txt", \@s_samples);
write_table("$outdir/a_species.txt", \@a_species);
write_table("$outdir/a_collection.txt", \@a_collection);

write_table("$outdir/a_blood_species.txt", \@a_blood_species);
write_table("$outdir/p_blood_species.txt", \@p_blood_species);

write_table("$outdir/a_elisa_pf.txt", \@a_elisa_pf);
write_table("$outdir/p_elisa_pf.txt", \@p_elisa_pf);


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
	  utm_to_latlon('WGS-84', uc(substr($data->{Grid}, 0, 3)), $data->{'UTM X'}, $data->{'UTM Y'}),
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

