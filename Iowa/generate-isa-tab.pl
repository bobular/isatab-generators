#!/usr/bin/env perl
# -*- mode: Cperl -*-
#
# usage: ./generate-isa-tab.pl [raw data files...] -o isa-tab-dir
#
#


use warnings;
use strict;
use feature "switch";

use Text::CSV::Hashify;
use Getopt::Long;
use DateTime::Format::Strptime;

my $locations_file = 'private-data/site-lookups-DL/sorted-unique.txt';
my $outdir;

GetOptions(
	   "locations=s" => \$locations_file,
	   "outdir=s" => \$outdir,
	  );

die "must give -outdir xxx on commanline\n" unless ($outdir);

mkdir $outdir unless -d $outdir;

my (@raw_data_files) = @ARGV;

my %tsv_parser_defaults = (binary => 1, eol => $/, sep_char => "\t");
my %csv_parser_defaults = (binary => 1, eol => $/, sep_char => ",");

# just some counters to avoid a gazillion warnings about TO DO items
my $cpg_warned = 0;
my $zero_warned = 0;


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
    warn "acknowledging dodgy site to be skipped: '$county' '$official_site'\n";
    $skipped_site{$raw_site_key} = 1;
    next;
  }


  $site2location{$raw_site_key} = { name => $official_site,
				    county => $county,
				    latitude => $latitude,
				    longitude => $longitude,
				  };
}

my @s_samples = ( ['Source Name', 'Sample Name', 'Description', 'Material Type', 'Term Source Ref', 'Term Accession Number', 'Characteristics [sex (EFO:0000695)]', 'Term Source Ref', 'Term Accession Number', 'Characteristics [developmental stage (EFO:0000399)]', 'Term Source Ref', 'Term Accession Number', 'Characteristics [sample size (VBcv:0000983)]' ] );

my @a_species = ( [ 'Sample Name', 'Assay Name', 'Description', 'Protocol REF', 'Characteristics [species assay result (VBcv:0000961)]', 'Term Source Ref', 'Term Accession Number' ] );

my @a_collection = ( [ 'Sample Name', 'Assay Name', 'Description', 'Protocol REF', 'Date', 'Comment [Raw date]', 'Characteristics [Collection site (VBcv:0000831)]', 'Term Source Ref', 'Term Accession Number', 'Characteristics [Collection site latitude (VBcv:0000817)]', 'Characteristics [Collection site longitude (VBcv:0000816)]' ] );


# serial number counter used in a_collection Assay Name
my $collection_counter = 0;
# then use a four (!) level hash to remember which Assay Name to use for each combination of Village, Date and Location
my %collection_name; # $collection_name{COUNTY}{SITENAME}{TRAPTYPE}{DATE} = "County_Site_TrapType_Date_C001"

# sample_name
# for each location/species
my $sample_counter = 0;
my %sample_name; # $sample_name{COUNTY}{SPECIES} = "County_Site_Species_S00001"

#
# loop over input files
#
foreach my $raw_data_file (@raw_data_files) {
  # some dates are missing the year - so we have to use the year from the filename sometimes!
  my ($year_from_filename) = $raw_data_file =~ /(20\d\d)/;
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

    # clean up the dates
    my $date = $row->{Date} || $row->{Collected};
    my $orig_date = $date;
    die unless $date;

#    if ($date =~ /,|and/ || ($date =~ m{^(\d+)/(\d+)-(\d+)/(\d+)$} && ($1==$3 || $1+1==$3))) {
    if (0 && $date =~ /,|and/) {
      warn "skipping data (for now) with comma-separated date '$date' in $raw_data_file\n";

      # TO DO
      # can get year from filename of course... hacky though!
      # and add as ranges
      # we still have a problem with discontinuous ranges though, e.g. '6/9, 6/10, 6/13'  - June 9, 10 and 13!

      next;
    }

    # incoming dates are in the format 8/22/2013 (in both types of file)
#warn "test '$date'\n";
    # edge case date range in the day of month...
    if ($date =~ m{^(\d+)/(\d+)-(\d+)/(\d{4})$}) {
      my ($month, $day1, $day2, $year) = ($1,$2,$3,$4);
      my $date1 = fix_date_to_iso("$month/$day1/$year");
      my $date2 = fix_date_to_iso("$month/$day2/$year");
      $date = "$date1/$date2"; # ISA-Tab Chado loader friendly date range
    } elsif ($date =~ m{^(\d+)/(\d+)-(\d+)/(\d+)/(\d{2})$}) { # a range straddling a month boundary with a two-digit year
      my ($month1, $day1, $month2, $day2, $year) = ($1,$2,$3,$4,$5);
      my $date1 = fix_date_to_iso("$month1/$day1/20$year");
      my $date2 = fix_date_to_iso("$month2/$day2/20$year");
      $date = "$date1/$date2"; # ISA-Tab Chado loader friendly date range
    } elsif (0 && $date =~ m{^(\d+)/(\d+)/(\d{2})$}) {
      my ($month, $day, $year) = ($1,$2,$3);
      die "bad year" if ($year > 20);
      # two digit year edge case
      $date = fix_date_to_iso("$month/$day/20$year")
    } elsif ($date =~ m{^(\d+)/(\d+)(?:-|,\s*|\s+and\s+)(?:\d+/\d+(?:-|,\s*))?(\d+)/(?:\d+-)?(\d+)\s*$} && ($1==$3 || $1+1==$3)) {
      # yearless range - if there are three dates, ignore the middle one!
      my $date1 = fix_date_to_iso("$1/$2/$year_from_filename");
      my $date2 = fix_date_to_iso("$3/$4/$year_from_filename");
      $date = "$date1/$date2"; # ISA-Tab Chado loader friendly date range
    } elsif ($date =~ m{^(\d+)/(\d+),\s?(?:\d+,\s?)*?(\d+)$}) {
      # month/date,date,date,date -> just take first and last dates
      my $date1 = fix_date_to_iso("$1/$2/$year_from_filename");
      my $date2 = fix_date_to_iso("$1/$3/$year_from_filename");
      $date = "$date1/$date2";
    } else {
      $date = fix_date_to_iso($date);
    }

    #if ($orig_date =~ /,|-|and/) {
    #  warn "$orig_date ---> $date\n";
    #}

    # (for CDC/gravid)
    my $trap_type = $row->{Trap} || "NJLT";

    # figure out the collection assay name
    my $a_collection_assay_name = $collection_name{$location->{county}}{$location->{name}}{$trap_type}{$date} //= whitespace_to_underscore(sprintf "%s %s %s %s C%04d", $location->{county}, $location->{name}, $trap_type, $date, ++$collection_counter);

    # NEED TO FIGURE OUT HOW TO DEAL WITH CDC (all in one row) vs NJLT (one species per row) 
    #
    # and also ask Dan about ZEROES for the CDC data
    #

    my @species_data;
    # array of hashes = [ { count => 123,
    #                         sex => 'female', sex_term_source_ref => 'PATO', sex_term_accession_number => '0000383',
    #                         species => 'Anopheles blahblah',
    #                         species_term_source_ref => 'VBsp', species_term_accession_number => '0001234',
    #                         WNV => 01u SLE => 01u, WEE => 01u, LACV => 01u,
    #                       }, ... ]
    # where 01u means 0, 1 or undef
    # and the viral assay counts are optional (not available for CDC)

    if ($row->{Species}) {
      my ($full_species_name, $species_tsr, $species_tan) = species_term($row->{Species});
      my ($sex, $sex_tsr, $sex_tan) = sex_term('F');
      push @species_data, {
			   count => $row->{'Pool Size'},
			   species => $full_species_name,
			   species_term_source_ref => $species_tsr,
			   species_term_accession_number => $species_tan,
			   sex => $sex,
			   sex_term_source_ref => $sex_tsr,
			   sex_term_accession_number => $sex_tan,
			   WNV => sanitise_virus_result($row->{WNV}),
			   SLE => sanitise_virus_result($row->{SLE}),
			   WEE => sanitise_virus_result($row->{WEE}),
			   LACV => sanitise_virus_result($row->{LACV}),
			   collection_protocol_ref => 'COLLECT_CDC',
			  };
    } elsif ($row->{location}) {
      # all the species count headings in the NJLT files end in M or F (male/female)
      foreach my $heading (grep /\s+[MF]$/, keys %$row) {

	my $count = $row->{$heading};
	if ($count > 0) {
	  my ($species, $mf) = $heading =~ /^(.+?)\s+([MF])/;
	  my ($full_species_name, $species_tsr, $species_tan) = species_term($species);
	  my ($sex, $sex_tsr, $sex_tan) = sex_term($mf);

	  push @species_data, {
			       count => $row->{$heading},
			       sex => 'female',
			       sex_term_source_ref => $sex_tsr,
			       sex_term_accession_number => $sex_tan,
			       species => $full_species_name,
			       species_term_source_ref => $species_tsr,
			       species_term_accession_number => $species_tan,
			       WNV => sanitise_virus_result($row->{WNV}),
			       SLE => sanitise_virus_result($row->{SLE}),
			       WEE => sanitise_virus_result($row->{WEE}),
			       LACV => sanitise_virus_result($row->{LACV}),
			       collection_protocol_ref => 'COLLECT_NJLT',
			      };
	} else {
	  warn "need to handle zero counts properly - ignored at the moment!\n" unless ($zero_warned++);
	  # TO DO - probably skip zeroes for the first pass at ISA-Tab/Chado-loading
	  # then deal with zeroes for both CDC and NJLT data properly
	}
      }
    } else {
      die "unexpected parsing error";
    }

    foreach my $data (@species_data) {
      # do s_sample row
      my $sample_name = $sample_name{$location->{county}}{$data->{species}} //= whitespace_to_underscore(sprintf "%s %s S%07d", $location->{county}, $data->{species}, ++$sample_counter);

      push @s_samples, [
			$raw_data_file,
			$sample_name,
			'', # TO DO: description - currently empty, maybe special case for zeroes
			'pool', 'EFO', '0000663', # Material Type
			$data->{sex}, $data->{sex_term_source_ref}, $data->{sex_term_accession_number}, # Sex
			'adult', 'IDOMAL', '0000655', # Developmental Stage
			$data->{count}  # Sample size
		       ];

      push @a_species, [
			$sample_name,
			"$sample_name.SPECIES",
			'', # TO DO? description
			'SPECIES',
			$data->{species}, $data->{species_term_source_ref}, $data->{species_term_accession_number}
		       ];

      push @a_collection, [
			   $sample_name,
			   $a_collection_assay_name,
			   '', # description TO DO
			   $data->{collection_protocol_ref},
			   $date,
			   $orig_date,
			   'State of Iowa', 'GAZ', '00004438',
			   $location->{latitude}, $location->{longitude},
			  ];

    }

  #  print "OK for $location->{name} from $location->{county}\n";

  }
}


write_table("$outdir/s_samples.txt", \@s_samples);
write_table("$outdir/a_species.txt", \@a_species);
write_table("$outdir/a_collection.txt", \@a_collection);


############# lookup subs ################

sub species_term {
  my $input = shift;
  given ($input) {
    when (/^(Ae\.|Aedes) sticticus$/) {
      return ('Aedes sticticus', 'VBsp', '0001144')
    }
    when (/^(Ae\.|Aedes) vexans ?$/) {
      return ('Aedes vexans', 'VBsp', '0000372')
    }
    when (/^(An\.|Anopheles) quadrimaculatus$/) {
      return ('Anopheles quadrimaculatus', 'VBsp', '0003441')
    }
    when (/^(Cx\.|Culex) tarsalis$/) {
      return ('Culex tarsalis', 'VBsp', '0002687')
    }
    when (/^(Ae\.|Aedes) trivittatus$/) {
      return ('Aedes trivittatus', 'VBsp', '0001159')
    }
    when (/^(An\.|Anopheles) punctipennis$/) {
      return ('Anopheles punctipennis', 'VBsp', '0003439')
    }
    when (/^(An\.|Anopheles) walkeri$/) {
      return ('Anopheles walkeri', 'VBsp', '0003469')
    }
    when (/^CPG$/ or /^(Cx\.|Culex) pipiens group$/) {
      warn "CPG/culex pipiens group not correctly handled yet\n" unless ($cpg_warned++); #<<< NEED NEW VBsp species complex
      return ('Culex', 'VBsp', '0002482')
    }
    when (/^(Ae\.|Aedes) japonicus$/) {
      return ('Aedes japonicus', 'VBsp', '0000761')
    }
    when (/^(Ae\.|Aedes) nigromaculis$/) {
      return ('Aedes nigromaculis', 'VBsp', '0001090')
    }
    when (/^(Ae\.|Aedes) sollicitans$/) {
      return ('Aedes sollicitans', 'VBsp', '0001138')
    }
    when (/^(Ae\.|Aedes) triseriatus ?$/) { # allows whitespace typo
      return ('Aedes triseriatus', 'VBsp', '0001206')
    }
    when (/^(An\.|Anopheles) earlei$/) {
      return ('Anopheles earlei', 'VBsp', '0000079')
    }
    when (/^(Cq\.|Coquilletidia) perturbans$/) {
      return ('Coquilletidia perturbans', 'VBsp', '40002347')
    }
    when (/^(Cs\.|Culiseta) [Ii]nornata$/) {
      return ('Culiseta inornata', 'VBsp', '0002409')
    }
    when (/^(Cx\.|Culex) erraticus$/) {
      return ('Culex erraticus', 'VBsp', '0003050')
    }
    when (/^(Cx\.|Culex) territans$/) {
      return ('Culex territans', 'VBsp', '0003218')
    }
    when (/^(Or\.|Orthopodomyia) signifera$/) {
      return ('Orthopodomyia signifera', 'VBsp', '0003681')
    }
    when (/^(Ps\.|Psorophora) ciliata$/) {
      return ('Psorophora ciliata', 'VBsp', '0001345')
    }
    when (/^(Ps\.|Psorophora) columbiae$/) {
      return ('Psorophora columbiae', 'VBsp', '0001307')
    }
    when (/^(Ps\.|Psorophora) cyanescens$/) {
      return ('Psorophora cyanescens', 'VBsp', '0001326')
    }
    when (/^(Ps\.|Psorophora) ferox$/) {
      return ('Psorophora ferox', 'VBsp', '0001328')
    }
    when (/^(Ps\.|Psorophora) horrida$/) {
      return ('Psorophora horrida', 'VBsp', '0001331')
    }
    when (/^(Ur\.|Uranotaenia) sapphirina$/) {
      return ('Uranotaenia sapphirina', 'VBsp', '0002128')
    }
    when (/^(Cs\.|Culiseta) impatiens$/) {
      return ('Culiseta impatiens', 'VBsp', '0002407')
    }
    when (/^Cx\. restuans ?$/) {
      return ('Culex restuans', 'VBsp', '0002657')
    }


    # these are from NJLT data
    when (/^Aedes \?\?\?$/) {
      return ('genus Aedes', 'VBsp', '0000253');
    }
    when (/^Aedes albopictus$/) {
      return ('Aedes albopictus', 'VBsp', '0000522');
    }
    when (/^Aedes atropalpus$/) {
      return ('Aedes atropalpus', 'VBsp', '0000977');
    }
    when (/^Aedes aurifer$/) {
      return ('Aedes aurifer', 'VBsp', '0000979');
    }
    when (/^Aedes campestris$/) {
      return ('Aedes campestris', 'VBsp', '0000994');
    }
    when (/^Aedes canadensis$/) {
      return ('Aedes canadensis', 'VBsp', '0000996');
    }
    when (/^Aedes cinereus$/) {
      return ('Aedes cinereus', 'VBsp', '0000255');
    }
    when (/^Aedes dorsalis$/) {
      return ('Aedes dorsalis', 'VBsp', '0001002');
    }
    when (/^Aedes dupreei$/) {
      return ('Aedes dupreei', 'VBsp', '0001024');
    }
    when (/^Aedes fitchii$/) {
      return ('Aedes fitchii', 'VBsp', '0001035');
    }
    when (/^Aedes flavescens$/) {
      return ('Aedes flavescens', 'VBsp', '0001036');
    }
    when (/^Aedes hendersoni$/) {
      return ('Aedes hendersoni', 'VBsp', '0001188');
    }
    when (/^Aedes implicatus$/) {
      return ('Aedes implicatus', 'VBsp', '0001055');
    }
    when (/^Aedes punctor$/) {
      return ('Aedes punctor', 'VBsp', '0001114');
    }
    when (/^Aedes riparius$/) {
      return ('Aedes riparius', 'VBsp', '0001122');
    }
    when (/^Aedes spencerii$/) {
      return ('Aedes spencerii', 'VBsp', '0001139');
    }
    when (/^Aedes stimulans$/) {
      return ('Aedes stimulans', 'VBsp', '0001146');
    }
    when (/^Aedes vexans nipponii$/) {
      return ('Aedes vexans nipponii', 'VBsp', '?TBD?');
    }

    when (/^Anopheles \?\?\?$/) {
      return ('genus Anopheles', 'VBsp', '0000015');
    }
    when (/^Anopheles barberi$/) {
      return ('Anopheles barberi', 'VBsp', '0000040');
    }
    when (/^Anopheles crucians$/) {
      return ('Anopheles crucians', 'VBsp', '0000072');
    }

    when (/^Culex \?\?\?$/) {
      return ('genus Culex', 'VBsp', '0002423');
    }
    when (/^Culex salinarius$/) {
      return ('Culex salinarius', 'VBsp', '0002661');
    }

    when (/^Culiseta \?\?\?$/) {
      return ('genus Culiseta', 'VBsp', '0002373');
    }
    when (/^Culiseta impatiens$/) {
      return ('Culiseta impatiens', 'VBsp', '0002407');
    }
    when (/^Culiseta melanura$/) {
      return ('Culiseta melanura', 'VBsp', '0002381');
    }
    when (/^Culiseta minnesotae$/) {
      return ('Culiseta minnesotae', 'VBsp', '0002390');
    }
    when (/^Culiseta morsitans$/) {
      return ('Culiseta morsitans', 'VBsp', '0002391');
    }

    when (/^Orthopodomyia \?\?\?$/) {
      return ('genus Orthopodomyia', 'VBsp', '0001301');
    }
    when (/^Orthopodomyia alba$/) {
      return ('Orthopodomyia alba', 'VBsp', '?TBD?');
    }

    when (/^Psorophora \?\?\?$/) {
      return ('genus Psorophora', 'VBsp', '0001304');
    }
    when (/^Psorophora discolor$/) {
      return ('Psorophora discolor', 'VBsp', '0001310');
    }
    when (/^Psorophora howardii$/) {
      return ('Psorophora howardii', 'VBsp', '0001348');
    }
    when (/^Psorophora signipennis$/) {
      return ('Psorophora signipennis', 'VBsp', '0001318');
    }

    default {
     die "fatal error: unknown morpho_species_term >$input<\n";
   }
  }
}

sub sex_term {
  my $input = shift;
  given ($input) {
    when (/^M$/i) {
      return ('male', 'PATO', '0000384')
    }
    when (/^F$/i) {
      return ('female', 'PATO', '0000383')
    }
    default {
      die "fatal error: unknown sex_term >$input<\n";
    }
  }
}


#############

sub load_tsv_file {
  my ($filename) = @_;
  return Text::CSV::Hashify->new( {
				   file   => $filename,
				   format => 'aoh',
				   %tsv_parser_defaults,
				  } )->all;
}

sub write_table {
  my ($filename, $arrayref) = @_;
  my $handle;
  open($handle, ">", $filename) || die "problem opening $filename for writing\n";
  my $tsv_writer = Text::CSV->new ( \%tsv_parser_defaults );
  foreach my $row (@{$arrayref}) {
    $tsv_writer->print($handle, $row);
  }
  close($handle);
  warn "sucessfully wrote $filename\n";
}


sub fix_date_to_iso {
  my $input = shift;
  my $date_parser = DateTime::Format::Strptime->new(
						    pattern   => '%m/%d/%Y',
						    locale    => 'en_US',
						    time_zone => 'Europe/London',
						   );
  my $dt = $date_parser->parse_datetime($input);

  die "could not parse date '$input'\n" unless (defined $dt);

  my $iso_ish_date = sprintf "%d-%02d-%02d", $dt->year, $dt->month, $dt->day;
  return $iso_ish_date;
}

sub whitespace_to_underscore {
  my ($input) = @_;
  $input =~ s/\s+/_/g;
  return $input;
}

sub sanitise_virus_result {
  my ($input) = @_;
  return 1 if ($input && $input =~ /pos/i);
  return 0 if ($input && $input =~ /neg/i);
  return undef;
}
