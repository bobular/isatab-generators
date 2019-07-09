#!/bin/env perl
#           -*- mode: cperl -*-


use strict;
use warnings;
use utf8::all;

use Spreadsheet::Read;
use Bio::Parser::ISATab;
use lib '../../VBPopBio/api/Bio-Chado-VBPopBio/lib';
use Bio::Chado::VBPopBio::Util::ISATab qw/write_extra_sheets/;
use Bio::Chado::VBPopBio::Util::Functions qw/ordered_hashref/;

my $datafile = './data/ACT-partner-drug-Surveyor-data-SUBSET.xlsx';
my $output_dir = './output';

# skip lines with these "mutation" alleles
my $skip_mt_regexp = qr/CIET|CMNK|SMNT|copy number/;


# read in the excel spreadsheet
my $book = Spreadsheet::Read->new($datafile);

# get the sheet with data in
my $sheet = $book->sheet(1);

my @headings = $sheet->row(1);

my %h2c; # heading => column index
for (my $i=0; $i<@headings; $i++) {
  $h2c{$headings[$i]} = $i;
}


my $maxrow = $sheet->maxrow;
my %seen_sig;

my %isatabs;  # sId => isatab data structures
my %samples;  # sId => hashref (shortcut to $isatabs->{sId}{studies}[0]{samples})
my %species_assay_samples; # same but pointing to $isatabs->{sId}{studies}[0]{study_assays][0]{samples}


for (my $i=2; $i<=$maxrow; $i++) {
  my @row = $sheet->row($i);

  # get all the sample + collection data
  my ($sNa, $lon, $lat, $sf, $sTo) = map { $row[$h2c{$_}] } qw/sNa lon lat sf sTo/;

  # study/publication data
  my ($sId, $tit, $pId, $aut, $pYear) = map { $row[$h2c{$_}] } qw/sId tit pId aut pYear/;

  # genotype data
  my ($mt, $tes, $pre) = map { $row[$h2c{$_}] } qw/mt tes pre/;

  # skip the short haplotype alleles
  next if ($mt =~ $skip_mt_regexp);

  # make a signature so that we don't process duplicate rows ('dru' column makes duplicates, and maybe others do too)
  my $sig = join ':', $sId, $lon, $lat, $sf, $sTo, $mt;
  next if ($seen_sig{$sig}++);

  my ($first_author) = $aut =~ /(\w+)/;

  $isatabs{$sId} //=
    {
     studies =>
     [
      {
       study_identifier => "WWARN-$sId",
       study_title => $tit,
       study_description => "Proof of concept data import from WWARN",
       study_public_release_date => $pYear,
       study_file_name => 's_samples.txt',

       study_tags =>
       [
        { study_tag => 'WWARN',
          study_tag_term_source_ref => 'VBcv',
          study_tag_term_accession_number => '???????',
        },
       ],

       study_design_descriptors =>
       [
        { study_design_type => 'observational design',
          study_design_type_term_source_ref => 'EFO',
          study_design_type_term_accession_number => '0000629',
        },
        { study_design_type => 'genotype design',
          study_design_type_term_source_ref => 'EFO',
          study_design_type_term_accession_number => '0001748',
        },
       ],

       study_publications =>
       [
        { study_pubmed_id => $pId,
          study_publication_author_list => $aut,
          study_publication_title => $tit,
          study_publication_status => 'published',
          study_publication_status_term_source_ref => 'EFO',
          study_publication_status_term_accession_number => '0001796',
        }
       ],

       study_contacts =>
       [
        { study_person_last_name => $first_author,
          study_person_email => "$first_author.$sId\@wwarn.placeholder",
        }
       ],

       study_assays =>
       [
        {
         study_assay_measurement_type => 'species identification assay',
         study_assay_file_name => 'a_species.txt',
         samples => $species_assay_samples{$sId} = ordered_hashref(),
        },
       ],


       samples => $samples{$sId} = ordered_hashref,
      }
     ]
    };


  # now make a sample id

  my $sample_id = sprintf "WWARN-%s-%s-%d-%d", $sId,
    ($sNa ? $sNa : "$lat,$lon"), $sf, $sTo;

  $samples{$sId}{$sample_id} //=
    {
     sample_name => $sample_id,
     description => "Potentially mixed sample of P. falciparum",
     material_type => { value => 'population', term_source_ref => 'OBI', term_accession_number => '0000181' },
    };

  $species_assay_samples{$sId}{$sample_id} //=
    {
     assays => { "$sample_id.spid" =>
                 { protocols => { "SPECIES" => { } },
                   characteristics => { 'species assay result (VBcv:0000961)' =>
                                        { value => 'Plasmodium falciparum', term_source_ref => 'VBsp', term_accession_number => '??????' } }
                 }
               }
    };

  # print join("\t", map { $_ // '' } @row)."\n";

}

foreach my $sId (keys %isatabs) {
  my $isatab = $isatabs{$sId};

  my $output_directory = "$output_dir/WWARN-$sId";
  my $writer = Bio::Parser::ISATab->new(directory=>$output_directory);
  $writer->write($isatab);
  write_extra_sheets($writer, $isatab);
}
