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

my $datafile = './data/ACT-partner-drug-Surveyor-data-Vbase.xlsx';
my $output_dir = './output';

# skip lines with these "mutation" alleles
my $skip_mt_regexp = qr/CIET|CMNK|CMNT|CMET|SMNT|NFD|YYY|copy number/;

# these are the loci we process
my @loci = ('pfcrt 76', 'pfmdr1 1246', 'pfmdr1 184', 'pfmdr1 86', 'pfmdr1 1042',  'pfmdr1 1034');

# all IRO
my %allele_accessions =
(
'pfcrt K76' => '0000169', 'pfcrt 76T' => '0000170', 'pfcrt 76K/T' => '0000171',
'pfmdr1 D1246' => '0000175', 'pfmdr1 1246Y' => '0000176', 'pfmdr1 1246D/Y' => '0000177',
'pfmdr1 Y184' => '0000179', 'pfmdr1 184F' => '0000180', 'pfmdr1 184Y/F' => '0000181',
'pfmdr1 N86' => '0000183', 'pfmdr1 86Y' => '0000184', 'pfmdr1 86N/Y' => '0000185',
'pfmdr1 N1042' => '0000187',
'pfmdr1 S1034' => '0000189',
);

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
my %collection_samples;    # ditto
my %genotyping_samples;

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

  my ($gene, $residue) = $mt =~ /^(\w+).+?(\d+)/;
  die "ERROR: can't determine locus from '$mt'\n" unless (defined $residue);
  my $locus = "$gene $residue";

  die "no accession for '$mt'\n" unless ($allele_accessions{$mt});

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
       study_submission_date => $pYear,
       study_public_release_date => $pYear,
       study_file_name => 's_samples.txt',

       study_tags =>
       [
        { study_tag => 'WWARN',
          study_tag_term_source_ref => 'VBcv',
          study_tag_term_accession_number => '0001114',
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
        { study_assay_measurement_type => 'species identification assay',
          study_assay_file_name => 'a_species.txt',
          samples => $species_assay_samples{$sId} = ordered_hashref(),
        },
        { study_assay_measurement_type => 'field collection',
          study_assay_file_name => 'a_collection.txt',
          samples => $collection_samples{$sId} = ordered_hashref(),
        },
        { study_assay_measurement_type => 'genotype assay',
          study_assay_file_name => 'a_genotyping.txt',
          samples => $genotyping_samples{$sId} = ordered_hashref(),
        }
       ],

       study_protocols =>
       [
        { study_protocol_name => 'SPECIES',
          study_protocol_type => 'species identification method',
          study_protocol_type_term_source_ref => 'MIRO',
          study_protocol_type_term_accession_number => '30000005',
          study_protocol_description => "For further details, please see the dataset's original publication (PMID:$pId).",
        },
        { study_protocol_name => 'COLLECT',
          study_protocol_type => 'field population catch',
          study_protocol_type_term_source_ref => 'MIRO',
          study_protocol_type_term_accession_number => '30000044',
          study_protocol_description => "For further details, please see the dataset's original publication (PMID:$pId).",
        },
        { study_protocol_name => 'GENO',
          study_protocol_type => 'genotyping',
          study_protocol_type_term_source_ref => 'EFO',
          study_protocol_type_term_accession_number => '0000750',
          study_protocol_description => "For further details, please see the dataset's original publication (PMID:$pId).",
        },
      ],

       samples => $samples{$sId} = ordered_hashref,
      }
     ]
    };


  # now make a sample id
  my $sample_id = sprintf "WWARN-%s-%s-%d-%d", $sId,
    ($sNa ? $sNa : "$lat,$lon"), $sf, $sTo;

  # clean up a bit - hope it doesn't cause any non-uniqueness
  $sample_id =~ s/\s+//g;
  $sample_id =~ s/--/-/g;

  $samples{$sId}{$sample_id} //=
    {
     sample_name => $sample_id,
     description => "Potentially mixed sample of P. falciparum",
     material_type => { value => 'population', term_source_ref => 'OBI', term_accession_number => '0000181' },
    };

  # and add the species assay
  $species_assay_samples{$sId}{$sample_id}{assays}{"$sample_id.spid"} //=
    { protocols => { "SPECIES" => { } },
      characteristics => { 'species assay result (VBcv:0000961)' =>
                           { value => 'Plasmodium falciparum', term_source_ref => 'VBsp', term_accession_number => '0003990' } }
    };

  # now handle the collection.  collection ID is equivalent to sample ID (only one species collected)
  $collection_samples{$sId}{$sample_id}{assays}{"$sample_id.coll"} //=
    { protocols => { "COLLECT" => { date => "$sf/$sTo" } },
      characteristics => { 'Collection site latitude (VBcv:0000817)' => { value => $lat },
                           'Collection site longitude (VBcv:0000816)' => { value => $lon },
                         },
    };

  # now the genotype assay
  my $geno_assay_id = "$sample_id.$locus.geno";
  $geno_assay_id =~ s/\s+/_/g;
  my $geno_assay =
    $genotyping_samples{$sId}{$sample_id}{assays}{$geno_assay_id} //=
      { protocols => { 'GENO' => { date => "$sf/$sTo" } },
        characteristics => { 'sample size (VBcv:0000983)' => { value => $tes },
                           },
        raw_data_files => { 'g_genotypes.txt' => { } }
      };

  my $geno_percent = sprintf "%.0f", 100*$pre/$tes;
  my $genotype_id = "$mt $geno_percent%";

  $geno_assay->{genotypes}{$genotype_id} //=
    {
     genotype_name => $genotype_id,
     type => { value => $mt, term_source_ref => "IRO", term_accession_number => $allele_accessions{$mt} },
     description => "$mt mutation frequency: $geno_percent% $pre/$tes",
     characteristics =>
     { 'variant frequency (SO:0001763)' => { value => $geno_percent,
                                             unit =>
                                             { value => 'percent', term_source_ref => 'UO', term_accession_number => '0000187' } }
     },
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

