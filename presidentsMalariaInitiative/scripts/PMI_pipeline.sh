#!/bin/bash
#
# README v 2015/07/24
#
# Runs the ISA-Tab generator then ships to PopBio
#
#


scriptdir=`dirname $0`

cd $scriptdir

echo "generating foreign keys (mapping RAW PMI values -to- VB freindly ontology terms)..."

python ./ontologyKeys_to_dict.py

echo "generating ISA-Tabs at: ../data/isatabs/*, but does not include i_investigations.txt..."

python ./dict_to_isatabGen.py           # data gets generated and put in ../data/isatab

echo "shipping ISA-Tabs..."

bash ./ship_to_popbio.sh                # files get tidied and copied to a directory of your choice

# then follow the admin notes to load into chado: https://docs.google.com/document/d/1w_3nhphdkEJCzu97wqLor57k9G8m2SGGu3schWSHy4A/edit#

echo -e "\nSUCCESS!!!\n"

echo "data should be ready to load into Chado.."

echo -e "\n"

echo "e.g. cd ~/popbio/; cd VBPopBio/api/Bio-Chado-VBPopBio; bin/load_project.pl --dry-run ~/popbio/data_andy/isatabs/andy_2015-07-17_PMI/"

echo "here are clear instructions: https://docs.google.com/document/d/1w_3nhphdkEJCzu97wqLor57k9G8m2SGGu3schWSHy4A/edit"

# then run ... 
