import argparse
import json
import pandas as pd
from modules.etl_pgx import ETLPGx
from modules.count_allele import CountHaplotype
from modules.count_diplotype import CountDiplotype
from modules.line_notify import send_message

# Argument parsing
parser = argparse.ArgumentParser(description='Perform ETL and generate reports for PGx data.')
parser.add_argument('--source', dest='source_path', required=True, help='Path to source data directory')
parser.add_argument('--output', dest='output_path', required=True, help='Path to output report directory')
args = parser.parse_args()

# Define paths
source_path = args.source_path
output_path = args.output_path

# ETL Process to update master data
etl_obj = ETLPGx(source_path=source_path, output_path='.out' + '/master_data.json')
etl_obj.update_json_file()

# Count Haplotypes and save to JSON
count_allele_path = '.out/count_allele/count_allele.json'
count_allele = CountHaplotype('.out' + '/master_data.json')
allele_data = count_allele.total_allele()
count_allele.save_json(count_allele_path)

# Count Diplotypes and save to JSON
count_diplotype_path = '.out/count_diplotype/count_diplotype.json'
count_diplotype = CountDiplotype('.out' + '/master_data.json')
dip_data = count_diplotype.total_diplotype()
count_diplotype.save_json(count_diplotype_path)

# Load master data from JSON
with open('.out/master_data.json', 'r') as file:
    master_data = json.load(file)

# Generate processed allele and diplotype data
result_allele = []
result_dip = []

for data in master_data:
    for gene_data in data['gene_detail']:
        for diplotype in gene_data['diplotype']:
            dict_dip = {
                'cou_dip_sample_id': data['sample_id'],
                'cou_dip_nbt_id': data['nbt_id'],
                'cou_dip_gene': gene_data['gene'],
                'lookupkey': gene_data['gene'] + '-' + diplotype
            }
            result_dip.append(dict_dip)
        
        for allele in gene_data['haplotype']:
            dict_allele = {
                'cou_hap_sample_id': data['sample_id'],
                'cou_hap_nbt_id': data['nbt_id'],
                'cou_hap_gene': gene_data['gene'],
                'cou_hap_haplotype': allele,
                'lookupkey': gene_data['gene'] + '-' + allele
            }
            result_allele.append(dict_allele)

# Convert processed data into DataFrames
df_master_allele = pd.DataFrame(result_allele)
df_master_dip = pd.DataFrame(result_dip)

df_allele = pd.DataFrame(allele_data)
df_allele['lookupkey'] = df_allele['gene'] + '-' + df_allele['allele']

# Merge and generate final allele and diplotype reports
# Allele reports
df_cou_allele = (
    pd.merge(df_master_allele, df_allele, how='inner', on='lookupkey')
    .sort_values(by=['cou_hap_sample_id', 'gene'])
    .drop(columns=['lookupkey', 'gene', 'allele'])
    .rename(columns={'allele_count':'cou_hap_counts', 'allele_total': 'cou_hap_total', 'allele_frequency': 'cou_hap_frequency'})
    .to_dict(orient='records')
)
output_allele_path = f'{output_path}/pgx_count_haplotype.json'
with open(output_allele_path, 'w') as file:
    json.dump(df_cou_allele, file, indent=4)

df_dip = pd.DataFrame(dip_data)
df_dip['lookupkey'] = df_dip['gene'] + '-' + df_dip['diplotype']

# Diplotype reports
df_cou_dip = (
    pd.merge(df_master_dip, df_dip, how='inner', on='lookupkey')
    .sort_values(by=['cou_dip_sample_id', 'gene'])
    .drop(columns=['lookupkey', 'gene', 'diplotype'])
    .rename(columns={'diplotype_count':'cou_dip_counts', 'diplotype_total': 'cou_dip_total', 'diplotype_frequency': 'cou_dip_frequency'})
    .to_dict(orient='records')
)
output_dip_path = f'{output_path}/pgx_count_diplotype.json'
with open(output_dip_path, 'w') as file:
    json.dump(df_cou_dip, file, indent=4)

# Send message to line notification
try:
    send_message(f'{etl_obj.status}\nCurrent have {len(master_data)} sample')
except:
    pass