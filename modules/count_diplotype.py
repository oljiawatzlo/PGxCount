import json
import pandas as pd
import numpy as np

class DiplotypeCounter:
    @staticmethod
    def count_diplotype(masterdata):
        """Count diplotype occurrences in the given masterdata.

        Args:
            masterdata (list): List of samples with gene details.

        Returns:
            dict: A dictionary containing diplotype counts by gender.
        """
        data = masterdata
        result_list = []
        
        # Loop through each sample's gene details
        for sample in data:
            gender = sample['gender']
            for gene_entry in sample['gene_detail']:
                gene = gene_entry['gene']
                diplotype_list = gene_entry['diplotype']
                
                number_diplotype = len(diplotype_list)
                
                count_dict = {}
                
                # Count occurrences of each diplotype
                for diplotype in diplotype_list:
                    if number_diplotype <=1:
                        if diplotype in count_dict:
                            count_dict[diplotype] += 1
                        else:
                            count_dict[diplotype] = 1
                    else:
                        count_dict[diplotype] = np.nan
        
                # Convert the count_dict into a list of dictionaries
                for diplotype, count in count_dict.items():
                    result_list.append({'gene': gene, 'gender': gender, 'diplotype': diplotype, 'diplotype_count': count})
        
        # Create a DataFrame from the result_list
        result_df = pd.DataFrame(result_list)
                    
        # Group and aggregate the results using Pandas
        result_all = (
            pd.DataFrame(result_list)
            .dropna()
            .groupby(['gene', 'gender', 'diplotype'])
            .sum()
            .reset_index()
            .sort_values(by="gene")
            .to_dict(orient='records')
        )            
        
        result_male = result_df[result_df['gender'] == 'male']
        result_female = result_df[result_df['gender'] == 'female']
        
        result_by_gender = {
            'all': result_all,
            'male': result_male.to_dict(orient='records'),
            'female': result_female.to_dict(orient='records')
        }
        
        return result_by_gender
    
class DiplotypeFormatter:
    @staticmethod
    def format_diplotype(data):
        """Format diplotype data for further processing.

        Args:
            data (dict): Diplotype data by gender.

        Returns:
            list: Formatted diplotype data.
        """
        df_all = pd.DataFrame(data['all'])
        df_all['gender'] = 'all'
        df_all['countkey'] = df_all['gender']+'-'+df_all['gene']+'-'+df_all['diplotype']
        df_all['totalkey'] = df_all['gender']+'-'+df_all['gene']
        df_all['lookupkey'] = df_all['gene']+'-'+df_all['diplotype']

        df_male = pd.DataFrame(data['male'])
        df_male['lookupkey'] = 'male'
        df_male['countkey'] = df_male['gender']+'-'+df_male['gene']+'-'+df_male['diplotype']
        df_male['totalkey'] = df_male['gender']+'-'+df_male['gene']
        df_male['lookupkey'] = df_male['gene']+'-'+df_male['diplotype']

        df_female = pd.DataFrame(data['female'])
        df_female['lookupkey'] = 'female'
        df_female['countkey'] = df_female['gender']+'-'+df_female['gene']+'-'+df_female['diplotype']
        df_female['totalkey'] = df_female['gender']+'-'+df_female['gene']
        df_female['lookupkey'] = df_female['gene']+'-'+df_female['diplotype']

        df = pd.concat([df_all, df_male, df_female], ignore_index=True)[['gender','countkey', 'totalkey','lookupkey', 'diplotype_count']].rename(columns={'gender': 'cou_gender'})
        
        # Create an empty list to collect the new rows
        new_rows = []

        list_gender = df['cou_gender'].drop_duplicates().to_list()
        list_lookupkey = df['lookupkey'].drop_duplicates().to_list()

        for key in list_lookupkey:
            list_filter_lookupkey = df[df['lookupkey'] == key]['cou_gender'].to_list()
            for gender in list_gender:
                if gender not in list_filter_lookupkey:
                    new_row_dict = {
                        'cou_gender': gender,
                        'countkey': f'{gender}-{key}',
                        'totalkey': f'{gender}-{str(key).split("-")[0]}',
                        'lookupkey': key,
                        'diplotype_count': float(0)
                    }
                    # Append the new row to the list
                    new_rows.append(new_row_dict)

        # Create a new DataFrame by concatenating the original DataFrame and the new rows
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)      
                
        df_count_dip = df.groupby('countkey', as_index=False)['diplotype_count'].sum().rename(columns={'diplotype_count': 'dip_cou'})

        df_count_total = df.groupby('totalkey', as_index=False)['diplotype_count'].sum().rename(columns={'diplotype_count': 'dip_total'})

        df_merge_cou = pd.merge(df, df_count_dip, how='inner', on='countkey')[['countkey','cou_gender', 'totalkey','lookupkey', 'dip_cou']].drop_duplicates().sort_values(by='lookupkey')
        df_merge_cou_total = pd.merge(df_merge_cou, df_count_total, how='inner', on='totalkey')[['countkey','cou_gender', 'totalkey','lookupkey', 'dip_cou', 'dip_total']].drop_duplicates().sort_values(by='lookupkey')

        master_data_path = '.out/master_data.json'

        result = []

        with open(master_data_path, 'r') as file:
            master_data = json.load(file)
            
        for i in master_data:   
            for j in i['gene_detail']:
                for k in j['diplotype']:
                    master_result = {}
                    master_result['sample_id'] = i['sample_id']
                    master_result['nbt_id'] = i['nbt_id']
                    master_result['gender'] = i['gender']
                    master_result['gene'] = j['gene']
                    master_result['diplotype'] = k
                    master_result['lookupkey'] = j['gene']+'-'+k
                    result.append(master_result)
                    
        df_master = pd.DataFrame(result)

        df_merge = pd.merge(df_master, df_merge_cou_total, how='inner', on='lookupkey').sort_values(by=['sample_id', 'gene'])
        df_merge['dip_ratio'] = (df_merge['dip_cou']/df_merge['dip_total']).round(4)
        df_merge.fillna(0, inplace=True)

        pgx_count_diplotype = []

        # Get unique combinations of 'sample_id' and 'gene'
        unique_combinations = df_merge[['sample_id', 'gene']].drop_duplicates()

        for index, row in unique_combinations.iterrows():
            sample_id = row['sample_id']
            gene = row['gene']
            
            # Filter the DataFrame for the current 'sample_id' and 'gene' combination
            filtered_df = df_merge[(df_merge['sample_id'] == sample_id) & (df_merge['gene'] == gene)]
            
            # Get unique 'diplotype' values for the current combination
            unique_diplotypes = filtered_df['diplotype'].unique()
            
            for dip in unique_diplotypes:
                dict_result = {
                    'cou_dip_sample_id': sample_id,
                    'cou_dip_nbt_id': sample_id,  # Assuming this should also be 'sample_id'
                    'cou_dip_gene': gene,
                    'cou_dip_diplotype': dip,
                    'cou_dip_gender_distribution': []
                }
                
                # Filter the DataFrame for the current 'sample_id', 'gene', and 'diplotype' combination
                filtered_dip_df = filtered_df[filtered_df['diplotype'] == dip].copy()
                
                # Define the custom sorting order
                custom_order = ['female', 'male', 'all']
                # Convert the 'Category' column to a Categorical data type with custom order
                filtered_dip_df['Category'] = pd.Categorical(filtered_dip_df['cou_gender'], categories=custom_order, ordered=True)
                
                # Sort the DataFrame by the 'Category' column
                filtered_dip_df = filtered_dip_df.sort_values(by='Category')

                # Reset the index if needed
                filtered_dip_df = filtered_dip_df.reset_index(drop=True)
                
                for index, dip_row in filtered_dip_df.iterrows():
                    gender_distribution = {
                        'cou_gender': dip_row['cou_gender'],
                        'dip_cou': dip_row['dip_cou'],
                        'dip_total': dip_row['dip_total'],
                        'dip_ratio': dip_row['dip_ratio']
                    }
                    dict_result['cou_dip_gender_distribution'].append(gender_distribution)
                
                pgx_count_diplotype.append(dict_result)
        return pgx_count_diplotype
    
if __name__ == '__main__':
    with open('.out/master_data.json', 'r') as file:
        master_data = json.load(file)
        
    data = DiplotypeCounter.count_diplotype(master_data)
    
    with open('.out/count_diplotype/count_diplotype.json', 'w') as file:
        json.dump(data, file, indent=4)
        
    result = DiplotypeFormatter.format_diplotype(data)
    
    with open('report/pgx_count_diplotype.json', 'w') as file:
        json.dump(result, file, indent=4)
