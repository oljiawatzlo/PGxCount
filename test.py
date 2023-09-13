class CountHaplotype:
    def __init__(self, masterdata) -> None:
        # self.masterdatafile_path = masterdatafile_path
        self.masterdata = masterdata
    
    def count_haplotype(self) -> list:
        data = self.masterdata
        result_list = []
        
        # Loop through each sample's gene details
        for sample in data:
            gender = sample['gender']
            for gene_entry in sample['gene_detail']:
                gene = gene_entry['gene']
                haplotype_list = gene_entry['haplotype']
                
                number_haplotype = len(haplotype_list)
                
                count_dict = {}
                
                # Count occurrences of each haplotype
                for haplotype in haplotype_list:
                    if number_haplotype <=1:
                        if haplotype in count_dict:
                            count_dict[haplotype] += 1
                        else:
                            count_dict[haplotype] = 1
                    else:
                        count_dict[haplotype] = np.nan
        
                # Convert the count_dict into a list of dictionaries
                for haplotype, count in count_dict.items():
                    result_list.append({'gene': gene, 'gender': gender, 'haplotype': haplotype, 'haplotype_count': count})
        
        # Create a DataFrame from the result_list
        result_df = pd.DataFrame(result_list)
                    
        # Group and aggregate the results using Pandas
        result_all = (
            pd.DataFrame(result_list)
            .dropna()
            .groupby(['gene', 'gender', 'haplotype'])
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

    
    def total_haplotype(self) -> list:        
        data = self.count_haplotype()

        df_all = pd.DataFrame(data['all'])
        df_all['gender'] = 'all'
        df_all['countkey'] = df_all['gender']+'-'+df_all['gene']+'-'+df_all['haplotype']
        df_all['totalkey'] = df_all['gender']+'-'+df_all['gene']
        df_all['lookupkey'] = df_all['gene']+'-'+df_all['haplotype']

        df_male = pd.DataFrame(data['male'])
        df_male['lookupkey'] = 'male'
        df_male['countkey'] = df_male['gender']+'-'+df_male['gene']+'-'+df_male['haplotype']
        df_male['totalkey'] = df_male['gender']+'-'+df_male['gene']
        df_male['lookupkey'] = df_male['gene']+'-'+df_male['haplotype']

        df_female = pd.DataFrame(data['female'])
        df_female['lookupkey'] = 'female'
        df_female['countkey'] = df_female['gender']+'-'+df_female['gene']+'-'+df_female['haplotype']
        df_female['totalkey'] = df_female['gender']+'-'+df_female['gene']
        df_female['lookupkey'] = df_female['gene']+'-'+df_female['haplotype']

        df = pd.concat([df_all, df_male, df_female], ignore_index=True)[['gender','countkey', 'totalkey','lookupkey', 'haplotype_count']].rename(columns={'gender': 'cou_gender'})

        ######
        
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
                        'haplotype_count': float(0)
                    }
                    # Append the new row to the list
                    new_rows.append(new_row_dict)

        # Create a new DataFrame by concatenating the original DataFrame and the new rows
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)      
        
        ######
                
        df_count_hap = df.groupby('countkey', as_index=False)['haplotype_count'].sum().rename(columns={'haplotype_count': 'hap_cou'})

        df_count_total = df.groupby('totalkey', as_index=False)['haplotype_count'].sum().rename(columns={'haplotype_count': 'hap_total'})

        df_merge_cou = pd.merge(df, df_count_hap, how='inner', on='countkey')[['countkey','cou_gender', 'totalkey','lookupkey', 'hap_cou']].drop_duplicates().sort_values(by='lookupkey')
        df_merge_cou_total = pd.merge(df_merge_cou, df_count_total, how='inner', on='totalkey')[['countkey','cou_gender', 'totalkey','lookupkey', 'hap_cou', 'hap_total']].drop_duplicates().sort_values(by='lookupkey')

        master_data_path = '.out/master_data.json'

        result = []

        with open(master_data_path, 'r') as file:
            master_data = json.load(file)
            
        for i in master_data:   
            for j in i['gene_detail']:
                for k in j['haplotype']:
                    master_result = {}
                    master_result['sample_id'] = i['sample_id']
                    master_result['nbt_id'] = i['nbt_id']
                    master_result['gender'] = i['gender']
                    master_result['gene'] = j['gene']
                    master_result['haplotype'] = k
                    master_result['lookupkey'] = j['gene']+'-'+k
                    result.append(master_result)
                    
        df_master = pd.DataFrame(result)

        df_merge = pd.merge(df_master, df_merge_cou_total, how='inner', on='lookupkey').sort_values(by=['sample_id', 'gene'])
        df_merge['hap_ratio'] = (df_merge['hap_cou']/df_merge['hap_total']).round(4)
        df_merge.fillna(0, inplace=True)

        pgx_count_haplotype = []

        # Get unique combinations of 'sample_id' and 'gene'
        unique_combinations = df_merge[['sample_id', 'gene']].drop_duplicates()

        for index, row in unique_combinations.iterrows():
            sample_id = row['sample_id']
            gene = row['gene']
            
            # Filter the DataFrame for the current 'sample_id' and 'gene' combination
            filtered_df = df_merge[(df_merge['sample_id'] == sample_id) & (df_merge['gene'] == gene)]
            
            # Get unique 'haplotype' values for the current combination
            unique_haplotypes = filtered_df['haplotype'].unique()
            
            for hap in unique_haplotypes:
                dict_result = {
                    'cou_hap_sample_id': sample_id,
                    'cou_hap_nbt_id': sample_id,  # Assuming this should also be 'sample_id'
                    'cou_hap_gene': gene,
                    'cou_hap_haplotype': hap,
                    'cou_hap_gender_distribution': []
                }
                
                # Filter the DataFrame for the current 'sample_id', 'gene', and 'haplotype' combination
                filtered_hap_df = filtered_df[filtered_df['haplotype'] == hap]
                
                for index, hap_row in filtered_hap_df.iterrows():
                    gender_distribution = {
                        'cou_gender': hap_row['cou_gender'],
                        'hap_cou': hap_row['hap_cou'],
                        'hap_total': hap_row['hap_total'],
                        'hap_ratio': hap_row['hap_ratio']
                    }
                    dict_result['cou_hap_gender_distribution'].append(gender_distribution)
                
                pgx_count_haplotype.append(dict_result)
        return pgx_count_haplotype