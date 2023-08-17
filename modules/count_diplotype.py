import json
import pandas as pd

class CountDiplotype:
    def __init__(self, masterdatafile_path) -> None:
        self.masterdatafile_path = masterdatafile_path
    
    def count_diplotype(self) -> list:
        try:
            # Load the JSON file containing master data
            with open(self.masterdatafile_path, 'r') as file:
                data = json.load(file)
        except FileNotFoundError:
            return 'FileNotFoundError'
        
        result_list = []
        
        # Loop through each sample's gene details
        for sample in data:
            for gene_entry in sample['gene_detail']:
                gene = gene_entry['gene']
                diplotype_list = gene_entry['diplotype']
                
                number_diplotype = len(diplotype_list)
                
                count_dict = {}
                
                # Count occurrences of each diplotype
                for diplotype in diplotype_list:
                    if diplotype in count_dict:
                        count_dict[diplotype] += (1 * (1 / number_diplotype))
                    else:
                        count_dict[diplotype] = (1 * (1 / number_diplotype))
        
                # Convert the count_dict into a list of dictionaries
                for diplotype, count in count_dict.items():
                    result_list.append({'gene': gene, 'diplotype': diplotype, 'diplotype_count': count})
                    
        # Group and aggregate the results using Pandas
        result = (
            pd.DataFrame(result_list)
            .groupby(['gene', 'diplotype'])
            .sum()
            .reset_index()
            .sort_values(by="gene")
            .to_dict(orient='records')
        )            
        
        return result
    
    def total_diplotype(self) -> list:        
        result_list = self.count_diplotype()
        
        # Calculate total count for each gene
        gene_totals = {}
        for entry in result_list:
            gene = entry['gene']
            count = entry['diplotype_count']
            
            if gene in gene_totals:
                gene_totals[gene] += count
            else:
                gene_totals[gene] = count
                    
        # Calculate ratios and update the entries
        for entry in result_list:
            gene = entry['gene']
            count = entry['diplotype_count']
            total_count = gene_totals[gene]
            
            ratio = count / total_count
            
            entry['diplotype_total'] = total_count
            entry['diplotype_frequency'] = ratio
        
        return result_list
    
    def save_json(self, file_path) -> None:
        data = self.total_diplotype()
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)   
        print('count_dipltype file saved successfully.')

