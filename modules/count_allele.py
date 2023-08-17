import json
import pandas as pd

class CountHaplotype:
    def __init__(self, masterdatafile_path) -> None:
        self.masterdatafile_path = masterdatafile_path
    
    def count_allele(self) -> list:
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
                haplotype_list = gene_entry['haplotype']
                
                number_allele = len(haplotype_list)
                
                count_dict = {}
                
                # Count occurrences of each allele
                for allele in haplotype_list:
                    if allele in count_dict:
                        count_dict[allele] += (1 * (2 / number_allele))
                    else:
                        count_dict[allele] = (1 * (2 / number_allele))
        
                # Convert the count_dict into a list of dictionaries
                for allele, count in count_dict.items():
                    result_list.append({'gene': gene, 'allele': allele, 'allele_count': count})
                    
        # Group and aggregate the results using Pandas
        result = (
            pd.DataFrame(result_list)
            .groupby(['gene', 'allele'])
            .sum()
            .reset_index()
            .sort_values(by="gene")
            .to_dict(orient='records')
        )            
        
        return result
    
    def total_allele(self) -> list:        
        result_list = self.count_allele()
        
        # Calculate total count for each gene
        gene_totals = {}
        for entry in result_list:
            gene = entry['gene']
            count = entry['allele_count']
            
            if gene in gene_totals:
                gene_totals[gene] += count
            else:
                gene_totals[gene] = count
                    
        # Calculate ratios and update the entries
        for entry in result_list:
            gene = entry['gene']
            count = entry['allele_count']
            total_count = gene_totals[gene]
            
            ratio = count / total_count
            
            entry['allele_total'] = total_count
            entry['allele_frequency'] = ratio
        
        return result_list
    
    def save_json(self, file_path) -> None:
        data = self.total_allele()
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)   
        print('count_allele file saved successfully.')