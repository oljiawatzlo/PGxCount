import os
import pandas as pd
import json

class ETLPGx:
    def __init__(self, source_path: str, output_path: str) -> None:
        self.source_path = source_path
        self.output_path = output_path
        self.status = ''
    
    def update_master_data(self) -> list:
        # Get a list of directories to process
        list_dir = self.handle_input_file()
        
        result = []
        for dir in list_dir:
            # Construct the path to the JSON file
            path = os.path.join(self.source_path, dir, "CPIC", "ReportCPIC", f"{dir}_pgx_cpic_summary.json")
            
            # Load the JSON data from the file
            with open(path, "r") as json_file:
                data = json.load(json_file)
            
            # Generate and append master data
            result.append(self.generate_master_data(data))
                
        return result
        
    def generate_master_data(self, data: dict) -> dict:
        result = {'sample_id': data[0]['cpi_sum_sample_id'], 'nbt_id': data[0]['cpi_sum_nbt_id']}
        df = pd.DataFrame(data)
        list_gene = df['cpi_sum_gene'].unique().tolist()
        list_gene_detail = []
        
        for gene in list_gene:
            # Extract diplotypes based on conditions
            if gene not in ['HLA-A', 'HLA-B', 'G6PD']:
                diplotypes = df[df['cpi_sum_gene'] == gene]['cpi_sum_guide_dip_name'].drop_duplicates().values[:]
            else:
                diplotypes = df[df['cpi_sum_gene'] == gene]['cpi_sum_print_dip_name'].drop_duplicates().values[:]
                
            list_diplotype = []
            list_haplotype = []
            
            for diplotype in diplotypes:
                if diplotype != 'N/A' and gene != 'G6PD':
                    hap1, hap2 = diplotype.split('/')
                    list_haplotype.extend([hap1, hap2])
                    list_diplotype.append(diplotype)
                elif gene == 'G6PD':
                    if '/' in diplotype:
                        hap1, hap2 = diplotype.split('/')
                        list_haplotype.extend([hap1, hap2])
                    else:
                        list_haplotype.append(diplotype)
            
            gene_detail = {
                'gene': gene,
                'diplotype': list_diplotype,
                'haplotype': list_haplotype
            }    
            list_gene_detail.append(gene_detail)
        
        result['gene_detail'] = list_gene_detail
        
        return result
    
    def update_json_file(self) -> str:
        try:
            list_update = self.update_master_data()
            self.append_to_json_file(list_update)
            list_update_sample_id = [item['sample_id'] for item in list_update]
            txt = f"Updated {len(list_update)} sample(s)\n"
            count = 0
            for sample_id in list_update_sample_id:
                count += 1
                txt += f"{count}. {sample_id}\n"
            self.status = txt
        except Exception as e:
            self.status = e
    
    def append_to_json_file(self, list_data: list) -> None:
        file_path = self.output_path
        
        try:
            # Read the existing JSON data from the file (if any).
            with open(file_path, 'r') as file:
                existing_data = json.load(file)
        except FileNotFoundError:
            # If the file doesn't exist, initialize with an empty list as data.
            existing_data = []

        # Append the new data to the existing JSON object.
        existing_data.extend(list_data)

        # Write the updated JSON object back to the file.
        with open(file_path, 'w') as file:
            json.dump(existing_data, file, indent=4)
        print('master file saved successfully.')
    
    def handle_input_file(self) -> list:
        result = []
        list_dir = os.listdir(self.source_path)
        
        try:
            # Load the JSON file
            with open(self.output_path, 'r') as file:
                data = json.load(file)
                specific_field_list = [item['sample_id'] for item in data]
        except FileNotFoundError:
            specific_field_list = []
        
        for dir in list_dir:
            if dir.isdigit() and dir not in specific_field_list:
                result.append(dir)
        return result