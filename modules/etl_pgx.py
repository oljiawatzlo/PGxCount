import os
import pandas as pd
import json
import traceback  # Import traceback to log exceptions

class ETLPGx:
    def __init__(self, source_path: str, gender_path: str, output_path: str = '.out/master_data.json') -> None:
        self.source_path = source_path
        self.output_path = output_path
        self.gender_path = gender_path
        self.__status__ = ''
    
    def set_status(self,list_new_file:list):
        try:
            txt = f"Updated {len(list_new_file)} sample(s)\n"
            count = 0
            print(list_new_file)
            for sample_id in list_new_file:
                count += 1
                txt += f"{count}. {sample_id}\n"
            self.__status__ += txt
        except Exception as e:
            # Log the exception traceback for debugging purposes
            traceback.print_exc()
            self.__status__ += str(e)
    
    def extract_master_data(self, list_new_file:list) -> list:
        """
        Update master data from JSON files in source_path.
        
        Returns:
            list: A list of updated master data records.
        """
        list_directories = list_new_file
        self.set_status(list_directories)
        result = []
        
        for directory in list_directories:
            path = os.path.join(self.source_path, directory, "CPIC", "ReportCPIC", f"{directory}_pgx_cpic_summary.json")
            
            with open(path, "r") as json_file:
                data = json.load(json_file)
            
            result.append(self.transform_master_data(data))
                
        return result
        
    def transform_master_data(self, data: dict) -> dict:
        """
        Generate master data from the provided data.

        Args:
            data (dict): Data from update_master_data() method.

        Returns:
            dict: A dictionary containing master data.
        """
        df_gender = pd.read_csv(self.gender_path, delimiter='\t', header=None, names=['sample', 'gender'], dtype={'sample': 'string', 'gender': 'string'})
        sample_id = str(data[0]['cpi_sum_sample_id'])
        gender = df_gender[df_gender['sample']==sample_id]['gender'].values[0]
        result = {'sample_id': sample_id, 'nbt_id': data[0]['cpi_sum_nbt_id'], 'gender': gender}
        df = pd.DataFrame(data)
        list_gene = df['cpi_sum_gene'].unique().tolist()
        list_gene_detail = []
              
        for gene in list_gene:
            if gene not in ['HLA-A', 'HLA-B', 'G6PD']:
                diplotypes = df[df['cpi_sum_gene'] == gene]['cpi_sum_guide_dip_name'].drop_duplicates().values[:]
            elif gene in ['HLA-A', 'HLA-B']:
                diplotypes = df[df['cpi_sum_gene'] == gene]['cpi_sum_print_dip_name'].drop_duplicates().values[:]
            else:
                if (gender == 'female' or 'Female' or 'FEMALE') and (gene == 'G6PD'):
                    diplotypes = df[df['cpi_sum_gene'] == gene]['cpi_sum_print_dip_name'].drop_duplicates().values[:]
                else:
                    diplotypes = 'N/A'
                
            list_diplotype = []
            list_haplotype = []
            
            for diplotype in diplotypes:
                if (diplotype != 'N/A') and (gene != 'G6PD'):
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
    
    def load_master_data(self, list_data: list) -> None:
        """
        Append data to the JSON file.
        
        Args:
            list_data (list): Data to be appended to the JSON file.
        """
        file_path = self.output_path
         
        existing_data = self.check_existing_data()

        existing_data.extend(list_data)

        with open(file_path, 'w') as file:
            json.dump(existing_data, file, indent=4)
        print('Master file saved successfully.')
        return existing_data
    
    def check_existing_data(self):
        file_path = self.output_path       
        try:
            with open(file_path, 'r') as file:
                existing_data = json.load(file)
        except FileNotFoundError:
            existing_data = []
        return existing_data
    
    def get_new_sample(self) -> list:
        """
        Get a list of directories to process.

        Returns:
            list: A list of directories to process.
        """
        result = []
        list_directories = os.listdir(self.source_path)
        try:
            with open(self.output_path, 'r') as file:
                data = json.load(file)
                specific_field_list = [item['sample_id'] for item in data]
        except FileNotFoundError:
            specific_field_list = []
        for directory in list_directories:
            if directory.isdigit() and directory not in specific_field_list:
                result.append(directory)
        return result

    def get_status(self) -> str:
        """
        Update the JSON file with new data and return a status message.
        
        Returns:
            str: A status message indicating the update.
        """
        return self.__status__
        
        