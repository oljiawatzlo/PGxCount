import json
class Utils:
    @staticmethod
    def read_json(path: str)->list:
        try:
            with open(path, 'r') as file:
                data = json.load(file)
            return data
        except FileNotFoundError:
            return print(f'{path} is file not found')
    
    @staticmethod    
    def save_json(result: list, path: str)->None:
        """_summary_

        Args:
            result (list): _description_
            path (str): _description_
        """
        with open(path, 'w') as file:
            json.dump(result, file, indent=4)
            print(f'Save {path} is Success')