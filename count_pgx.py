import argparse
from modules.etl_pgx import ETLPGx
from modules.count_haplotype import HaplotypeCounter, HaplotypeFormatter
from modules.count_diplotype import DiplotypeCounter, DiplotypeFormatter
from modules.utils import Utils
from modules.line_notify import send_message

def main(args):
    # Define paths
    source_path = args.source_path
    gender_path = args.gender_path
    output_path = args.output_path

    # ETL Process to update master data
    etl_obj = ETLPGx(source_path=source_path, gender_path=gender_path)
    # Check new sample directory
    list_new_sample = etl_obj.get_new_sample()
    # etl_obj.update_json_file()
    extract_data = etl_obj.extract_master_data(list_new_sample)
    master_data = etl_obj.load_master_data(extract_data)

    data_hap = HaplotypeCounter.count_haplotype(master_data)
    Utils.save_json(data_hap, '.out/count_haplotype/count_haplotype.json')
    result_hap = HaplotypeFormatter.format_haplotype(data_hap)
    Utils.save_json(result_hap, f'{output_path}/pgx_count_haplotype.json')

    data_dip = DiplotypeCounter.count_diplotype(master_data)
    Utils.save_json(data_dip, '.out/count_diplotype/count_diplotype.json')
    result_dip = DiplotypeFormatter.format_diplotype(data_dip)
    Utils.save_json(result_dip, f'{output_path}/pgx_count_diplotype.json')

    # Send message to line notification
    try:
        send_message(f'{etl_obj.get_status()}\nCurrent have {len(master_data)} sample')
    except:
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL PGx Data Processing")
    parser.add_argument("--source_path", type=str, required=True, help="Path to the source data directory")
    parser.add_argument("--gender_path", type=str, required=True, help="Path to the gender data file")
    parser.add_argument("--output_path", type=str, required=True, help="Path to the output directory")
    args = parser.parse_args()
    main(args)