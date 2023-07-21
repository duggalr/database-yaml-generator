import os
from dotenv import load_dotenv
load_dotenv()

from google.cloud import bigquery
import openai
import argparse

import yaml_prompt


class OpenAIConnection:
    
    def __init__(self):
        openai.api_key = os.environ['OPENAI_API_KEY']

    def generate_schema_text(self, schema_list, table_name):
        table_schema_text = f'# Table Name: {table_name}\n# Schema:\n'
        table_schema_text += '# ' + '\n# '.join([f"{di['col_name']}: {di['col_type']}" for di in schema_list])
        return table_schema_text

    def generate_yaml_prompt(self, schema_information):
        return yaml_prompt.prompt.format(question=schema_information)

    def run_chat_gpt_completion(self, prompt):
        message_list = [{"role": "user", "content": prompt}]
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo-16k-0613",
            messages=message_list
        )
        message_text = response["choices"][0]["message"]["content"]
        return message_text


class BigQueryDBConnection:

    def __init__(self, credentials_fp):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_fp
        self.client = bigquery.Client()

    def _save_yaml_file(self, file_path, content):
        print(f'Saving YAML file: {file_path}')
        with open(file_path, 'w') as file:
            file.write(content)

    def generate_table_schema_dict(self, table_id):
        try:
            table = self.client.get_table(table_id)
        except Exception as e:
            raise Exception(f"Error retrieving table {table_id}: {str(e)}")

        table_schema = table.schema
        tb_schema_list = []
        for sch_field in table_schema:
            col_name = sch_field.name
            print(f"On Column: {col_name}")
            col_type = sch_field.field_type
            tb_schema_list.append({
                'col_name': col_name,
                'col_type': col_type
            })
        return tb_schema_list

    def generate_dataset_schema_dict(self, dataset_id):
        try:
            tables = self.client.list_tables(dataset_id)
        except Exception as e:
            raise Exception(f"Error retrieving tables for dataset {dataset_id}: {str(e)}")

        table_dict = {}
        for tb in tables:
            print(f"On Table: {tb.table_id}")
            full_table_id = f"{dataset_id}.{tb.table_id}"
            
            try:
                table = self.client.get_table(full_table_id)
            except Exception as e:
                raise Exception(f"Error retrieving table {full_table_id}: {str(e)}")
            
            table_schema = table.schema
            tb_schema_list = []
            for sch_field in table_schema:
                col_name = sch_field.name
                
                col_type = sch_field.field_type
                tb_schema_list.append({
                    'col_name': col_name,
                    'col_type': col_type
                })
            table_dict[tb.table_id] = tb_schema_list

        return table_dict

    def process_bq_table(self, opai, bq_table_name, output_dir):
        print(f'Generating column descriptions for {bq_table_name} using GPT...')
        table_schema = self.generate_table_schema_dict(table_id=bq_table_name)
        table_desc_prompt = opai.generate_schema_text(table_schema, bq_table_name)
        yaml_prompt = opai.generate_yaml_prompt(table_desc_prompt)
        res = opai.run_chat_gpt_completion(yaml_prompt)

        fn = f'{bq_table_name}.yaml'
        out_fp = os.path.join(output_dir, fn)
        self._save_yaml_file(out_fp, res)

    def process_bq_dataset(self, opai, bq_dataset_name, output_dir):
        dataset_schema_dict = self.generate_dataset_schema_dict(dataset_id=bq_dataset_name)
        for tb_name, table_schema_list in dataset_schema_dict.items():
            print(f"Generating column descriptions for {tb_name} using GPT...")
            table_desc_prompt = opai.generate_schema_text(table_schema_list, tb_name)
            yaml_prompt = opai.generate_yaml_prompt(table_desc_prompt)
            res = opai.run_chat_gpt_completion(yaml_prompt)

            fn = f'{bq_dataset_name}.{tb_name}.yaml'
            out_fp = os.path.join(output_dir, fn)
            self._save_yaml_file(out_fp, res)



def main(args):
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    bq_connection = BigQueryDBConnection(
        credentials_fp=args.bigquery_credentials_filepath
    )
    opai = OpenAIConnection()

    bq_table_name = None
    bq_dataset_name = None

    user_input = input('Would you like to generate a YAML for a BigQuery Table or BigQuery Dataset? Enter 1 for the former, 2 for the latter: ')
    if user_input == '1':
        bq_table_name = input('Enter your BigQuery Table ID. It must be the full identifier (i.e., project_id.dataset_id.table_id): ')
    elif user_input == '2':
        bq_dataset_name = input('Enter your BigQuery Dataset ID. It must be the full identifier (i.e., project_id.dataset_id): ')
    else:
        raise Exception("Invalid input entered for the question. Should be 1 or 2.")

    if bq_table_name:
        bq_connection.process_bq_table(opai, bq_table_name, output_dir)
    elif bq_dataset_name:
        bq_connection.process_bq_dataset(opai, bq_dataset_name, output_dir)
    else:
        raise Exception("No BigQuery Table or Dataset specified.")


## Test: 
# python3 table_yaml_generator.py --bigquery_credentials_filepath /Users/rahul/Desktop/wh_service_accounts/lifeline-oauth-2-31ec407e8880.json --output_dir yaml_output

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='ERD Generator',
        description='Generate DBT Yaml Files for your BigQuery Warehouse.',
    )
    parser.add_argument('--bigquery_credentials_filepath')
    parser.add_argument('--output_dir')

    args = parser.parse_args()
    main(args)
