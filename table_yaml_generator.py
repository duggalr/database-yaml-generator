import os
from dotenv import load_dotenv
load_dotenv()

import ast
from abc import ABC, abstractmethod
import warnings
import argparse
import pickle

from google.cloud import bigquery
from sqlalchemy import MetaData, create_engine, inspect
import openai

import yaml_prompt



class OpenAIConnection(object):
    
    def __init__(self):
        openai.api_key = os.environ['OPENAI_API_KEY']

    def generate_schema_text(self, schema_list, table_name):

        table_schema_text = f'# Table Name: {table_name}' + '\n# Schema:\n' + '# ' +  '\n# '.join([f"{di['col_name']}: {di['col_type']}" for di in schema_list])
#         table_schema_text += """\nTask: Return a list of descriptions generated for each column name from above, along with a description of the table. Follow example format below, when returning the text:
# Table Description: <description>        
# Column Descriptions:
# col_name_one: <description>
# col_name_two: <description>
# """
        return table_schema_text

    def generate_yaml_prompt(self, schema_information):
        return yaml_prompt.prompt.format(question = schema_information)

    def run_chat_gpt_completion(self, prompt):
        message_list = [{"role": "user", "content": prompt}]
        response = openai.ChatCompletion.create(
            model = "gpt-3.5-turbo-0613",
            messages = message_list
        )
        message_text = response["choices"][0]["message"]['content']
        # print('response-message:', message_text)
        return message_text



class BigQueryDBConnection(object):
    
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
        except:
            raise Exception(f"Table {table_id} is invalid. Are you sure you have specified the full table identifier and have the appropriate permissions to view the table?")

        table_schema = table.schema
        tb_schema_list = []
        for sch_field in table_schema:
            col_name = sch_field.name
            col_type = sch_field.field_type
            tb_schema_list.append({
                'col_name': col_name,
                'col_type': col_type
            })

        return tb_schema_list

    def generate_dataset_schema_dict(self, dataset_id):
        tables = self.client.list_tables(dataset_id)

        table_dict = {}
        for tb in tables:
            print(f"On Table: {tb.table_id}")

            full_table_id = f"{dataset_id}.{tb.table_id}"
            table = self.client.get_table(full_table_id)

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

    def process_bq_table(self, bqc, opai, bq_table_name, output_dir):
        print(f'Generating column descriptions for {bq_table_name} using GPT...')
        table_schema = bqc.generate_table_schema_dict(table_id=bq_table_name)
        table_desc_prompt = opai.generate_schema_text(table_schema, bq_table_name)
        yaml_prompt = opai.generate_yaml_prompt(table_desc_prompt)
        res = opai.run_chat_gpt_completion(yaml_prompt)

        fn = f'{bq_table_name}.yaml'
        out_fp = os.path.join(output_dir, fn)
        self._save_yaml_file(out_fp, res)

    def process_bq_dataset(self, bqc, opai, bq_dataset_name, output_dir):
        dataset_schema_dict = bqc.generate_dataset_schema_dict(dataset_id=bq_dataset_name)

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

    bqc = BigQueryDBConnection(
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
        bqc.process_bq_table(bqc, opai, bq_table_name, output_dir)
    elif bq_dataset_name:
        bqc.process_bq_dataset(bqc, opai, bq_dataset_name, output_dir)
    else:
        raise Exception("No BigQuery Table or Dataset specified.")



## Test: 
# python3 table_yaml_generator.py --bigquery_credentials_filepath /Users/rahul/Desktop/wh_service_accounts/lifeline-oauth-2-31ec407e8880.json --output_dir yaml_output

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog = 'ERD Generator',
        description = 'Generate DBT Yaml Files for your BigQuery Warehouse.',
    )
    parser.add_argument('--bigquery_credentials_filepath')
    parser.add_argument('--output_dir')
    # parser.add_argument('--bigquery_dataset_name', nargs='?', help='Should be full bigquery dataset id: project_id.dataset_id')
    # parser.add_argument('--bigquery_table_name', nargs='?', help='Should be full bigquery table id: project_id.dataset_id.table_id')

    args = parser.parse_args()
    main(args)

