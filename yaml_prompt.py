prompt = """The goal is to generate a YAML file, that contains the table schema and descriptions, for the table and each of the columns.
You will be provided with the table name, and the schema for the table. Please use this, to generate the file, as demonstrated in an example below.

## Example:
# Table Name: `example_table`
# Schema:
# id: INT
# name: TEXT
# age: INT
# created_at: TIMESTAMP

## Output:
version: 2 
models: 
    - name: example_table
    description: This is an example table, which contains people's names, ages, and when they were added to the table.
    columns:
        - name: id
        description: This is the primary key of the table.
        - name: name
        description: This is a person's name.
        - name: age
        description: This is a person's age.
        - name: created_at
        description: This is a timestamp indicating when the person was added to the table.

## Question:
{question}
"""
