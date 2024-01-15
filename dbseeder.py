#!/usr/bin/python3
#########################################
# Database Seeding Script
###########################################

import os
import sys
import random
import requests 
import mysql.connector
import json
import dotenv
import openai 
from openai import OpenAI
from dotenv import load_dotenv


# Function to parse command line arguments
def parse_args():
    parser = argparse.ArgumentParser(description="Database Seeding Script")
    parser.add_argument('-d', '--dbname', type=str, help="Database name", required=True)
    return parser.parse_args()

# Function to create a database connection
def create_database_connection(dbname):
    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',  # External host for the Lando service
            user='lamp',       # Username from Lando config
            passwd='lamp',     # Password from Lando config
            database=dbname,   # Database name from command line argument
            port=3307          # External port from Lando config
        )
        print("Connection to MySQL DB successful")
        return connection
    except mysql.connector.Error as e:
        print(f"The error '{e}' occurred")
        return None

# Function to execute a query and return results
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except mysql.connector.Error as e:
        print(f"The error '{e}' occurred")
        return None

# Function to get database schema
def get_database_schema(connection, db_name):
  schema_query = f"SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{db_name}'"
  result = execute_query(connection, schema_query)
   
  # Initialize an empty dictionary to hold the schema
  schema = {}
  
  # Process the result
  for table_name, column_name, data_type in result:
    # If the table name is not in the schema, add it with an empty dictionary as its value
    if table_name not in schema:
      schema[table_name] = {}
    # Add the column name and data type to the table's dictionary in the schema
    schema[table_name][column_name] = data_type
  
  return schema

# Function to generate data using ChatGPT API
def generate_data_with_chatgpt(prompt, apikey):
    
    client = OpenAI(api_key=apikey)
    print("Prompt: " + prompt)
    completion = client.chat.completions.create(
      # model="gpt-3.5-turbo",
       model="gpt-4-1106-preview",
      messages=[
        {"role": "system", "content": "You are a skilled database analyst, skilled in databses and generating belivable and consistent data. No extra data is needed, nor any explainations nor comments, just data, in the form of a sql insert command"},
        {"role": "user", "content": prompt},  
      ]
    )
    
    # print(completion.choices[0].message.content)
    # Parse the content of the message as JSON
    generated_data=completion.choices[0].message.content
    return generated_data


  # Main execution
def main():
  # Load environment variables from .env file
  load_dotenv()  
  # API Key for ChatGPT
  api_key = os.getenv("OPENAI_API_KEY")
  dbname = os.getenv("DB_NAME")
  # Connect to the database
  # # Create a database connection
  connection = create_database_connection(dbname)
  if connection:
    # Retrieve the database schema
    schema = get_database_schema(connection, dbname)
    # Initialize SQL code string
    sql_code = ""
    # Process the schema and generate SQL code
    for table, columns in schema.items():
      # Construct a prompt based on the table and its columns
      columns_prompt=str(columns)
      # columns_prompt = ', '.join([f"{column}:{datatype}" for column, datatype in columns])
      prompt = f"For Table:{table}, Given this data structure, {columns_prompt}, generate 10 lines of realistic and consistent data."
      # Use ChatGPT API to generate data
      sql_code = generate_data_with_chatgpt(prompt, api_key)
      # Output the generated SQL code
      print(sql_code)
      # Close the connection
      connection.close()
    else:
      print("No database connection. Exiting...")

if __name__ == "__main__":
    main()
