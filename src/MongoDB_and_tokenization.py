import io
import pandas as pad
import boto3
import mysql.connector
from transformers import AutoTokenizer
import pymongo
from datetime import datetime


def tokenize_sequences(db_host,db_user,db_pwd,db_name,model_name):

    connection = mysql.connector.connect(
        host=db_host,
        port=3307,                    
        user=db_user,
        password=db_pwd,
        database=db_name
    )

    connection._sql_mode = ""
    cursor = connection.cursor(dictionary=True)

    select_query = """SELECT id,title, subpart, content, resume FROM texts"""
    cursor.execute(select_query)
    tables = cursor.fetchall()
    print(len(tables))

    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
    mongo_db = mongo_client["curated"]
    mongo_collection = mongo_db["wikitext"]

    tokenizer = AutoTokenizer.from_pretrained(model_name,do_lower_case=False) 


    for index in range (len(tables)):
        tokenize_sequence = tokenizer(tables[index]["title"], truncation = True,padding="max_length",max_length=1024)["input_ids"]
        document = {
        "id": tables[index]["id"],
        "title": tables[index]["title"],
        "tokens": tokenize_sequence,
        "metadata": {
            "source": "mysql",
            "processed_at": datetime.utcnow().isoformat()
                    }
        }
        mongo_collection.insert_one(document)
    print(f"MongoDB done,{mongo_collection.name}")
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process data from staging to curated bucket")
    parser.add_argument("--db_host", type=str, required=True, help="Name of the staging S3 bucket")
    parser.add_argument("--db_user", type=str, required=True, help="Name of the curated S3 bucket")
    parser.add_argument("--db_pwd", type=str, required=True, help="Name of the input file in the staging bucket")
    parser.add_argument("--db_name", type=str, required=True, help="Name of the output file in the curated bucket")
    parser.add_argument("--model_name", type=str, default="facebook/esm2_t6_8M_UR50D", help="Tokenizer model name")
    args = parser.parse_args()

    tokenize_sequences(args.db_host,args.db_user,args.db_pwd,args.db_name,args.model_name)
