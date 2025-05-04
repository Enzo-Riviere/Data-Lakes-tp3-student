from datasets import load_dataset
import os
import pandas as pad
import boto3
import argparse

def load_dataset_Salesforce():
    ds = load_dataset("Salesforce/wikitext", "wikitext-2-v1")

    for split in ds.keys():
        with open(f"data/raw/{split}/wikitext_{split}.txt", "w", encoding="utf-8") as f:
            for item in ds[split]:
                f.write(item["text"].strip() + "\n")


if __name__ == "__main__":
    
    '''parser = argparse.ArgumentParser(description="Unpack and combine protein data")
    parser.add_argument("--input_dir", type=str, required=True, help="Path to input directory")
    parser.add_argument("--bucket_name", type=str, required=True, help="Name of the S3 bucket")
    parser.add_argument("--output_file_name", type=str, required=True, help="Path to output combined CSV file")
    args = parser.parse_args()

    unpack_data(args.input_dir,args.bucket_name, args.output_file_name)'''

    load_dataset_Salesforce()