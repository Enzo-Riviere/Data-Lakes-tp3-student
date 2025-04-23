import os
import pandas as pad
import boto3

def unpack_data(input_dir,bucket_name, output_file):
    """
    Unpacks and combines multiple CSV files from a directory into a single CSV file.

    Parameters:
    input_dir (str): Path to the directory containing the CSV files.
    output_file (str): Path to the output combined CSV file.
    """

    #S3 
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')

    # Step 1: Initialize an empty list to store DataFrames
    
    df_list = []    

    # Step 2: Loop over files in the input directory

    for file in os.listdir(input_dir):
        
        filename = os.path.join(input_dir,file)
        print(filename)
        
        # Step 3: Check if the file is a CSV or matches a naming pattern
        if filename.endswith(".csv") or "data" in filename: 
            
            # Step 4: Read the CSV file using pandas
            df = pad.read_csv(
                filename,
                names=['sequence', 'family_accession', 'sequence_name', 'aligned_sequence', 'family_id'])
            
            # Step 5: Append the DataFrame to the list
            df_list.append(df)
        
    # Step 6: Concatenate all DataFrames
    df_concat = pad.concat(df_list, ignore_index=True, verify_integrity=True, sort=False)
    print("Concat done")
    # Step 7: Save the combined DataFrame to output_file
    df_concat.to_csv(f'tmp/{output_file}', index=False)
    print("To_csv done")
    #S3
    s3.upload_file(f"tmp/{output_file}", bucket_name, output_file)
    print("Upload csv in s3 done")
    pass


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Unpack and combine protein data")
    parser.add_argument("--input_dir", type=str, required=True, help="Path to input directory")
    parser.add_argument("--bucket_name", type=str, required=True, help="Name of the S3 bucket")
    parser.add_argument("--output_file_name", type=str, required=True, help="Path to output combined CSV file")
    args = parser.parse_args()

    unpack_data(args.input_dir,args.bucket_name, args.output_file_name)
