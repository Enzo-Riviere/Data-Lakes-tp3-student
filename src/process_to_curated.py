import io
import pandas as pad
import boto3
from transformers import AutoTokenizer


def tokenize_sequences(bucket_staging, bucket_curated, input_file, output_file):
    
    model_name="facebook/esm2_t6_8M_UR50D"

    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')

    response = s3.get_object(Bucket=bucket_staging, Key=input_file)
    data = pad.read_csv(io.BytesIO(response['Body'].read()))
    print("Step 1 done")

    tokenizer = AutoTokenizer.from_pretrained(model_name,do_lower_case=False) #do_lower_case, don't put the sequence in lower case because ACGT isn't the same that acgt

    tokenize_data = []
    for sequence in data["sequence"]:
        tokenize_sequence = tokenizer(sequence, truncation = True,padding="max_length",max_length=1024, return_tensors="np") #np for numpy, padding to complete the length token to max_length, Truncation to stop a sequence at max_length length
        tokenize_data.append(tokenize_sequence["input_ids"][0])  

    tokenized_df = pad.DataFrame(tokenize_data)
    tokenized_df.columns = [f"token_{i}" for i in range(tokenized_df.shape[1])]
    print("Step 2 done")

    metadata = data.drop(columns=["sequence"])  # Drop the original sequence column
    processed_data = pad.concat([metadata, tokenized_df], axis=1)

    processed_data.to_csv(f"tmp/{output_file}", index=False)
    s3.upload_file(f"tmp/{output_file}", bucket_curated, f"{output_file}")
    print("Step 3 done")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process data from staging to curated bucket")
    parser.add_argument("--bucket_staging", type=str, required=True, help="Name of the staging S3 bucket")
    parser.add_argument("--bucket_curated", type=str, required=True, help="Name of the curated S3 bucket")
    parser.add_argument("--input_file", type=str, required=True, help="Name of the input file in the staging bucket")
    parser.add_argument("--output_file", type=str, required=True, help="Name of the output file in the curated bucket")
    parser.add_argument("--model_name", type=str, default="facebook/esm2_t6_8M_UR50D", help="Tokenizer model name")
    args = parser.parse_args()

    tokenize_sequences(args.bucket_staging, args.bucket_curated, args.input_file, args.output_file)
