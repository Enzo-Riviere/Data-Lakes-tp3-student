import re
import os
import pandas as pad
import boto3

def unpack_data(input_dir,bucket_name, output_file):
    #S3 
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')

    df_list = []    
    for subfolder in ['train', 'test', 'validation']:
        subfolder_path = os.path.join(input_dir, subfolder)

        for file in os.listdir(subfolder_path):
            filename = os.path.join(subfolder_path,file)
            
            if filename.endswith(".csv"): 
                df = pad.read_csv(filename) 
                df_list.append(df)
        
    
    df_concat = pad.concat(df_list, ignore_index=True, verify_integrity=True, sort=False)
    print("Concat done")

    df_concat.to_csv(f'data/{output_file}', index=False)
    print("To_csv done")
    
    s3.upload_file(f"data/{output_file}", bucket_name, output_file)
    print("Upload csv in s3 done")

def organize_data_to_apply_bronze(title,texte):
    
    resume = re.search(r"^(.*?)(?=\s*=\s*=[^=\n]+=\s*=\s*)", texte, re.DOTALL)

    sous_sections = re.findall(r"\n\s*=+\s*(.*?)\s*=+\s*\n(.*?)(?=\n\s*=+\s*.*?\s*=+\s*\n|\Z)" , texte, re.DOTALL)
    
    return title,resume,sous_sections


def extract_text(input_dir):
    
    for subfolder in ['train', 'test', 'validation']:
        df_list_big = []
        subfolder_path = os.path.join(input_dir, subfolder)
        
        for file in os.listdir(subfolder_path):
            
            filename = os.path.join(subfolder_path,file)


            if filename.endswith(".txt"):

                with open(filename, "r", encoding="utf-8") as f:
                    texte = f.read()

                    texte = re.sub(r"<unk>", "", texte)
                    texte = re.sub(r"@-@", "-", texte)
                    texte = re.sub(r'\n+', '\n', texte)

                    pattern =  r"(?:^|\n\s*)=\s*([^=\n]+)\s*=\s*\n(.*?)(?=\n\s*=\s*[^=\n]+\s*=\n|\Z)"

                    matches = re.findall(pattern, texte, flags=re.MULTILINE | re.DOTALL)

                    for idx, (title,text_detected) in enumerate(matches):
                        title,resume,sous_sections=organize_data_to_apply_bronze(title,text_detected)
                        df_list_medium=[]
                        for (sous_section_title,sous_section_content) in sous_sections:
                            sous_section_title = sous_section_title.strip("= ").strip()
                            df = pad.DataFrame([{
                                        "title": title,
                                        "subpart": sous_section_title,
                                        "content": sous_section_content,
                                        "resume": resume.group(1)
                                    }])
                            df_list_medium.append(df)
                        
                        if df_list_medium:
                            df_concat = pad.concat(df_list_medium, ignore_index=True, verify_integrity=True, sort=False)
                            df_list_big.append(df_concat)
                    df_concat = pad.concat(df_list_big, ignore_index=True, verify_integrity=True, sort=False)
                    df_concat.to_csv(f'{subfolder_path}/{subfolder}.csv', index=False)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Unpack and combine protein data")
    parser.add_argument("--input_dir", type=str, required=True, help="Path to input directory")
    parser.add_argument("--bucket_name", type=str, required=True, help="Name of the S3 bucket")
    parser.add_argument("--output_file_name", type=str, required=True, help="Path to output combined CSV file")
    args = parser.parse_args()
    
    extract_text(args.input_dir)
    unpack_data(args.input_dir,args.bucket_name, args.output_file_name)
