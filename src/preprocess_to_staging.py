import io
import pandas as pad
import boto3
import numpy as np
import tqdm
import joblib
from collections import OrderedDict
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
import os



#We can't use Numba because it's not compatible with the DataFrame, skit-learn etc.. code
def split_data_func(data,label_encoder):
    """
    Exercice : Fonction pour prétraiter les données brutes et les préparer pour l'entraînement de modèles.

    Objectifs :
    1. Charger les données brutes à partir d’un fichier CSV.
    2. Nettoyer les données (par ex. : supprimer les valeurs manquantes).
    3. Encoder les labels catégoriels (colonne `family_accession`) en entiers.
    4. Diviser les données en ensembles d’entraînement, de validation et de test selon une logique définie.
    5. Sauvegarder les ensembles prétraités et des métadonnées utiles.

    Indices :
    - Utilisez `LabelEncoder` pour encoder les catégories.
    - Utilisez `train_test_split` pour diviser les indices des données.
    - Utilisez `to_csv` pour sauvegarder les fichiers prétraités.
    - Calculez les poids de classes en utilisant les comptes des classes.
    """
    
    # Step 4: Distribute data pas applicable trop de class différentes
    #For each unique class:
    # - If count == 1: go to test set
    # - If count == 2: 1 to dev, 1 to test
    # - If count == 3: 1 to train, 1 to dev, 1 to test
    # - Else: stratified split (train/dev/test)
    
    train_indices = []
    dev_indices = []
    test_indices = []
    
    label_to_indices = data.groupby("family_accession_encoded").groups
    
    # Logic or assigning indices to train/dev/test
    for index, cls in enumerate(tqdm.tqdm(label_encoder.classes_)): 
        
        indices = list(label_to_indices[index]) 
        
        if len(indices) == 1 : 
            test_indices.append(indices[0])
        
        elif len(indices)==2 :
            test_indices.append(indices[0])
            dev_indices.append(indices[1])
        
        elif len(indices)==3:
            test_indices.append(indices[0])
            dev_indices.append(indices[1])
            train_indices.append(indices[2])
        
        else:
            temp_train, temp_remain = train_test_split(indices, test_size=2/3, random_state=42)
            temp_dev, temp_test = train_test_split(temp_remain, test_size=0.5, random_state=42)
            train_indices.extend(temp_train)
            dev_indices.extend(temp_dev)
            test_indices.extend(temp_test)
    print("Step 4 done")

    # Step 5: Convert index lists to numpy arrays
    train_array = np.array(train_indices)
    dev_array = np.array(dev_indices)
    test_array = np.array(test_indices)
    print("Step 5 done")

    return train_array,dev_array,test_array


def preprocess_to_staging(bucket_raw, bucket_staging, input_file, output_prefix):
    
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')

    # Step 1: Download raw data
    response = s3.get_object(Bucket=bucket_raw, Key=input_file)  #get it from the raw bucket
    data = pad.read_csv(io.BytesIO(response['Body'].read()))
    print("Step 1 done")

    # Step 2: Handle missing values
    data = data.dropna()
    print("Step 2 done")

    # Step 3: Encode the 'family_accession' to numeric labels
    label_encoder = LabelEncoder()
    label_encoder.fit(data["family_accession"])
    data["family_accession_encoded"] = label_encoder.transform(data["family_accession"])

    # Save the label encoder[]
    joblib.dump(label_encoder,'label_encoder.pkl')
    
    # Save the label mapping to a text file
    with open("label_mapping.txt", "w", encoding="utf-8") as f:
        for i, label in enumerate(label_encoder.classes_):
            f.write(f"{i}: {label}\n")
    print("Step 3 done")

    train_array, dev_array, test_array = split_data_func(data, label_encoder)
    
    # Step 6: Create DataFrames from the selected indices
    df_train = data.iloc[train_array]
    df_dev = data.iloc[dev_array]
    df_test = data.iloc[test_array]
    print("Step 6 done")

    # Step 7: Drop unused columns: family_id, sequence_name, etc.
    df_train = df_train.drop(['family_id','sequence_name', "family_accession"], axis=1)
    df_dev = df_dev.drop(['family_id','sequence_name', "family_accession"], axis=1)
    df_test = df_test.drop(['family_id','sequence_name', "family_accession"], axis=1)
    print("Step 7 done")

    # Step 8: Save train/dev/test datasets as CSV
    df_train.to_csv(f"tmp/{output_prefix}_train.csv", index=False)
    df_dev.to_csv(f"tmp/{output_prefix}_dev.csv", index=False)
    df_test.to_csv(f"tmp/{output_prefix}_test.csv", index=False)
    

    s3.upload_file(f"tmp/{output_prefix}_train.csv", bucket_staging, f"{output_prefix}_train.csv")
    s3.upload_file(f"tmp/{output_prefix}_dev.csv", bucket_staging, f"{output_prefix}_dev.csv")
    s3.upload_file(f"tmp/{output_prefix}_test.csv", bucket_staging, f"{output_prefix}_test.csv")
    print("Step 8 done")

   # Step 9: Calculate class weights from the training set
    class_counts = df_train['family_accession_encoded'].value_counts()
    print(class_counts)
    class_weights = 1. / class_counts
    class_weights /= class_weights.sum()
    print("Step 9 done")

     # Step 10: Normalize weights and scale
    min_weight = class_weights.max()
    weight_scaling_factor = 1 / min_weight
    class_weights *= weight_scaling_factor
    print("Step 10 done")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Preprocess data from raw to staging bucket")
    parser.add_argument("--bucket_raw", type=str, required=True, help="Name of the raw S3 bucket")
    parser.add_argument("--bucket_staging", type=str, required=True, help="Name of the staging S3 bucket")
    parser.add_argument("--input_file", type=str, required=True, help="Name of the input file in raw bucket")
    parser.add_argument("--output_prefix", type=str, required=True, help="Prefix for output files in staging bucket")
    args = parser.parse_args()

    preprocess_to_staging(args.bucket_raw, args.bucket_staging, args.input_file, args.output_prefix)
