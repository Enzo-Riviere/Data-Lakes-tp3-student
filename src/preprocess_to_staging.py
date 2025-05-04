import io
import pandas as pad
import boto3
import mysql.connector

def preprocess_to_staging(bucket_raw, db_host, input_file,db_user, db_pwd,db_name):
    
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')


    response = s3.get_object(Bucket=bucket_raw, Key=input_file)  #get it from the raw bucket
    data = pad.read_csv(io.BytesIO(response['Body'].read()))
    print("Download data done")

    
    data = data.dropna()
    print("Data dropNa done")

    data = data.drop(data[data["title"] == " "].index)

    data = data.drop_duplicates()
    print("Data drop duplicates")

    data = data.astype(str)
    print("Data as type")

    connection = mysql.connector.connect(
        host=db_host,
        port=3306,                    
        user=db_user,
        password=db_pwd,
        database=db_name
    )

    connection._sql_mode = ""

    cursor = connection.cursor()
    cursor.execute("SELECT DATABASE()")
    current_db = cursor.fetchone()
    print("Current database:", current_db)

    create_table_query = """
    DROP TABLE IF EXISTS texts;
    CREATE TABLE texts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        subpart VARCHAR(255) NOT NULL,
        content TEXT NOT NULL,
        resume TEXT NOT NULL
    );
    """

    try:
        cursor.execute(create_table_query)
        connection.commit()  # Assure-toi de commettre les modifications

    except mysql.connector.Error as err:
        print("Error: ", err)

    connection.close()

    connection = mysql.connector.connect(
        host=db_host,
        port=3306,                    
        user=db_user,
        password=db_pwd,
        database=db_name
    )
    cursor = connection.cursor()

    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    if tables:
        print("Tables in the database:", tables)
    else:
        print("No tables found in the database.")


    insert_query = """INSERT INTO texts(title,subpart,content,resume) VALUES (%s, %s, %s, %s)"""

    for index, row in data.iterrows():
        values = (row['title'], row['subpart'], row['content'], row['resume'])
        cursor.execute(insert_query, values)
    connection.commit()
    connection.close()

    connection = mysql.connector.connect(
        host=db_host,
        port=3306,                    
        user=db_user,
        password=db_pwd,
        database=db_name
    )
    cursor = connection.cursor()

    print(cursor.rowcount, "record inserted.")
    print ("Insertion Done")
    
    cursor.execute("SELECT COUNT(*) FROM texts;")
    count = cursor.fetchone()
    print (f" Number of valid rows : {count}")

    cursor.close()
    connection.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Preprocess data from raw to staging bucket")
    parser.add_argument("--bucket_raw", type=str, required=True, help="Name of the raw S3 bucket")
    parser.add_argument("--db_host", type=str, required=True, help="Database hosting")
    parser.add_argument("--db_user", type=str, required=True, help="Database user")
    parser.add_argument("--db_password", type=str, required=True, help="Database password")
    parser.add_argument("--db_name", type=str, required=True, help="Database name")
    parser.add_argument("--input_file", type=str, required=True, help="Name of the input file in raw bucket")
    args = parser.parse_args()

    preprocess_to_staging(args.bucket_raw, args.db_host, args.input_file, args.db_user,args.db_password,args.db_name)
