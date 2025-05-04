# 🛠️ Data Lakes & Data Integration

This repository contains the solution provided by **Enzo Riviere** for **TP3 - Data Lakes**.

---

## 🐳 Prerequisites: Install Docker

To run this project, you'll need Docker. The setup includes:
- a **MySQL** container,
- a **LocalStack** container (for S3 emulation),
- a **MongoDB** container.

### 1. Set up Docker

Create a `docker-compose.yaml` file with the following content:

```yaml
version: '3.8'
services:
  localstack:
    image: localstack/localstack
    container_name: localstack
    ports:
      - "4566:4566"
      - "4572:4572"
    environment:
      - SERVICES=s3
      - DOCKER_HOST=unix:///var/run/docker.sock

  mongodb:
    image: mongo:latest
    container_name: mongodb
    ports:
      - "27017:27017"
```

To start LocalStack and MongoDB:

```bash
docker-compose up -d
```

Then start the MySQL container separately:

```bash
docker run --name mysql \
  -e MYSQL_ROOT_PASSWORD=root \
  -e MYSQL_DATABASE=staging \
  -p 3306:3306 \
  -d mysql:latest
```

---

## 📦 Python Dependencies

Install the required Python packages:

```bash
pip install pandas boto3 localstack-client pymongo transformers
pip install mysql-connector-python pymysql
```

---

## 🪣 Create S3 Buckets (via LocalStack)

Create the S3 buckets using the following commands:

```bash
aws --endpoint-url=http://localhost:4566 s3 mb s3://raw
aws --endpoint-url=http://localhost:4566 s3 mb s3://staging
aws --endpoint-url=http://localhost:4566 s3 mb s3://curated
```

---

## 🚀 Launch the Project

Run the following scripts in order:

```bash
python3 download.py

python3 build/unpack_to_raw.py \
  --input_dir data/raw \
  --bucket_name raw \
  --output_file_name data_combined.csv

python3 src/preprocess_to_staging.py \
  --bucket_raw raw \
  --db_host localhost \
  --db_user root \
  --db_password root \
  --db_name staging \
  --input_file data_combined.csv

python3 src/MongoDB_and_tokenization.py \
  --db_host localhost \
  --db_user root \
  --db_pwd root \
  --db_name staging \
  --model_name distilbert/distilbert-base-uncased
```