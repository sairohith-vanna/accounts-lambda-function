from operator import ge
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy import Column, Integer, String, BigInteger
import boto3
import csv
import os

s3 = boto3.client('s3')
Base = declarative_base()

class AccountRelation(Base):
    __tablename__ = "account"

    id = Column(Integer, primary_key=True)
    relationship_name = Column(String)
    customer_id = Column(BigInteger)
    parent_account = Column(BigInteger)
    branch = Column(String)

def get_db_engine():
    database_url = os.environ['DB_URL']
    db_engine = create_engine(database_url)
    return db_engine

# Uncomment and run below code only for the first time.
# Initializes the database with the table. Running the
# second time would be an attempt to re-generate the
# table which would fail if it does already exist
#
# db_engine = get_db_engine()
# Base.metadata.create_all(db_engine)

def read_csv_from_bucket(bucket, object_key):
    print('Fetching the data from S3')
    response = s3.get_object(Bucket=bucket, Key=object_key)
    csv_data: str = response['Body'].read().decode("UTF-8")
    print(f'csv data from S3: \n{csv_data}')
    return csv_data

def persist_relations(csv_data):
    csv_reader = csv.reader(csv_data.splitlines()[1:])
    account_relations = []
    with Session(get_db_engine()) as session:
        for row in csv_reader:
            new_relation = AccountRelation(
                relationship_name=row[0],
                customer_id=int(row[1]),
                parent_account=int(row[2]),
                branch=row[3]
            )
            account_relations.append(new_relation)
        session.add_all(account_relations)
        session.commit()
    

def lambda_handler(event, context):
    s3_file_event: dict = event
    event_records = s3_file_event.get('Records')
    s3_event =  event_records[0].get('s3', None)
    if s3_event:
        src_bucket = s3_event['bucket']['name']
        account_src_key = s3_event['object']['key']
        print(f'Bucket: {src_bucket}, Object: {account_src_key}')
        csv_data_raw = read_csv_from_bucket(src_bucket, account_src_key)
        persist_relations(csv_data=csv_data_raw)
