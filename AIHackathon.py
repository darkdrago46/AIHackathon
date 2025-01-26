import os
import random

import streamlit as st
import boto3
from dotenv import load_dotenv
from datetime import datetime
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

load_dotenv()

# AWS S3 Configuration
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = "ap-south-1"
BUCKET_NAME = "myaihackathon"

def generate_document_id():
    # Generate a random 10-digit number and prepend "doc-"
    return f"doc-{random.randint(10**9, 10**10 - 1)}"

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)

dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

table = dynamodb.Table("DocumentMetadata")

# Upload File to S3
def upload_to_s3_with_metadata_and_dynamodb(file, bucket_name, file_title, upload_timestamp, tags):

    file_name = generate_document_id()
    try:
        s3_client.upload_fileobj(
            file,
            bucket_name,
            file_name,
        )
        print(f"File '{file_title}' successfully uploaded to S3 bucket '{bucket_name}'!")
    except NoCredentialsError:
        return "AWS credentials not available. Check your environment configuration."
    except PartialCredentialsError:
        return "Incomplete AWS credentials provided."
    except Exception as e:
        return f"An error occurred: {str(e)}"

    print(tags)
    dynamodb_item = {
        "DocumentID": file_name,  # Partition key
        "Title": file_title,  # Document title
        "UploadTimestamp": upload_timestamp,  # Time of upload
        "Tags": tags,  # Tags (e.g., keywords or categories)
        "BucketName": bucket_name  # Bucket name for reference
    }

    # Add item to DynamoDB
    try:
        table.put_item(Item=dynamodb_item)
        print(f"Metadata added to DynamoDB: {dynamodb_item}")
    except Exception as dynamodb_error:
        print(f"Error adding metadata to DynamoDB: {dynamodb_error}")
        return dynamodb_error

    return f"File '{file_title}' successfully uploaded'{bucket_name}'!"


# Streamlit UI
st.title("Document Uploader to S3")

# Input widgets
file_title = st.text_input("Enter File Title", placeholder="e.g., Project Report").lower()
file_tags = st.text_input("Enter Tags (comma-separated)", placeholder="e.g., ML, AI, Research").lower()


# File uploader widget
uploaded_file = st.file_uploader(
    "Upload a PDF file to upload to S3",
    type=["pdf"],
)

if uploaded_file and file_title:

    # Prepare metadata
    upload_timestamp = datetime.utcnow().isoformat() + "Z"  # UTC timestamp in ISO 8601 format

    with st.spinner("Uploading file"):
        # Upload the file to S3
        response_message = upload_to_s3_with_metadata_and_dynamodb(uploaded_file, BUCKET_NAME, file_title, upload_timestamp, file_tags)
        st.write(response_message)
else:
    st.info("Please provide a file and a title to proceed.")



