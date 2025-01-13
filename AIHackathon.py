import os

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

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)

# Upload File to S3
def upload_to_s3_with_metadata(file, bucket_name, file_name, metadata):
    try:
        s3_client.upload_fileobj(
            file,
            bucket_name,
            file_name,
            ExtraArgs={"Metadata": metadata},
        )
        return f"File '{file_name}' with metadata successfully uploaded to S3 bucket '{bucket_name}'!"
    except NoCredentialsError:
        return "AWS credentials not available. Check your environment configuration."
    except PartialCredentialsError:
        return "Incomplete AWS credentials provided."
    except Exception as e:
        return f"An error occurred: {str(e)}"

# Streamlit UI
st.title("Document Uploader to S3")

# Input widgets
file_title = st.text_input("Enter File Title", placeholder="e.g., Project Report")
file_tags = st.text_input("Enter Tags (comma-separated)", placeholder="e.g., ML, AI, Research")


# File uploader widget
uploaded_file = st.file_uploader(
    "Upload a PDF file to upload to S3",
    type=["pdf"],
)

if uploaded_file and file_title:
    file_name = uploaded_file.name  # Get file name from the uploaded file

    # Prepare metadata
    upload_timestamp = datetime.utcnow().isoformat() + "Z"  # UTC timestamp in ISO 8601 format
    metadata = {
        "title": file_title,
        "tags": file_tags,
        "upload_timestamp": upload_timestamp,
    }

    with st.spinner("Uploading file to S3 with metadata..."):
        # Upload the file to S3
        response_message = upload_to_s3_with_metadata(uploaded_file, BUCKET_NAME, file_name, metadata)
        if "successfully uploaded" in response_message.lower():
            st.success(response_message)
        else:
            st.error(response_message)
else:
    st.info("Please provide a file and a title to proceed.")
