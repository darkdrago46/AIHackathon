import os

import streamlit as st
import boto3
from boto3.dynamodb.conditions import Attr
from dotenv import load_dotenv

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

dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

table = dynamodb.Table("DocumentMetadata")

def search_documents_by_metadata(search_query, search_field):
    """
    Search DynamoDB for metadata and generate pre-signed URLs for matching documents.

    :param search_query: The value to search for (e.g., 'Project').
    :param search_field: The metadata field to search (e.g., 'Title' or 'Tags').
    :return: List of documents with metadata and S3 pre-signed URLs.
    """
    try:
        # Query DynamoDB with a filter expression
        response = table.scan(
            FilterExpression=Attr(search_field).contains(search_query)
        )

        documents = []
        for item in response.get("Items", []):
            document_id = item["DocumentID"]  # Document ID is the S3 file name

            # Generate a pre-signed URL for the S3 document
            presigned_url = generate_presigned_url(BUCKET_NAME, document_id)

            # Collect document metadata and pre-signed URL
            documents.append({
                "title": item.get("Title"),
                "tags": item.get("Tags"),
                "UploadTimestamp": item.get("UploadTimestamp"),
                "document_url": presigned_url,
            })

        return documents
    except Exception as e:
        return f"Error searching for documents: {str(e)}"


def generate_presigned_url(bucket_name, document_id, expiration=3600):
    """
    Generate a pre-signed URL for an S3 object.

    :param bucket_name: The name of the S3 bucket.
    :param document_id: The S3 object key (document ID).
    :param expiration: URL expiration time in seconds (default: 1 hour).
    :return: Pre-signed URL or an error message.
    """
    try:
        return s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": document_id},
            ExpiresIn=expiration,
        )
    except Exception as e:
        return f"Error generating pre-signed URL: {str(e)}"


# Streamlit UI for Searching Documents
st.title("Search Documents by Metadata")

# Input fields for search
search_field = st.selectbox(
    "Search by Metadata Field",
    ["Title", "Tags"],  # Fields to search by
)
search_query = st.text_input(f"Enter {search_field} to Search", "")

# Perform search when the button is clicked
if st.button("Search"):
    with st.spinner("Searching for documents..."):
        results = search_documents_by_metadata(search_query, search_field)

        if isinstance(results, list) and results:
            st.success(f"Found {len(results)} document(s) matching your query.")

            # Display results
            for doc in results:
                st.write(f"**Title**: {doc['title']}")
                st.write(f"**Tags**: {doc['tags']}")
                st.write(f"**Uploaded On**: {doc['upload_timestamp']}")
                st.markdown(f"[View Document]({doc['document_url']})", unsafe_allow_html=True)
                st.write("---")
        elif isinstance(results, list):
            st.warning("No matching documents found.")
        else:
            st.error(results)
