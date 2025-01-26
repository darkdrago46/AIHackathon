import boto3
import os
import pinecone
from sentence_transformers import SentenceTransformer
import PyPDF2
from dotenv import load_dotenv

load_dotenv()

# AWS S3 Configuration
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = "ap-south-1"
BUCKET_NAME = "myaihackathon"\

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")


pinecone.init(
    api_key="YOUR_API_KEY",
)

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)

# Step 2: Create or Connect to an Index
index_name = "document-search"
if index_name not in pinecone.list_indexes():
    pinecone.create_index(
        name=index_name,
        dimension=384,  # Example dimension for 'all-MiniLM-L6-v2'
        metric="cosine"
    )
index = pinecone.Index(index_name)

# Initialize embedding model
model = SentenceTransformer('all-MiniLM-L6-v2')  # Replace with a larger model if needed

# Function to fetch and process documents from S3
def process_documents_from_s3():
    # List documents in the bucket
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
    documents = response.get('Contents', [])

    for doc in documents:
        document_id = doc['Key']
        print(f"Processing document: {document_id}")

        # Download the file
        file_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=document_id)
        text = extract_text_from_pdf(file_obj['Body'])

        # Generate embeddings
        embedding = model.encode(text)

        # Store embeddings and metadata in a vector database (e.g., Pinecone)
        store_in_vector_database(document_id, embedding, text)

def extract_text_from_pdf(file_obj):
    pdf_reader = PyPDF2.PdfReader(file_obj)
    text = ""
    for page in pdf_reader.pages:
        text += page.extract_text()
    return text


def store_in_vector_database(document_id, embedding, text):
    """
    Store document embedding and metadata in Pinecone.

    :param document_id: Unique identifier for the document (e.g., S3 key).
    :param embedding: The embedding vector for the document content.
    :param text: The text content of the document.
    """
    try:
        # Upsert the document embedding with metadata into Pinecone
        index.upsert([
            {
                "id": document_id,
                "values": embedding.tolist(),
                "metadata": {
                    "content": text,
                }
            }
        ])
        print(f"Document {document_id} successfully stored in Pinecone.")
    except Exception as e:
        print(f"Error storing document in Pinecone: {str(e)}")
