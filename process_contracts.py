# File: process_contracts.py
# Save this in: Documents/contract-pipeline/process_contracts.py

import os
import json
from datetime import datetime
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from openai import AzureOpenAI
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential

# Load environment variables
load_dotenv()

# ==================================================
# CONFIGURATION
# ==================================================
STORAGE_CONNECTION = os.getenv('STORAGE_CONNECTION_STRING')
OPENAI_KEY = os.getenv('AZURE_OPENAI_KEY')
OPENAI_ENDPOINT = os.getenv('AZURE_OPENAI_ENDPOINT')
SEARCH_ENDPOINT = os.getenv('SEARCH_ENDPOINT')
SEARCH_KEY = os.getenv('SEARCH_KEY')

# Container names
RAW_CONTAINER = "contracts-raw"
PROCESSED_CONTAINER = "contracts-processed"

# Processing settings
CHUNK_SIZE = 500  # Characters per chunk
CHUNK_OVERLAP = 100  # Overlapping characters

# ==================================================
# STAGE 1: Download Contracts from Blob Storage
# ==================================================
def download_contracts():
    """
    Download all JSON files from contracts-raw container
    Returns list of contract documents
    """
    print("\n" + "="*60)
    print("📥 STAGE 1: Downloading Contracts from Blob Storage")
    print("="*60)

    try:
        # Connect to storage
        blob_service = BlobServiceClient.from_connection_string(STORAGE_CONNECTION)
        container_client = blob_service.get_container_client(RAW_CONTAINER)

        contracts = []
        blob_list = container_client.list_blobs()

        for blob in blob_list:
            if blob.name.endswith('.json'):
                print(f"   📄 Downloading: {blob.name}")

                # Download blob content
                blob_client = container_client.get_blob_client(blob.name)
                content = blob_client.download_blob().readall()

                # Parse JSON
                try:
                    doc = json.loads(content)
                    contracts.append({
                        'name': blob.name,
                        'content': doc,
                        'blob': blob
                    })
                except json.JSONDecodeError as e:
                    print(f"   ⚠️ Error parsing {blob.name}: {e}")
                    continue

        print(f"\n✅ Downloaded {len(contracts)} contracts")
        return contracts

    except Exception as e:
        print(f"❌ Error downloading contracts: {e}")
        raise

# ==================================================
# STAGE 2: Chunk Documents
# ==================================================
def chunk_document(contract):
    """
    Break document into smaller chunks with overlap
    Returns list of text chunks
    """
    print(f"\n✂️ STAGE 2: Chunking {contract['name']}")

    doc = contract['content']

    # Extract text based on Document Intelligence format
    full_text = ""

    if 'content' in doc:
        # Standard format
        full_text = doc['content']
    elif 'analyzeResult' in doc and 'content' in doc['analyzeResult']:
        # Document Intelligence v3.0+ format
        full_text = doc['analyzeResult']['content']
    elif 'pages' in doc:
        # Paginated format
        full_text = ' '.join([
            page.get('content', '')
            for page in doc['pages']
        ])
    else:
        # Fallback
        full_text = json.dumps(doc)
        print("   ⚠️ Unknown format, using entire JSON")

    # Create chunks
    chunks = []
    position = 0

    while position < len(full_text):
        chunk_end = min(position + CHUNK_SIZE, len(full_text))
        chunk_text = full_text[position:chunk_end].strip()

        if chunk_text:
            chunks.append({
                'id': f"{contract['name'].replace('.json', '')}_chunk_{len(chunks)}",
                'text': chunk_text,
                'chunk_index': len(chunks),
                'contract_name': contract['name'],
                'char_start': position,
                'char_end': chunk_end
            })

        position += (CHUNK_SIZE - CHUNK_OVERLAP)

    print(f"   ✅ Created {len(chunks)} chunks ({len(full_text)} total characters)")
    return chunks

# ==================================================
# STAGE 3: Generate Embeddings
# ==================================================
def embed_chunks(chunks):
    """
    Generate vector embeddings using Azure OpenAI
    Returns list of embedded documents
    """
    print(f"\n🧠 STAGE 3: Generating Embeddings for {len(chunks)} chunks")

    try:
        # Initialize OpenAI client
        client = AzureOpenAI(
            api_key=OPENAI_KEY,
            api_version="2024-02-01",
            azure_endpoint=OPENAI_ENDPOINT
        )

        embedded_docs = []
        failed = 0

        for i, chunk in enumerate(chunks):
            try:
                # Generate embedding
                response = client.embeddings.create(
                    input=chunk['text'],
                    model="text-embedding-ada-002"
                )

                # Create document for search index
                embedded_docs.append({
                    'id': chunk['id'],
                    'content': chunk['text'],
                    'contract_name': chunk['contract_name'],
                    'chunk_index': chunk['chunk_index'],
                    'contentVector': response.data[0].embedding
                })

                # Progress indicator
                if (i + 1) % 10 == 0 or (i + 1) == len(chunks):
                    print(f"   Progress: {i + 1}/{len(chunks)} chunks processed")

            except Exception as e:
                failed += 1
                print(f"   ⚠️ Failed to embed chunk {chunk['id']}: {e}")
                continue

        print(f"   ✅ Generated {len(embedded_docs)} embeddings")
        if failed > 0:
            print(f"   ⚠️ Failed: {failed} chunks")

        return embedded_docs

    except Exception as e:
        print(f"❌ Error generating embeddings: {e}")
        raise

# ==================================================
# STAGE 4: Index Documents in Azure Search
# ==================================================
def index_documents(documents):
    """
    Upload documents to Azure Cognitive Search
    Returns number of successfully indexed documents
    """
    print(f"\n📊 STAGE 4: Indexing {len(documents)} documents in Azure Search")

    try:
        # Initialize search client
        search_client = SearchClient(
            endpoint=SEARCH_ENDPOINT,
            index_name="contracts-vector-index",
            credential=AzureKeyCredential(SEARCH_KEY)
        )

        # Upload documents
        result = search_client.upload_documents(documents=documents)

        # Count successes
        succeeded = sum([1 for r in result if r.succeeded])
        failed = len(result) - succeeded

        print(f"   ✅ Indexed {succeeded} documents")
        if failed > 0:
            print(f"   ⚠️ Failed: {failed} documents")
            # Show first error
            for r in result:
                if not r.succeeded:
                    print(f"      Error: {r.error_message}")
                    break

        return succeeded

    except Exception as e:
        print(f"❌ Error indexing documents: {e}")
        raise

# ==================================================
# STAGE 5: Mark as Processed (Move to processed container)
# ==================================================
def mark_as_processed(contract):
    """
    Move processed file to contracts-processed container
    """
    try:
        blob_service = BlobServiceClient.from_connection_string(STORAGE_CONNECTION)

        # Source
        source_container = blob_service.get_container_client(RAW_CONTAINER)
        source_blob = source_container.get_blob_client(contract['name'])

        # Destination
        dest_container = blob_service.get_container_client(PROCESSED_CONTAINER)
        dest_blob = dest_container.get_blob_client(contract['name'])

        # Copy
        dest_blob.start_copy_from_url(source_blob.url)

        # Delete original
        source_blob.delete_blob()

        print(f"   ✅ Moved to processed container")

    except Exception as e:
        print(f"   ⚠️ Could not move file: {e}")

# ==================================================
# MAIN PIPELINE
# ==================================================
def main():
    """
    Main processing pipeline
    """
    print("\n" + "="*60)
    print("🚀 CONTRACT PROCESSING PIPELINE")
    print("="*60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Download contracts
        contracts = download_contracts()

        if not contracts:
            print("\n⚠️ No contracts found in contracts-raw container")
            print("   Upload JSON files to the container and run again")
            return

        # Process each contract
        total_documents = 0

        for i, contract in enumerate(contracts, 1):
            print(f"\n{'='*60}")
            print(f"📄 Processing Contract {i}/{len(contracts)}: {contract['name']}")
            print(f"{'='*60}")

            # Chunk
            chunks = chunk_document(contract)

            # Embed
            embedded_docs = embed_chunks(chunks)

            # Index
            if embedded_docs:
                indexed = index_documents(embedded_docs)
                total_documents += indexed

            # Mark as processed
            mark_as_processed(contract)

        # Summary
        print("\n" + "="*60)
        print("✅ PIPELINE COMPLETE!")
        print("="*60)
        print(f"Processed: {len(contracts)} contracts")
        print(f"Indexed: {total_documents} total documents")
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60 + "\n")

    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        raise

# ==================================================
# RUN SCRIPT
# ==================================================
if __name__ == "__main__":
    main()