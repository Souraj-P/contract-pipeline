# File: search_contracts.py
# Save this in: Documents/contract-pipeline/search_contracts.py

import os
import json
from dotenv import load_dotenv
from openai import AzureOpenAI
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential

# Load environment variables
load_dotenv()

OPENAI_KEY = os.getenv('AZURE_OPENAI_KEY')
OPENAI_ENDPOINT = os.getenv('AZURE_OPENAI_ENDPOINT')
SEARCH_ENDPOINT = os.getenv('SEARCH_ENDPOINT')
SEARCH_KEY = os.getenv('SEARCH_KEY')


def search_contracts(query_text, top_k=5):
    """
    Search contracts using natural language query
    """

    print(f"\n🔍 Searching for: '{query_text}'")
    print("="*60)

    try:
        # Generate embedding for query
        client = AzureOpenAI(
            api_key=OPENAI_KEY,
            api_version="2024-02-01",
            azure_endpoint=OPENAI_ENDPOINT
        )

        print("   Generating query embedding...")
        query_embedding = client.embeddings.create(
            input=query_text,
            model="text-embedding-ada-002"
        ).data[0].embedding

        # Search
        search_client = SearchClient(
            endpoint=SEARCH_ENDPOINT,
            index_name="contracts-vector-index",
            credential=AzureKeyCredential(SEARCH_KEY)
        )

        print("   Searching vector database...")
        vector_query = {
            "kind": "vector",
            "vector": query_embedding,
            "fields": "contentVector",
            "k": top_k
        }

        results = search_client.search(
            search_text="",
            vector_queries=[vector_query],
            select=["id", "content", "contract_name", "chunk_index"]
        )

        # Display results
        print(f"\n✅ Found {top_k} results:\n")

        for i, result in enumerate(results, 1):
            score = result.get('@search.score', 0)
            contract = result.get('contract_name', 'unknown')
            content = result.get('content', '')
            chunk_idx = result.get('chunk_index', 0)

            print(f"Result {i}:")
            print(f"   📄 Contract: {contract}")
            print(f"   📍 Chunk: {chunk_idx}")
            print(f"   ⭐ Score: {score:.4f}")
            print(f"   📝 Content: {content[:200]}...")
            print()

    except Exception as e:
        print(f"❌ Search failed: {e}")
        raise


def main():
    """
    Interactive search interface
    """

    print("\n" + "="*60)
    print("🔎 CONTRACT SEARCH")
    print("="*60)
    print("Type your question or 'quit' to exit\n")

    while True:
        query = input("Search query: ").strip()

        if query.lower() in ['quit', 'exit', 'q']:
            print("Goodbye!")
            break

        if not query:
            print("Please enter a search query\n")
            continue

        search_contracts(query)
        print()


if __name__ == "__main__":
    main()