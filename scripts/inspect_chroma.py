"""
Quick verification script to check ChromaDB indexing is correct.
Run inside the airflow-scheduler container:
  docker compose exec airflow-scheduler python /opt/airflow/dags/verify_chroma_index.py
"""

import chromadb

CHROMA_PERSIST_DIR = "/data/chroma"
COMPANIES = ["NVDA", "JPM", "WMT", "GE", "DG"]

def main():
    print(f"\n{'='*60}")
    print(f"ChromaDB Verification Report")
    print(f"{'='*60}")

    client = chromadb.PersistentClient(path=CHROMA_PERSIST_DIR)

    # 1. List all collections
    collections = client.list_collections()
    print(f"\n✅ Collections found: {len(collections)}")
    for col in collections:
        print(f"   - {col.name}")

    if not collections:
        print("❌ No collections found — indexing may have failed!")
        return

    # 2. Check count per company in each collection
    print(f"\n{'='*60}")
    print("Document counts per company per collection:")
    print(f"{'='*60}")

    total_overall = 0
    for col in collections:
        collection = client.get_collection(col.name)
        total = collection.count()
        total_overall += total
        print(f"\nCollection: {col.name}  (total: {total})")

        for ticker in COMPANIES:
            results = collection.get(
                where={"company_id": ticker},
                include=["metadatas"]
            )
            count = len(results["ids"])
            status = "✅" if count > 0 else "❌"
            print(f"   {status} {ticker}: {count} documents")

    print(f"\n{'='*60}")
    print(f"TOTAL documents indexed across all collections: {total_overall}")
    print(f"{'='*60}")

    # 3. Sample query test
    print(f"\n{'='*60}")
    print("Sample query test: 'AI strategy data center'")
    print(f"{'='*60}")

    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer("all-MiniLM-L6-v2")
    query_embedding = model.encode(["AI strategy data center"])[0].tolist()

    for col in collections:
        collection = client.get_collection(col.name)
        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=3,
            include=["metadatas", "distances"]
        )
        print(f"\nTop 3 results from '{col.name}':")
        for i, (meta, dist) in enumerate(zip(
            results["metadatas"][0],
            results["distances"][0]
        )):
            print(f"  [{i+1}] company={meta.get('company_id')} "
                  f"source={meta.get('source_type')} "
                  f"score={1-dist:.3f}")

    print(f"\n✅ Verification complete!")

if __name__ == "__main__":
    main()