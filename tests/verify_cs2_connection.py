from app.services.snowflake import SnowflakeService

sf = SnowflakeService()

print("Testing CS2 database connection...")

# -------------------------------------------------
# Helper to normalize row keys to uppercase
# -------------------------------------------------
def normalize_row(row):
    return {k.upper(): v for k, v in row.items()}


# -------------------------------------------------
# Test documents_sec
# -------------------------------------------------
docs_count = sf.execute_query("""
    SELECT COUNT(*) AS CNT
    FROM documents_sec
""")

print("RAW RESULT:", docs_count)
print("FIRST ROW:", docs_count[0])
print("TYPE:", type(docs_count[0]))


# -------------------------------------------------
# Test document_chunks_sec
# -------------------------------------------------
chunks_count = sf.execute_query("""
    SELECT COUNT(*) AS CNT
    FROM document_chunks_sec
""")

chunks_row = normalize_row(chunks_count[0])
print(f"✓ document_chunks_sec: {chunks_row['CNT']:,} chunks")


# -------------------------------------------------
# Test current section distribution
# -------------------------------------------------
sections = sf.execute_query("""
    SELECT SECTION, COUNT(*) AS CNT
    FROM document_chunks_sec
    GROUP BY SECTION
    ORDER BY CNT DESC
""")

print("\nCurrent section distribution:")

for row in sections:
    r = normalize_row(row)
    section_name = r["SECTION"] if r["SECTION"] else "NULL"
    print(f"  {section_name:30s}: {r['CNT']:,}")

print("\n✓ CS2 database is accessible")
print("✓ Ready to start CS3")
