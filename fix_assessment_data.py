#!/usr/bin/env python3
"""Fix assessment data to match Pydantic enums"""

from app.services.snowflake import db

print("🔧 Fixing assessment data...")

# Fix 1: Lowercase assessment_type
print("\n1. Converting assessment_type to lowercase...")
db.execute_update("""
    UPDATE assessments
    SET assessment_type = LOWER(assessment_type)
""")
print("   ✅ Done")

# Fix 2: Change 'complete' to 'approved'
print("\n2. Fixing 'complete' status...")
db.execute_update("""
    UPDATE assessments
    SET status = 'approved'
    WHERE status = 'complete' OR status = 'COMPLETE'
""")
print("   ✅ Done")

# Fix 3: Lowercase all status values
print("\n3. Converting status to lowercase...")
db.execute_update("""
    UPDATE assessments
    SET status = LOWER(status)
""")
print("   ✅ Done")

# Verify
print("\n🔍 Verifying...")
result = db.execute_query("""
    SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN assessment_type = LOWER(assessment_type) THEN 1 END) as lowercase_types,
        COUNT(CASE WHEN status IN ('draft', 'in_progress', 'submitted', 'approved', 'superseded') THEN 1 END) as valid_status
    FROM assessments
""")

if result:
    r = result[0]
    print(f"   Total assessments: {r.get('TOTAL')}")
    print(f"   Lowercase types: {r.get('LOWERCASE_TYPES')}")
    print(f"   Valid status: {r.get('VALID_STATUS')}")

print("\n✅ All fixed!")
db.close()
