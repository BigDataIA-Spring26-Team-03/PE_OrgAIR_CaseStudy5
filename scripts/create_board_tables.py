# scripts/create_board_tables.py

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.snowflake import db


def create_board_tables():
    """Create board governance tables in Snowflake."""

    print("=" * 70)
    print("Creating Board Governance Tables in Snowflake")
    print("=" * 70)

    schema_path = (
        Path(__file__).parent.parent
        / "app"
        / "database"
        / "schema_board_composition.sql"
    )

    if not schema_path.exists():
        print(f"Schema file not found: {schema_path}")
        return

    with open(schema_path, "r") as f:
        schema_sql = f.read()

    # Split and execute
    statements = [s.strip() for s in schema_sql.split(";") if s.strip()]

    try:
        db.connect()

        for i, statement in enumerate(statements, 1):
            if statement:
                print(f"\n[{i}/{len(statements)}] Executing...")
                preview = statement[:50].replace("\n", " ")
                print(f"   {preview}...")

                db.execute_update(statement)
                print("   Success")

        print("\n" + "=" * 70)
        print("All board governance tables created successfully!")
        print("=" * 70)

        # Verify tables exist
        print("\nVerifying tables...")
        result = db.execute_query(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = CURRENT_SCHEMA()
            AND table_name IN ('BOARD_GOVERNANCE_SIGNALS', 'BOARD_MEMBERS')
        """
        )

        for row in result:
            print(f"   {row['TABLE_NAME']}")

    except Exception as e:
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()
    finally:
        db.close()


if __name__ == "__main__":
    create_board_tables()
