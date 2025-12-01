"""
Migration script for Activity System hourly/weekly patterns.

This script migrates activity patterns from object format to array format:
- Old: {"0": 3, "1": 5, "2": 10, ...}
- New: [3, 5, 10, ...]

Usage:
    # Dry run (see what would change):
    python migrate_activity_patterns.py --dry-run

    # Actually migrate:
    python migrate_activity_patterns.py
"""

import asyncio
import argparse
import sys
from database.DatabaseManager import DatabaseManager, get_collection
from ecom_system.activity_system.activity_system import ActivitySystem
from loggers.log_config import setup_logging


async def main(dry_run: bool = True):
    """Run the migration."""
    # Setup logging
    logger = setup_logging("activity_migration")

    print("=" * 70)
    print("Activity Pattern Migration Script")
    print("=" * 70)
    print(f"Mode: {'DRY RUN (no changes will be made)' if dry_run else 'LIVE MIGRATION'}")
    print("=" * 70)
    print()

    try:
        # Initialize database manager
        print("Initializing database connection...")
        db_manager = DatabaseManager()
        await db_manager.initialize()
        print("✓ Database connected")
        print()

        # Create activity system
        print("Initializing Activity System...")
        activity_system = ActivitySystem(db_manager, database_name="Activity")
        await activity_system.initialize()
        print("✓ Activity System initialized")
        print()

        # Run migration
        print("Starting migration...")
        print("-" * 70)
        stats = await activity_system.migrate_patterns_to_arrays(dry_run=dry_run)
        print("-" * 70)
        print()

        # Print results
        print("Migration Results:")
        print(f"  Total documents scanned: {stats.get('total_documents', 0)}")
        print(f"  Documents needing migration: {stats.get('documents_needing_migration', 0)}")
        print(f"  Hourly patterns migrated: {stats.get('hourly_pattern_migrated', 0)}")
        print(f"  Weekly patterns migrated: {stats.get('weekly_pattern_migrated', 0)}")
        print(f"  Errors: {stats.get('errors', 0)}")
        print()

        if "error" in stats:
            print(f"ERROR: {stats['error']}")
            return 1

        if dry_run and stats.get('documents_needing_migration', 0) > 0:
            print("=" * 70)
            print("This was a dry run. To actually migrate the data, run:")
            print("  python migrate_activity_patterns.py")
            print("=" * 70)
        elif not dry_run:
            print("=" * 70)
            print("✓ Migration completed successfully!")
            print("=" * 70)
        else:
            print("=" * 70)
            print("✓ No documents need migration. All patterns are already in array format.")
            print("=" * 70)

        return 0

    except Exception as e:
        print(f"FATAL ERROR: {e}", file=sys.stderr)
        logger.error(f"Migration failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Migrate activity patterns from object to array format"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Run in dry-run mode (no changes will be made)"
    )

    args = parser.parse_args()

    # If no --dry-run flag, default to dry run and require explicit confirmation
    if not args.dry_run:
        print("WARNING: This will modify your database!")
        confirm = input("Are you sure you want to proceed? (yes/no): ")
        if confirm.lower() != "yes":
            print("Migration cancelled.")
            sys.exit(0)
        dry_run = False
    else:
        dry_run = True

    exit_code = asyncio.run(main(dry_run=dry_run))
    sys.exit(exit_code)
