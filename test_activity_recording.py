"""
Quick test to verify activity recording works without WriteError.

This test verifies that the fix for the MongoDB WriteError conflict is working.
"""

import asyncio
import time
from database.DatabaseManager import DatabaseManager
from ecom_system.activity_system.activity_system import ActivitySystem


async def test_activity_recording():
    """Test that activity recording doesn't cause WriteError."""
    print("=" * 70)
    print("Testing Activity Recording (WriteError Fix)")
    print("=" * 70)
    print()

    try:
        # Initialize database
        print("1. Connecting to database...")
        db_manager = DatabaseManager()
        await db_manager.initialize()
        print("   ✓ Connected")
        print()

        # Initialize activity system
        print("2. Initializing Activity System...")
        activity_system = ActivitySystem(db_manager, database_name="Activity")
        await activity_system.initialize()
        print("   ✓ Initialized")
        print()

        # Test recording activity for a NEW user (this was causing the error)
        test_user_id = f"test_user_{int(time.time())}"
        test_guild_id = "1265120128295632926"

        print(f"3. Recording message activity for NEW user: {test_user_id}")
        activity_data = {
            "channel_id": "1265122713639583824",
            "channel_name": "test-channel",
            "message_length": 42,
            "emoji_count": 2,
            "link_count": 0,
            "has_attachments": False,
            "is_thread": False,
            "has_embeds": False
        }

        await activity_system.record_activity(
            user_id=test_user_id,
            guild_id=test_guild_id,
            activity_type="message",
            activity_data=activity_data
        )
        print("   ✓ First activity recorded successfully!")
        print()

        # Record another activity to test incrementing existing arrays
        print("4. Recording second message activity...")
        await activity_system.record_activity(
            user_id=test_user_id,
            guild_id=test_guild_id,
            activity_type="message",
            activity_data=activity_data
        )
        print("   ✓ Second activity recorded successfully!")
        print()

        # Verify the data structure
        print("5. Verifying data structure...")
        user_data = await activity_system.get_user_activity_summary(test_user_id, test_guild_id)

        if user_data:
            patterns = user_data.get("activity_patterns", {})
            hourly = patterns.get("hourly_pattern", None)
            weekly = patterns.get("weekly_pattern", None)

            print(f"   Hourly pattern type: {type(hourly).__name__}")
            print(f"   Hourly pattern length: {len(hourly) if isinstance(hourly, list) else 'N/A'}")
            print(f"   Weekly pattern type: {type(weekly).__name__}")
            print(f"   Weekly pattern length: {len(weekly) if isinstance(weekly, list) else 'N/A'}")

            if isinstance(hourly, list) and len(hourly) == 24:
                print("   ✓ Hourly pattern is correct array format!")
            else:
                print("   ✗ Hourly pattern is WRONG format!")

            if isinstance(weekly, list) and len(weekly) == 7:
                print("   ✓ Weekly pattern is correct array format!")
            else:
                print("   ✗ Weekly pattern is WRONG format!")

            print(f"\n   Total activities: {user_data.get('activity_summary', {}).get('total_activities', 0)}")
        else:
            print("   ✗ Could not retrieve user data!")

        print()
        print("=" * 70)
        print("✓ TEST PASSED - No WriteError occurred!")
        print("=" * 70)
        return True

    except Exception as e:
        print()
        print("=" * 70)
        print(f"✗ TEST FAILED: {e}")
        print("=" * 70)
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(test_activity_recording())
    exit(0 if success else 1)
