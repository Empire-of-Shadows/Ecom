import copy
import logging
from typing import Dict, Any, Optional

from database.DatabaseManager import get_collection, ensure_database_connection, DatabaseConnectionError, db_manager
from ecom_system.helpers.helpers import utc_now_ts, utc_today_key, utc_week_key, utc_month_key
from ecom_system.leveling.sub_system.messages import MessageLevelingSystem
from ecom_system.leveling.sub_system.voice import VoiceLevelingSystem
from ecom_system.leveling.sub_system.reactions import ReactionLevelingSystem
from ecom_system.achievement_system.achievement_system import AchievementSystem

logger = logging.getLogger(__name__)


class LevelingSystem:
    """
    Base leveling system class for message, voice, and reaction interactions.
    Connects to a database and provides foundation for subclassing.
    """

    def __init__(self):
        """Initialize database connections using global DatabaseManager."""
        # Primary user stats collection
        self.users = get_collection("Users", "Stats")
        self.user_achievements = get_collection("Users", "AcheievementProgress")

        # Leveling settings collections
        self.ls_master = get_collection("LevelingSettings", "Master")
        self.ls_message = get_collection("LevelingSettings", "Message")
        self.ls_voice = get_collection("LevelingSettings", "Voice")
        self.ls_reaction = get_collection("LevelingSettings", "Reaction")

        # Activity logging
        self.activity_logs = get_collection("Activity", "Events")

        # Daily stats tracking
        self.daily_stats = {
            "processed_messages": 0,
            "processed_voice": 0,
            "processed_reactions": 0
        }

        self.message_system = MessageLevelingSystem(self)
        self.voice_system = VoiceLevelingSystem(self)
        self.reaction_system = ReactionLevelingSystem(self)
        self.achievement_system = AchievementSystem(self)
        # Bot instance will be set later
        self.bot = None

        logger.info("🚀 LevelingSystem base class initialized")

    def set_bot(self, bot):
        """
        Set the bot instance and initialize level-up messaging.

        Args:
            bot: Discord bot instance
        """
        try:
            self.bot = bot

            # Import here to avoid circular imports
            from ecom_system.helpers.leveled_up import LevelUpMessages

            # Initialize level-up message handler
            level_up_handler = LevelUpMessages(bot)
            self.message_system.level_up_messages = level_up_handler

            logger.info("✅ Level-up messaging system initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize level-up messaging system: {e}", exc_info=True)
            # Set to None to ensure we don't have a partially initialized state
            self.message_system.level_up_messages = None
            raise


    async def initialize(self):
        """Initialize the leveling system and ensure database connections."""
        try:
            # Ensure database connection is ready
            if not await ensure_database_connection():
                raise DatabaseConnectionError("Failed to establish database connection for LevelingSystem")

            # Verify critical collections are accessible
            await self.verify_critical_collections()

            logger.info("✅ LevelingSystem fully initialized and ready")
            return True

        except Exception as e:
            logger.error(f"❌ LevelingSystem initialization failed: {e}", exc_info=True)
            raise

    async def shutdown(self):
        """Gracefully shutdown the leveling system and stop background tasks."""
        try:
            logger.info("✅ LevelingSystem shutdown complete")
        except Exception as e:
            logger.error(f"❌ Error during LevelingSystem shutdown: {e}", exc_info=True)

    async def verify_critical_collections(self):
        """Verify that critical collections are accessible."""
        critical_collections = [
            ("Users", self.users),
            ("LevelingSettings", self.ls_master)
        ]

        for name, collection in critical_collections:
            try:
                # Test collection access with a simple operation
                await collection.find_one({}, projection={"_id": 1})
                logger.debug(f"✅ LevelingSystem collection verified: {name}")
            except Exception as e:
                logger.error(f"❌ LevelingSystem collection access failed for {name}: {e}")
                raise DatabaseConnectionError(f"Collection {name} not accessible") from e

    async def get_user_data(self, user_id: str, guild_id: str) -> Optional[Dict[str, Any]]:
        """Get user data from database."""
        try:
            user_data = await self.users.find_one({"user_id": user_id, "guild_id": guild_id})
            return user_data
        except Exception as e:
            logger.error(f"❌ Error getting user data: {e}")
            return None

    async def get_enhanced_user_data(self, user_id: str, guild_id: str) -> Optional[Dict[str, Any]]:
        """Get user data and ensure it's validated and migrated."""
        user_doc = await self.get_user_data(user_id, guild_id)
        if not user_doc:
            # If user doesn't exist, create a fresh profile.
            # The achievement system might be the first to query a user who hasn't sent a message yet.
            user_doc = await self.create_enhanced_user_profile(user_id, guild_id)
            await self.users.insert_one(user_doc)
            logger.info(f"📝 Created new user profile via get_enhanced_user_data: G:{guild_id} U:{user_id}")
            return user_doc

        validated_doc, was_migrated = await self.validate_and_migrate_user_document(user_id, guild_id, user_doc)
        if was_migrated:
            # If migration was needed, replace the entire document to ensure consistency
            await self.users.replace_one(
                {"user_id": user_id, "guild_id": guild_id},
                validated_doc,
                upsert=True
            )
            logger.info(f"🔄 User document replaced after migration via get_enhanced_user_data: G:{guild_id} U:{user_id}")

        return validated_doc

    async def validate_and_migrate_user_document(self, user_id: str, guild_id: str, user_doc: Dict[str, Any]) -> Dict[
        str, Any]:
        """
        Validate user document structure and migrate if necessary.
        Returns the validated/migrated document and migration status.
        """
        try:
            # Get the expected default structure
            default_profile = await self.create_enhanced_user_profile(user_id, guild_id)

            # Check if document needs migration
            needs_migration = False
            migrated_doc = copy.deepcopy(user_doc)

            # Store legacy data that needs to be preserved
            legacy_data = self._extract_legacy_data(migrated_doc)

            # 1. Check top-level fields
            for field in default_profile.keys():
                if field not in migrated_doc:
                    migrated_doc[field] = copy.deepcopy(default_profile[field])
                    needs_migration = True
                    logger.info(f"🔧 Added missing top-level field: {field} for G:{guild_id} U:{user_id}")

            # 2. Handle specific legacy field migrations
            migrated_doc, field_migrations = self._migrate_legacy_fields(migrated_doc, legacy_data)
            needs_migration = needs_migration or field_migrations

            # 3. Validate nested structures
            nested_structures = [
                "daily_streak",
                "message_stats",
                "voice_stats",
                "social_stats",
                "quality_stats",
                "last_rewarded",
                "achievements",
                "challenges",
                "preferences"
            ]

            for nested_field in nested_structures:
                if nested_field in migrated_doc:
                    # Ensure all sub-fields exist
                    migrated_doc[nested_field] = self._migrate_nested_structure(
                        migrated_doc[nested_field],
                        default_profile[nested_field],
                        f"{nested_field}"
                    )
                else:
                    # Missing entire nested structure
                    migrated_doc[nested_field] = copy.deepcopy(default_profile[nested_field])
                    needs_migration = True
                    logger.info(f"🔧 Added missing nested structure: {nested_field} for G:{guild_id} U:{user_id}")

            # 4. Special handling for timestamp fields to ensure they're valid
            timestamp_fields = ["created_at", "updated_at", "streak_timestamp", "last_voice_activity"]
            for ts_field in timestamp_fields:
                if ts_field in migrated_doc and not isinstance(migrated_doc[ts_field], (int, float)):
                    migrated_doc[ts_field] = utc_now_ts()
                    needs_migration = True
                    logger.info(f"🔧 Fixed invalid timestamp: {ts_field} for G:{guild_id} U:{user_id}")

            # 5. Ensure required identifiers are present and correct
            if migrated_doc.get("user_id") != user_id:
                migrated_doc["user_id"] = user_id
                needs_migration = True

            if migrated_doc.get("guild_id") != guild_id:
                migrated_doc["guild_id"] = guild_id
                needs_migration = True

            # 6. Preserve additional fields that aren't in default structure (like favorites, UUID)
            migrated_doc = self._preserve_extra_fields(migrated_doc, user_doc, default_profile)

            # 7. Update timestamp if migration occurred
            if needs_migration:
                migrated_doc["updated_at"] = utc_now_ts()
                logger.info(f"🔄 Document migrated for G:{guild_id} U:{user_id}")

            return migrated_doc, needs_migration

        except Exception as e:
            logger.error(f"❌ Error validating/migrating user document: {e}")
            # Return a fresh profile if migration fails
            return await self.create_enhanced_user_profile(user_id, guild_id), True

    def _extract_legacy_data(self, user_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Extract legacy data that needs to be migrated to new structure."""
        legacy_data = {}

        # Extract legacy streak data from message_stats
        if "message_stats" in user_doc:
            msg_stats = user_doc["message_stats"]
            if "daily_streak" in msg_stats:
                legacy_data["legacy_daily_streak"] = msg_stats["daily_streak"]
            if "streak_timestamp" in msg_stats:
                legacy_data["legacy_streak_timestamp"] = msg_stats["streak_timestamp"]
            if "longest_streak" in msg_stats:
                legacy_data["legacy_longest_streak"] = msg_stats["longest_streak"]

        # Extract top-level fields that should be in nested structures
        if "longest_streak" in user_doc:
            legacy_data["legacy_top_streak"] = user_doc["longest_streak"]
        if "streak_timestamp" in user_doc:
            legacy_data["legacy_top_streak_ts"] = user_doc["streak_timestamp"]

        return legacy_data

    def _migrate_legacy_fields(self, migrated_doc: Dict[str, Any], legacy_data: Dict[str, Any]) -> tuple:
        """Migrate legacy fields to new structure."""
        needs_migration = False

        # Migrate streak data from message_stats to top level and daily_streak
        if "legacy_daily_streak" in legacy_data:
            # Migrate to daily_streak.count
            if "daily_streak" not in migrated_doc:
                migrated_doc["daily_streak"] = {}

            if "count" not in migrated_doc["daily_streak"]:
                migrated_doc["daily_streak"]["count"] = legacy_data["legacy_daily_streak"]
                needs_migration = True
                logger.info("🔧 Migrated daily_streak from message_stats")

        if "legacy_streak_timestamp" in legacy_data:
            # Migrate to daily_streak.timestamp
            if "daily_streak" not in migrated_doc:
                migrated_doc["daily_streak"] = {}

            if "timestamp" not in migrated_doc["daily_streak"]:
                migrated_doc["daily_streak"]["timestamp"] = legacy_data["legacy_streak_timestamp"]
                needs_migration = True
                logger.info("🔧 Migrated streak_timestamp from message_stats")

        # Migrate longest_streak from message_stats to top level
        if "legacy_longest_streak" in legacy_data and "longest_streak" not in migrated_doc:
            migrated_doc["longest_streak"] = legacy_data["legacy_longest_streak"]
            needs_migration = True
            logger.info("🔧 Migrated longest_streak from message_stats")

        # Clean up legacy fields from message_stats if they exist
        if "message_stats" in migrated_doc:
            msg_stats = migrated_doc["message_stats"]
            legacy_fields = ["daily_streak", "streak_timestamp", "longest_streak"]
            for field in legacy_fields:
                if field in msg_stats:
                    del msg_stats[field]
                    needs_migration = True
                    logger.info(f"🔧 Cleaned legacy field: message_stats.{field}")

        return migrated_doc, needs_migration

    def _migrate_nested_structure(self, current_data: Dict[str, Any], default_structure: Dict[str, Any], path: str) -> \
    Dict[str, Any]:
        """Recursively migrate nested structures to match default structure."""
        migrated = copy.deepcopy(current_data)

        for field, default_value in default_structure.items():
            if field not in migrated:
                # Field is missing, add it with default value
                migrated[field] = copy.deepcopy(default_value)
            elif isinstance(default_value, dict) and isinstance(migrated[field], dict):
                # Recursively migrate nested dictionaries
                migrated[field] = self._migrate_nested_structure(
                    migrated[field], default_value, f"{path}.{field}"
                )
            elif type(default_value) != type(migrated[field]):
                # Type mismatch, use default value
                migrated[field] = copy.deepcopy(default_value)

        return migrated

    def _preserve_extra_fields(self, migrated_doc: Dict[str, Any], original_doc: Dict[str, Any],
                               default_profile: Dict[str, Any]) -> Dict[str, Any]:
        """Preserve extra fields that exist in original but not in default structure."""
        for field in original_doc.keys():
            if field not in default_profile and field not in migrated_doc:
                migrated_doc[field] = original_doc[field]

        return migrated_doc

    async def update_user_data(self, user_id: str, guild_id: str, update_data: Dict[str, Any]):
        """
        Safe update that validates structure before applying updates.
        """
        try:
            # Get current user data
            current_data = await self.get_user_data(user_id, guild_id)

            if not current_data:
                # Create new profile if doesn't exist
                new_profile = await self.create_enhanced_user_profile(user_id, guild_id)
                await self.users.insert_one(new_profile)
                logger.info(f"📝 Created new user profile: G:{guild_id} U:{user_id}")
                return

            # Validate and migrate if necessary
            validated_data, was_migrated = await self.validate_and_migrate_user_document(
                user_id, guild_id, current_data
            )

            # If migration was needed, replace the entire document
            if was_migrated:
                validated_data["updated_at"] = utc_now_ts()
                await self.users.replace_one(
                    {"user_id": user_id, "guild_id": guild_id},
                    validated_data,
                    upsert=True
                )
                logger.info(f"🔄 Document replaced after migration: G:{guild_id} U:{user_id}")
                return

            # Otherwise proceed with normal update
            has_operators = any(key.startswith('$') for key in update_data.keys())

            if has_operators:
                update_doc = update_data
                if "$set" in update_doc:
                    update_doc["$set"]["updated_at"] = utc_now_ts()
                else:
                    update_doc["$set"] = {"updated_at": utc_now_ts()}
            else:
                update_data["updated_at"] = utc_now_ts()
                update_doc = {"$set": update_data}

            # Log the update details
            self._log_update_changes(user_id, guild_id, update_doc, current_data)

            await self.users.update_one(
                {"user_id": user_id, "guild_id": guild_id},
                update_doc,
                upsert=True
            )

            logger.debug(f"✅ User data updated successfully: G:{guild_id} U:{user_id}")

        except Exception as e:
            logger.error(f"❌ Error updating user data: {e}")

    def _log_update_changes(self, user_id: str, guild_id: str, update_doc: Dict[str, Any],
                            current_data: Optional[Dict[str, Any]]):
        """
        Log detailed information about what's changing in the update.

        Args:
            user_id: User ID being updated
            guild_id: Guild ID context
            update_doc: MongoDB update document with operators
            current_data: Current user data before update (None if new user)
        """
        try:
            logger.info(f"📝 Update User Data - G:{guild_id} U:{user_id}")

            # Handle new user creation
            if not current_data:
                logger.info(f"  🆕 Creating new user profile")
                logger.debug(f"  Initial data: {update_doc}")
                return

            # Log each operation type
            for operator, fields in update_doc.items():
                if operator == "$set":
                    logger.info(f"  📌 SET operations:")
                    for field, new_value in fields.items():
                        if field == "updated_at":
                            continue  # Skip timestamp logging

                        old_value = self._get_nested_value(current_data, field)

                        # Format the values for readability
                        old_str = self._format_value(old_value)
                        new_str = self._format_value(new_value)

                        if old_value != new_value:
                            logger.info(f"    • {field}: {old_str} → {new_str}")
                        else:
                            logger.debug(f"    • {field}: {old_str} (unchanged)")

                elif operator == "$inc":
                    logger.info(f"  ➕ INCREMENT operations:")
                    for field, increment in fields.items():
                        old_value = self._get_nested_value(current_data, field)
                        new_value = (old_value or 0) + increment

                        old_str = self._format_value(old_value)
                        new_str = self._format_value(new_value)

                        logger.info(f"    • {field}: {old_str} + {increment} = {new_str}")

                elif operator == "$push":
                    logger.info(f"  📥 PUSH operations:")
                    for field, value in fields.items():
                        logger.info(f"    • {field}: appending {self._format_value(value)}")

                elif operator == "$pull":
                    logger.info(f"  📤 PULL operations:")
                    for field, value in fields.items():
                        logger.info(f"    • {field}: removing {self._format_value(value)}")

                elif operator == "$unset":
                    logger.info(f"  🗑️  UNSET operations:")
                    for field in fields.keys():
                        old_value = self._get_nested_value(current_data, field)
                        logger.info(f"    • {field}: removing (was {self._format_value(old_value)})")

                elif operator == "$addToSet":
                    logger.info(f"  📌 ADD_TO_SET operations:")
                    for field, value in fields.items():
                        logger.info(f"    • {field}: adding unique {self._format_value(value)}")

                else:
                    logger.debug(f"  🔧 {operator}: {fields}")

        except Exception as e:
            logger.warning(f"⚠️ Failed to log update changes: {e}")

    def _get_nested_value(self, data: Dict[str, Any], field_path: str) -> Any:
        """
        Get a value from nested dictionary using dot notation.

        Args:
            data: Dictionary to search
            field_path: Dot-separated path (e.g., "message_stats.messages")

        Returns:
            Value at the path, or None if not found
        """
        try:
            keys = field_path.split('.')
            value = data
            for key in keys:
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    return None
                if value is None:
                    return None
            return value
        except Exception:
            return None

    def _format_value(self, value: Any) -> str:
        """
        Format a value for logging in a readable way.

        Args:
            value: Value to format

        Returns:
            Formatted string representation
        """
        if value is None:
            return "None"
        elif isinstance(value, (int, float)):
            if isinstance(value, float) and value > 1000000000:  # Likely a timestamp
                try:
                    from datetime import datetime, timezone
                    dt = datetime.fromtimestamp(value, tz=timezone.utc)
                    return f"{value} ({dt.strftime('%Y-%m-%d %H:%M:%S')} UTC)"
                except Exception:
                    return str(value)
            return str(value)
        elif isinstance(value, bool):
            return str(value)
        elif isinstance(value, str):
            if len(value) > 50:
                return f'"{value[:47]}..."'
            return f'"{value}"'
        elif isinstance(value, (list, tuple)):
            if len(value) > 3:
                return f"[{len(value)} items]"
            return str(value)
        elif isinstance(value, dict):
            if len(value) > 3:
                return f"{{{len(value)} keys}}"
            return str(value)
        else:
            return str(value)

    async def get_guild_settings(self, guild_id: str) -> Dict[str, Any]:
        """Get guild leveling settings."""
        try:
            settings = await self.ls_master.find_one({"guild_id": guild_id})
            return settings or {}
        except Exception as e:
            logger.error(f"❌ Error getting guild settings: {e}")
            return {}

    def xp_for_level(self, level: int) -> int:
        """Calculate the total XP required to reach a specific level."""
        return 50 * level * (level + 1)

    def xp_to_next_level(self, current_level: int) -> int:
        """Calculate the XP needed to advance to the next level."""
        return self.xp_for_level(current_level + 1) - self.xp_for_level(current_level)

    def check_level_up(self, current_xp: int, current_level: int) -> tuple:
        """Check if the user levels up based on XP."""
        new_level = current_level
        leveled_up = False

        while True:
            next_level_xp = self.xp_for_level(new_level + 1)
            if current_xp >= next_level_xp:
                new_level += 1
                leveled_up = True
            else:
                break

        return new_level, leveled_up

    async def create_enhanced_user_profile(self, user_id: str, guild_id: str) -> Dict[str, Any]:
        """Create a comprehensive user profile with all enhanced features"""
        now_ts = utc_now_ts()
        today_key = utc_today_key()
        week_key = utc_week_key()
        month_key = utc_month_key()

        return {
            "guild_id": guild_id,
            "user_id": user_id,
            "longest_streak": 0,
            "streak_timestamp": 0,
            "level": 1,
            "prestige_level": 0,
            "xp": 0,
            "embers": 0,
            "created_at": now_ts,
            "updated_at": now_ts,
            "daily_streak": {
                "count": 0,
                "timestamp": 0
            },
            "message_stats": {
                "last_message_time": 0,
                "messages": 0,
                "longest_message": 0,
                "with_attachments": 0,
                "with_links": 0,
                "got_reactions": 0,
                "reacted_messages": 0,
                "average_message_length": 0,
                "today_embers": 0,
                "today_key": today_key,
                "today_xp": 0,
                "monthly_embers": 0,
                "monthly_xp": 0,
                "weekly_embers": 0,
                "weekly_xp": 0,
            },
            "voice_stats": {
                "active_seconds": 0,
                "deafened_time": 0,
                "muted_time": 0,
                "self_deafened_time": 0,
                "self_muted_time": 0,
                "total_active_percentage": 0,
                "total_unmuted_percentage": 0,
                "voice_seconds": 0,
                "voice_sessions": 0,
                "today_embers": 0,
                "today_key": today_key,
                "today_xp": 0,
                "monthly_embers": 0,
                "monthly_xp": 0,
                "weekly_embers": 0,
                "weekly_xp": 0,
                "average_session_length": 0,
                "total_time": 0,
                "active_time": 0,
                "sessions": 0,
                "month_key": month_key,
                "week_key": week_key,
            },
            "social_stats": {
                "guild_streak": 0,
                "collaboration_score": 0,
                "helpfulness_rating": 0,
                "mentor_activities": 0,
                "last_interaction": 0,
            },
            "quality_stats": {
                "average_score": 0,
                "high_quality_count": 0,
                "constructive_messages": 0,
                "last_quality_score": 0,
                "last_updated": 0,
                "total_messages": 0
            },
            "last_rewarded": {
                "message": 0.0,
                "voice": 0.0,
                "got_reaction": 0.0,
                "give_reaction": 0.0
            },
            "achievements": {
                "unlocked_count": 0,
                "rare_achievements": 0,
                "last_unlock": 0,
            },
            "challenges": {
                "daily_completed": 0,
                "weekly_completed": 0,
                "monthly_completed": 0,
                "streak": 0,
            },
            "preferences": {
                "notifications": True,
                "public_stats": True,
                "achievement_announcements": True,
            }
        }