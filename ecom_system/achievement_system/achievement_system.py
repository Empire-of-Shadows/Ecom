import asyncio
import logging
import os
import time
from typing import Dict, List, Any, Optional, Tuple, Coroutine
import pymongo
from pymongo import UpdateOne

from database.DatabaseManager import get_collection_registry
from .achievement_condition_system import AchievementConditionSystem
from ecom_system.helpers.helpers import utc_now_ts, ctx
from loggers.log_factory import log_performance
from dotenv import load_dotenv

from .progress.achievement_progress import AchievementProgressSystem

load_dotenv()

REWARDS_CHANNEL_ID = os.getenv("REWARDS_CHANNEL_ID")
from core.bot import bot as discord_bot

logger = logging.getLogger(__name__)

_initialization_lock = asyncio.Lock()
_initialization_done = False
_definitions_cache: Optional[List[Dict[str, Any]]] = None
_instance_count = 0  # debug aid to understand how many instances are created
_startup_log_emitted = False


class AchievementSystem:
    """
    Comprehensive achievement system for tracking and rewarding user milestones.

    Features:
    - Multi-category achievements (basic, level, activity, voice, social, special, prestige)
    - Rarity-based reward scaling (common, uncommon, rare, epic, legendary)
    - Progress tracking for incremental achievements
    - Achievement unlock bonuses and notifications
    - Conditional and time-based achievements
    - Achievement statistics and analytics
    """

    def __init__(self, leveling_system):
        """Initialize with reference to parent LevelingSystem"""
        self.leveling_system = leveling_system

        self.logger = logger

        # Initialize condition checking system
        self.condition_system = AchievementConditionSystem(leveling_system)

        self.achievement_definitions = None
        self._definitions_loaded = False

        logger.info("🏆 AchievementSystem initialized with comprehensive tracking")

        # If definitions were already loaded globally, reuse them and avoid scheduling another eager load
        global _initialization_done, _definitions_cache
        if _initialization_done and _definitions_cache is not None:
            self.achievement_definitions = _definitions_cache
            self._definitions_loaded = True
            logger.debug("AchievementSystem reused global achievement definitions cache")
        else:
            # Only schedule eager load if we haven't globally initialized yet
            asyncio.create_task(self._eager_load_achievements())

    async def _eager_load_achievements(self):
        """Eagerly load achievements during initialization"""
        try:
            # Small delay to ensure database connections are ready
            await asyncio.sleep(0.1)

            # Only attempt eager load if not globally initialized yet
            global _initialization_done, _startup_log_emitted
            if not _initialization_done:
                await self._ensure_definitions_loaded()
                # Log once on the first successful load
                if _initialization_done and not _startup_log_emitted:
                    _startup_log_emitted = True
                    logger.info("✅ Achievements loaded during startup")
            else:
                logger.debug("Skipping eager load: global achievement definitions already initialized")
        except Exception as e:
            logger.error(f"❌ Failed to eagerly load achievements: {e}", exc_info=True)

    async def _ensure_definitions_loaded(self) -> list[dict[str, Any]] | dict[str, list[dict[str, Any]]]:
        """Ensure achievement definitions are loaded with proper synchronization"""
        # Fast path: instance has them
        if self._definitions_loaded and self.achievement_definitions is not None:
            return self.achievement_definitions

        # Fast path: another instance already populated the global cache
        global _initialization_done, _definitions_cache
        if _initialization_done and _definitions_cache is not None:
            self.achievement_definitions = _definitions_cache
            self._definitions_loaded = True
            logger.debug("Definitions loaded from global cache")
            return self.achievement_definitions

        async with _initialization_lock:
            # Double-check under the lock
            if self._definitions_loaded and self.achievement_definitions is not None:
                return self.achievement_definitions
            if _initialization_done and _definitions_cache is not None:
                self.achievement_definitions = _definitions_cache
                self._definitions_loaded = True
                logger.debug("Definitions loaded from global cache (under lock)")
                return self.achievement_definitions

            # Load definitions
            logger.debug("Loading achievement definitions from database...")
            definitions = await self._initialize_achievement_definitions()
            self.achievement_definitions = definitions
            self._definitions_loaded = True

            # Populate global cache so other instances reuse it
            _definitions_cache = definitions
            _initialization_done = True
            return self.achievement_definitions


    async def _initialize_achievement_definitions(self) -> Dict[str, List[Dict[str, Any]]]:
        """Dynamically load achievement definitions from all collections in Achievements database"""
        try:
            # Get the collection registry to discover all achievement collections
            collection_registry = get_collection_registry()

            if "achievements" not in collection_registry:
                logger.warning("No Achievements database found")
                return {}

            all_achievements = {}
            total_achievements = 0

            # Load from each collection in the Achievements database
            for collection_name, collection_ref in collection_registry["achievements"].items():
                try:
                    # Load all achievements from this collection
                    category_achievements = await collection_ref.find().to_list(length=None)

                    if category_achievements:
                        all_achievements[collection_name.lower()] = category_achievements
                        total_achievements += len(category_achievements)
                        logger.info(f"Loaded {len(category_achievements)} achievements from {collection_name} collection")
                    else:
                        logger.warning(f"No achievements found in {collection_name} collection")
                        all_achievements[collection_name.lower()] = []

                except Exception as e:
                    logger.error(f"Failed to load achievements from {collection_name} collection: {e}")
                    all_achievements[collection_name.lower()] = []

            if total_achievements == 0:
                logger.warning(
                    "No achievements found in any collection. Achievement system will run with empty definitions.")
                return {}

            logger.info(f"Loaded {total_achievements} total achievements from {len(all_achievements)} collections")
            return all_achievements

        except Exception as e:
            logger.error(f"Failed to load achievements from database: {e}", exc_info=True)
            return {}

    # =========================
    # Core Achievement Processing
    # =========================
    @log_performance("check_achievements")
    async def check_and_update_achievements(self, user_id: str, guild_id: str, activity_data: Dict):
        """Comprehensive achievement checking and updating system"""
        start_time = time.time()

        try:
            settings = await self.leveling_system.get_guild_settings(guild_id)
            if not settings.get("achievements", {}).get("enabled", True):
                logger.debug(f"Achievements disabled for guild: {ctx(guild_id=guild_id)}")
                return

            # Get user's current achievements
            user_achievements = await self._get_user_achievements(user_id, guild_id)

            # Get comprehensive user data
            user_data = await self.leveling_system.get_enhanced_user_data(user_id, guild_id)
            if not user_data:
                logger.warning(f"No user data found for achievement check: {ctx(guild_id=guild_id, user_id=user_id)}")
                return

            # Ensure definitions are loaded with proper synchronization
            definitions_result = await self._ensure_definitions_loaded()

            # Handle both return types from _ensure_definitions_loaded
            if isinstance(definitions_result, dict):
                category_definitions = definitions_result
                definitions = []
                for category_achievements in definitions_result.values():
                    if isinstance(category_achievements, list):
                        # Keep only proper achievement documents
                        definitions.extend([
                            ach for ach in category_achievements
                            if isinstance(ach, dict) and "id" in ach and "category" in ach
                        ])
            else:
                definitions = [
                    ach for ach in (definitions_result or [])
                    if isinstance(ach, dict) and "id" in ach and "category" in ach
                ]
                category_definitions = {}

            # Debug logging
            logger.debug(
                f"Final definitions type: {type(definitions)}, length: {len(definitions) if definitions else 0}")

            try:
                # ===== PROGRESS TRACKING UPDATE =====
                # Pass category-keyed dict as expected by progress system
                await AchievementProgressSystem.update_achievement_progress_tracking(user_id, guild_id, activity_data,
                                                                                     category_definitions, self.leveling_system.bot.activity_system)
            except Exception as e:
                logger.error(f"Error updating achievement progress tracking: {e}")

            # Filter achievements to check - only check unlocked achievements
            achievements_to_check = []
            for ach in definitions:
                if ach["id"] not in user_achievements["unlocked"]:
                    achievements_to_check.append(ach)
                # No need to log anything for already unlocked achievements - this is normal behavior

            logger.info(
                f"Evaluating {len(achievements_to_check)} potential achievements for user {user_id} in guild {guild_id}.")
            unlocked_achievements = []

            # Check each achievement using the condition system
            for achievement in achievements_to_check:
                logger.debug(f"  - Checking '{achievement['id']}' ({achievement['name']})")
                try:
                    if await self.condition_system.check_achievement_condition(
                            achievement, user_id, guild_id, activity_data, user_data, user_achievements
                    ):
                        user_achievements["unlocked"].append(achievement["id"])
                        unlocked_achievements.append(achievement)

                        # Log the achievement unlock
                        logger.info(
                            f"🏆 Achievement unlocked: '{achievement['name']}' ({achievement['rarity']}) "
                            f"for {ctx(guild_id=guild_id, user_id=user_id)}")

                        # Update progress tracking
                        await self._update_achievement_progress(user_id, guild_id, achievement["id"], activity_data)

                except Exception as achievement_error:
                    logger.error(f"Error checking achievement '{achievement['id']}': {achievement_error}")
                    continue

            # Process unlocked achievements
            if unlocked_achievements:
                # Update user achievements in database
                await self._save_user_achievements(user_id, guild_id, user_achievements)

                # Grant achievement rewards
                total_rewards = await self._grant_achievement_rewards(user_id, guild_id, unlocked_achievements)

                # Send notifications
                await self._send_achievement_notifications(user_id, guild_id, unlocked_achievements, settings)

                # Update performance metrics
                self.leveling_system.performance_monitor.performance_metrics["achievements_unlocked"] += len(
                    unlocked_achievements)

                processing_time = (time.time() - start_time) * 1000
                logger.info(
                    f"🎉 Granted {len(unlocked_achievements)} achievement(s) with total rewards: {total_rewards} "
                    f"(processed in {processing_time:.2f}ms)")

            # ===== OPTIONAL: UPDATE PROGRESS AGAIN AFTER UNLOCKS =====
            # This ensures progress is updated for any achievements that were close to unlocking
            if unlocked_achievements:
                await AchievementProgressSystem.update_achievement_progress_tracking(user_id, guild_id, activity_data,
                                                                                     category_definitions, self.leveling_system.bot.activity_system)

        except Exception as e:
            logger.error(f"❌ Error in achievement system: {e}", exc_info=True)

    # =========================
    # Achievement Data Management
    # =========================
    async def _get_user_achievements(self, user_id: str, guild_id: str) -> Dict:
        """Get user achievements with default structure"""
        try:
            user_achievements = await self.leveling_system.user_achievements.find_one(
                {"user_id": user_id, "guild_id": guild_id}
            )

            if not user_achievements:
                return {
                    "user_id": user_id,
                    "guild_id": guild_id,
                    "unlocked": [],
                    "progress": {},
                    "created_at": utc_now_ts(),
                    "updated_at": utc_now_ts()
                }

            return user_achievements

        except Exception as e:
            logger.error(f"Error getting user achievements: {e}")
            return {
                "user_id": user_id,
                "guild_id": guild_id,
                "unlocked": [],
                "progress": {},
                "created_at": utc_now_ts(),
                "updated_at": utc_now_ts()
            }

    async def _save_user_achievements(self, user_id: str, guild_id: str, user_achievements: Dict):
        """Save user achievements to database"""
        try:
            await self.leveling_system.user_achievements.update_one(
                {"user_id": user_id, "guild_id": guild_id},
                {
                    "$set": {
                        "unlocked": user_achievements["unlocked"],
                        "progress": user_achievements.get("progress", {}),
                        "updated_at": utc_now_ts()
                    },
                    "$setOnInsert": {"created_at": user_achievements.get("created_at", utc_now_ts())}
                },
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error saving user achievements: {e}")

    # =========================
    # Database Management Methods
    # =========================
    async def add_achievement(self, achievement_data: Dict[str, Any]) -> Tuple[bool, str]:
        """Add a new achievement to the database"""
        try:
            if not achievement_data.get("id"):
                return False, "Achievement ID is required"

            # Check if achievement already exists
            existing = await self.leveling_system.achievements.find_one({"id": achievement_data["id"]})
            if existing:
                return False, f"Achievement '{achievement_data['id']}' already exists"

            # Add timestamps
            now = utc_now_ts()
            achievement_data.update({
                "created_at": now,
                "updated_at": now,
                "enabled": achievement_data.get("enabled", True)
            })

            # Validate required fields
            required_fields = ["id", "name", "description", "category", "rarity", "rewards", "conditions"]
            for field in required_fields:
                if field not in achievement_data:
                    return False, f"Missing required field: {field}"

            await self.leveling_system.achievements.insert_one(achievement_data)
            logger.info(f"Added new achievement: {achievement_data['id']}")
            return True, f"Achievement '{achievement_data['name']}' added successfully"

        except Exception as e:
            logger.error(f"Failed to add achievement: {e}", exc_info=True)
            return False, f"Failed to add achievement: {str(e)}"

    async def update_achievement(self, achievement_id: str, updates: Dict[str, Any]) -> Tuple[bool, str]:
        """Update an existing achievement in the database"""
        try:
            updates["updated_at"] = utc_now_ts()

            result = await self.leveling_system.achievements.update_one(
                {"id": achievement_id},
                {"$set": updates}
            )

            if result.modified_count > 0:
                logger.info(f"Updated achievement: {achievement_id}")
                return True, f"Achievement '{achievement_id}' updated successfully"
            else:
                return False, f"Achievement '{achievement_id}' not found or no changes made"

        except Exception as e:
            logger.error(f"Failed to update achievement {achievement_id}: {e}", exc_info=True)
            return False, f"Failed to update achievement: {str(e)}"

    async def remove_achievement(self, achievement_id: str) -> Tuple[bool, str]:
        """Remove an achievement from the database"""
        try:
            result = await self.leveling_system.achievements.delete_one({"id": achievement_id})

            if result.deleted_count > 0:
                logger.info(f"Removed achievement: {achievement_id}")
                return True, f"Achievement '{achievement_id}' removed successfully"
            else:
                return False, f"Achievement '{achievement_id}' not found"

        except Exception as e:
            logger.error(f"Failed to remove achievement {achievement_id}: {e}", exc_info=True)
            return False, f"Failed to remove achievement: {str(e)}"

    async def toggle_achievement(self, achievement_id: str) -> Tuple[bool, str]:
        """Toggle achievement enabled/disabled status"""
        try:
            achievement = await self.leveling_system.achievements.find_one({"id": achievement_id})
            if not achievement:
                return False, f"Achievement '{achievement_id}' not found"

            new_status = not achievement.get("enabled", True)

            result = await self.leveling_system.achievements.update_one(
                {"id": achievement_id},
                {"$set": {"enabled": new_status, "updated_at": utc_now_ts()}}
            )

            if result.modified_count > 0:
                status_text = "enabled" if new_status else "disabled"
                logger.info(f"Achievement {achievement_id} {status_text}")
                return True, f"Achievement '{achievement_id}' {status_text}"
            else:
                return False, "Failed to update achievement status"

        except Exception as e:
            logger.error(f"Failed to toggle achievement {achievement_id}: {e}", exc_info=True)
            return False, f"Failed to toggle achievement: {str(e)}"

    async def list_achievements(self, category: str = None, enabled_only: bool = True) -> List[Dict[str, Any]]:
        """List achievements from database with optional filtering"""
        try:
            query = {}
            if category:
                query["category"] = category
            if enabled_only:
                query["enabled"] = True

            achievements = await self.leveling_system.achievements.find(query).to_list(length=None)

            logger.debug(f"Retrieved {len(achievements)} achievements from database")
            return achievements

        except Exception as e:
            logger.error(f"Failed to list achievements: {e}", exc_info=True)
            return []

    async def get_achievement(self, achievement_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific achievement from database"""
        try:
            achievement = await self.leveling_system.achievements.find_one({"id": achievement_id})
            return achievement
        except Exception as e:
            logger.error(f"Failed to get achievement {achievement_id}: {e}", exc_info=True)
            return None

    async def reload_achievements(self) -> bool:
        """Reload achievement definitions from database"""
        try:
            async with _initialization_lock:
                self.achievement_definitions = await self._initialize_achievement_definitions()
                self._definitions_loaded = True
                # Update global cache so all instances get the fresh definitions
                global _definitions_cache, _initialization_done
                _definitions_cache = self.achievement_definitions
                _initialization_done = True
            logger.info("Achievement definitions reloaded from database")
            return True
        except Exception as e:
            logger.error(f"Failed to reload achievements: {e}", exc_info=True)
            return False


    # =========================
    # Achievement Rewards and Notifications
    # =========================
    async def _grant_achievement_rewards(self, user_id: str, guild_id: str, achievements: List[Dict]) -> Dict:
        """Grant rewards for unlocked achievements"""
        total_xp = 0
        total_embers = 0
        titles_granted = []
        special_rewards = []

        try:
            for achievement in achievements:
                rewards = achievement.get("rewards", {})

                # Add XP and embers
                xp_reward = rewards.get("xp", 0)
                embers_reward = rewards.get("embers", 0)
                total_xp += xp_reward
                total_embers += embers_reward

                # Track titles and special rewards
                if "title" in rewards:
                    titles_granted.append(rewards["title"])

                if "special_badge" in rewards:
                    special_rewards.append("special_badge")

                if "unique_badge" in rewards:
                    special_rewards.append("unique_badge")

                if "exclusive_perks" in rewards:
                    special_rewards.append("exclusive_perks")

            # Update user in database
            if total_xp > 0 or total_embers > 0:
                await self.leveling_system.users.update_one(
                    {"user_id": user_id, "guild_id": guild_id},
                    {
                        "$inc": {
                            "xp": total_xp,
                            "embers": total_embers,
                            "achievements.unlocked_count": len(achievements)
                        },
                        "$set": {
                            "achievements.last_unlock": utc_now_ts()
                        }
                    }
                )

            reward_summary = {
                "xp": total_xp,
                "embers": total_embers,
                "count": len(achievements),
                "titles": titles_granted,
                "special_rewards": special_rewards
            }

            logger.info(f"💰 Granted rewards for {len(achievements)} achievements: {total_xp} XP, {total_embers} Embers.")
            logger.debug(f"Detailed rewards summary: {reward_summary}")
            return reward_summary

        except Exception as e:
            logger.error(f"❌ Error granting achievement rewards: {e}")
            return {"xp": 0, "embers": 0, "count": 0, "titles": [], "special_rewards": []}

    async def _send_achievement_notifications(self, user_id: str, guild_id: str, achievements: List[Dict],
                                              settings: Dict):
        """Send achievement notifications to rewards channel"""
        try:
            achievements_cfg = settings.get("achievements", {})

            if not achievements_cfg.get("show_progress", True):
                return

            # Send to rewards channel if configured
            if REWARDS_CHANNEL_ID:
                try:
                    channel = discord_bot.get_channel(int(REWARDS_CHANNEL_ID))
                    if channel is None:
                        # Fallback to fetch if not cached
                        channel = await discord_bot.fetch_channel(int(REWARDS_CHANNEL_ID))
                    if channel:
                        await self._send_achievement_message_to_channel(channel, user_id, guild_id, achievements)
                    else:
                        logger.warning(f"Rewards channel {REWARDS_CHANNEL_ID} not found (get_channel/fetch_channel returned None)")
                except Exception as channel_error:
                    logger.error(f"Error sending message to rewards channel: {channel_error}")
            else:
                logger.warning("REWARDS_CHANNEL_ID not configured, skipping channel notifications")

            # Group achievements by rarity for better presentation
            by_rarity = {}
            for achievement in achievements:
                rarity = achievement.get("rarity", "common")
                if rarity not in by_rarity:
                    by_rarity[rarity] = []
                by_rarity[rarity].append(achievement)

            # Log achievement unlocks
            for rarity, ach_list in by_rarity.items():
                logger.info(f"🎉 {rarity.title()} achievements unlocked for user {user_id}: "
                            f"{[ach['name'] for ach in ach_list]}")

            # Rare achievement announcements
            rare_achievements = [ach for ach in achievements if ach.get("rarity") in ["epic", "legendary"]]
            if rare_achievements and achievements_cfg.get("rare_achievement_announcement", True):
                logger.info(
                    f"🌟 RARE ACHIEVEMENT UNLOCK: User {user_id} unlocked {len(rare_achievements)} rare achievements!")

        except Exception as e:
            logger.error(f"❌ Error sending achievement notifications: {e}")

    async def _send_achievement_message_to_channel(self, channel, user_id: str, guild_id: str,
                                                   achievements: List[Dict]):
        """Send formatted achievement message to the specified channel"""
        try:
            # Create achievement announcement message
            user_mention = f"<@{user_id}>"

            if len(achievements) == 1:
                achievement = achievements[0]
                rarity_emoji = self._get_rarity_emoji(achievement.get("rarity", "common"))
                message = (
                    f"🏆 **Achievement Unlocked!** {rarity_emoji}\n"
                    f"{user_mention} has earned: **{achievement['name']}**\n"
                    f"*{achievement.get('description', 'No description available')}*"
                )
            else:
                # Multiple achievements
                achievement_list = []
                for ach in achievements:
                    rarity_emoji = self._get_rarity_emoji(ach.get("rarity", "common"))
                    achievement_list.append(f"{rarity_emoji} **{ach['name']}**")

                message = (
                        f"🏆 **Multiple Achievements Unlocked!**\n"
                        f"{user_mention} has earned:\n" +
                        "\n".join(achievement_list)
                )

            # Add rewards information
            total_xp = sum(ach.get("rewards", {}).get("xp", 0) for ach in achievements)
            total_embers = sum(ach.get("rewards", {}).get("embers", 0) for ach in achievements)

            rewards_text = []
            if total_xp > 0:
                rewards_text.append(f"**{total_xp:,}** XP")
            if total_embers > 0:
                rewards_text.append(f"**{total_embers:,}** Embers")

            if rewards_text:
                message += f"\n\n💰 **Rewards:** {' • '.join(rewards_text)}"

            await channel.send(message)
            logger.info(f"Achievement notification sent to rewards channel for user {user_id}")

        except Exception as e:
            logger.error(f"Error sending achievement message to channel: {e}")

    def _get_rarity_emoji(self, rarity: str) -> str:
        """Get emoji for achievement rarity"""
        rarity_emojis = {
            "common": "⚪",
            "uncommon": "🟢",
            "rare": "🔵",
            "epic": "🟣",
            "legendary": "🟡"
        }
        return rarity_emojis.get(rarity.lower(), "⚪")

    async def _update_achievement_progress(self, user_id: str, guild_id: str, achievement_id: str, activity_data: Dict):
        """Update achievement progress tracking after unlock"""
        try:
            # Since this achievement was just unlocked, remove it from progress tracking
            # to prevent unnecessary progress calculations for already unlocked achievements
            await self.leveling_system.user_achievements.update_one(
                {"user_id": user_id, "guild_id": guild_id},
                {
                    "$unset": {f"progress.{achievement_id}": ""},
                    "$set": {"updated_at": utc_now_ts()}
                }
            )

            logger.debug(
                f"Cleaned up progress tracking for unlocked achievement: {achievement_id} for {ctx(guild_id=guild_id, user_id=user_id)}"
            )

        except Exception as e:
            logger.error(f"Error updating achievement progress: {e}")

    # =========================
    # Achievement Statistics and Analytics
    # =========================
    async def get_user_achievement_stats(self, user_id: str, guild_id: str) -> Dict[str, Any]:
        """Get comprehensive achievement statistics for a user"""
        try:
            user_achievements = await self._get_user_achievements(user_id, guild_id)
            unlocked_ids = user_achievements.get("unlocked", [])

            # Ensure definitions are loaded
            definitions = await self._ensure_definitions_loaded()

            # Get details of unlocked achievements
            unlocked_details = [ach for ach in definitions if ach["id"] in unlocked_ids]

            # Calculate statistics
            stats = {
                "total_unlocked": len(unlocked_ids),
                "total_available": len(definitions),
                "completion_percentage": round((len(unlocked_ids) / len(definitions)) * 100, 1),
                "by_category": {},
                "by_rarity": {},
                "recent_unlocks": [],
                "next_achievements": await self._get_next_achievements(user_id, guild_id),
                "total_rewards_earned": {"xp": 0, "embers": 0}
            }

            # Categorize unlocked achievements
            for achievement in unlocked_details:
                category = achievement.get("category", "unknown")
                rarity = achievement.get("rarity", "common")

                stats["by_category"][category] = stats["by_category"].get(category, 0) + 1
                stats["by_rarity"][rarity] = stats["by_rarity"].get(rarity, 0) + 1

                # Sum rewards
                rewards = achievement.get("rewards", {})
                stats["total_rewards_earned"]["xp"] += rewards.get("xp", 0)
                stats["total_rewards_earned"]["embers"] += rewards.get("embers", 0)

            logger.debug(f"Generated achievement stats for user: {ctx(guild_id=guild_id, user_id=user_id)}")
            return stats

        except Exception as e:
            logger.error(f"Error getting user achievement stats: {e}")
            return {"error": "Failed to retrieve achievement stats"}

    async def _get_next_achievements(self, user_id: str, guild_id: str, limit: int = 5) -> List[Dict]:
        """Get next achievements user is close to unlocking"""
        try:
            user_data = await self.leveling_system.get_enhanced_user_data(user_id, guild_id)
            user_achievements = await self._get_user_achievements(user_id, guild_id)
            unlocked_ids = user_achievements.get("unlocked", [])

            # Filter to unearned achievements and calculate progress
            next_achievements = []

            # Ensure definitions are loaded
            definitions = await self._ensure_definitions_loaded()

            for achievement in definitions:
                if achievement["id"] in unlocked_ids:
                    continue

                progress = await self._calculate_achievement_progress(achievement, user_data)
                if progress > 0:  # Only include achievements with some progress
                    next_achievements.append({
                        "achievement": achievement,
                        "progress_percentage": progress
                    })

            # Sort by progress and return top achievements
            next_achievements.sort(key=lambda x: x["progress_percentage"], reverse=True)
            return next_achievements[:limit]

        except Exception as e:
            logger.error(f"Error getting next achievements: {e}")
            return []

    async def _calculate_achievement_progress(self, achievement: Dict, user_data: Dict) -> float:
        """Calculate progress percentage towards an achievement"""
        try:
            conditions = achievement.get("conditions", {})
            condition_type = conditions.get("type", "simple")
            condition_data = conditions.get("data", {})
            threshold = condition_data.get("threshold", 1)

            if condition_type == "level":
                current = user_data.get("level", 1)
            elif condition_type == "messages":
                current = user_data.get("message_stats", {}).get("messages", 0)
            elif condition_type == "voice_time":
                current = user_data.get("voice_stats", {}).get("total_time", 0)
            elif condition_type == "voice_sessions":
                current = user_data.get("voice_stats", {}).get("sessions", 0)
            elif condition_type == "daily_streak":
                field = condition_data.get("field", "daily_streak.count")
                current = self.condition_system._get_nested_value(user_data, field)
            elif condition_type == "reactions_given":
                current = user_data.get("message_stats", {}).get("reacted_messages", 0)
            elif condition_type == "got_reactions":
                current = user_data.get("message_stats", {}).get("got_reactions", 0)
            elif condition_type == "attachment_messages":
                current = user_data.get("message_stats", {}).get("attachment_messages", 0)
            elif condition_type == "links_sent":
                current = user_data.get("message_stats", {}).get("links_sent", 0)
            elif condition_type == "attachments_sent":
                current = user_data.get("message_stats", {}).get("attachments_sent", 0)
            elif condition_type == "quality_streak":
                current = user_data.get("message_stats", {}).get("quality_streak", 0)
            elif condition_type == "prestige_level":
                current = user_data.get("prestige_level", 0)
            elif condition_type == "time_based":
                created_at = user_data.get("created_at", 0)
                current_time = utc_now_ts()
                unit = condition_data.get("unit", "days")

                time_diff = current_time - created_at
                if unit == "days":
                    current = time_diff / 86400
                elif unit == "hours":
                    current = time_diff / 3600
                elif unit == "minutes":
                    current = time_diff / 60
                else:
                    current = time_diff
            elif condition_type == "combination":
                # For combination conditions, calculate based on individual requirements
                operator = condition_data.get("operator", "and")
                requirements = condition_data.get("requirements", [])

                if not requirements:
                    return 0

                progress_values = []
                for requirement in requirements:
                    req_type = requirement.get("type")
                    req_threshold = requirement.get("threshold", 1)
                    field = requirement.get("field", "")

                    if req_type == "level":
                        req_current = user_data.get("level", 1)
                    else:
                        req_current = self.condition_system._get_nested_value(user_data, field) if field else 0

                    req_progress = min(100, (req_current / req_threshold) * 100) if req_threshold > 0 else 100
                    progress_values.append(req_progress)

                # For combination conditions, use average progress for "and", max for "or"
                if operator == "and":
                    current = sum(progress_values) / len(progress_values) if progress_values else 0
                    return round(current, 1)
                elif operator == "or":
                    current = max(progress_values) if progress_values else 0
                    return round(current, 1)
                else:
                    return 0
            # Handle complex conditions that don't have simple progress calculation
            elif condition_type in ["simple", "time_pattern", "weekend_activity", "custom"]:
                # These conditions are binary (met or not met), so progress is either 0% or 100%
                # We could potentially check if they're met and return 100%, but for now return 0
                return 0
            else:
                return 0  # Unknown condition type

            progress = min(100, (current / threshold) * 100) if threshold > 0 else 100
            return round(progress, 1)

        except Exception as e:
            logger.error(f"Error calculating achievement progress: {e}")
            return 0

    # =========================
    # Guild Achievement Statistics
    # =========================
    async def get_guild_achievement_stats(self, guild_id: str) -> Dict[str, Any]:
        """Get achievement statistics for entire guild"""
        try:
            # Get all user achievements for guild
            cursor = self.leveling_system.user_achievements.find({"guild_id": guild_id})
            guild_achievements = await cursor.to_list(None)

            if not guild_achievements:
                return {"total_users": 0, "achievements_unlocked": 0}

            stats = {
                "total_users": len(guild_achievements),
                "total_achievements_unlocked": 0,
                "by_category": {},
                "by_rarity": {},
                "most_common_achievements": {},
                "rarest_achievements": {},
                "average_completion": 0
            }

            # Process each user's achievements
            all_unlocked = []
            completion_percentages = []

            # Ensure definitions are loaded
            definitions = await self._ensure_definitions_loaded()

            for user_ach in guild_achievements:
                unlocked = user_ach.get("unlocked", [])
                all_unlocked.extend(unlocked)
                completion_percentages.append(
                    (len(unlocked) / len(definitions)) * 100
                )

            stats["total_achievements_unlocked"] = len(all_unlocked)
            # Avoid float type issues: store as int percentage
            avg_completion = (
            sum(completion_percentages) / len(completion_percentages)) if completion_percentages else 0.0
            stats["average_completion"] = int(round(avg_completion))

            # Count achievement frequencies
            from collections import Counter
            achievement_counts = Counter(all_unlocked)
            stats["most_common_achievements"] = dict(achievement_counts.most_common(10))

            # Categorize achievements
            for ach_id in all_unlocked:
                achievement = next((ach for ach in definitions if ach["id"] == ach_id), None)
                if achievement:
                    category = achievement.get("category", "unknown")
                    rarity = achievement.get("rarity", "common")

                    stats["by_category"][category] = stats["by_category"].get(category, 0) + 1
                    stats["by_rarity"][rarity] = stats["by_rarity"].get(rarity, 0) + 1

            logger.debug(f"Generated guild achievement stats: {ctx(guild_id=guild_id)}")
            return stats

        except Exception as e:
            logger.error(f"Error getting guild achievement stats: {e}")
            return {"error": "Failed to retrieve guild achievement stats"}