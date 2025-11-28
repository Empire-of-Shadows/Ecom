
import logging
from typing import Dict, List, Any, Optional

from database.DatabaseManager import get_collection_registry, get_collection
from ecom_system.achievement_system.progress.level_tracker import LevelingProgressTracker
from ecom_system.achievement_system.progress.message_tracker import MessagesProgressTracker
from ecom_system.achievement_system.progress.voice_tracker import VoiceProgressTracker
from ecom_system.achievement_system.progress.reactions_tracker import ReactionsProgressTracker
from ecom_system.achievement_system.progress.time_based_tracker import TimeBasedProgressTracker
from ecom_system.achievement_system.progress.db_time_tracker import DBTimeProgressTracker

logger = logging.getLogger(__name__)


class LevelProgressHandler:
    """
    Enhanced progress handler for level-based achievements.
    Delegates to specialized trackers for level condition types.
    """

    def __init__(self, progress_system):
        """Initialize with reference to parent AchievementProgressSystem"""
        self.progress_system = progress_system
        self.logger = logger

        # Initialize specialized trackers
        self.leveling_tracker = LevelingProgressTracker(progress_system)

    async def update_progress(self, user_id: str, guild_id: str, activity_data: Dict,
                              unearned_achievements: List[Dict]) -> Dict[str, Dict[str, Any]]:
        """
        Update progress for level achievements using specialized trackers
        """
        try:
            all_progress_updates = {}

            # Leveling progress tracking
            leveling_progress = await self.leveling_tracker.update_progress(
                user_id, guild_id, activity_data, unearned_achievements
            )
            all_progress_updates.update(leveling_progress)

            return all_progress_updates

        except Exception as e:
            logger.error(f"Error in LevelProgressHandler.update_progress: {e}", exc_info=True)
            return {}

    async def get_progress_summary(self, user_id: str, guild_id: str, achievements: List[Dict],
                                   unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get progress summary across all level trackers"""
        try:
            # Get summaries from each tracker
            leveling_summary = await self.leveling_tracker.get_progress_summary(
                user_id, guild_id, achievements, unlocked_ids, progress_data
            )

            return leveling_summary

        except Exception as e:
            logger.error(f"Error getting level progress summary: {e}", exc_info=True)
            return {"total": 0, "completed": 0, "in_progress": 0, "completion_percentage": 0.0}

    async def get_detailed_progress(self, user_id: str, guild_id: str, achievements: List[Dict],
                                    unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get detailed progress across all level trackers"""
        try:
            # Get detailed progress from each tracker
            leveling_details = await self.leveling_tracker.get_detailed_progress(
                user_id, guild_id, achievements, unlocked_ids, progress_data
            )

            return {
                "category": "level",
                "trackers": {
                    "leveling": leveling_details
                },
                "summary": await self.get_progress_summary(user_id, guild_id, achievements, unlocked_ids, progress_data)
            }

        except Exception as e:
            logger.error(f"Error getting detailed level progress: {e}", exc_info=True)
            return {"category": "level", "trackers": {}, "summary": {}}


class MessageProgressHandler:
    """
    Enhanced progress handler for activity-based achievements.
    Delegates to specialized trackers for different condition types.
    """

    def __init__(self, progress_system):
        """Initialize with reference to parent AchievementProgressSystem"""
        self.progress_system = progress_system
        self.logger = logger

        # Initialize specialized trackers
        self.messages_tracker = MessagesProgressTracker(progress_system)
        # Note: Removed leveling_tracker from here since it's now in LevelProgressHandler

    async def update_progress(self, user_id: str, guild_id: str, activity_data: Dict,
                              unearned_achievements: List[Dict]) -> Dict[str, Dict[str, Any]]:
        """
        Update progress for activity achievements using specialized trackers
        """
        try:
            all_progress_updates = {}

            # Messages progress tracking
            messages_progress = await self.messages_tracker.update_progress(
                user_id, guild_id, activity_data, unearned_achievements
            )
            all_progress_updates.update(messages_progress)

            # Future trackers can be added here:
            # voice_progress = await self.voice_tracker.update_progress(...)
            # reaction_progress = await self.reaction_tracker.update_progress(...)

            return all_progress_updates

        except Exception as e:
            logger.error(f"Error in ActivityProgressHandler.update_progress: {e}", exc_info=True)
            return {}

    async def get_progress_summary(self, user_id: str, guild_id: str, achievements: List[Dict],
                                   unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get progress summary across all activity trackers"""
        try:
            # Get summaries from each tracker
            messages_summary = await self.messages_tracker.get_progress_summary(
                user_id, guild_id, achievements, unlocked_ids, progress_data
            )

            return messages_summary

        except Exception as e:
            logger.error(f"Error getting activity progress summary: {e}", exc_info=True)
            return {"total": 0, "completed": 0, "in_progress": 0, "completion_percentage": 0.0}

    async def get_detailed_progress(self, user_id: str, guild_id: str, achievements: List[Dict],
                                    unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get detailed progress across all activity trackers"""
        try:
            # Get detailed progress from each tracker
            messages_details = await self.messages_tracker.get_detailed_progress(
                user_id, guild_id, achievements, unlocked_ids, progress_data
            )

            return {
                "category": "activity",
                "trackers": {
                    "messages": messages_details
                },
                "summary": await self.get_progress_summary(user_id, guild_id, achievements, unlocked_ids, progress_data)
            }

        except Exception as e:
            logger.error(f"Error getting detailed activity progress: {e}", exc_info=True)
            return {"category": "activity", "trackers": {}, "summary": {}}


class VoiceProgressHandler:
    """Handler for voice-based achievements progress tracking"""

    def __init__(self, progress_system):
        """Initialize with reference to parent system"""
        self.progress_system = progress_system
        self.voice_tracker = VoiceProgressTracker(progress_system)
        self.logger = logger

    async def update_progress(self, user_id: str, guild_id: str, activity_data: Dict,
                              unearned_achievements: List[Dict]) -> Dict[str, Dict[str, Any]]:
        """
        Update progress for voice achievements using VoiceProgressTracker

        Args:
            user_id: User ID
            guild_id: Guild ID
            activity_data: Activity data from events
            unearned_achievements: List of unearned voice achievements

        Returns:
            Dict of achievement_id -> progress_data
        """
        try:
            return await self.voice_tracker.update_progress(
                user_id, guild_id, activity_data, unearned_achievements
            )
        except Exception as e:
            logger.error(f"Error in voice progress handler: {e}", exc_info=True)
            return {}

    async def get_progress_summary(self, user_id: str, guild_id: str, achievements: List[Dict],
                                   unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get progress summary for voice achievements"""
        try:
            return await self.voice_tracker.get_progress_summary(
                user_id, guild_id, achievements, unlocked_ids, progress_data
            )
        except Exception as e:
            logger.error(f"Error getting voice progress summary: {e}", exc_info=True)
            return {"total": 0, "completed": 0, "in_progress": 0, "completion_percentage": 0.0}

    async def get_detailed_progress(self, user_id: str, guild_id: str, achievements: List[Dict],
                                    unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get detailed progress for voice achievements"""
        try:
            return await self.voice_tracker.get_detailed_progress(
                user_id, guild_id, achievements, unlocked_ids, progress_data
            )
        except Exception as e:
            logger.error(f"Error getting detailed voice progress: {e}", exc_info=True)
            return {"category": "voice", "achievements": [], "summary": {}}


class ReactionsProgressHandler:
    """Handler for reaction-based achievements progress tracking"""

    def __init__(self, progress_system):
        """Initialize with reference to parent system"""
        self.progress_system = progress_system
        self.reactions_tracker = ReactionsProgressTracker(progress_system)
        self.logger = logger

    async def update_progress(self, user_id: str, guild_id: str, activity_data: Dict,
                              unearned_achievements: List[Dict]) -> Dict[str, Dict[str, Any]]:
        """Update progress for reaction achievements using ReactionsProgressTracker"""
        try:
            return await self.reactions_tracker.update_progress(
                user_id, guild_id, activity_data, unearned_achievements
            )
        except Exception as e:
            logger.error(f"Error in reaction progress handler: {e}", exc_info=True)
            return {}

    async def get_progress_summary(self, user_id: str, guild_id: str, achievements: List[Dict],
                                   unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get progress summary for reaction achievements"""
        try:
            return await self.reactions_tracker.get_progress_summary(
                user_id, guild_id, achievements, unlocked_ids, progress_data
            )
        except Exception as e:
            logger.error(f"Error getting reaction progress summary: {e}", exc_info=True)
            return {"total": 0, "completed": 0, "in_progress": 0, "completion_percentage": 0.0}

    async def get_detailed_progress(self, user_id: str, guild_id: str, achievements: List[Dict],
                                    unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get detailed progress for reaction achievements"""
        try:
            return await self.reactions_tracker.get_detailed_progress(
                user_id, guild_id, achievements, unlocked_ids, progress_data
            )
        except Exception as e:
            logger.error(f"Error getting detailed reaction progress: {e}", exc_info=True)
            return {"category": "reactions", "achievements": [], "summary": {}}


class TimeBasedProgressHandler:
    """Handler for time-based achievements progress tracking"""

    def __init__(self, progress_system):
        """Initialize with reference to parent system"""
        self.progress_system = progress_system
        self.time_based_tracker = TimeBasedProgressTracker(progress_system)
        self.logger = logger

    async def update_progress(self, user_id: str, guild_id: str, activity_data: Dict,
                              unearned_achievements: List[Dict]) -> Dict[str, Dict[str, Any]]:
        """Update progress for time-based achievements using TimeBasedProgressTracker"""
        try:
            return await self.time_based_tracker.update_progress(
                user_id, guild_id, activity_data, unearned_achievements
            )
        except Exception as e:
            logger.error(f"Error in time-based progress handler: {e}", exc_info=True)
            return {}

    async def get_progress_summary(self, user_id: str, guild_id: str, achievements: List[Dict],
                                   unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get progress summary for time-based achievements"""
        try:
            return await self.time_based_tracker.get_progress_summary(
                user_id, guild_id, achievements, unlocked_ids, progress_data
            )
        except Exception as e:
            logger.error(f"Error getting time-based progress summary: {e}", exc_info=True)
            return {"total": 0, "completed": 0, "in_progress": 0, "completion_percentage": 0.0}

    async def get_detailed_progress(self, user_id: str, guild_id: str, achievements: List[Dict],
                                    unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get detailed progress for time-based achievements"""
        try:
            return await self.time_based_tracker.get_detailed_progress(
                user_id, guild_id, achievements, unlocked_ids, progress_data
            )
        except Exception as e:
            logger.error(f"Error getting detailed time-based progress: {e}", exc_info=True)
            return {"category": "time_based", "achievements": [], "summary": {}}


class DBTimeProgressHandler:
    """Handler for DB-backed time-based achievements progress tracking"""

    def __init__(self, progress_system):
        """Initialize with reference to parent system"""
        self.progress_system = progress_system
        self.db_time_tracker = DBTimeProgressTracker(progress_system)
        self.logger = logger

    async def update_progress(self, user_id: str, guild_id: str, activity_data: Dict,
                              unearned_achievements: List[Dict]) -> Dict[str, Dict[str, Any]]:
        """Update progress for DB-backed time-based achievements using DBTimeProgressTracker"""
        try:
            return await self.db_time_tracker.update_progress(
                user_id, guild_id, activity_data, unearned_achievements
            )
        except Exception as e:
            logger.error(f"Error in DB time progress handler: {e}", exc_info=True)
            return {}

    async def get_progress_summary(self, user_id: str, guild_id: str, achievements: List[Dict],
                                   unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get progress summary for DB-backed time-based achievements"""
        try:
            return await self.db_time_tracker.get_progress_summary(
                user_id, guild_id, achievements, unlocked_ids, progress_data
            )
        except Exception as e:
            logger.error(f"Error getting DB time progress summary: {e}", exc_info=True)
            return {"total": 0, "completed": 0, "in_progress": 0, "completion_percentage": 0.0}

    async def get_detailed_progress(self, user_id: str, guild_id: str, achievements: List[Dict],
                                    unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get detailed progress for DB-backed time-based achievements"""
        try:
            return await self.db_time_tracker.get_detailed_progress(
                user_id, guild_id, achievements, unlocked_ids, progress_data
            )
        except Exception as e:
            logger.error(f"Error getting detailed DB time progress: {e}", exc_info=True)
            return {"category": "db_time", "achievements": [], "summary": {}}


class AchievementProgressSystem:
    """
    Standalone achievement progress tracking system.
    Handles accessing category-specific progress handlers and provides unified interface.
    """

    # ===== INITIALIZATION =====
    def __init__(self, database_manager):
        """Initialize with database manager for data access"""
        self.db = database_manager
        self.logger = logger

        # Initialize category handlers
        self.category_handlers = {
            "message": MessageProgressHandler(self),
            "level": LevelProgressHandler(self),
            "voice": VoiceProgressHandler(self),
            "reactions": ReactionsProgressHandler(self),
            "time_based": TimeBasedProgressHandler(self),
            "db_time": DBTimeProgressHandler(self),
            # Future categories can be added here
        }
        logger.info("🏆 AchievementProgressSystem initialized with specialized trackers")

    # ===== STATIC PROGRESS UPDATE METHOD =====
    @staticmethod
    async def update_achievement_progress_tracking(user_id: str, guild_id: str, activity_data: Dict,
                                                   achievement_definitions: Dict | List[Dict] | None,
                                                   activity_buffer: Any):
        """
        Standalone function to update progress after the achievement system processes unlocks.
        This updates percentage progress for incremental achievements.

        Requirements addressed:
        - Read and write user achievement progress from Users.AcheievementProgress
        - Read user stats from Users.Stats
        - Load Activity achievements from Achievements.Activity if not provided
        """
        try:
            # Collections needed
            user_achievements_collection = get_collection("Users", "AcheievementProgress")
            user_stats_collection = get_collection("Users", "Stats")

            if user_achievements_collection is None:
                logger.error("Cannot access Users.AcheievementProgress collection for progress tracking")
                return
            if user_stats_collection is None:
                logger.error("Cannot access Users.Stats collection for progress tracking")
                return

            # Get all achievement collections dynamically
            collection_registry = get_collection_registry()
            achievements_collections = {}

            if "achievements" in collection_registry:
                for collection_name, collection_ref in collection_registry["achievements"].items():
                    # Skip Settings collection as mentioned
                    if collection_name.lower() != "settings":
                        achievements_collections[collection_name.lower()] = collection_ref
                        logger.debug(f"Added {collection_name} collection for progress tracking")
            else:
                logger.error("No Achievements database found in collection registry")
                return

            if not achievements_collections:
                logger.error("No achievement collections found for progress tracking")
                return

            # Create temporary progress system instance for processing, wiring required collections
            temp_db = type('TempDB', (), {
                'user_achievements': user_achievements_collection,  # Users.AcheievementProgress
                'users': user_stats_collection,  # Users.Stats (used by trackers)
                'local_db_path': activity_buffer.local_db.db_path if activity_buffer and hasattr(activity_buffer, 'local_db') else None,
                **{f'achievements_{name}': collection for name, collection in achievements_collections.items()}
            })()

            progress_system = AchievementProgressSystem(temp_db)

            logger.debug(f"[Progress] Starting update_achievement_progress_tracking for G:{guild_id} U:{user_id}")

            # Normalize or load definitions
            definitions_by_category = await progress_system._normalize_or_load_definitions(achievement_definitions)

            # Update progress for all categories
            progress_updates = await progress_system.update_progress(
                user_id, guild_id, activity_data, definitions_by_category
            )

            logger.debug(f"[Progress] Category updates computed: {list(progress_updates.keys())}")

            if progress_updates:
                # Get current user achievements to check for unlocks
                user_achievements = await progress_system._get_user_achievements(user_id, guild_id)
                unlocked_ids = user_achievements.get("unlocked", [])

                # Check each updated achievement for completion
                achievements_to_remove = []
                for category_name, category_progress in progress_updates.items():
                    for achievement_id, progress_data in category_progress.items():
                        progress_percentage = progress_data.get("progress_percentage", 0)

                        if progress_percentage >= 100:
                            if achievement_id in unlocked_ids:
                                achievements_to_remove.append(achievement_id)
                                logger.debug(f"Removing {achievement_id} from progress tracking - achievement unlocked")
                            else:
                                logger.warning(
                                    f"Achievement {achievement_id} shows 100% progress but is not unlocked for user {user_id} in guild {guild_id}."
                                )

                # Remove completed achievements from progress tracking
                if achievements_to_remove:
                    unset_data = {}
                    for achievement_id in achievements_to_remove:
                        unset_data[f"progress.{achievement_id}"] = ""

                    await user_achievements_collection.update_one(
                        {"user_id": user_id, "guild_id": guild_id},
                        {
                            "$unset": unset_data,
                            "$set": {"updated_at": progress_system._utc_now_ts()}
                        }
                    )

                    logger.info(
                        f"Removed {len(achievements_to_remove)} completed achievements from progress tracking for user {user_id}"
                    )

                logger.debug(f"Achievement progress tracking updated for user {user_id} in guild {guild_id}")
            else:
                logger.debug(f"[Progress] No progress updates for G:{guild_id} U:{user_id}")

        except Exception as e:
            logger.error(f"Error in update_achievement_progress_tracking: {e}", exc_info=True)

    async def update_progress(self, user_id: str, guild_id: str, activity_data: Dict,
                              achievement_definitions: Dict[str, List[Dict]]):
        """
        Main method to update achievement progress across all categories
        """
        try:
            # Get user's current achievements and progress
            user_achievements = await self._get_user_achievements(user_id, guild_id)

            # Filter out already unlocked achievements
            unlocked_ids = user_achievements.get("unlocked", [])

            # Update progress for each category
            progress_updates: Dict[str, Dict[str, Any]] = {}

            for category_name, handler in self.category_handlers.items():
                try:
                    category_achievements = achievement_definitions.get(category_name, [])

                    # Filter to only unearned achievements in this category
                    unearned_achievements = [
                        ach for ach in category_achievements
                        if ach.get("id") not in unlocked_ids and ach.get("enabled", True)
                    ]

                    if unearned_achievements:
                        category_progress = await handler.update_progress(
                            user_id, guild_id, activity_data, unearned_achievements
                        )
                        if category_progress:
                            progress_updates[category_name] = category_progress

                except Exception as e:
                    logger.error(f"Error updating progress for category {category_name}: {e}", exc_info=True)
                    continue

            # Save progress updates to database
            if progress_updates:
                await self._save_progress_updates(user_id, guild_id, progress_updates)

            return progress_updates

        except Exception as e:
            logger.error(f"Error in update_progress: {e}", exc_info=True)
            return {}

    async def get_category_progress(self, user_id: str, guild_id: str, category: str,
                                    achievements: Optional[List[Dict]] = None,
                                    include_detailed: bool = False) -> Dict[str, Any]:
        """
        Get progress for a specific category of achievements
        """
        try:
            # Validate category exists
            if category not in self.category_handlers:
                logger.warning(f"Unknown category requested: {category}")
                return {"error": f"Unknown category: {category}"}

            handler = self.category_handlers[category]

            # Get user achievements
            user_achievements = await self._get_user_achievements(user_id, guild_id)
            unlocked_ids = user_achievements.get("unlocked", [])
            progress_data = user_achievements.get("progress", {})

            # Use provided achievements or load from database
            if achievements is None:
                achievements = await self._load_category_achievements(category)

            if include_detailed:
                return await handler.get_detailed_progress(
                    user_id, guild_id, achievements, unlocked_ids, progress_data
                )
            else:
                return await handler.get_progress_summary(
                    user_id, guild_id, achievements, unlocked_ids, progress_data
                )

        except Exception as e:
            logger.error(f"Error getting category progress for {category}: {e}", exc_info=True)
            return {"error": f"Failed to get progress for category {category}"}

    # ===== HELPER METHODS =====
    async def _get_user_achievements(self, user_id: str, guild_id: str) -> Dict[str, Any]:
        """Get user achievements from database"""
        try:
            user_achievements = await self.db.user_achievements.find_one(
                {"user_id": user_id, "guild_id": guild_id}
            )

            if not user_achievements:
                return {
                    "user_id": user_id,
                    "guild_id": guild_id,
                    "unlocked": [],
                    "progress": {},
                    "created_at": self._utc_now_ts(),
                    "updated_at": self._utc_now_ts()
                }

            return user_achievements

        except Exception as e:
            logger.error(f"Error getting user achievements: {e}", exc_info=True)
            return {
                "user_id": user_id,
                "guild_id": guild_id,
                "unlocked": [],
                "progress": {},
                "created_at": self._utc_now_ts(),
                "updated_at": self._utc_now_ts()
            }

    async def _save_progress_updates(self, user_id: str, guild_id: str, progress_updates: Dict[str, Dict[str, Any]]):
        """Save progress updates to database"""
        try:
            # Flatten progress updates for database storage
            flattened_updates = {}
            for category, achievements in progress_updates.items():
                for achievement_id, progress_data in achievements.items():
                    flattened_updates[f"progress.{achievement_id}"] = progress_data

            if flattened_updates:
                flattened_updates["updated_at"] = self._utc_now_ts()

                await self.db.user_achievements.update_one(
                    {"user_id": user_id, "guild_id": guild_id},
                    {
                        "$set": flattened_updates,
                        "$setOnInsert": {"created_at": self._utc_now_ts(), "unlocked": []}
                    },
                    upsert=True
                )

                logger.debug(f"Progress updates saved for user {user_id} in guild {guild_id}")

        except Exception as e:
            logger.error(f"Error saving progress updates: {e}", exc_info=True)

    async def _normalize_or_load_definitions(self, achievement_definitions):
        """
        Normalize achievement definitions to a dict by category.
        Input can be None, Dict, or List[Dict].
        """
        if achievement_definitions is None:
            # Load all definitions from DB
            return await self._load_all_achievement_definitions_from_db()

        elif isinstance(achievement_definitions, dict):
            # Already categorized, return as-is
            return achievement_definitions

        elif isinstance(achievement_definitions, list):
            # Group by category
            definitions_by_category = {}
            for achievement in achievement_definitions:
                if isinstance(achievement, dict) and "category" in achievement:
                    category = achievement["category"]
                    if category not in definitions_by_category:
                        definitions_by_category[category] = []
                    definitions_by_category[category].append(achievement)
            return definitions_by_category

        else:
            logger.warning(f"Unknown achievement definitions type: {type(achievement_definitions)}")
            return {}

    async def _load_all_achievement_definitions_from_db(self) -> Dict[str, List[Dict]]:
        """Load all achievement definitions from database collections"""
        try:
            all_achievements = {}

            # Get all achievement collection references from database manager
            for attr_name in dir(self.db):
                if attr_name.startswith('achievements_'):
                    category_name = attr_name.replace('achievements_', '')
                    collection = getattr(self.db, attr_name)

                    try:
                        achievements = await collection.find().to_list(length=None)
                        if achievements:
                            all_achievements[category_name] = achievements
                            logger.debug(f"Loaded {len(achievements)} achievements from {category_name} category")
                    except Exception as e:
                        logger.error(f"Error loading achievements from {category_name}: {e}")
                        all_achievements[category_name] = []

            return all_achievements

        except Exception as e:
            logger.error(f"Error loading all achievement definitions: {e}", exc_info=True)
            return {}

    async def _load_activity_definitions_from_db(self) -> List[Dict]:
        """Load activity achievement definitions from database"""
        try:
            if hasattr(self.db, 'achievements_activity'):
                achievements = await self.db.achievements_activity.find().to_list(length=None)
                logger.debug(f"Loaded {len(achievements)} activity achievements from database")
                return achievements
            else:
                logger.warning("No activity achievements collection found")
                return []

        except Exception as e:
            logger.error(f"Error loading activity achievements: {e}", exc_info=True)
            return []

    async def _load_category_achievements(self, category: str) -> List[Dict]:
        """Load achievements for a specific category"""
        try:
            collection_name = f'achievements_{category}'
            if hasattr(self.db, collection_name):
                collection = getattr(self.db, collection_name)
                achievements = await collection.find().to_list(length=None)
                logger.debug(f"Loaded {len(achievements)} achievements for category {category}")
                return achievements
            else:
                logger.warning(f"No collection found for category: {category}")
                return []

        except Exception as e:
            logger.error(f"Error loading achievements for category {category}: {e}", exc_info=True)
            return []

    def _utc_now_ts(self) -> float:
        """Get current UTC timestamp"""
        import time
        return time.time()