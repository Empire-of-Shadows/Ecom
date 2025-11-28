import logging
from typing import Dict, Any, List, Optional, Tuple
import time

logger = logging.getLogger(__name__)


class MessagesProgressTracker:
    """
    Dedicated progress tracker for message-based achievements.

    Handles tracking progress for achievements like:
    - messages_10, messages_100, messages_1000, messages_5000, messages_10000
    - first_message
    - Any message count-based achievements
    """

    def __init__(self, progress_system):
        """Initialize with reference to parent AchievementProgressSystem"""
        self.progress_system = progress_system
        self.logger = logger

    async def update_progress(self, user_id: str, guild_id: str, activity_data: Dict,
                              unearned_achievements: List[Dict]) -> Dict[str, Dict[str, Any]]:
        """
        Update progress for message-based achievements

        Args:
            user_id: User ID
            guild_id: Guild ID
            activity_data: Activity data from events
            unearned_achievements: List of message achievements not yet unlocked

        Returns:
            Dict of achievement_id -> progress_data for achievements that had progress updates
        """
        try:
            # Get user's current message stats
            user_stats = await self._get_user_stats(user_id, guild_id)
            current_messages = self._get_current_message_count(user_stats)

            progress_updates = {}

            # Process each message-based achievement
            for achievement in unearned_achievements:
                if not self._is_message_achievement(achievement):
                    continue

                achievement_id = achievement.get("id")
                if not achievement_id:
                    continue

                # Calculate progress for this achievement
                progress_data = await self._calculate_message_progress(
                    achievement, current_messages, user_id, guild_id
                )

                if progress_data:
                    progress_updates[achievement_id] = progress_data
                    logger.debug(f"Updated progress for {achievement_id}: {progress_data['progress_percentage']:.1f}%")

            return progress_updates

        except Exception as e:
            logger.error(f"Error updating message progress for user {user_id}: {e}", exc_info=True)
            return {}

    def _is_message_achievement(self, achievement: Dict) -> bool:
        """Check if this is a message-based achievement"""
        conditions = achievement.get("conditions", {})
        condition_type = conditions.get("type")

        # Check for explicit message condition type
        if condition_type == "messages":
            return True

        # Check for field-based conditions that reference message stats
        if condition_type == "field":
            field = conditions.get("data", {}).get("field", "")
            if "message" in field.lower():
                return True

        # Check achievement ID patterns
        achievement_id = achievement.get("id", "")
        if achievement_id.startswith("messages_") or achievement_id == "first_message":
            return True

        return False

    async def _get_user_stats(self, user_id: str, guild_id: str) -> Dict:
        """Get user statistics from Users.Stats collection"""
        try:
            user_stats = await self.progress_system.db.users.find_one(
                {"user_id": user_id, "guild_id": guild_id}
            )
            return user_stats or {}
        except Exception as e:
            logger.error(f"Error getting user stats: {e}", exc_info=True)
            return {}

    def _get_current_message_count(self, user_stats: Dict) -> int:
        """Extract current message count from user stats"""
        try:
            # Check various possible locations for message count
            message_stats = user_stats.get("message_stats", {})

            # Try different field names that might contain message count
            possible_fields = ["messages", "total_messages", "message_count", "count"]

            for field in possible_fields:
                if field in message_stats:
                    return int(message_stats[field])

            # Check top-level fields
            for field in possible_fields:
                if field in user_stats:
                    return int(user_stats[field])

            # Default to 0 if no message count found
            return 0

        except (ValueError, TypeError) as e:
            logger.error(f"Error extracting message count from user stats: {e}")
            return 0

    async def _calculate_message_progress(self, achievement: Dict, current_messages: int,
                                          user_id: str, guild_id: str) -> Optional[Dict[str, Any]]:
        """Calculate progress for a specific message achievement"""
        try:
            conditions = achievement.get("conditions", {})
            condition_data = conditions.get("data", {})

            # Get target value from different possible locations
            target_value = self._extract_target_value(achievement, condition_data)

            if target_value is None or target_value <= 0:
                logger.warning(f"Invalid target value for achievement {achievement.get('id')}")
                return None

            # Calculate progress percentage
            progress_percentage = min((current_messages / target_value) * 100, 100.0)

            # Determine field path for this achievement
            field_path = self._determine_field_path(achievement, condition_data)

            progress_data = {
                "last_updated": time.time(),
                "condition_type": "messages",
                "current_value": current_messages,
                "target_value": target_value,
                "progress_percentage": progress_percentage,
                "field": field_path
            }

            return progress_data

        except Exception as e:
            logger.error(f"Error calculating message progress: {e}", exc_info=True)
            return None

    def _extract_target_value(self, achievement: Dict, condition_data: Dict) -> Optional[int]:
        """Extract target message count from achievement definition"""
        # Try condition data first
        threshold = condition_data.get("threshold")
        if threshold is not None:
            try:
                return int(threshold)
            except (ValueError, TypeError):
                pass

        # Try to extract from achievement ID (e.g., "messages_1000" -> 1000)
        achievement_id = achievement.get("id", "")
        if achievement_id.startswith("messages_"):
            try:
                number_part = achievement_id.replace("messages_", "")
                return int(number_part)
            except (ValueError, TypeError):
                pass

        # Special case for first_message
        if achievement_id == "first_message":
            return 1

        # Try achievement metadata
        metadata = achievement.get("metadata", {})
        for key in ["target", "required", "count", "threshold"]:
            if key in metadata:
                try:
                    return int(metadata[key])
                except (ValueError, TypeError):
                    continue

        return None

    def _determine_field_path(self, achievement: Dict, condition_data: Dict) -> str:
        """Determine the field path for this message achievement"""
        # Check if field is explicitly specified
        field = condition_data.get("field")
        if field:
            return field

        # Default field path for message stats
        return "message_stats.messages"

    async def get_progress_summary(self, user_id: str, guild_id: str, achievements: List[Dict],
                                   unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get progress summary for message achievements"""
        try:
            message_achievements = [ach for ach in achievements if self._is_message_achievement(ach)]

            if not message_achievements:
                return {
                    "total": 0,
                    "completed": 0,
                    "in_progress": 0,
                    "completion_percentage": 0.0
                }

            total = len(message_achievements)
            completed = len([ach for ach in message_achievements if ach.get("id") in unlocked_ids])
            in_progress = len([ach for ach in message_achievements
                               if ach.get("id") in progress_data and ach.get("id") not in unlocked_ids])

            completion_percentage = (completed / total * 100) if total > 0 else 0.0

            return {
                "total": total,
                "completed": completed,
                "in_progress": in_progress,
                "completion_percentage": round(completion_percentage, 1)
            }

        except Exception as e:
            logger.error(f"Error getting message progress summary: {e}", exc_info=True)
            return {"total": 0, "completed": 0, "in_progress": 0, "completion_percentage": 0.0}

    async def get_detailed_progress(self, user_id: str, guild_id: str, achievements: List[Dict],
                                    unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get detailed progress for message achievements"""
        try:
            message_achievements = [ach for ach in achievements if self._is_message_achievement(ach)]

            detailed_progress = {
                "category": "messages",
                "achievements": [],
                "summary": await self.get_progress_summary(user_id, guild_id, achievements, unlocked_ids, progress_data)
            }

            # Get current message count
            user_stats = await self._get_user_stats(user_id, guild_id)
            current_messages = self._get_current_message_count(user_stats)

            for achievement in message_achievements:
                achievement_id = achievement.get("id")

                # Check if unlocked
                if achievement_id in unlocked_ids:
                    status = "completed"
                    progress_info = {
                        "current_value": current_messages,
                        "target_value": self._extract_target_value(achievement,
                                                                   achievement.get("conditions", {}).get("data", {})),
                        "progress_percentage": 100.0
                    }
                elif achievement_id in progress_data:
                    status = "in_progress"
                    progress_info = progress_data[achievement_id]
                else:
                    status = "locked"
                    target_value = self._extract_target_value(achievement,
                                                              achievement.get("conditions", {}).get("data", {}))
                    progress_info = {
                        "current_value": current_messages,
                        "target_value": target_value,
                        "progress_percentage": min((current_messages / target_value * 100),
                                                   100.0) if target_value else 0.0
                    }

                detailed_progress["achievements"].append({
                    "id": achievement_id,
                    "name": achievement.get("name", achievement_id),
                    "description": achievement.get("description", ""),
                    "status": status,
                    "progress": progress_info
                })

            return detailed_progress

        except Exception as e:
            logger.error(f"Error getting detailed message progress: {e}", exc_info=True)
            return {"category": "messages", "achievements": [], "summary": {}}