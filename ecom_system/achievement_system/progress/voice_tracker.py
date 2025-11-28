import logging
from typing import Dict, Any, List, Optional
import time

logger = logging.getLogger(__name__)


class VoiceProgressTracker:
    """
    Dedicated progress tracker for voice-based achievements.

    Handles tracking progress for achievements like:
    - voice_hours_50, voice_hours_100, voice_hours_200, etc.
    - voice_sessions_100, voice_sessions_500, voice_sessions_1000, etc.
    - Any voice time or session count-based achievements
    """

    def __init__(self, progress_system):
        """Initialize with reference to parent AchievementProgressSystem"""
        self.progress_system = progress_system
        self.logger = logger

    async def update_progress(self, user_id: str, guild_id: str, activity_data: Dict,
                              unearned_achievements: List[Dict]) -> Dict[str, Dict[str, Any]]:
        """
        Update progress for voice-based achievements

        Args:
            user_id: User ID
            guild_id: Guild ID
            activity_data: Activity data from events
            unearned_achievements: List of voice achievements not yet unlocked

        Returns:
            Dict of achievement_id -> progress_data for achievements that had progress updates
        """
        try:
            # Get user's current voice stats
            user_stats = await self._get_user_stats(user_id, guild_id)
            voice_stats = self._extract_voice_stats(user_stats)

            progress_updates = {}

            # Process each voice-based achievement
            for achievement in unearned_achievements:
                if not self._is_voice_achievement(achievement):
                    continue

                achievement_id = achievement.get("id")
                if not achievement_id:
                    continue

                # Calculate progress for this achievement
                progress_data = await self._calculate_voice_progress(
                    achievement, voice_stats, user_id, guild_id
                )

                if progress_data:
                    progress_updates[achievement_id] = progress_data
                    logger.debug(
                        f"Updated voice progress for {achievement_id}: {progress_data['progress_percentage']:.1f}%")

            return progress_updates

        except Exception as e:
            logger.error(f"Error updating voice progress for user {user_id}: {e}", exc_info=True)
            return {}

    def _is_voice_achievement(self, achievement: Dict) -> bool:
        """Check if this is a voice-based achievement"""
        conditions = achievement.get("conditions", {})
        condition_type = conditions.get("type")

        # Check for explicit voice condition types
        if condition_type in ["voice_time", "voice_sessions"]:
            return True

        # Check for field-based conditions that reference voice stats
        if condition_type == "field":
            field = conditions.get("data", {}).get("field", "")
            if "voice" in field.lower():
                return True

        # Check achievement ID patterns
        achievement_id = achievement.get("id", "")
        if achievement_id.startswith("voice_") or "voice" in achievement_id.lower():
            return True

        # Check category
        category = achievement.get("category", "")
        if category.lower() == "voice":
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

    def _extract_voice_stats(self, user_stats: Dict) -> Dict:
        """Extract voice statistics from user stats"""
        try:
            voice_stats = user_stats.get("voice_stats", {})

            # Return a normalized structure that matches the actual database fields
            return {
                # Map actual database fields
                "total_time": voice_stats.get("voice_seconds", 0.0),  # voice_seconds from actual structure
                "total_time_hours": voice_stats.get("voice_seconds", 0.0) / 3600.0,  # convert to hours
                "sessions": voice_stats.get("voice_sessions", 0),  # voice_sessions from actual structure
                "active_time": voice_stats.get("active_seconds", 0.0),  # active_seconds from actual structure
                "average_session_length": voice_stats.get("average_session_length", 0.0),

                # Additional fields that are available in the actual structure
                "active_seconds": voice_stats.get("active_seconds", 0.0),
                "voice_seconds": voice_stats.get("voice_seconds", 0.0),
                "deafened_time": voice_stats.get("deafened_time", 0.0),
                "muted_time": voice_stats.get("muted_time", 0.0),
                "self_deafened_time": voice_stats.get("self_deafened_time", 0.0),
                "self_muted_time": voice_stats.get("self_muted_time", 0.0),
                "total_active_percentage": voice_stats.get("total_active_percentage", 0.0),
                "total_unmuted_percentage": voice_stats.get("total_unmuted_percentage", 0.0),

                # Today/weekly/monthly stats
                "today_embers": voice_stats.get("today_embers", 0),
                "today_xp": voice_stats.get("today_xp", 0),
                "weekly_embers": voice_stats.get("weekly_embers", 0),
                "weekly_xp": voice_stats.get("weekly_xp", 0),
                "monthly_embers": voice_stats.get("monthly_embers", 0),
                "monthly_xp": voice_stats.get("monthly_xp", 0),
                "today_key": voice_stats.get("today_key", ""),
            }
        except Exception as e:
            logger.error(f"Error extracting voice stats: {e}")
            return {
                "total_time": 0.0,
                "total_time_hours": 0.0,
                "sessions": 0,
                "active_time": 0.0,
                "average_session_length": 0.0,
                "active_seconds": 0.0,
                "voice_seconds": 0.0,
                "deafened_time": 0.0,
                "muted_time": 0.0,
                "self_deafened_time": 0.0,
                "self_muted_time": 0.0,
                "total_active_percentage": 0.0,
                "total_unmuted_percentage": 0.0,
                "today_embers": 0,
                "today_xp": 0,
                "weekly_embers": 0,
                "weekly_xp": 0,
                "monthly_embers": 0,
                "monthly_xp": 0,
                "today_key": "",
            }

    async def _calculate_voice_progress(self, achievement: Dict, voice_stats: Dict,
                                        user_id: str, guild_id: str) -> Optional[Dict[str, Any]]:
        """Calculate progress for a specific voice achievement"""
        try:
            conditions = achievement.get("conditions", {})
            condition_type = conditions.get("type")
            condition_data = conditions.get("data", {})

            # Get target value and current value based on condition type
            target_value, current_value, field_path = self._get_voice_values(
                condition_type, condition_data, voice_stats, achievement
            )

            if target_value is None or target_value <= 0:
                logger.warning(f"Invalid target value for voice achievement {achievement.get('id')}")
                return None

            # Calculate progress percentage
            progress_percentage = min((current_value / target_value) * 100, 100.0)

            progress_data = {
                "last_updated": time.time(),
                "condition_type": condition_type,
                "current_value": current_value,
                "target_value": target_value,
                "progress_percentage": progress_percentage,
                "field": field_path
            }

            return progress_data

        except Exception as e:
            logger.error(f"Error calculating voice progress: {e}", exc_info=True)
            return None

    def _get_voice_values(self, condition_type: str, condition_data: Dict,
                          voice_stats: Dict, achievement: Dict) -> tuple:
        """Get target value, current value, and field path for voice achievement"""
        try:
            # Default values
            target_value = None
            current_value = 0
            field_path = "voice_stats"

            if condition_type == "voice_time":
                # Voice time achievements (threshold usually in seconds)
                target_value = condition_data.get("threshold")
                field = condition_data.get("field", "voice_stats.voice_seconds")

                if "voice_seconds" in field or "total_time" in field:
                    current_value = voice_stats.get("voice_seconds", 0.0)  # Use actual field
                    field_path = field
                elif "active_seconds" in field or "active_time" in field:
                    current_value = voice_stats.get("active_seconds", 0.0)  # Use actual field
                    field_path = field
                else:
                    # Fallback to voice_seconds
                    current_value = voice_stats.get("voice_seconds", 0.0)
                    field_path = "voice_stats.voice_seconds"

            elif condition_type == "voice_sessions":
                # Voice session count achievements
                target_value = condition_data.get("threshold")
                field = condition_data.get("field", "voice_stats.voice_sessions")

                current_value = voice_stats.get("sessions", 0)  # Using mapped field
                field_path = field

            elif condition_type == "field":
                # Generic field-based condition
                field = condition_data.get("field", "")
                target_value = condition_data.get("threshold")

                if "voice_stats.voice_seconds" in field:
                    current_value = voice_stats.get("voice_seconds", 0.0)
                elif "voice_stats.active_seconds" in field:
                    current_value = voice_stats.get("active_seconds", 0.0)
                elif "voice_stats.voice_sessions" in field:
                    current_value = voice_stats.get("sessions", 0)
                elif "voice_stats.total_time" in field:
                    # Legacy field mapping
                    current_value = voice_stats.get("voice_seconds", 0.0)
                else:
                    # Try to extract from field path
                    current_value = self._extract_nested_value(voice_stats, field)

                field_path = field

            else:
                # Try to extract from achievement ID or other metadata
                target_value = self._extract_target_from_metadata(achievement, condition_data)
                achievement_id = achievement.get("id", "")

                if "hours" in achievement_id.lower():
                    current_value = voice_stats.get("total_time_hours", 0.0)
                    field_path = "voice_stats.voice_seconds"
                elif "sessions" in achievement_id.lower():
                    current_value = voice_stats.get("sessions", 0)
                    field_path = "voice_stats.voice_sessions"

            return target_value, current_value, field_path

        except Exception as e:
            logger.error(f"Error getting voice values: {e}")
            return None, 0, "voice_stats"

    def _extract_nested_value(self, data: Dict, field_path: str) -> float:
        """Extract value from nested field path"""
        try:
            parts = field_path.split(".")
            value = data
            for part in parts:
                if isinstance(value, dict) and part in value:
                    value = value[part]
                else:
                    return 0
            return float(value) if value is not None else 0
        except (ValueError, TypeError):
            return 0

    def _extract_target_from_metadata(self, achievement: Dict, condition_data: Dict) -> Optional[float]:
        """Extract target value from achievement metadata"""
        # Try condition data first
        threshold = condition_data.get("threshold")
        if threshold is not None:
            try:
                return float(threshold)
            except (ValueError, TypeError):
                pass

        # Try to extract from achievement ID
        achievement_id = achievement.get("id", "")

        # Handle voice_hours_X pattern
        if achievement_id.startswith("voice_hours_"):
            try:
                hours_str = achievement_id.replace("voice_hours_", "")
                hours = float(hours_str)
                # Convert hours to seconds for comparison
                return hours * 3600.0
            except (ValueError, TypeError):
                pass

        # Handle voice_sessions_X pattern
        if achievement_id.startswith("voice_sessions_"):
            try:
                sessions_str = achievement_id.replace("voice_sessions_", "")
                return float(sessions_str)
            except (ValueError, TypeError):
                pass

        # Try achievement metadata
        metadata = achievement.get("metadata", {})
        for key in ["target", "required", "count", "threshold"]:
            if key in metadata:
                try:
                    return float(metadata[key])
                except (ValueError, TypeError):
                    continue

        return None

    async def get_progress_summary(self, user_id: str, guild_id: str, achievements: List[Dict],
                                   unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get progress summary for voice achievements"""
        try:
            voice_achievements = [ach for ach in achievements if self._is_voice_achievement(ach)]

            if not voice_achievements:
                return {
                    "total": 0,
                    "completed": 0,
                    "in_progress": 0,
                    "completion_percentage": 0.0
                }

            total = len(voice_achievements)
            completed = len([ach for ach in voice_achievements if ach.get("id") in unlocked_ids])
            in_progress = len([ach for ach in voice_achievements
                               if ach.get("id") in progress_data and ach.get("id") not in unlocked_ids])

            completion_percentage = (completed / total * 100) if total > 0 else 0.0

            return {
                "total": total,
                "completed": completed,
                "in_progress": in_progress,
                "completion_percentage": round(completion_percentage, 1)
            }

        except Exception as e:
            logger.error(f"Error getting voice progress summary: {e}", exc_info=True)
            return {"total": 0, "completed": 0, "in_progress": 0, "completion_percentage": 0.0}

    async def get_detailed_progress(self, user_id: str, guild_id: str, achievements: List[Dict],
                                    unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get detailed progress for voice achievements"""
        try:
            voice_achievements = [ach for ach in achievements if self._is_voice_achievement(ach)]

            detailed_progress = {
                "category": "voice",
                "achievements": [],
                "summary": await self.get_progress_summary(user_id, guild_id, achievements, unlocked_ids, progress_data)
            }

            # Get current voice stats
            user_stats = await self._get_user_stats(user_id, guild_id)
            voice_stats = self._extract_voice_stats(user_stats)

            for achievement in voice_achievements:
                achievement_id = achievement.get("id")

                # Check if unlocked
                if achievement_id in unlocked_ids:
                    status = "completed"
                    progress_info = {
                        "current_value": self._get_current_value_for_achievement(achievement, voice_stats),
                        "target_value": self._get_target_value_for_achievement(achievement),
                        "progress_percentage": 100.0
                    }
                elif achievement_id in progress_data:
                    status = "in_progress"
                    progress_info = progress_data[achievement_id]
                else:
                    status = "locked"
                    current_value = self._get_current_value_for_achievement(achievement, voice_stats)
                    target_value = self._get_target_value_for_achievement(achievement)
                    progress_info = {
                        "current_value": current_value,
                        "target_value": target_value,
                        "progress_percentage": min((current_value / target_value * 100),
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
            logger.error(f"Error getting detailed voice progress: {e}", exc_info=True)
            return {"category": "voice", "achievements": [], "summary": {}}

    def _get_current_value_for_achievement(self, achievement: Dict, voice_stats: Dict) -> float:
        """Get current value for a specific achievement"""
        conditions = achievement.get("conditions", {})
        condition_type = conditions.get("type")
        condition_data = conditions.get("data", {})

        if condition_type == "voice_time":
            return voice_stats.get("voice_seconds", 0.0)  # Use actual database field
        elif condition_type == "voice_sessions":
            return voice_stats.get("sessions", 0)  # Using mapped field from extraction
        else:
            # Fallback based on achievement ID
            achievement_id = achievement.get("id", "")
            if "hours" in achievement_id.lower():
                return voice_stats.get("total_time_hours", 0.0)
            elif "sessions" in achievement_id.lower():
                return voice_stats.get("sessions", 0)
            return 0

    def _get_target_value_for_achievement(self, achievement: Dict) -> float:
        """Get target value for a specific achievement"""
        conditions = achievement.get("conditions", {})
        condition_data = conditions.get("data", {})

        target = self._extract_target_from_metadata(achievement, condition_data)
        return target if target is not None else 0