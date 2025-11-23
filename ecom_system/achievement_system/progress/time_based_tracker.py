import logging
from typing import Dict, Any, List, Optional
import time

from ecom_system.helpers.helpers import utc_now_ts
from loggers.logger_setup import get_logger

logger = get_logger("TimeBasedTracker", level=logging.DEBUG, json_format=False, colored_console=True)


class TimeBasedProgressTracker:
    """
    Dedicated progress tracker for time-based achievements.
    """

    def __init__(self, progress_system):
        """Initialize with reference to parent AchievementProgressSystem"""
        self.progress_system = progress_system
        self.logger = logger

    async def update_progress(self, user_id: str, guild_id: str, activity_data: Dict,
                              unearned_achievements: List[Dict]) -> Dict[str, Dict[str, Any]]:
        """
        Update progress for time-based achievements.
        """
        progress_updates = {}
        user_stats = await self._get_user_stats(user_id, guild_id)
        
        for achievement in unearned_achievements:
            if not self._is_time_based_achievement(achievement):
                continue

            achievement_id = achievement.get("id")
            if not achievement_id:
                continue

            progress_data = self._calculate_time_based_progress(achievement, user_stats)
            if progress_data:
                progress_updates[achievement_id] = progress_data
        
        return progress_updates

    def _is_time_based_achievement(self, achievement: Dict) -> bool:
        """Check if this is a time-based achievement."""
        return achievement.get("conditions", {}).get("type") == "time_based"

    async def _get_user_stats(self, user_id: str, guild_id: str) -> Dict:
        """Get user statistics from Users.Stats collection."""
        try:
            user_stats = await self.progress_system.db.users.find_one(
                {"user_id": user_id, "guild_id": guild_id}
            )
            return user_stats or {}
        except Exception as e:
            self.logger.error(f"Error getting user stats: {e}", exc_info=True)
            return {}

    def _calculate_time_based_progress(self, achievement: Dict, user_stats: Dict) -> Optional[Dict[str, Any]]:
        """Calculate progress for a specific time-based achievement."""
        try:
            conditions = achievement.get("conditions", {})
            condition_data = conditions.get("data", {})
            
            unit = condition_data.get("unit", "days")
            target_value = condition_data.get("threshold", 1)
            created_at = user_stats.get("created_at", 0)
            
            if not created_at:
                return None

            current_time = utc_now_ts()
            time_diff = current_time - created_at

            if unit == "days":
                current_value = time_diff / 86400
            elif unit == "hours":
                current_value = time_diff / 3600
            elif unit == "minutes":
                current_value = time_diff / 60
            else:  # seconds
                current_value = time_diff

            if target_value <= 0:
                return None

            progress_percentage = min((current_value / target_value) * 100, 100.0)

            return {
                "last_updated": time.time(),
                "condition_type": "time_based",
                "current_value": current_value,
                "target_value": target_value,
                "progress_percentage": progress_percentage,
                "field": "created_at"
            }
        except Exception as e:
            self.logger.error(f"Error calculating time-based progress: {e}", exc_info=True)
            return None
