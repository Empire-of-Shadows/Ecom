import logging
from typing import Dict, Any, List, Optional
import time

from loggers.logger_setup import get_logger

logger = get_logger("ReactionsTracker", level=logging.DEBUG, json_format=False, colored_console=True)


class ReactionsProgressTracker:
    """
    Dedicated progress tracker for reaction-based achievements.
    """

    def __init__(self, progress_system):
        """Initialize with reference to parent AchievementProgressSystem"""
        self.progress_system = progress_system
        self.logger = logger

    async def update_progress(self, user_id: str, guild_id: str, activity_data: Dict,
                              unearned_achievements: List[Dict]) -> Dict[str, Dict[str, Any]]:
        """
        Update progress for reaction-based achievements.
        """
        progress_updates = {}
        user_stats = await self._get_user_stats(user_id, guild_id)
        
        for achievement in unearned_achievements:
            if not self._is_reaction_achievement(achievement):
                continue

            achievement_id = achievement.get("id")
            if not achievement_id:
                continue

            progress_data = self._calculate_reaction_progress(achievement, user_stats)
            if progress_data:
                progress_updates[achievement_id] = progress_data
        
        return progress_updates

    def _is_reaction_achievement(self, achievement: Dict) -> bool:
        """Check if this is a reaction-based achievement."""
        condition_type = achievement.get("conditions", {}).get("type")
        return condition_type in ["reactions_given", "got_reactions"]

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

    def _calculate_reaction_progress(self, achievement: Dict, user_stats: Dict) -> Optional[Dict[str, Any]]:
        """Calculate progress for a specific reaction achievement."""
        try:
            conditions = achievement.get("conditions", {})
            condition_type = conditions.get("type")
            condition_data = conditions.get("data", {})
            
            field = condition_data.get("field")
            if not field:
                return None

            current_value = user_stats.get("message_stats", {}).get(field, 0)
            target_value = condition_data.get("threshold", 1)

            if target_value <= 0:
                return None

            progress_percentage = min((current_value / target_value) * 100, 100.0)

            return {
                "last_updated": time.time(),
                "condition_type": condition_type,
                "current_value": current_value,
                "target_value": target_value,
                "progress_percentage": progress_percentage,
                "field": field
            }
        except Exception as e:
            self.logger.error(f"Error calculating reaction progress: {e}", exc_info=True)
            return None
