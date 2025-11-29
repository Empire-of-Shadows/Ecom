import logging
from typing import Dict, Any, List, Optional
import time

from ecom_system.helpers.helpers import utc_now_ts

logger = logging.getLogger(__name__)


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

    async def get_progress_summary(self, user_id: str, guild_id: str, achievements: List[Dict],
                                   unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get progress summary for time-based achievements"""
        try:
            time_based_achievements = [ach for ach in achievements if self._is_time_based_achievement(ach)]

            if not time_based_achievements:
                return {
                    "total": 0,
                    "completed": 0,
                    "in_progress": 0,
                    "completion_percentage": 0.0
                }

            total = len(time_based_achievements)
            completed = len([ach for ach in time_based_achievements if ach.get("id") in unlocked_ids])
            in_progress = len([ach for ach in time_based_achievements
                               if ach.get("id") in progress_data and ach.get("id") not in unlocked_ids])

            completion_percentage = (completed / total * 100) if total > 0 else 0.0

            return {
                "total": total,
                "completed": completed,
                "in_progress": in_progress,
                "completion_percentage": round(completion_percentage, 1)
            }

        except Exception as e:
            logger.error(f"Error getting time-based progress summary: {e}", exc_info=True)
            return {"total": 0, "completed": 0, "in_progress": 0, "completion_percentage": 0.0}

    async def get_detailed_progress(self, user_id: str, guild_id: str, achievements: List[Dict],
                                    unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get detailed progress for time-based achievements"""
        try:
            time_based_achievements = [ach for ach in achievements if self._is_time_based_achievement(ach)]

            detailed_progress = {
                "category": "time_based",
                "achievements": [],
                "summary": await self.get_progress_summary(user_id, guild_id, achievements, unlocked_ids, progress_data)
            }

            # Get current user stats
            user_stats = await self._get_user_stats(user_id, guild_id)

            for achievement in time_based_achievements:
                achievement_id = achievement.get("id")
                conditions = achievement.get("conditions", {})
                condition_data = conditions.get("data", {})

                # Calculate current time elapsed
                created_at = user_stats.get("created_at", 0)
                if created_at:
                    current_time = utc_now_ts()
                    time_diff = current_time - created_at
                    unit = condition_data.get("unit", "days")

                    if unit == "days":
                        current_value = time_diff / 86400
                    elif unit == "hours":
                        current_value = time_diff / 3600
                    elif unit == "minutes":
                        current_value = time_diff / 60
                    else:  # seconds
                        current_value = time_diff
                else:
                    current_value = 0

                target_value = condition_data.get("threshold", 1)

                # Check if unlocked
                if achievement_id in unlocked_ids:
                    status = "completed"
                    progress_info = {
                        "current_value": current_value,
                        "target_value": target_value,
                        "progress_percentage": 100.0
                    }
                elif achievement_id in progress_data:
                    status = "in_progress"
                    progress_info = progress_data[achievement_id]
                else:
                    status = "locked"
                    progress_info = {
                        "current_value": current_value,
                        "target_value": target_value,
                        "progress_percentage": min((current_value / target_value * 100),
                                                   100.0) if target_value > 0 else 0.0
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
            logger.error(f"Error getting detailed time-based progress: {e}", exc_info=True)
            return {"category": "time_based", "achievements": [], "summary": {}}
