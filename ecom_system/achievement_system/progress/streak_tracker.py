import logging
from typing import Dict, Any, List, Optional
import time

logger = logging.getLogger(__name__)


class StreakProgressTracker:
    """
    Dedicated progress tracker for streak-based achievements.
    """

    def __init__(self, progress_system):
        """Initialize with reference to parent AchievementProgressSystem"""
        self.progress_system = progress_system
        self.logger = logger

    async def update_progress(self, user_id: str, guild_id: str, activity_data: Dict,
                              unearned_achievements: List[Dict]) -> Dict[str, Dict[str, Any]]:
        """
        Update progress for streak-based achievements
        """
        try:
            user_stats = await self._get_user_stats(user_id, guild_id)
            current_streak = self._get_current_streak_count(user_stats)

            progress_updates = {}

            for achievement in unearned_achievements:
                if not self._is_streak_achievement(achievement):
                    continue

                achievement_id = achievement.get("id")
                if not achievement_id:
                    continue

                progress_data = self._calculate_streak_progress(
                    achievement, current_streak
                )

                if progress_data:
                    progress_updates[achievement_id] = progress_data
                    self.logger.debug(f"Updated progress for {achievement_id}: {progress_data['progress_percentage']:.1f}%")

            return progress_updates

        except Exception as e:
            self.logger.error(f"Error updating streak progress for user {user_id}: {e}", exc_info=True)
            return {}

    def _is_streak_achievement(self, achievement: Dict) -> bool:
        """Check if this is a streak-based achievement."""
        conditions = achievement.get("conditions", {})
        return conditions.get("type") == "daily_streak"

    async def _get_user_stats(self, user_id: str, guild_id: str) -> Dict:
        """Get user statistics from Users.Stats collection"""
        try:
            user_stats = await self.progress_system.db.users.find_one(
                {"user_id": user_id, "guild_id": guild_id}
            )
            return user_stats or {}
        except Exception as e:
            self.logger.error(f"Error getting user stats: {e}", exc_info=True)
            return {}

    def _get_current_streak_count(self, user_stats: Dict) -> int:
        """Extract current daily streak count from user stats"""
        try:
            return user_stats.get("daily_streak", {}).get("count", 0)
        except (ValueError, TypeError) as e:
            self.logger.error(f"Error extracting daily streak count from user stats: {e}")
            return 0

    def _calculate_streak_progress(self, achievement: Dict, current_streak: int) -> Optional[Dict[str, Any]]:
        """Calculate progress for a specific streak achievement"""
        try:
            conditions = achievement.get("conditions", {})
            condition_data = conditions.get("data", {})

            target_value = self._extract_target_value(achievement, condition_data)

            if target_value is None or target_value <= 0:
                self.logger.warning(f"Invalid target value for achievement {achievement.get('id')}")
                return None

            progress_percentage = min((current_streak / target_value) * 100, 100.0)

            field_path = self._determine_field_path(condition_data)

            progress_data = {
                "last_updated": time.time(),
                "condition_type": "daily_streak",
                "current_value": current_streak,
                "target_value": target_value,
                "progress_percentage": progress_percentage,
                "field": field_path
            }

            return progress_data

        except Exception as e:
            self.logger.error(f"Error calculating streak progress: {e}", exc_info=True)
            return None

    def _extract_target_value(self, achievement: Dict, condition_data: Dict) -> Optional[int]:
        """Extract target streak count from achievement definition"""
        threshold = condition_data.get("threshold")
        if threshold is not None:
            try:
                return int(threshold)
            except (ValueError, TypeError):
                pass

        achievement_id = achievement.get("id", "")
        if achievement_id.startswith("streak_"):
            try:
                number_part = achievement_id.replace("streak_", "")
                return int(number_part)
            except (ValueError, TypeError):
                pass

        return None

    def _determine_field_path(self, condition_data: Dict) -> str:
        """Determine the field path for this streak achievement"""
        return condition_data.get("field", "daily_streak.count")

    async def get_progress_summary(self, user_id: str, guild_id: str, achievements: List[Dict],
                                   unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get progress summary for streak achievements"""
        try:
            streak_achievements = [ach for ach in achievements if self._is_streak_achievement(ach)]

            if not streak_achievements:
                return {
                    "total": 0,
                    "completed": 0,
                    "in_progress": 0,
                    "completion_percentage": 0.0
                }

            total = len(streak_achievements)
            completed = len([ach for ach in streak_achievements if ach.get("id") in unlocked_ids])
            in_progress = len([ach for ach in streak_achievements
                               if ach.get("id") in progress_data and ach.get("id") not in unlocked_ids])

            completion_percentage = (completed / total * 100) if total > 0 else 0.0

            return {
                "total": total,
                "completed": completed,
                "in_progress": in_progress,
                "completion_percentage": round(completion_percentage, 1)
            }

        except Exception as e:
            logger.error(f"Error getting streak progress summary: {e}", exc_info=True)
            return {"total": 0, "completed": 0, "in_progress": 0, "completion_percentage": 0.0}

    async def get_detailed_progress(self, user_id: str, guild_id: str, achievements: List[Dict],
                                    unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get detailed progress for streak achievements"""
        try:
            streak_achievements = [ach for ach in achievements if self._is_streak_achievement(ach)]

            detailed_progress = {
                "category": "streak",
                "achievements": [],
                "summary": await self.get_progress_summary(user_id, guild_id, achievements, unlocked_ids, progress_data)
            }

            # Get current user stats
            user_stats = await self._get_user_stats(user_id, guild_id)
            current_streak = self._get_current_streak_count(user_stats)

            for achievement in streak_achievements:
                achievement_id = achievement.get("id")
                conditions = achievement.get("conditions", {})
                condition_data = conditions.get("data", {})

                target_value = self._extract_target_value(achievement, condition_data)
                if target_value is None:
                    target_value = 0

                # Check if unlocked
                if achievement_id in unlocked_ids:
                    status = "completed"
                    progress_info = {
                        "current_value": current_streak,
                        "target_value": target_value,
                        "progress_percentage": 100.0
                    }
                elif achievement_id in progress_data:
                    status = "in_progress"
                    progress_info = progress_data[achievement_id]
                else:
                    status = "locked"
                    progress_info = {
                        "current_value": current_streak,
                        "target_value": target_value,
                        "progress_percentage": min((current_streak / target_value * 100),
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
            logger.error(f"Error getting detailed streak progress: {e}", exc_info=True)
            return {"category": "streak", "achievements": [], "summary": {}}
