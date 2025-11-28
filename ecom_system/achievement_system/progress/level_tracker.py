import logging
from typing import Dict, Any, List, Optional, Tuple
import time

logger = logging.getLogger(__name__)


class LevelingProgressTracker:
    """
    Dedicated progress tracker for leveling-based achievements.

    Handles tracking progress for achievements like:
    - level_5, level_10, level_25, level_50, level_100
    - Any level-based achievements
    - XP-based achievements
    """

    def __init__(self, progress_system):
        """Initialize with reference to parent AchievementProgressSystem"""
        self.progress_system = progress_system
        self.logger = logger

    async def update_progress(self, user_id: str, guild_id: str, activity_data: Dict,
                              unearned_achievements: List[Dict]) -> Dict[str, Dict[str, Any]]:
        """
        Update progress for leveling-based achievements

        Args:
            user_id: User ID
            guild_id: Guild ID
            activity_data: Activity data from events
            unearned_achievements: List of leveling achievements not yet unlocked

        Returns:
            Dict of achievement_id -> progress_data for achievements that had progress updates
        """
        try:
            # Get user's current level and XP stats
            user_stats = await self._get_user_stats(user_id, guild_id)
            current_level = self._get_current_level(user_stats)
            current_xp = self._get_current_xp(user_stats)

            progress_updates = {}

            # Process each leveling-based achievement
            for achievement in unearned_achievements:
                if not self._is_leveling_achievement(achievement):
                    continue

                achievement_id = achievement.get("id")
                if not achievement_id:
                    continue

                # Calculate progress for this achievement
                progress_data = await self._calculate_leveling_progress(
                    achievement, current_level, current_xp, user_id, guild_id
                )

                if progress_data:
                    progress_updates[achievement_id] = progress_data
                    logger.debug(
                        f"Updated leveling progress for {achievement_id}: {progress_data['progress_percentage']:.1f}%")

            return progress_updates

        except Exception as e:
            logger.error(f"Error updating leveling progress for user {user_id}: {e}", exc_info=True)
            return {}

    def _is_leveling_achievement(self, achievement: Dict) -> bool:
        """Check if this is a leveling-based achievement"""
        conditions = achievement.get("conditions", {})
        condition_type = conditions.get("type")

        # Check for explicit level condition type
        if condition_type == "level":
            return True

        # Check for field-based conditions that reference level or XP
        if condition_type == "field":
            field = conditions.get("data", {}).get("field", "")
            if any(keyword in field.lower() for keyword in ["level", "xp", "experience"]):
                return True

        # Check achievement ID patterns
        achievement_id = achievement.get("id", "")
        if achievement_id.startswith("level_") or achievement_id.startswith("xp_"):
            return True

        # Check achievement category
        category = achievement.get("category", "").lower()
        if category in ["level", "leveling", "xp", "experience"]:
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

    def _get_current_level(self, user_stats: Dict) -> int:
        """Extract current level from user stats"""
        try:
            # Try direct level field
            if "level" in user_stats:
                return int(user_stats["level"])

            # Try nested level fields
            if "stats" in user_stats and "level" in user_stats["stats"]:
                return int(user_stats["stats"]["level"])

            if "leveling" in user_stats and "level" in user_stats["leveling"]:
                return int(user_stats["leveling"]["level"])

            # Default to level 1 if no level found
            return 1

        except (ValueError, TypeError) as e:
            logger.error(f"Error extracting level from user stats: {e}")
            return 1

    def _get_current_xp(self, user_stats: Dict) -> int:
        """Extract current XP from user stats"""
        try:
            # Try different possible XP field names
            possible_fields = ["xp", "experience", "exp", "total_xp"]

            for field in possible_fields:
                if field in user_stats:
                    return int(user_stats[field])

            # Try nested XP fields
            if "stats" in user_stats:
                for field in possible_fields:
                    if field in user_stats["stats"]:
                        return int(user_stats["stats"][field])

            if "leveling" in user_stats:
                for field in possible_fields:
                    if field in user_stats["leveling"]:
                        return int(user_stats["leveling"][field])

            # Default to 0 if no XP found
            return 0

        except (ValueError, TypeError) as e:
            logger.error(f"Error extracting XP from user stats: {e}")
            return 0

    async def _calculate_leveling_progress(self, achievement: Dict, current_level: int,
                                           current_xp: int, user_id: str, guild_id: str) -> Optional[Dict[str, Any]]:
        """Calculate progress for a specific leveling achievement"""
        try:
            conditions = achievement.get("conditions", {})
            condition_data = conditions.get("data", {})
            condition_type = conditions.get("type")

            # Determine if this is a level-based or XP-based achievement
            is_xp_based = self._is_xp_based_achievement(achievement, condition_data)

            if is_xp_based:
                current_value = current_xp
                target_value = self._extract_target_xp(achievement, condition_data)
                field_path = self._determine_xp_field_path(achievement, condition_data)
            else:
                current_value = current_level
                target_value = self._extract_target_level(achievement, condition_data)
                field_path = self._determine_level_field_path(achievement, condition_data)

            if target_value is None or target_value <= 0:
                logger.warning(f"Invalid target value for achievement {achievement.get('id')}")
                return None

            # Calculate progress percentage
            progress_percentage = min((current_value / target_value) * 100, 100.0)

            progress_data = {
                "last_updated": time.time(),
                "condition_type": "xp" if is_xp_based else "level",
                "current_value": current_value,
                "target_value": target_value,
                "progress_percentage": progress_percentage,
                "field": field_path
            }

            return progress_data

        except Exception as e:
            logger.error(f"Error calculating leveling progress: {e}", exc_info=True)
            return None

    def _is_xp_based_achievement(self, achievement: Dict, condition_data: Dict) -> bool:
        """Determine if this achievement is XP-based rather than level-based"""
        # Check field path
        field = condition_data.get("field", "")
        if any(keyword in field.lower() for keyword in ["xp", "experience", "exp"]):
            return True

        # Check achievement ID
        achievement_id = achievement.get("id", "")
        if achievement_id.startswith("xp_") or "xp" in achievement_id.lower():
            return True

        # Default to level-based
        return False

    def _extract_target_level(self, achievement: Dict, condition_data: Dict) -> Optional[int]:
        """Extract target level from achievement definition"""
        # Try condition data first
        threshold = condition_data.get("threshold")
        if threshold is not None:
            try:
                return int(threshold)
            except (ValueError, TypeError):
                pass

        # Try to extract from achievement ID (e.g., "level_25" -> 25)
        achievement_id = achievement.get("id", "")
        if achievement_id.startswith("level_"):
            try:
                number_part = achievement_id.replace("level_", "")
                return int(number_part)
            except (ValueError, TypeError):
                pass

        # Try achievement metadata
        metadata = achievement.get("metadata", {})
        for key in ["target_level", "required_level", "level", "threshold"]:
            if key in metadata:
                try:
                    return int(metadata[key])
                except (ValueError, TypeError):
                    continue

        return None

    def _extract_target_xp(self, achievement: Dict, condition_data: Dict) -> Optional[int]:
        """Extract target XP from achievement definition"""
        # Try condition data first
        threshold = condition_data.get("threshold")
        if threshold is not None:
            try:
                return int(threshold)
            except (ValueError, TypeError):
                pass

        # Try to extract from achievement ID (e.g., "xp_10000" -> 10000)
        achievement_id = achievement.get("id", "")
        if achievement_id.startswith("xp_"):
            try:
                number_part = achievement_id.replace("xp_", "")
                return int(number_part)
            except (ValueError, TypeError):
                pass

        # Try achievement metadata
        metadata = achievement.get("metadata", {})
        for key in ["target_xp", "required_xp", "xp", "experience", "threshold"]:
            if key in metadata:
                try:
                    return int(metadata[key])
                except (ValueError, TypeError):
                    continue

        return None

    def _determine_level_field_path(self, achievement: Dict, condition_data: Dict) -> str:
        """Determine the field path for level achievements"""
        # Check if field is explicitly specified
        field = condition_data.get("field")
        if field:
            return field

        # Default field path for level
        return "level"

    def _determine_xp_field_path(self, achievement: Dict, condition_data: Dict) -> str:
        """Determine the field path for XP achievements"""
        # Check if field is explicitly specified
        field = condition_data.get("field")
        if field:
            return field

        # Default field path for XP
        return "xp"

    async def get_progress_summary(self, user_id: str, guild_id: str, achievements: List[Dict],
                                   unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get progress summary for leveling achievements"""
        try:
            leveling_achievements = [ach for ach in achievements if self._is_leveling_achievement(ach)]

            if not leveling_achievements:
                return {
                    "total": 0,
                    "completed": 0,
                    "in_progress": 0,
                    "completion_percentage": 0.0
                }

            total = len(leveling_achievements)
            completed = len([ach for ach in leveling_achievements if ach.get("id") in unlocked_ids])
            in_progress = len([ach for ach in leveling_achievements
                               if ach.get("id") in progress_data and ach.get("id") not in unlocked_ids])

            completion_percentage = (completed / total * 100) if total > 0 else 0.0

            return {
                "total": total,
                "completed": completed,
                "in_progress": in_progress,
                "completion_percentage": round(completion_percentage, 1)
            }

        except Exception as e:
            logger.error(f"Error getting leveling progress summary: {e}", exc_info=True)
            return {"total": 0, "completed": 0, "in_progress": 0, "completion_percentage": 0.0}

    async def get_detailed_progress(self, user_id: str, guild_id: str, achievements: List[Dict],
                                    unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get detailed progress for leveling achievements"""
        try:
            leveling_achievements = [ach for ach in achievements if self._is_leveling_achievement(ach)]

            detailed_progress = {
                "category": "leveling",
                "achievements": [],
                "summary": await self.get_progress_summary(user_id, guild_id, achievements, unlocked_ids, progress_data)
            }

            # Get current level and XP
            user_stats = await self._get_user_stats(user_id, guild_id)
            current_level = self._get_current_level(user_stats)
            current_xp = self._get_current_xp(user_stats)

            for achievement in leveling_achievements:
                achievement_id = achievement.get("id")

                # Determine current and target values
                condition_data = achievement.get("conditions", {}).get("data", {})
                is_xp_based = self._is_xp_based_achievement(achievement, condition_data)

                if is_xp_based:
                    current_value = current_xp
                    target_value = self._extract_target_xp(achievement, condition_data)
                else:
                    current_value = current_level
                    target_value = self._extract_target_level(achievement, condition_data)

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
                        "progress_percentage": min((current_value / target_value * 100), 100.0) if target_value else 0.0
                    }

                detailed_progress["achievements"].append({
                    "id": achievement_id,
                    "name": achievement.get("name", achievement_id),
                    "description": achievement.get("description", ""),
                    "status": status,
                    "progress": progress_info,
                    "type": "xp" if is_xp_based else "level"
                })

            return detailed_progress

        except Exception as e:
            logger.error(f"Error getting detailed leveling progress: {e}", exc_info=True)
            return {"category": "leveling", "achievements": [], "summary": {}}