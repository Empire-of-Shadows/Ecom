import logging
from typing import Dict, Any, List, Optional
import time
import aiosqlite

logger = logging.getLogger(__name__)


class DBTimeProgressTracker:
    """
    Dedicated progress tracker for DB-backed time-based achievements.
    """

    def __init__(self, progress_system):
        """Initialize with reference to parent AchievementProgressSystem"""
        self.progress_system = progress_system
        self.logger = logger
        # Day of week mappings - SQLite strftime('%w') format
        self.SQLITE_DAY_MAPPING = {
            'sunday': 0, 'sun': 0,
            'monday': 1, 'mon': 1,
            'tuesday': 2, 'tue': 2, 'tues': 2,
            'wednesday': 3, 'wed': 3,
            'thursday': 4, 'thu': 4, 'thur': 4, 'thurs': 4,
            'friday': 5, 'fri': 5,
            'saturday': 6, 'sat': 6
        }
        self.WEEKDAYS = [1, 2, 3, 4, 5]
        self.WEEKENDS = [0, 6]

    async def update_progress(self, user_id: str, guild_id: str, activity_data: Dict,
                              unearned_achievements: List[Dict]) -> Dict[str, Dict[str, Any]]:
        """
        Update progress for DB-backed time-based achievements.
        """
        progress_updates = {}
        
        for achievement in unearned_achievements:
            if not self._is_db_time_achievement(achievement):
                continue

            achievement_id = achievement.get("id")
            if not achievement_id:
                continue

            progress_data = await self._calculate_db_time_progress(achievement, user_id, guild_id)
            if progress_data:
                progress_updates[achievement_id] = progress_data
        
        return progress_updates

    def _is_db_time_achievement(self, achievement: Dict) -> bool:
        """Check if this is a DB-backed time-based achievement."""
        condition_type = achievement.get("conditions", {}).get("type")
        return condition_type in ["time_pattern", "weekend_activity", "day_of_week", "day_of_month", "weekday_weekend"]

    async def _calculate_db_time_progress(self, achievement: Dict, user_id: str, guild_id: str) -> Optional[Dict[str, Any]]:
        """Calculate progress for a specific DB-backed time achievement."""
        try:
            conditions = achievement.get("conditions", {})
            condition_type = conditions.get("type")
            condition_data = conditions.get("data", {})
            
            current_value, target_value = await self._get_progress_values(condition_type, condition_data, user_id, guild_id)

            if target_value <= 0:
                return None

            progress_percentage = min((current_value / target_value) * 100, 100.0)

            return {
                "last_updated": time.time(),
                "condition_type": condition_type,
                "current_value": current_value,
                "target_value": target_value,
                "progress_percentage": progress_percentage
            }
        except Exception as e:
            self.logger.error(f"Error calculating DB time progress: {e}", exc_info=True)
            return None

    async def _get_progress_values(self, condition_type: str, condition_data: Dict, user_id: str, guild_id: str) -> tuple[int, int]:
        """Get current and target values for DB-backed time achievements."""
        if condition_type == "time_pattern":
            return await self._calculate_time_pattern_progress_db(condition_data, user_id, guild_id)
        elif condition_type == "weekend_activity":
            return await self._calculate_weekend_activity_progress_db(condition_data, user_id, guild_id)
        elif condition_type == "day_of_week":
            return await self._calculate_day_of_week_progress_db(condition_data, user_id, guild_id)
        elif condition_type == "day_of_month":
            return await self._calculate_day_of_month_progress_db(condition_data, user_id, guild_id)
        elif condition_type == "weekday_weekend":
            return await self._calculate_weekday_weekend_progress_db(condition_data, user_id, guild_id)
        return 0, 1
    
    async def _calculate_time_pattern_progress_db(self, condition_data: Dict, user_id: str, guild_id: str) -> tuple[int, int]:
        """Calculates progress for time pattern conditions from local database data."""
        threshold = condition_data.get("threshold", 10)
        try:
            time_range = condition_data.get("time_range", {})
            if not time_range or "start" not in time_range or "end" not in time_range:
                return 0, threshold

            start_hour = int(time_range["start"].split(":")[0])
            start_minute = int(time_range["start"].split(":")[1])
            end_hour = int(time_range["end"].split(":")[0])
            end_minute = int(time_range["end"].split(":")[1])

            async with aiosqlite.connect(self.progress_system.db.local_db_path) as db:
                if start_hour <= end_hour:
                    cursor = await db.execute("""
                                              SELECT COUNT(DISTINCT DATE(datetime(timestamp, 'unixepoch')))
                                              FROM user_activities
                                              WHERE guild_id = ? AND user_id = ?
                                                AND event_type IN ('message_create', 'voice_state_update', 'reaction_add')
                                                AND (CAST(strftime('%H', datetime(timestamp, 'unixepoch')) AS INTEGER) > ? OR
                                                    (CAST(strftime('%H', datetime(timestamp, 'unixepoch')) AS INTEGER) = ? AND
                                                     CAST(strftime('%M', datetime(timestamp, 'unixepoch')) AS INTEGER) >= ?))
                                                AND (CAST(strftime('%H', datetime(timestamp, 'unixepoch')) AS INTEGER) < ? OR
                                                    (CAST(strftime('%H', datetime(timestamp, 'unixepoch')) AS INTEGER) = ? AND
                                                     CAST(strftime('%M', datetime(timestamp, 'unixepoch')) AS INTEGER) <= ?))
                                              """,
                                              (guild_id, user_id, start_hour, start_hour, start_minute, end_hour, end_hour, end_minute))
                else:
                    cursor = await db.execute("""
                                              SELECT COUNT(DISTINCT DATE(datetime(timestamp, 'unixepoch')))
                                              FROM user_activities
                                              WHERE guild_id = ? AND user_id = ?
                                                AND event_type IN ('message_create', 'voice_state_update', 'reaction_add')
                                                AND (CAST(strftime('%H', datetime(timestamp, 'unixepoch')) AS INTEGER) >= ? OR
                                                     CAST(strftime('%H', datetime(timestamp, 'unixepoch')) AS INTEGER) <= ?)
                                              """, (guild_id, user_id, start_hour, end_hour))
                
                active_days = (await cursor.fetchone())[0] or 0
            return active_days, threshold
        except Exception as e:
            self.logger.error(f"Error calculating time pattern progress: {e}")
            return 0, threshold

    async def _calculate_weekend_activity_progress_db(self, condition_data: Dict, user_id: str, guild_id: str) -> tuple[int, int]:
        """Calculates progress for weekend activity conditions from local database data."""
        threshold = condition_data.get("threshold", 8)
        try:
            min_activity_per_weekend = condition_data.get("min_activity_per_weekend", 10)
            async with aiosqlite.connect(self.progress_system.db.local_db_path) as db:
                cursor = await db.execute("""
                                          SELECT COUNT(*) FROM (
                                              SELECT 1
                                              FROM user_activities
                                              WHERE guild_id = ? AND user_id = ?
                                                AND event_type IN ('message_create', 'voice_state_update', 'reaction_add')
                                                AND CAST(strftime('%w', datetime(timestamp, 'unixepoch')) AS INTEGER) IN (0, 6)
                                              GROUP BY strftime('%Y-%W', datetime(timestamp, 'unixepoch'))
                                              HAVING COUNT(*) >= ?
                                          )
                                          """, (guild_id, user_id, min_activity_per_weekend))
                active_weekends = (await cursor.fetchone())[0] or 0
            return active_weekends, threshold
        except Exception as e:
            self.logger.error(f"Error calculating weekend activity progress: {e}")
            return 0, threshold

    async def _calculate_day_of_week_progress_db(self, condition_data: Dict, user_id: str, guild_id: str) -> tuple[int, int]:
        """Calculates progress for day of week conditions from local database data."""
        threshold = condition_data.get("threshold", 1)
        try:
            days = condition_data.get("days", [])
            min_activity_per_day = condition_data.get("min_activity_per_day", 1)
            if not days:
                return 0, threshold

            day_numbers = [self.SQLITE_DAY_MAPPING[day.lower()] for day in days if day.lower() in self.SQLITE_DAY_MAPPING]
            if not day_numbers:
                return 0, threshold

            async with aiosqlite.connect(self.progress_system.db.local_db_path) as db:
                placeholders = ','.join(['?' for _ in day_numbers])
                cursor = await db.execute(f"""
                                              SELECT COUNT(DISTINCT DATE(datetime(timestamp, 'unixepoch')))
                                              FROM user_activities
                                              WHERE guild_id = ? AND user_id = ?
                                                AND event_type IN ('message_create', 'voice_state_update', 'reaction_add')
                                                AND CAST(strftime('%w', datetime(timestamp, 'unixepoch')) AS INTEGER) IN ({placeholders})
                                              GROUP BY DATE(datetime(timestamp, 'unixepoch'))
                                              HAVING COUNT(*) >= ?
                                              """, [guild_id, user_id] + day_numbers + [min_activity_per_day])
                
                active_days = len(await cursor.fetchall())
            return active_days, threshold
        except Exception as e:
            self.logger.error(f"Error calculating day of week progress: {e}")
            return 0, threshold

    async def _calculate_day_of_month_progress_db(self, condition_data: Dict, user_id: str, guild_id: str) -> tuple[int, int]:
        """Calculates progress for day of month conditions from local database data."""
        threshold = condition_data.get("threshold", 1)
        try:
            days_of_month = condition_data.get("days_of_month", [])
            min_activity_per_day = condition_data.get("min_activity_per_day", 1)
            if not days_of_month:
                return 0, threshold

            valid_days = [day for day in days_of_month if isinstance(day, int) and 1 <= day <= 31]
            if not valid_days:
                return 0, threshold

            async with aiosqlite.connect(self.progress_system.db.local_db_path) as db:
                placeholders = ','.join(['?' for _ in valid_days])
                cursor = await db.execute(f"""
                                              SELECT COUNT(DISTINCT DATE(datetime(timestamp, 'unixepoch')))
                                              FROM user_activities
                                              WHERE guild_id = ? AND user_id = ?
                                                AND event_type IN ('message_create', 'voice_state_update', 'reaction_add')
                                                AND CAST(strftime('%d', datetime(timestamp, 'unixepoch')) AS INTEGER) IN ({placeholders})
                                              GROUP BY DATE(datetime(timestamp, 'unixepoch'))
                                              HAVING COUNT(*) >= ?
                                              """, [guild_id, user_id] + valid_days + [min_activity_per_day])
                active_days = len(await cursor.fetchall())
            return active_days, threshold
        except Exception as e:
            self.logger.error(f"Error calculating day of month progress: {e}")
            return 0, threshold

    async def _calculate_weekday_weekend_progress_db(self, condition_data: Dict, user_id: str, guild_id: str) -> tuple[int, int]:
        """Calculates progress for weekday vs weekend conditions from local database data."""
        threshold = condition_data.get("threshold", 1)
        try:
            day_type = condition_data.get("day_type", "weekday")
            min_activity_per_day = condition_data.get("min_activity_per_day", 1)

            if day_type.lower() == "weekday":
                target_days = self.WEEKDAYS
            elif day_type.lower() == "weekend":
                target_days = self.WEEKENDS
            else:
                return 0, threshold

            async with aiosqlite.connect(self.progress_system.db.local_db_path) as db:
                placeholders = ','.join(['?' for _ in target_days])
                cursor = await db.execute(f"""
                                              SELECT COUNT(DISTINCT DATE(datetime(timestamp, 'unixepoch')))
                                              FROM user_activities
                                              WHERE guild_id = ? AND user_id = ?
                                                AND event_type IN ('message_create', 'voice_state_update', 'reaction_add')
                                                AND CAST(strftime('%w', datetime(timestamp, 'unixepoch')) AS INTEGER) IN ({placeholders})
                                              GROUP BY DATE(datetime(timestamp, 'unixepoch'))
                                              HAVING COUNT(*) >= ?
                                              """, [guild_id, user_id] + target_days + [min_activity_per_day])
                active_days = len(await cursor.fetchall())
            return active_days, threshold
        except Exception as e:
            self.logger.error(f"Error calculating weekday/weekend progress: {e}")
            return 0, threshold

    async def get_progress_summary(self, user_id: str, guild_id: str, achievements: List[Dict],
                                   unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get progress summary for DB-backed time-based achievements"""
        try:
            db_time_achievements = [ach for ach in achievements if self._is_db_time_achievement(ach)]

            if not db_time_achievements:
                return {
                    "total": 0,
                    "completed": 0,
                    "in_progress": 0,
                    "completion_percentage": 0.0
                }

            total = len(db_time_achievements)
            completed = len([ach for ach in db_time_achievements if ach.get("id") in unlocked_ids])
            in_progress = len([ach for ach in db_time_achievements
                               if ach.get("id") in progress_data and ach.get("id") not in unlocked_ids])

            completion_percentage = (completed / total * 100) if total > 0 else 0.0

            return {
                "total": total,
                "completed": completed,
                "in_progress": in_progress,
                "completion_percentage": round(completion_percentage, 1)
            }

        except Exception as e:
            logger.error(f"Error getting DB time progress summary: {e}", exc_info=True)
            return {"total": 0, "completed": 0, "in_progress": 0, "completion_percentage": 0.0}

    async def get_detailed_progress(self, user_id: str, guild_id: str, achievements: List[Dict],
                                    unlocked_ids: List[str], progress_data: Dict) -> Dict[str, Any]:
        """Get detailed progress for DB-backed time-based achievements"""
        try:
            db_time_achievements = [ach for ach in achievements if self._is_db_time_achievement(ach)]

            detailed_progress = {
                "category": "db_time",
                "achievements": [],
                "summary": await self.get_progress_summary(user_id, guild_id, achievements, unlocked_ids, progress_data)
            }

            for achievement in db_time_achievements:
                achievement_id = achievement.get("id")
                conditions = achievement.get("conditions", {})
                condition_type = conditions.get("type")
                condition_data = conditions.get("data", {})

                # Get current values for this achievement
                try:
                    current_value, target_value = await self._get_progress_values(
                        condition_type, condition_data, user_id, guild_id
                    )
                except Exception as e:
                    logger.error(f"Error getting progress values for {achievement_id}: {e}")
                    current_value, target_value = 0, 1

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
            logger.error(f"Error getting detailed DB time progress: {e}", exc_info=True)
            return {"category": "db_time", "achievements": [], "summary": {}}
