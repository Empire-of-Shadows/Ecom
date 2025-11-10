"""
Daily streak tracking and validation for the leveling system.
Handles streak calculations, validations, and updates across the database system.
"""

import logging
from typing import Dict, Any, Tuple
from ecom_system.helpers.helpers import utc_now_ts, utc_today_key
from loggers.logger_setup import get_logger

logger = get_logger("DailyStreak", level=logging.DEBUG, json_format=False, colored_console=True)


def check_and_update_streak(user_data: Dict[str, Any]) -> Tuple[int, bool]:
    """
    Check if the daily streak should be incremented and calculate the new streak.

    Args:
        user_data: User data dictionary containing daily_streak information

    Returns:
        Tuple of (new_streak_count, should_update)
        - new_streak_count: The calculated streak count
        - should_update: Whether the streak should be updated in the database
    """
    now = utc_now_ts()
    today_key = utc_today_key()

    # Get current streak data
    daily_streak = user_data.get("daily_streak", {})
    last_streak_timestamp = daily_streak.get("timestamp", 0)
    current_streak = daily_streak.get("count", 0)

    # If no previous streak data, this is the first day
    if last_streak_timestamp == 0:
        logger.debug(f"    • First streak day detected")
        return 1, True

    # Convert timestamps to date keys for comparison
    from datetime import datetime, timezone
    last_date_key = datetime.fromtimestamp(last_streak_timestamp, tz=timezone.utc).strftime('%Y-%m-%d')

    logger.debug(f"    • Last streak: {last_date_key}, Current: {today_key}")
    logger.debug(f"    • Current streak count: {current_streak}")

    # Same day - no update needed
    if last_date_key == today_key:
        logger.debug(f"    • Already counted today, maintaining streak: {current_streak}")
        return current_streak, False

    # Check if streak is consecutive (previous day)
    last_datetime = datetime.fromtimestamp(last_streak_timestamp, tz=timezone.utc)
    current_datetime = datetime.fromtimestamp(now, tz=timezone.utc)
    days_diff = (current_datetime.date() - last_datetime.date()).days

    if days_diff == 1:
        # Consecutive day - increment streak
        new_streak = current_streak + 1
        logger.debug(f"    • Consecutive day! Streak: {current_streak} → {new_streak}")
        return new_streak, True
    elif days_diff > 1:
        # Streak broken - reset to 1
        logger.debug(f"    • Streak broken (gap of {days_diff} days), resetting to 1")
        return 1, True
    else:
        # This shouldn't happen if date comparison above works correctly
        logger.warning(f"    • Unexpected days_diff: {days_diff}")
        return current_streak, False


def get_streak_bonus(streak_count: int, max_bonus: float = 2.0, bonus_per_day: float = 0.1) -> float:
    """
    Calculate the streak bonus multiplier based on streak count.

    Args:
        streak_count: Current streak count
        max_bonus: Maximum bonus multiplier (default: 2.0 = 200%)
        bonus_per_day: Bonus added per streak day (default: 0.1 = 10%)

    Returns:
        Float multiplier (1.0 = no bonus, 2.0 = 100% bonus)
    """
    if streak_count <= 0:
        return 1.0

    bonus = 1.0 + (streak_count * bonus_per_day)
    capped_bonus = min(bonus, max_bonus)

    logger.debug(f"    • Streak bonus: {capped_bonus:.2f}x (streak: {streak_count})")
    return capped_bonus


def create_streak_update_data(new_streak_count: int) -> Dict[str, Any]:
    """
    Create the MongoDB update document for streak data.

    Args:
        new_streak_count: The new streak count to set

    Returns:
        Dictionary with MongoDB update operators
    """
    now = utc_now_ts()

    return {
        "daily_streak.count": new_streak_count,
        "daily_streak.timestamp": now
    }


def log_streak_change(user_id: str, guild_id: str, old_streak: int, new_streak: int):
    """
    Log streak changes for debugging and monitoring.

    Args:
        user_id: User ID
        guild_id: Guild ID
        old_streak: Previous streak count
        new_streak: New streak count
    """
    if new_streak > old_streak:
        logger.info(f"    🔥 Streak increased: {old_streak} → {new_streak} (U:{user_id} G:{guild_id})")
    elif new_streak < old_streak:
        logger.warning(f"    💔 Streak broken: {old_streak} → {new_streak} (U:{user_id} G:{guild_id})")
    else:
        logger.debug(f"    ➡️  Streak unchanged: {new_streak} (U:{user_id} G:{guild_id})")