import logging
import os
from typing import Dict, Any

import aiosqlite

from ecom_system.helpers.helpers import utc_now_ts
from loggers.logger_setup import get_logger
from dotenv import load_dotenv
load_dotenv()

WEBHOOK = os.getenv("DISCORD_ERROR_WEBHOOK")

logger = get_logger("AchievementConditionSystem", level=logging.DEBUG, json_format=False, colored_console=True)


class AchievementConditionSystem:
	"""
	Dedicated system for checking achievement conditions.

	Handles all types of achievement conditions including:
	- Simple activity matching
	- Level-based conditions
	- Field-based conditions (messages, voice, reactions, etc.)
	- Time-based conditions
	- Combination conditions
	- Custom conditions
	- Time pattern conditions
	- Weekend activity conditions
	"""
	# Day of week mappings - SQLite strftime('%w') format
	SQLITE_DAY_MAPPING = {
		'sunday': 0, 'sun': 0,
		'monday': 1, 'mon': 1,
		'tuesday': 2, 'tue': 2, 'tues': 2,
		'wednesday': 3, 'wed': 3,
		'thursday': 4, 'thu': 4, 'thur': 4, 'thurs': 4,
		'friday': 5, 'fri': 5,
		'saturday': 6, 'sat': 6
	}

	# Standard weekday definition (Monday-Friday)
	WEEKDAYS = [1, 2, 3, 4, 5]  # Monday through Friday
	WEEKENDS = [0, 6]           # Sunday and Saturday

	def __init__(self, leveling_system):
		"""Initialize with reference to parent LevelingSystem"""
		self.leveling_system = leveling_system
		self.logger = logger
		self._missing_activity_buffer_warned = False

	def _activity_buffer_available(self) -> bool:
		"""Check if the activity buffer is available; log warning only once if missing."""
		has_buffer = hasattr(self.leveling_system, 'activity_buffer') and self.leveling_system.activity_buffer
		if not has_buffer:
			if not self._missing_activity_buffer_warned:
				self._missing_activity_buffer_warned = True
				logger.warning("Activity buffer not available; DB-backed time/day/weekend conditions will be skipped")
			else:
				logger.debug("Activity buffer still not available; skipping DB-backed time/day/weekend condition")
		return has_buffer

	async def check_achievement_condition(
			self, achievement: Dict, user_id: str, guild_id: str,
			activity_data: Dict, user_data: Dict, user_achievements: Dict
	) -> bool:
		"""Check if achievement condition is met using database-stored conditions"""
		try:
			if not achievement.get("enabled", True):
				logger.debug(f"Achievement {achievement.get('id')} is disabled")
				return False

			conditions = achievement.get("conditions", {})
			condition_type = conditions.get("type", "simple")
			condition_data = conditions.get("data", {})

			logger.debug(f"Checking achievement condition: {achievement.get('id')} - type: {condition_type}")

			# Level-based conditions
			if condition_type == "level":
				return self._check_level_condition(condition_data, user_data)

			# Message-based conditions
			elif condition_type == "messages":
				return self._check_field_condition(condition_data, user_data)

			# Voice time conditions
			elif condition_type == "voice_time":
				return self._check_field_condition(condition_data, user_data)

			# Voice session conditions
			elif condition_type == "voice_sessions":
				return self._check_field_condition(condition_data, user_data)

			# Streak conditions
			elif condition_type == "daily_streak":
				return self._check_field_condition(condition_data, user_data)

			# Reaction conditions
			elif condition_type == "reactions_given":
				return self._check_field_condition(condition_data, user_data)

			elif condition_type == "got_reactions":
				return self._check_field_condition(condition_data, user_data)

			# Attachment messages conditions
			elif condition_type == "attachment_messages":
				return self._check_field_condition(condition_data, user_data)

			# Quality conditions
			elif condition_type == "quality_streak":
				return self._check_field_condition(condition_data, user_data)

			# Time pattern conditions
			elif condition_type == "time_pattern":
				return await self._check_time_pattern_condition_db(condition_data, user_id, guild_id)

			# Weekend activity conditions
			elif condition_type == "weekend_activity":
				return await self._check_weekend_activity_condition_db(condition_data, user_id, guild_id)

			# Day of week conditions
			elif condition_type == "day_of_week":
				return await self._check_day_of_week_condition_db(condition_data, user_id, guild_id)

			# Day of month conditions
			elif condition_type == "day_of_month":
				return await self._check_day_of_month_condition_db(condition_data, user_id, guild_id)

			# Weekday vs Weekend conditions
			elif condition_type == "weekday_weekend":
				return await self._check_weekday_weekend_condition_db(condition_data, user_id, guild_id)

			# Prestige conditions
			elif condition_type == "prestige_level":
				return self._check_field_condition(condition_data, user_data)

			# Combination conditions
			elif condition_type == "combination":
				return await self._check_combination_requirements_db(condition_data, user_data)

			# Time-based conditions
			elif condition_type == "time_based":
				return self._check_time_based_condition(condition_data, user_data)

			# Custom conditions - allows for future expansion
			elif condition_type == "custom":
				return await self._check_custom_condition(condition_data, user_id, guild_id, activity_data, user_data)

			else:
				logger.warning(f"Unknown achievement condition type: {condition_type}")
				return False

		except Exception as e:
			logger.error(f"Error checking achievement condition: {e}")
			return False

	async def _check_combination_requirements_db(self, condition_data: Dict, user_data: Dict) -> bool:
		"""Check combination requirements using database conditions"""
		try:
			operator = condition_data.get("operator", "and")
			requirements = condition_data.get("requirements", [])

			results = []
			for requirement in requirements:
				req_type = requirement.get("type")
				threshold = requirement.get("threshold", 1)
				comparison = requirement.get("comparison", "gte")
				field = requirement.get("field", "")

				if req_type == "level":
					current_value = user_data.get("level", 1)
				else:
					current_value = self._get_nested_value(user_data, field) if field else 0

				result = self._compare_values(current_value, threshold, comparison)
				results.append(result)

			# Apply operator
			if operator == "and":
				return all(results)
			elif operator == "or":
				return any(results)
			else:
				logger.warning(f"Unknown combination operator: {operator}")
				return False

		except Exception as e:
			logger.error(f"Error checking combination requirements: {e}")
			return False

	def _check_level_condition(self, condition_data: Dict, user_data: Dict) -> bool:
		"""Check level-based conditions"""
		threshold = condition_data.get("threshold", 1)
		comparison = condition_data.get("comparison", "gte")
		current_level = user_data.get("level", 1)

		return self._compare_values(current_level, threshold, comparison)

	def _check_field_condition(self, condition_data: Dict, user_data: Dict) -> bool:
		"""Check field-based conditions using dot notation"""
		field_path = condition_data.get("field", "")
		threshold = condition_data.get("threshold", 1)
		comparison = condition_data.get("comparison", "gte")

		# Navigate nested dictionary using dot notation
		current_value = self._get_nested_value(user_data, field_path)

		return self._compare_values(current_value, threshold, comparison)

	def _check_time_based_condition(self, condition_data: Dict, user_data: Dict) -> bool:
		"""Check time-based conditions"""
		threshold = condition_data.get("threshold", 1)
		unit = condition_data.get("unit", "days")
		comparison = condition_data.get("comparison", "gte")

		created_at = user_data.get("created_at", 0)
		current_time = utc_now_ts()

		# Convert to appropriate units
		time_diff = current_time - created_at
		if unit == "days":
			time_diff = time_diff / 86400  # Convert seconds to days
		elif unit == "hours":
			time_diff = time_diff / 3600  # Convert seconds to hours
		elif unit == "minutes":
			time_diff = time_diff / 60  # Convert seconds to minutes
		# Default is seconds

		return self._compare_values(time_diff, threshold, comparison)


	async def _check_custom_condition(self, condition_data: Dict, user_id: str, guild_id: str,
									  activity_data: Dict, user_data: Dict) -> bool:
		"""Check custom conditions - extensible for future needs"""
		try:
			custom_type = condition_data.get("custom_type", "")

			# You can add custom condition types here
			if custom_type == "special_event":
				return self._check_special_event_condition(condition_data, user_data)
			elif custom_type == "guild_specific":
				return await self._check_guild_specific_condition(condition_data, guild_id, user_data)
			else:
				logger.warning(f"Unknown custom condition type: {custom_type}")
				return False

		except Exception as e:
			logger.error(f"Error checking custom condition: {e}")
			return False

	def _get_nested_value(self, data: Dict, field_path: str, default=0):
		"""Get nested dictionary value using dot notation (e.g., 'message_stats.messages')"""
		try:
			keys = field_path.split('.')
			value = data
			for key in keys:
				if isinstance(value, dict) and key in value:
					value = value[key]
				else:
					return default
			return value if value is not None else default
		except Exception as e:
			logger.error(f"Error getting nested value for path '{field_path}': {e}")
			return default

	def _compare_values(self, current_value, threshold, comparison="gte"):
		"""Compare values based on comparison operator"""
		try:
			current_value = float(current_value) if current_value is not None else 0
			threshold = float(threshold)

			if comparison == "gte":
				return current_value >= threshold
			elif comparison == "gt":
				return current_value > threshold
			elif comparison == "lte":
				return current_value <= threshold
			elif comparison == "lt":
				return current_value < threshold
			elif comparison == "eq":
				return current_value == threshold
			elif comparison == "ne":
				return current_value != threshold
			else:
				logger.warning(f"Unknown comparison operator: {comparison}")
				return False

		except (ValueError, TypeError) as e:
			logger.error(
				f"Error comparing values: current={current_value}, threshold={threshold}, comparison={comparison} - {e}")
			return False

	async def _check_time_pattern_condition_db(self, condition_data: Dict, user_id: str, guild_id: str) -> bool:
		"""Check time pattern conditions from local database data - UPDATED FOR LOCAL DB"""
		try:
			threshold = condition_data.get("threshold", 10)
			time_range = condition_data.get("time_range", {})
			comparison = condition_data.get("comparison", "gte")

			if not time_range or "start" not in time_range or "end" not in time_range:
				logger.warning(f"Invalid time range for time pattern condition: {time_range}")
				return False

			start_hour = int(time_range["start"].split(":")[0])
			start_minute = int(time_range["start"].split(":")[1])
			end_hour = int(time_range["end"].split(":")[0])
			end_minute = int(time_range["end"].split(":")[1])

			# Use local database instead of MongoDB aggregation
			if not self._activity_buffer_available():
				return False

			await self.leveling_system.activity_buffer.local_db.initialize()

			async with aiosqlite.connect(self.leveling_system.activity_buffer.local_db.db_path) as db:
				# Query for user's activity within the specified time range
				if start_hour <= end_hour:
					# Normal time range (e.g., 05:00 to 08:00)
					cursor = await db.execute("""
											  SELECT DATE(datetime(timestamp, 'unixepoch')) as activity_date,
													 COUNT(*)                               as activity_count
											  FROM user_activities
											  WHERE guild_id = ?
												AND user_id = ?
												AND event_type IN ('message_create', 'voice_state_update', 'reaction_add')
												AND (
												  (CAST(strftime('%H', datetime(timestamp, 'unixepoch')) AS INTEGER) >
												   ?) OR
												  (CAST(strftime('%H', datetime(timestamp, 'unixepoch')) AS INTEGER) =
												   ? AND
												   CAST(strftime('%M', datetime(timestamp, 'unixepoch')) AS INTEGER) >=
												   ?)
												  )
												AND (
												  (CAST(strftime('%H', datetime(timestamp, 'unixepoch')) AS INTEGER) <
												   ?) OR
												  (CAST(strftime('%H', datetime(timestamp, 'unixepoch')) AS INTEGER) =
												   ? AND
												   CAST(strftime('%M', datetime(timestamp, 'unixepoch')) AS INTEGER) <=
												   ?)
												  )
											  GROUP BY activity_date
											  HAVING activity_count >= 1
											  """,
											  (guild_id, user_id, start_hour, start_hour, start_minute, end_hour,
											   end_hour, end_minute))
				else:
					# Overnight time range (e.g., 23:00 to 02:00)
					cursor = await db.execute("""
											  SELECT DATE(datetime(timestamp, 'unixepoch')) as activity_date,
													 COUNT(*)                               as activity_count
											  FROM user_activities
											  WHERE guild_id = ?
												AND user_id = ?
												AND event_type IN ('message_create', 'voice_state_update', 'reaction_add')
												AND (
												  CAST(strftime('%H', datetime(timestamp, 'unixepoch')) AS INTEGER) >=
												  ? OR
												  CAST(strftime('%H', datetime(timestamp, 'unixepoch')) AS INTEGER) <=
												  ?
												  )
											  GROUP BY activity_date
											  HAVING activity_count >= 1
											  """, (guild_id, user_id, start_hour, end_hour))

				active_days = 0
				async for row in cursor:
					active_days += 1

			logger.debug(
				f"Time pattern check: {active_days} days active in time range {time_range['start']}-{time_range['end']}")

			return self._compare_values(active_days, threshold, comparison)

		except Exception as e:
			logger.error(f"Error checking time pattern condition: {e}")
			return False

	# ... existing code ...

	async def _check_weekend_activity_condition_db(self, condition_data: Dict, user_id: str, guild_id: str) -> bool:
		"""Check weekend activity conditions from local database data - UPDATED FOR LOCAL DB"""
		try:
			threshold = condition_data.get("threshold", 8)
			comparison = condition_data.get("comparison", "gte")
			min_activity_per_weekend = condition_data.get("min_activity_per_weekend", 10)

			# Use local database instead of MongoDB aggregation
			if not self._activity_buffer_available():
				return False

			await self.leveling_system.activity_buffer.local_db.initialize()

			async with aiosqlite.connect(self.leveling_system.activity_buffer.local_db.db_path) as db:
				# Query for weekend activity (Saturday = 6, Sunday = 0 in SQLite strftime '%w')
				cursor = await db.execute("""
										  SELECT strftime('%Y-%W', datetime(timestamp, 'unixepoch')) as year_week,
												 COUNT(*)                                            as activity_count
										  FROM user_activities
										  WHERE guild_id = ?
											AND user_id = ?
											AND event_type IN ('message_create', 'voice_state_update', 'reaction_add')
											AND CAST(strftime('%w', datetime(timestamp, 'unixepoch')) AS INTEGER) IN
												(0, 6)
										  GROUP BY year_week
										  HAVING activity_count >= ?
										  """, (guild_id, user_id, min_activity_per_weekend))

				active_weekends = 0
				async for row in cursor:
					active_weekends += 1

			logger.debug(
				f"Weekend activity check: {active_weekends} weekends with {min_activity_per_weekend}+ activities")

			return self._compare_values(active_weekends, threshold, comparison)

		except Exception as e:
			logger.error(f"Error checking weekend activity condition: {e}")
			return False

	async def _check_day_of_week_condition_db(self, condition_data: Dict, user_id: str, guild_id: str) -> bool:
		"""Check day of week conditions from local database data"""
		try:
			threshold = condition_data.get("threshold", 1)
			days = condition_data.get("days", [])
			comparison = condition_data.get("comparison", "gte")
			min_activity_per_day = condition_data.get("min_activity_per_day", 1)

			if not days:
				logger.warning("No days specified for day_of_week condition")
				return False

			# Convert day names to SQLite day numbers
			day_numbers = []
			for day in days:
				day_lower = day.lower()
				if day_lower in self.SQLITE_DAY_MAPPING:
					day_numbers.append(self.SQLITE_DAY_MAPPING[day_lower])
				else:
					logger.warning(f"Unknown day name: {day}")
					return False

			if not day_numbers:
				logger.warning("No valid days found for day_of_week condition")
				return False

			# Use local database instead of MongoDB aggregation
			if not self._activity_buffer_available():
				return False

			await self.leveling_system.activity_buffer.local_db.initialize()

			async with aiosqlite.connect(self.leveling_system.activity_buffer.local_db.db_path) as db:
				# Create placeholder string for IN clause
				placeholders = ','.join(['?' for _ in day_numbers])

				cursor = await db.execute(f"""
											  SELECT DATE(datetime(timestamp, 'unixepoch')) as activity_date,
													 COUNT(*)                               as activity_count
											  FROM user_activities
											  WHERE guild_id = ?
												AND user_id = ?
												AND event_type IN ('message_create', 'voice_state_update', 'reaction_add')
												AND CAST(strftime('%w', datetime(timestamp, 'unixepoch')) AS INTEGER) IN ({placeholders})
											  GROUP BY activity_date
											  HAVING activity_count >= ?
											  """, [guild_id, user_id] + day_numbers + [min_activity_per_day])

				active_days = 0
				async for row in cursor:
					active_days += 1

			logger.debug(
				f"Day of week check: {active_days} days active on specified days with {min_activity_per_day}+ activities")

			return self._compare_values(active_days, threshold, comparison)

		except Exception as e:
			logger.error(f"Error checking day of week condition: {e}")
			return False

	async def _check_day_of_month_condition_db(self, condition_data: Dict, user_id: str, guild_id: str) -> bool:
		"""Check day of month conditions from local database data"""
		try:
			threshold = condition_data.get("threshold", 1)
			days_of_month = condition_data.get("days_of_month", [])
			comparison = condition_data.get("comparison", "gte")
			min_activity_per_day = condition_data.get("min_activity_per_day", 1)

			if not days_of_month:
				logger.warning("No days of month specified for day_of_month condition")
				return False

			# Validate day numbers (1-31)
			valid_days = []
			for day in days_of_month:
				if isinstance(day, int) and 1 <= day <= 31:
					valid_days.append(day)
				else:
					logger.warning(f"Invalid day of month: {day}")

			if not valid_days:
				logger.warning("No valid days of month found for day_of_month condition")
				return False

			# Use local database instead of MongoDB aggregation
			if not self._activity_buffer_available():
				return False

			await self.leveling_system.activity_buffer.local_db.initialize()

			async with aiosqlite.connect(self.leveling_system.activity_buffer.local_db.db_path) as db:
				# Create placeholder string for IN clause
				placeholders = ','.join(['?' for _ in valid_days])

				cursor = await db.execute(f"""
											  SELECT DATE(datetime(timestamp, 'unixepoch')) as activity_date,
													 COUNT(*)                               as activity_count
											  FROM user_activities
											  WHERE guild_id = ?
												AND user_id = ?
												AND event_type IN ('message_create', 'voice_state_update', 'reaction_add')
												AND CAST(strftime('%d', datetime(timestamp, 'unixepoch')) AS INTEGER) IN ({placeholders})
											  GROUP BY activity_date
											  HAVING activity_count >= ?
											  """, [guild_id, user_id] + valid_days + [min_activity_per_day])

				active_days = 0
				async for row in cursor:
					active_days += 1

			logger.debug(
				f"Day of month check: {active_days} days active on specified days of month with {min_activity_per_day}+ activities")

			return self._compare_values(active_days, threshold, comparison)

		except Exception as e:
			logger.error(f"Error checking day of month condition: {e}")
			return False

	async def _check_weekday_weekend_condition_db(self, condition_data: Dict, user_id: str, guild_id: str) -> bool:
		"""Check weekday vs weekend conditions from local database data"""
		try:
			threshold = condition_data.get("threshold", 1)
			day_type = condition_data.get("day_type", "weekday")  # "weekday" or "weekend"
			comparison = condition_data.get("comparison", "gte")
			min_activity_per_day = condition_data.get("min_activity_per_day", 1)

			# Determine which days to check
			if day_type.lower() == "weekday":
				target_days = self.WEEKDAYS  # Monday-Friday
			elif day_type.lower() == "weekend":
				target_days = self.WEEKENDS  # Saturday-Sunday
			else:
				logger.warning(f"Invalid day_type: {day_type}. Must be 'weekday' or 'weekend'")
				return False

			# Use local database instead of MongoDB aggregation
			if not self._activity_buffer_available():
				return False

			await self.leveling_system.activity_buffer.local_db.initialize()

			async with aiosqlite.connect(self.leveling_system.activity_buffer.local_db.db_path) as db:
				# Create placeholder string for IN clause
				placeholders = ','.join(['?' for _ in target_days])

				cursor = await db.execute(f"""
											  SELECT DATE(datetime(timestamp, 'unixepoch')) as activity_date,
													 COUNT(*)                               as activity_count
											  FROM user_activities
											  WHERE guild_id = ?
												AND user_id = ?
												AND event_type IN ('message_create', 'voice_state_update', 'reaction_add')
												AND CAST(strftime('%w', datetime(timestamp, 'unixepoch')) AS INTEGER) IN ({placeholders})
											  GROUP BY activity_date
											  HAVING activity_count >= ?
											  """, [guild_id, user_id] + target_days + [min_activity_per_day])

				active_days = 0
				async for row in cursor:
					active_days += 1

			logger.debug(
				f"Weekday/Weekend check ({day_type}): {active_days} days active with {min_activity_per_day}+ activities")

			return self._compare_values(active_days, threshold, comparison)

		except Exception as e:
			logger.error(f"Error checking weekday_weekend condition: {e}")
			return False

	def _check_special_event_condition(self, condition_data: Dict, user_data: Dict) -> bool:
		"""Check special event conditions"""
		try:
			event_type = condition_data.get("event_type", "")
			event_data = condition_data.get("event_data", {})

			if event_type == "birthday":
				return self._check_birthday_event(event_data, user_data)
			elif event_type == "anniversary":
				return self._check_anniversary_event(event_data, user_data)
			elif event_type == "holiday":
				return self._check_holiday_event(event_data, user_data)
			elif event_type == "server_milestone":
				return self._check_server_milestone_event(event_data, user_data)
			elif event_type == "seasonal":
				return self._check_seasonal_event(event_data, user_data)
			else:
				logger.warning(f"Unknown special event type: {event_type}")
				return False

		except Exception as e:
			logger.error(f"Error checking special event condition: {e}")
			return False

	def _check_birthday_event(self, event_data: Dict, user_data: Dict) -> bool:
		"""Check if it's user's birthday or anniversary"""
		try:
			import datetime
			current_time = utc_now_ts()
			current_date = datetime.datetime.fromtimestamp(current_time)

			# Check if user has birthday set
			birthday = user_data.get("profile", {}).get("birthday")
			if not birthday:
				return False

			# Parse birthday (assuming format: "MM-DD" or timestamp)
			if isinstance(birthday, str):
				try:
					birthday_month, birthday_day = map(int, birthday.split("-"))
					return (current_date.month == birthday_month and
							current_date.day == birthday_day)
				except ValueError:
					return False
			elif isinstance(birthday, (int, float)):
				birthday_date = datetime.datetime.fromtimestamp(birthday)
				return (current_date.month == birthday_date.month and
						current_date.day == birthday_date.day)

			return False

		except Exception as e:
			logger.error(f"Error checking birthday event: {e}")
			return False

	def _check_anniversary_event(self, event_data: Dict, user_data: Dict) -> bool:
		"""Check if it's user's server anniversary"""
		try:
			import datetime
			current_time = utc_now_ts()
			current_date = datetime.datetime.fromtimestamp(current_time)

			created_at = user_data.get("created_at", 0)
			if not created_at:
				return False

			join_date = datetime.datetime.fromtimestamp(created_at)

			# Check if it's the anniversary (same month and day, but different year)
			return (current_date.month == join_date.month and
					current_date.day == join_date.day and
					current_date.year > join_date.year)

		except Exception as e:
			logger.error(f"Error checking anniversary event: {e}")
			return False

	def _check_holiday_event(self, event_data: Dict, user_data: Dict) -> bool:
		"""Check if it's a specific holiday"""
		try:
			import datetime
			current_time = utc_now_ts()
			current_date = datetime.datetime.fromtimestamp(current_time)

			holiday_dates = {
				"new_year": (1, 1),
				"valentines": (2, 14),
				"april_fools": (4, 1),
				"halloween": (10, 31),
				"christmas": (12, 25),
			}

			holiday = event_data.get("holiday", "").lower()
			if holiday in holiday_dates:
				month, day = holiday_dates[holiday]
				return current_date.month == month and current_date.day == day

			return False

		except Exception as e:
			logger.error(f"Error checking holiday event: {e}")
			return False

	def _check_server_milestone_event(self, event_data: Dict, user_data: Dict) -> bool:
		"""Check if server has reached a milestone"""
		try:
			milestone_type = event_data.get("milestone_type", "")
			milestone_value = event_data.get("milestone_value", 0)

			if milestone_type == "member_count":
				# This would require access to current server member count
				# Placeholder implementation
				logger.debug(f"Server milestone check for {milestone_type}: {milestone_value}")
				return False
			elif milestone_type == "server_age_days":
				# Check if server is X days old
				server_created = event_data.get("server_created_at", 0)
				if server_created:
					current_time = utc_now_ts()
					days_old = (current_time - server_created) / 86400
					return days_old >= milestone_value
				return False

			return False

		except Exception as e:
			logger.error(f"Error checking server milestone event: {e}")
			return False

	def _check_seasonal_event(self, event_data: Dict, user_data: Dict) -> bool:
		"""Check if it's a specific season"""
		try:
			import datetime
			current_time = utc_now_ts()
			current_date = datetime.datetime.fromtimestamp(current_time)

			season_ranges = {
				"spring": [(3, 20), (6, 20)],  # March 20 - June 20
				"summer": [(6, 21), (9, 22)],  # June 21 - September 22
				"autumn": [(9, 23), (12, 20)],  # September 23 - December 20
				"winter": [(12, 21), (3, 19)]  # December 21 - March 19
			}

			season = event_data.get("season", "").lower()
			if season in season_ranges:
				start_month, start_day = season_ranges[season][0]
				end_month, end_day = season_ranges[season][1]

				current_month_day = (current_date.month, current_date.day)

				if season == "winter":  # Winter spans across years
					return (current_month_day >= (start_month, start_day) or
							current_month_day <= (end_month, end_day))
				else:
					return ((start_month, start_day) <= current_month_day <= (end_month, end_day))

			return False

		except Exception as e:
			logger.error(f"Error checking seasonal event: {e}")
			return False

	async def _check_guild_specific_condition(self, condition_data: Dict, guild_id: str, user_data: Dict) -> bool:
		"""Check guild-specific conditions"""
		try:
			condition_type = condition_data.get("condition_type", "")

			if condition_type == "guild_role":
				return await self._check_guild_role_condition(condition_data, guild_id, user_data)
			elif condition_type == "guild_permission":
				return await self._check_guild_permission_condition(condition_data, guild_id, user_data)
			elif condition_type == "guild_channel_activity":
				return await self._check_guild_channel_activity_condition(condition_data, guild_id, user_data)
			elif condition_type == "guild_boost_status":
				return await self._check_guild_boost_condition(condition_data, guild_id, user_data)
			elif condition_type == "guild_custom_metric":
				return await self._check_guild_custom_metric_condition(condition_data, guild_id, user_data)
			else:
				logger.warning(f"Unknown guild-specific condition type: {condition_type}")
				return False

		except Exception as e:
			logger.error(f"Error checking guild-specific condition: {e}")
			return False

	async def _check_guild_role_condition(self, condition_data: Dict, guild_id: str, user_data: Dict) -> bool:
		"""Check if user has specific role in guild"""
		try:
			required_role = condition_data.get("role_name", "")
			role_level = condition_data.get("role_level", 0)  # For hierarchical roles

			user_roles = user_data.get("roles", [])

			if required_role:
				return required_role in user_roles
			elif role_level > 0:
				# Check if user has role of sufficient level
				user_role_levels = user_data.get("role_levels", {})
				max_level = max(user_role_levels.values()) if user_role_levels else 0
				return max_level >= role_level

			return False

		except Exception as e:
			logger.error(f"Error checking guild role condition: {e}")
			return False

	async def _check_guild_permission_condition(self, condition_data: Dict, guild_id: str, user_data: Dict) -> bool:
		"""Check if user has specific permission in guild"""
		try:
			required_permission = condition_data.get("permission", "")
			permissions = user_data.get("permissions", [])

			return required_permission in permissions

		except Exception as e:
			logger.error(f"Error checking guild permission condition: {e}")
			return False

	async def _check_guild_channel_activity_condition(self, condition_data: Dict, guild_id: str,
													  user_data: Dict) -> bool:
		"""Check activity in specific guild channels"""
		try:
			channel_type = condition_data.get("channel_type", "")
			threshold = condition_data.get("threshold", 1)
			comparison = condition_data.get("comparison", "gte")

			if channel_type == "voice_channels":
				activity_count = user_data.get("voice_stats", {}).get("channel_activity", {}).get(guild_id, 0)
			elif channel_type == "text_channels":
				activity_count = user_data.get("message_stats", {}).get("channel_activity", {}).get(guild_id, 0)
			else:
				return False

			return self._compare_values(activity_count, threshold, comparison)

		except Exception as e:
			logger.error(f"Error checking guild channel activity condition: {e}")
			return False

	async def _check_guild_boost_condition(self, condition_data: Dict, guild_id: str, user_data: Dict) -> bool:
		"""Check if user is boosting the guild"""
		try:
			boost_status = user_data.get("boost_status", {}).get(guild_id, False)
			boost_duration = user_data.get("boost_duration", {}).get(guild_id, 0)
			min_duration = condition_data.get("min_duration_days", 0)

			if not boost_status:
				return False

			if min_duration > 0:
				duration_days = boost_duration / 86400  # Convert seconds to days
				return duration_days >= min_duration

			return True

		except Exception as e:
			logger.error(f"Error checking guild boost condition: {e}")
			return False

	async def _check_guild_custom_metric_condition(self, condition_data: Dict, guild_id: str, user_data: Dict) -> bool:
		"""Check custom guild-specific metrics"""
		try:
			metric_name = condition_data.get("metric_name", "")
			threshold = condition_data.get("threshold", 1)
			comparison = condition_data.get("comparison", "gte")

			custom_metrics = user_data.get("custom_metrics", {}).get(guild_id, {})
			metric_value = custom_metrics.get(metric_name, 0)

			return self._compare_values(metric_value, threshold, comparison)

		except Exception as e:
			logger.error(f"Error checking guild custom metric condition: {e}")
			return False

	# Helper Section

	def _get_day_numbers_from_names(self, day_names: list) -> list:
		"""Convert day names to SQLite day numbers"""
		day_numbers = []
		for day_name in day_names:
			day_lower = day_name.lower()
			if day_lower in self.SQLITE_DAY_MAPPING:
				day_numbers.append(self.SQLITE_DAY_MAPPING[day_lower])
		return day_numbers

	def _is_weekday(self, day_number: int) -> bool:
		"""Check if a day number represents a weekday (Monday-Friday)"""
		return day_number in self.WEEKDAYS

	def _is_weekend(self, day_number: int) -> bool:
		"""Check if a day number represents a weekend (Saturday-Sunday)"""
		return day_number in self.WEEKENDS

	def _get_day_name_from_number(self, day_number: int) -> str:
		"""Convert SQLite day number to day name"""
		day_names = {
			0: "Sunday", 1: "Monday", 2: "Tuesday", 3: "Wednesday",
			4: "Thursday", 5: "Friday", 6: "Saturday"
		}
		return day_names.get(day_number, "Unknown")
