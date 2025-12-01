import time
import asyncio
import logging
import discord
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timezone, timedelta
from database.DatabaseManager import DatabaseManager, get_collection, DatabaseOperationError


class ActivitySystem:
    """
    Enhanced activity system that organizes data by guild -> user -> activity data.
    Tracks comprehensive user activity with detailed metrics and analytics.

    Features:
    - Hierarchical data organization (guild -> user -> activity data)
    - Daily activity patterns and streaks
    - Comprehensive activity metrics
    - Real-time analytics and reporting
    - Activity buffer integration
    - Performance optimized queries
    """

    def __init__(self, db_manager: DatabaseManager, database_name: str = "Activity"):
        """
        Initialize the ActivitySystem.

        Args:
            db_manager: An instance of the DatabaseManager.
            database_name: The name of the database to use.
        """
        self.db_manager = db_manager
        self.database_name = database_name
        self.collection_name = "UserActivity"  # Changed from "LastSeen" for better organization
        self.collection = None
        self.logger = logging.getLogger(__name__)

        # Cache for frequently accessed data
        self._guild_cache = {}
        self._cache_timeout = 300  # 5 minutes

    async def initialize(self):
        """
        Initializes the system by getting the collection and ensuring indexes are created.
        This should be called after the DatabaseManager is initialized.
        """
        self.logger.info("Initializing Enhanced ActivitySystem...")
        try:
            self.collection = get_collection(self.database_name, self.collection_name)
            await self._create_indexes()
            self.logger.info("✅ Enhanced ActivitySystem initialized successfully.")
        except DatabaseOperationError as e:
            self.logger.error(f"❌ Could not get collection '{self.database_name}.{self.collection_name}'. "
                              f"Please ensure the database is running and accessible. Error: {e}", exc_info=True)
            raise
        except Exception as e:
            self.logger.error(f"❌ An unexpected error occurred during ActivitySystem initialization: {e}",
                              exc_info=True)
            raise

    async def _ensure_user_document_exists(self, user_id: str, guild_id: str, timestamp: int):
        """
        Ensure a user document exists with proper array structure before incrementing.

        This prevents MongoDB WriteError conflicts between $setOnInsert and $inc operations
        on array fields.

        Args:
            user_id: User ID
            guild_id: Guild ID
            timestamp: Current timestamp for creation
        """
        try:
            # Try to insert a new document with proper structure
            # If document already exists, this will fail silently (expected)
            await self.collection.update_one(
                {"user_id": user_id, "guild_id": guild_id},
                {
                    "$setOnInsert": {
                        "user_id": user_id,
                        "guild_id": guild_id,
                        "created_at": timestamp,
                        "activity_patterns": {
                            "hourly_pattern": [0] * 24,
                            "weekly_pattern": [0] * 7,
                            "activity_streak": 0,
                            "longest_streak": 0,
                            "most_active_hour": 0,
                            "most_active_day": 0,
                            "last_streak_date": ""
                        },
                        "activity_summary": {
                            "total_activities": 0,
                            "message_count": 0,
                            "voice_minutes": 0.0,
                            "reaction_count": 0,
                            "total_voice_sessions": 0,
                            "unique_channels": []
                        },
                        "quality_metrics": {
                            "total_message_length": 0,
                            "emoji_usage": 0,
                            "link_shares": 0,
                            "attachment_shares": 0,
                            "thread_participation": 0,
                            "custom_emoji_usage": 0,
                            "unicode_emoji_usage": 0
                        },
                        "daily_stats": {}
                    }
                },
                upsert=True
            )
        except Exception as e:
            # If there's an error here, log it but don't fail the activity recording
            self.logger.debug(f"Document ensure operation for {user_id}: {e}")

    async def _create_indexes(self):
        """
        Create comprehensive indexes for efficient querying across all activity patterns.
        """
        if self.collection is None:
            self.logger.warning("Collection is not initialized, skipping index creation.")
            return

        self.logger.info("Creating database indexes for enhanced activity tracking...")
        try:
            # Primary compound index for guild-user queries
            await self.collection.create_index(
                [("guild_id", 1), ("user_id", 1)],
                name="guild_user_idx",
                unique=True
            )

            # Index for last activity time queries (most common query pattern)
            await self.collection.create_index(
                [("last_activity_timestamp", -1)],
                name="last_activity_idx"
            )

            # Guild-based activity queries
            await self.collection.create_index(
                [("guild_id", 1), ("last_activity_timestamp", -1)],
                name="guild_activity_time_idx"
            )

            # Activity type specific indexes
            await self.collection.create_index(
                [("guild_id", 1), ("activity_summary.last_message_time", -1)],
                name="guild_last_message_idx"
            )

            await self.collection.create_index(
                [("guild_id", 1), ("activity_summary.last_voice_time", -1)],
                name="guild_last_voice_idx"
            )

            await self.collection.create_index(
                [("guild_id", 1), ("activity_summary.last_reaction_time", -1)],
                name="guild_last_reaction_idx"
            )

            # Activity streak and pattern indexes
            await self.collection.create_index(
                [("guild_id", 1), ("activity_patterns.activity_streak", -1)],
                name="guild_streak_idx"
            )

            # Daily stats index for time-based queries
            await self.collection.create_index(
                [("guild_id", 1), ("last_activity_date", -1)],
                name="guild_daily_activity_idx"
            )

            self.logger.info("✅ Database indexes for enhanced activity tracking created.")
        except Exception as e:
            self.logger.error(f"❌ Error creating indexes: {e}", exc_info=True)

    async def record_activity(self, user_id: str, guild_id: str, activity_type: str,
                              activity_data: Optional[Dict[str, Any]] = None):
        """
        Record comprehensive user activity with detailed tracking and analytics.

        Args:
            user_id: The ID of the user.
            guild_id: The ID of the guild where the activity occurred.
            activity_type: The type of activity ('message', 'voice', 'reaction').
            activity_data: Optional detailed data about the activity.
        """
        if self.collection is None:
            self.logger.error("Cannot record activity: collection is not initialized.")
            return

        current_timestamp = int(time.time())
        current_date = datetime.fromtimestamp(current_timestamp, tz=timezone.utc).strftime('%Y-%m-%d')
        current_hour = datetime.fromtimestamp(current_timestamp, tz=timezone.utc).hour
        current_weekday = datetime.fromtimestamp(current_timestamp, tz=timezone.utc).weekday()

        # Prepare enhanced activity data with defaults
        activity_details = activity_data or {}

        try:
            # Ensure document exists with proper array structure
            # This prevents MongoDB conflict errors between $setOnInsert and $inc on array fields
            await self._ensure_user_document_exists(user_id, guild_id, current_timestamp)

            # Build the update operations dictionary.
            # Start with operators that are always present.
            update_operations = {
                # Set fields that change with every activity
                "$set": {
                    "last_activity_timestamp": current_timestamp,
                    "last_activity_type": activity_type,
                    "last_activity_date": current_date,
                    "updated_at": current_timestamp
                },
                # Increment counters (arrays now exist from ensure_document)
                "$inc": {
                    "activity_summary.total_activities": 1,
                    f"activity_patterns.hourly_pattern.{current_hour}": 1,
                    f"activity_patterns.weekly_pattern.{current_weekday}": 1,
                }
            }

            # Activity type specific modifications are now ADDED to the base dictionary.
            # The helper methods will add their specific $set, $inc, or $addToSet operations.
            # This avoids any path conflicts, as no field is initialized in $setOnInsert and then
            # modified by another operator.
            if activity_type == "message":
                await self._process_message_activity(
                    update_operations, activity_details, current_timestamp
                )
            elif activity_type == "voice":
                await self._process_voice_activity(
                    update_operations, activity_details, current_timestamp
                )
            elif activity_type == "reaction":
                await self._process_reaction_activity(
                    update_operations, activity_details, current_timestamp
                )

            # Update daily stats - this was another source of conflict.
            # It's now safe because 'daily_stats' is not in $setOnInsert.
            daily_key = f"daily_stats.{current_date}"
            update_operations["$inc"][f"{daily_key}.{activity_type}_count"] = 1
            update_operations["$set"][f"{daily_key}.last_activity"] = current_timestamp

            # Perform the atomic update
            result = await self.collection.update_one(
                {"user_id": user_id, "guild_id": guild_id},
                update_operations,
                upsert=True
            )

            # Update activity patterns and streaks after successful record
            if result.acknowledged:
                await self._update_activity_patterns(user_id, guild_id, current_timestamp, current_date)

            self.logger.debug(
                f"Recorded {activity_type} activity for user {user_id} in guild {guild_id} "
                f"with {len(activity_details)} metadata fields"
            )

        except Exception as e:
            self.logger.error(f"❌ Failed to record activity for user {user_id}: {e}", exc_info=True)

    async def _process_message_activity(self, update_ops: Dict, activity_data: Dict, timestamp: int):
        """Process message-specific activity data."""
        update_ops["$set"]["activity_summary.last_message_time"] = timestamp
        update_ops["$inc"]["activity_summary.message_count"] = 1

        # Track message quality metrics
        if "message_length" in activity_data:
            length = activity_data["message_length"]
            update_ops["$inc"]["quality_metrics.total_message_length"] = length

        if "emoji_count" in activity_data and activity_data["emoji_count"] > 0:
            update_ops["$inc"]["quality_metrics.emoji_usage"] = activity_data["emoji_count"]

        if "link_count" in activity_data and activity_data["link_count"] > 0:
            update_ops["$inc"]["quality_metrics.link_shares"] = activity_data["link_count"]

        if activity_data.get("has_attachments", False):
            update_ops["$inc"]["quality_metrics.attachment_shares"] = 1

        if activity_data.get("is_thread", False):
            update_ops["$inc"]["quality_metrics.thread_participation"] = 1

        # Track unique channels
        if "channel_id" in activity_data:
            update_ops["$addToSet"] = update_ops.get("$addToSet", {})
            update_ops["$addToSet"]["activity_summary.unique_channels"] = activity_data["channel_id"]

        # Store latest message context
        update_ops["$set"]["last_message_context"] = {
            "channel_id": activity_data.get("channel_id"),
            "channel_name": activity_data.get("channel_name", "Unknown"),
            "message_length": activity_data.get("message_length", 0),
            "has_links": activity_data.get("link_count", 0) > 0,
            "has_embeds": activity_data.get("has_embeds", False),
            "timestamp": timestamp
        }

    async def _process_voice_activity(self, update_ops: Dict, activity_data: Dict, timestamp: int):
        """Process voice-specific activity data."""
        update_ops["$set"]["activity_summary.last_voice_time"] = timestamp

        # Track voice session data
        if activity_data.get("event_type") == "join":
            update_ops["$inc"]["activity_summary.total_voice_sessions"] = 1

        if "session_duration" in activity_data:
            duration_minutes = activity_data["session_duration"] / 60
            update_ops["$inc"]["activity_summary.voice_minutes"] = duration_minutes

        # Track unique voice channels
        if "channel_id" in activity_data:
            update_ops["$addToSet"] = update_ops.get("$addToSet", {})
            update_ops["$addToSet"]["activity_summary.unique_channels"] = activity_data["channel_id"]

        # Store latest voice context
        update_ops["$set"]["last_voice_context"] = {
            "event_type": activity_data.get("event_type", "unknown"),
            "channel_id": activity_data.get("channel_id"),
            "channel_name": activity_data.get("channel_name", "Unknown"),
            "self_mute": activity_data.get("self_mute", False),
            "self_deaf": activity_data.get("self_deaf", False),
            "timestamp": timestamp
        }

    async def _process_reaction_activity(self, update_ops: Dict, activity_data: Dict, timestamp: int):
        """Process reaction-specific activity data."""
        update_ops["$set"]["activity_summary.last_reaction_time"] = timestamp
        update_ops["$inc"]["activity_summary.reaction_count"] = 1

        # Track reaction patterns
        if activity_data.get("is_custom_emoji", False):
            update_ops["$inc"]["quality_metrics.custom_emoji_usage"] = 1
        else:
            update_ops["$inc"]["quality_metrics.unicode_emoji_usage"] = 1

        # Store latest reaction context
        update_ops["$set"]["last_reaction_context"] = {
            "event_type": activity_data.get("event_type", "add"),
            "emoji": activity_data.get("emoji"),
            "is_custom": activity_data.get("is_custom_emoji", False),
            "channel_id": activity_data.get("channel_id"),
            "channel_name": activity_data.get("channel_name", "Unknown"),
            "timestamp": timestamp
        }

    async def _update_activity_patterns(self, user_id: str, guild_id: str, timestamp: int, current_date: str):
        """
        Update activity patterns including streaks, peak hours, and behavioral analytics.
        """
        try:
            current_datetime = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            current_hour = current_datetime.hour
            current_weekday = current_datetime.weekday()

            # Get current user document to calculate streaks
            user_doc = await self.collection.find_one({"user_id": user_id, "guild_id": guild_id})

            if not user_doc:
                return

            activity_patterns = user_doc.get("activity_patterns", {})
            daily_stats = user_doc.get("daily_stats", {})

            # Calculate activity streak
            yesterday = (current_datetime - timedelta(days=1)).strftime('%Y-%m-%d')
            last_streak_date = activity_patterns.get("last_streak_date", "")
            current_streak = activity_patterns.get("activity_streak", 0)

            # Streak logic
            if current_date != last_streak_date:
                if yesterday == last_streak_date:
                    # Continuing streak
                    current_streak += 1
                elif yesterday in daily_stats:
                    # Continuing streak (user was active yesterday)
                    current_streak += 1
                else:
                    # Streak broken, start new one
                    current_streak = 1

                longest_streak = max(current_streak, activity_patterns.get("longest_streak", 0))
            else:
                # Same day, no streak change
                longest_streak = activity_patterns.get("longest_streak", 0)

            # Update patterns
            pattern_updates = {
                "activity_patterns.activity_streak": current_streak,
                "activity_patterns.longest_streak": longest_streak,
                "activity_patterns.last_streak_date": current_date,
                "activity_patterns.most_active_hour": current_hour,
                "activity_patterns.most_active_day": current_weekday
            }

            await self.collection.update_one(
                {"user_id": user_id, "guild_id": guild_id},
                {"$set": pattern_updates}
            )

            self.logger.debug(
                f"Updated activity patterns for user {user_id}: streak={current_streak}, "
                f"longest={longest_streak}, hour={current_hour}, day={current_weekday}"
            )

        except Exception as e:
            self.logger.error(f"❌ Error updating activity patterns: {e}", exc_info=True)

    async def get_user_activity_summary(self, user_id: str, guild_id: str) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive activity summary for a user in a guild.

        Returns:
            Complete user activity profile with analytics
        """
        try:
            user_activity = await self.collection.find_one(
                {"user_id": user_id, "guild_id": guild_id}
            )

            if not user_activity:
                return None

            # Enhance with calculated metrics
            if user_activity:
                user_activity = await self._enhance_user_summary(user_activity)

            return user_activity

        except Exception as e:
            self.logger.error(f"❌ Error getting user activity summary: {e}", exc_info=True)
            return None

    async def _enhance_user_summary(self, user_activity: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance user activity summary with calculated metrics."""
        try:
            activity_summary = user_activity.get("activity_summary", {})
            quality_metrics = user_activity.get("quality_metrics", {})
            activity_patterns = user_activity.get("activity_patterns", {})

            # Normalize activity patterns to ensure arrays are in correct format
            activity_patterns = self._normalize_activity_patterns(activity_patterns)

            # Calculate average message length
            total_messages = activity_summary.get("message_count", 0)
            total_length = quality_metrics.get("total_message_length", 0)

            if total_messages > 0:
                quality_metrics["avg_message_length"] = total_length / total_messages

            # Calculate activity diversity score
            unique_channels = len(activity_summary.get("unique_channels", []))
            total_activities = activity_summary.get("total_activities", 0)

            if total_activities > 0:
                activity_summary["channel_diversity_score"] = min(unique_channels / max(total_activities * 0.1, 1), 1.0)
                activity_summary["voice_to_message_ratio"] = (
                        activity_summary.get("voice_minutes", 0) / max(total_messages, 1)
                )

            # Add time-based metrics
            created_at = user_activity.get("created_at", 0)
            last_activity = user_activity.get("last_activity_timestamp", 0)

            if created_at > 0 and last_activity > 0:
                total_days = max((last_activity - created_at) / 86400, 1)
                activity_summary["activities_per_day"] = total_activities / total_days

            # Add time-of-day breakdown
            hourly_pattern = activity_patterns.get("hourly_pattern", [])
            if hourly_pattern:
                activity_patterns["time_of_day_breakdown"] = self.analyze_time_of_day_distribution(hourly_pattern)

            # Add weekend vs weekday breakdown
            weekly_pattern = activity_patterns.get("weekly_pattern", [])
            if len(weekly_pattern) == 7:
                weekday_total = sum(weekly_pattern[:5])  # Mon-Fri
                weekend_total = sum(weekly_pattern[5:])  # Sat-Sun
                activity_patterns["weekend_vs_weekday"] = {
                    "weekday_total": weekday_total,
                    "weekend_total": weekend_total,
                    "weekday_percentage": round((weekday_total / max(weekday_total + weekend_total, 1)) * 100, 1),
                    "weekend_percentage": round((weekend_total / max(weekday_total + weekend_total, 1)) * 100, 1),
                    "prefers_weekend": weekend_total > weekday_total
                }

            user_activity["activity_summary"] = activity_summary
            user_activity["quality_metrics"] = quality_metrics
            user_activity["activity_patterns"] = activity_patterns

            return user_activity

        except Exception as e:
            self.logger.error(f"Error enhancing user summary: {e}", exc_info=True)
            return user_activity

    async def get_guild_activity_overview(self, guild_id: str, days: int = 7) -> Dict[str, Any]:
        """
        Get comprehensive activity overview for a guild with advanced analytics.

        Args:
            guild_id: The guild ID to analyze
            days: Number of days to look back

        Returns:
            Detailed guild activity analytics
        """
        try:
            cutoff_timestamp = int(time.time()) - (days * 24 * 60 * 60)

            # Aggregate pipeline for comprehensive guild analytics
            pipeline = [
                {
                    "$match": {
                        "guild_id": guild_id,
                        "last_activity_timestamp": {"$gte": cutoff_timestamp}
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "total_users": {"$sum": 1},
                        "total_messages": {"$sum": "$activity_summary.message_count"},
                        "total_voice_minutes": {"$sum": "$activity_summary.voice_minutes"},
                        "total_reactions": {"$sum": "$activity_summary.reaction_count"},
                        "total_activities": {"$sum": "$activity_summary.total_activities"},
                        "avg_streak": {"$avg": "$activity_patterns.activity_streak"},
                        "max_streak": {"$max": "$activity_patterns.longest_streak"},
                        "unique_channels": {"$addToSet": {"$arrayElemAt": ["$activity_summary.unique_channels", 0]}},
                        "most_active_hours": {"$addToSet": "$activity_patterns.most_active_hour"},
                        "users": {
                            "$push": {
                                "user_id": "$user_id",
                                "total_activities": "$activity_summary.total_activities",
                                "messages": "$activity_summary.message_count",
                                "voice_minutes": "$activity_summary.voice_minutes",
                                "reactions": "$activity_summary.reaction_count",
                                "last_active": "$last_activity_timestamp",
                                "streak": "$activity_patterns.activity_streak"
                            }
                        }
                    }
                }
            ]

            result = await self.collection.aggregate(pipeline).to_list(length=1)

            if not result:
                return self._empty_guild_overview(guild_id, days)

            data = result[0]

            # Sort and limit top users
            top_users = sorted(
                data.get("users", []),
                key=lambda x: x.get("total_activities", 0),
                reverse=True
            )[:25]

            # Calculate peak activity hour
            hour_counts = {}
            for hour in data.get("most_active_hours", []):
                hour_counts[hour] = hour_counts.get(hour, 0) + 1
            peak_hour = max(hour_counts.items(), key=lambda x: x[1])[0] if hour_counts else 0

            return {
                "guild_id": guild_id,
                "period_days": days,
                "analysis_timestamp": int(time.time()),
                "summary": {
                    "active_users": data.get("total_users", 0),
                    "total_messages": data.get("total_messages", 0),
                    "total_voice_minutes": round(data.get("total_voice_minutes", 0), 1),
                    "total_reactions": data.get("total_reactions", 0),
                    "total_activities": data.get("total_activities", 0),
                    "avg_user_streak": round(data.get("avg_streak", 0), 1),
                    "longest_user_streak": data.get("max_streak", 0),
                    "unique_channels_used": len(set(data.get("unique_channels", []))),
                    "peak_activity_hour": peak_hour
                },
                "top_users": top_users,
                "analytics": {
                    "messages_per_user": round(data.get("total_messages", 0) / max(data.get("total_users", 1), 1), 1),
                    "voice_minutes_per_user": round(
                        data.get("total_voice_minutes", 0) / max(data.get("total_users", 1), 1), 1),
                    "activities_per_user": round(data.get("total_activities", 0) / max(data.get("total_users", 1), 1),
                                                 1),
                    "hour_distribution": hour_counts
                }
            }

        except Exception as e:
            self.logger.error(f"❌ Error getting guild activity overview: {e}", exc_info=True)
            return {"guild_id": guild_id, "error": str(e)}

    def _empty_guild_overview(self, guild_id: str, days: int) -> Dict[str, Any]:
        """Return empty guild overview structure."""
        return {
            "guild_id": guild_id,
            "period_days": days,
            "analysis_timestamp": int(time.time()),
            "summary": {
                "active_users": 0,
                "total_messages": 0,
                "total_voice_minutes": 0.0,
                "total_reactions": 0,
                "total_activities": 0,
                "avg_user_streak": 0.0,
                "longest_user_streak": 0,
                "unique_channels_used": 0,
                "peak_activity_hour": 0
            },
            "top_users": [],
            "analytics": {
                "messages_per_user": 0.0,
                "voice_minutes_per_user": 0.0,
                "activities_per_user": 0.0,
                "hour_distribution": {}
            }
        }

    async def get_weekly_active_users_count(self, guild_id: str) -> int:
        """
        Get the number of unique users who were active in the last week.

        Args:
            guild_id: The guild ID

        Returns:
            Count of weekly active users
        """
        if self.collection is None:
            self.logger.error("Cannot get weekly active users: collection is not initialized.")
            return 0

        one_week_ago_timestamp = int(time.time()) - 7 * 24 * 60 * 60

        try:
            count = await self.collection.count_documents({
                "guild_id": guild_id,
                "last_activity_timestamp": {"$gte": one_week_ago_timestamp}
            })
            self.logger.info(f"Found {count} weekly active users for guild {guild_id}.")
            return count
        except Exception as e:
            self.logger.error(f"❌ Failed to get weekly active users for guild {guild_id}: {e}", exc_info=True)
            return 0

    async def get_user_daily_activity(self, user_id: str, guild_id: str, days: int = 30) -> Dict[str, Dict[str, Any]]:
        """
        Get detailed daily activity breakdown for a user over specified days.

        Args:
            user_id: User ID to analyze
            guild_id: Guild ID context
            days: Number of days to look back

        Returns:
            Dictionary mapping dates to activity data
        """
        try:
            user_activity = await self.collection.find_one(
                {"user_id": user_id, "guild_id": guild_id},
                {"daily_stats": 1, "activity_patterns": 1}
            )

            if not user_activity or "daily_stats" not in user_activity:
                return {}

            daily_stats = user_activity["daily_stats"]

            # Filter to only include the requested number of days
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
            cutoff_str = cutoff_date.strftime('%Y-%m-%d')

            filtered_stats = {}
            for date, stats in daily_stats.items():
                if date >= cutoff_str:
                    # Enhance daily stats with calculated metrics
                    enhanced_stats = dict(stats)
                    total_activities = (
                            enhanced_stats.get("message_count", 0) +
                            enhanced_stats.get("voice_count", 0) +
                            enhanced_stats.get("reaction_count", 0)
                    )
                    enhanced_stats["total_activities"] = total_activities
                    filtered_stats[date] = enhanced_stats

            return filtered_stats

        except Exception as e:
            self.logger.error(f"❌ Error getting user daily activity: {e}", exc_info=True)
            return {}

    async def get_activity_leaderboard(self, guild_id: str, activity_type: str = "total",
                                       days: int = 7, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get activity leaderboard for a guild.

        Args:
            guild_id: Guild to analyze
            activity_type: Type of activity ('total', 'messages', 'voice', 'reactions', 'streak')
            days: Time period to analyze
            limit: Number of top users to return

        Returns:
            List of top users with their activity metrics
        """
        try:
            cutoff_timestamp = int(time.time()) - (days * 24 * 60 * 60)

            # Define sort field based on activity type
            sort_fields = {
                "total": "activity_summary.total_activities",
                "messages": "activity_summary.message_count",
                "voice": "activity_summary.voice_minutes",
                "reactions": "activity_summary.reaction_count",
                "streak": "activity_patterns.activity_streak"
            }

            sort_field = sort_fields.get(activity_type, "activity_summary.total_activities")

            cursor = self.collection.find(
                {
                    "guild_id": guild_id,
                    "last_activity_timestamp": {"$gte": cutoff_timestamp}
                },
                {
                    "user_id": 1,
                    "activity_summary": 1,
                    "activity_patterns": 1,
                    "last_activity_timestamp": 1
                }
            ).sort(sort_field, -1).limit(limit)

            leaderboard = []
            rank = 1

            async for doc in cursor:
                summary = doc.get("activity_summary", {})
                patterns = doc.get("activity_patterns", {})

                leaderboard.append({
                    "rank": rank,
                    "user_id": doc["user_id"],
                    "total_activities": summary.get("total_activities", 0),
                    "message_count": summary.get("message_count", 0),
                    "voice_minutes": round(summary.get("voice_minutes", 0), 1),
                    "reaction_count": summary.get("reaction_count", 0),
                    "current_streak": patterns.get("activity_streak", 0),
                    "longest_streak": patterns.get("longest_streak", 0),
                    "last_active": doc.get("last_activity_timestamp", 0),
                    "score": doc.get(sort_field.split(".")[-1], 0)
                })
                rank += 1

            return leaderboard

        except Exception as e:
            self.logger.error(f"❌ Error getting activity leaderboard: {e}", exc_info=True)
            return []

    async def get_activity_insights(self, guild_id: str, days: int = 30) -> Dict[str, Any]:
        """
        Get advanced activity insights and trends for a guild.

        Args:
            guild_id: Guild to analyze
            days: Analysis period

        Returns:
            Comprehensive activity insights
        """
        try:
            cutoff_timestamp = int(time.time()) - (days * 24 * 60 * 60)

            # Aggregation pipeline for insights
            pipeline = [
                {"$match": {"guild_id": guild_id, "last_activity_timestamp": {"$gte": cutoff_timestamp}}},
                {
                    "$group": {
                        "_id": None,
                        "total_users": {"$sum": 1},
                        "avg_activities_per_user": {"$avg": "$activity_summary.total_activities"},
                        "most_active_hour": {"$max": "$activity_patterns.most_active_hour"},
                        "avg_streak": {"$avg": "$activity_patterns.activity_streak"},
                        "users_with_streaks": {
                            "$sum": {"$cond": [{"$gt": ["$activity_patterns.activity_streak", 0]}, 1, 0]}},
                        "total_unique_channels": {"$addToSet": "$activity_summary.unique_channels"},
                        "hourly_activity": {"$push": "$activity_patterns.hourly_pattern"},
                        "weekly_activity": {"$push": "$activity_patterns.weekly_pattern"}
                    }
                }
            ]

            result = await self.collection.aggregate(pipeline).to_list(length=1)

            if not result:
                return {"guild_id": guild_id, "insights": {}, "trends": {}}

            data = result[0]

            # Calculate insights
            total_users = data.get("total_users", 0)
            users_with_streaks = data.get("users_with_streaks", 0)
            streak_participation = (users_with_streaks / max(total_users, 1)) * 100

            # Aggregate hourly patterns for time-of-day analysis
            hourly_activity = data.get("hourly_activity", [])
            guild_hourly_pattern = [0] * 24
            for user_pattern in hourly_activity:
                if len(user_pattern) == 24:
                    for i, count in enumerate(user_pattern):
                        guild_hourly_pattern[i] += count

            # Get time-of-day breakdown
            time_of_day_data = self.analyze_time_of_day_distribution(guild_hourly_pattern)

            return {
                "guild_id": guild_id,
                "analysis_period_days": days,
                "insights": {
                    "total_active_users": total_users,
                    "avg_activities_per_user": round(data.get("avg_activities_per_user", 0), 1),
                    "most_active_hour": data.get("most_active_hour", 0),
                    "average_streak": round(data.get("avg_streak", 0), 1),
                    "streak_participation_rate": round(streak_participation, 1),
                    "channel_diversity": len(set().union(*data.get("total_unique_channels", [])))
                },
                "trends": {
                    "peak_activity_times": self._analyze_hourly_patterns(data.get("hourly_activity", [])),
                    "time_of_day_breakdown": time_of_day_data,
                    "weekly_distribution": self._analyze_weekly_patterns(data.get("weekly_activity", [])),
                    "engagement_level": self._calculate_engagement_level(data)
                },
                "generated_at": int(time.time())
            }

        except Exception as e:
            self.logger.error(f"❌ Error getting activity insights: {e}", exc_info=True)
            return {"guild_id": guild_id, "error": str(e)}

    def _analyze_hourly_patterns(self, hourly_data: List[List[int]]) -> Dict[str, Any]:
        """Analyze hourly activity patterns."""
        try:
            if not hourly_data:
                return {"peak_hours": [], "low_hours": [], "pattern": "unknown"}

            # Sum activity across all users for each hour
            hourly_totals = [0] * 24
            for user_pattern in hourly_data:
                if len(user_pattern) == 24:
                    for i, count in enumerate(user_pattern):
                        hourly_totals[i] += count

            # Find peak and low activity hours
            max_activity = max(hourly_totals)
            min_activity = min(hourly_totals)

            peak_hours = [i for i, activity in enumerate(hourly_totals) if activity >= max_activity * 0.8]
            low_hours = [i for i, activity in enumerate(hourly_totals) if activity <= min_activity * 1.2]

            return {
                "peak_hours": peak_hours,
                "low_hours": low_hours,
                "hourly_distribution": hourly_totals,
                "peak_activity_count": max_activity
            }

        except Exception as e:
            self.logger.error(f"Error analyzing hourly patterns: {e}")
            return {"peak_hours": [], "low_hours": [], "pattern": "error"}

    def _analyze_weekly_patterns(self, weekly_data: List[List[int]]) -> Dict[str, Any]:
        """Analyze weekly activity patterns."""
        try:
            if not weekly_data:
                return {"most_active_days": [], "least_active_days": [], "pattern": "unknown"}

            # Sum activity across all users for each day of week
            weekly_totals = [0] * 7
            for user_pattern in weekly_data:
                if len(user_pattern) == 7:
                    for i, count in enumerate(user_pattern):
                        weekly_totals[i] += count

            day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

            # Create day-activity pairs and sort
            day_activity_pairs = list(zip(day_names, weekly_totals))
            sorted_days = sorted(day_activity_pairs, key=lambda x: x[1], reverse=True)

            return {
                "most_active_days": [day for day, _ in sorted_days[:3]],
                "least_active_days": [day for day, _ in sorted_days[-3:]],
                "weekly_distribution": dict(day_activity_pairs),
                "weekend_vs_weekday": {
                    "weekday_total": sum(weekly_totals[:5]),
                    "weekend_total": sum(weekly_totals[5:])
                }
            }

        except Exception as e:
            self.logger.error(f"Error analyzing weekly patterns: {e}")
            return {"most_active_days": [], "least_active_days": [], "pattern": "error"}

    def _calculate_engagement_level(self, data: Dict) -> str:
        """Calculate overall engagement level."""
        try:
            avg_activities = data.get("avg_activities_per_user", 0)
            streak_participation = (data.get("users_with_streaks", 0) / max(data.get("total_users", 1), 1)) * 100

            if avg_activities >= 50 and streak_participation >= 70:
                return "very_high"
            elif avg_activities >= 25 and streak_participation >= 50:
                return "high"
            elif avg_activities >= 10 and streak_participation >= 30:
                return "moderate"
            elif avg_activities >= 5:
                return "low"
            else:
                return "very_low"

        except Exception:
            return "unknown"

    # Data Normalization Methods

    @staticmethod
    def _normalize_pattern_to_array(pattern_data: Union[Dict, List], expected_length: int) -> List[int]:
        """
        Normalize pattern data to array format.

        Handles legacy object format: {"0": 3, "1": 5, ...}
        Converts to array format: [3, 5, ...]

        Args:
            pattern_data: Pattern data (can be dict or list)
            expected_length: Expected array length (24 for hourly, 7 for weekly)

        Returns:
            List of integers with specified length
        """
        # If already a list of correct length, return it
        if isinstance(pattern_data, list):
            if len(pattern_data) == expected_length:
                return pattern_data
            else:
                # Wrong length, return zeros
                return [0] * expected_length

        # If it's a dict (legacy format), convert to array
        if isinstance(pattern_data, dict):
            result = [0] * expected_length
            for key, value in pattern_data.items():
                try:
                    index = int(key)
                    if 0 <= index < expected_length:
                        result[index] = int(value) if value else 0
                except (ValueError, TypeError):
                    continue
            return result

        # Invalid format, return zeros
        return [0] * expected_length

    def _normalize_activity_patterns(self, activity_patterns: Dict) -> Dict:
        """
        Normalize activity patterns to ensure arrays are in correct format.

        Args:
            activity_patterns: Raw activity patterns from database

        Returns:
            Normalized activity patterns with arrays
        """
        if not activity_patterns:
            return activity_patterns

        # Normalize hourly pattern
        if "hourly_pattern" in activity_patterns:
            activity_patterns["hourly_pattern"] = self._normalize_pattern_to_array(
                activity_patterns["hourly_pattern"], 24
            )

        # Normalize weekly pattern
        if "weekly_pattern" in activity_patterns:
            activity_patterns["weekly_pattern"] = self._normalize_pattern_to_array(
                activity_patterns["weekly_pattern"], 7
            )

        return activity_patterns

    # Time-of-Day and Weekend/Weekday Categorization Methods

    @staticmethod
    def categorize_hour_to_time_of_day(hour: int) -> str:
        """
        Categorize an hour (0-23) into a time-of-day period.

        Args:
            hour: Hour in 24-hour format (0-23)

        Returns:
            Time period: 'morning', 'afternoon', 'evening', 'night', or 'overnight'
        """
        if 6 <= hour < 12:
            return "morning"
        elif 12 <= hour < 18:
            return "afternoon"
        elif 18 <= hour < 23:
            return "evening"
        elif hour == 23 or hour < 2:
            return "night"
        else:  # 2-5
            return "overnight"

    @staticmethod
    def categorize_weekday(weekday: int) -> str:
        """
        Categorize a weekday (0-6) into 'weekend' or 'weekday'.

        Args:
            weekday: Day of week (0=Monday, 6=Sunday)

        Returns:
            'weekend' or 'weekday'
        """
        return "weekend" if weekday in [5, 6] else "weekday"

    def analyze_time_of_day_distribution(self, hourly_pattern: List[int]) -> Dict[str, Any]:
        """
        Analyze hourly activity pattern and break it down by time-of-day periods.

        Args:
            hourly_pattern: List of 24 integers representing activity by hour

        Returns:
            Dictionary with time-of-day breakdowns and insights
        """
        try:
            if not hourly_pattern or len(hourly_pattern) != 24:
                return self._empty_time_of_day_distribution()

            # Initialize period counters
            periods = {
                "morning": 0,      # 6-11
                "afternoon": 0,    # 12-17
                "evening": 0,      # 18-22
                "night": 0,        # 23-1
                "overnight": 0     # 2-5
            }

            # Sum activities by period
            for hour, activity_count in enumerate(hourly_pattern):
                period = self.categorize_hour_to_time_of_day(hour)
                periods[period] += activity_count

            total_activities = sum(periods.values())

            # Calculate percentages
            percentages = {}
            for period, count in periods.items():
                percentages[period] = round((count / max(total_activities, 1)) * 100, 1)

            # Find most and least active periods
            sorted_periods = sorted(periods.items(), key=lambda x: x[1], reverse=True)
            most_active_period = sorted_periods[0][0] if sorted_periods else "unknown"
            least_active_period = sorted_periods[-1][0] if sorted_periods else "unknown"

            return {
                "total_activities": total_activities,
                "by_period": periods,
                "percentages": percentages,
                "most_active_period": most_active_period,
                "least_active_period": least_active_period,
                "sorted_periods": [period for period, _ in sorted_periods]
            }

        except Exception as e:
            self.logger.error(f"Error analyzing time-of-day distribution: {e}")
            return self._empty_time_of_day_distribution()

    def _empty_time_of_day_distribution(self) -> Dict[str, Any]:
        """Return empty time-of-day distribution structure."""
        return {
            "total_activities": 0,
            "by_period": {
                "morning": 0,
                "afternoon": 0,
                "evening": 0,
                "night": 0,
                "overnight": 0
            },
            "percentages": {
                "morning": 0.0,
                "afternoon": 0.0,
                "evening": 0.0,
                "night": 0.0,
                "overnight": 0.0
            },
            "most_active_period": "unknown",
            "least_active_period": "unknown",
            "sorted_periods": []
        }

    async def get_user_time_of_day_breakdown(self, user_id: str, guild_id: str) -> Dict[str, Any]:
        """
        Get detailed time-of-day activity breakdown for a specific user.

        Args:
            user_id: User ID to analyze
            guild_id: Guild ID context

        Returns:
            Comprehensive time-of-day analysis for the user
        """
        try:
            user_activity = await self.collection.find_one(
                {"user_id": user_id, "guild_id": guild_id},
                {"activity_patterns": 1, "activity_summary": 1}
            )

            if not user_activity:
                return {
                    "user_id": user_id,
                    "guild_id": guild_id,
                    "error": "User not found",
                    "time_of_day_breakdown": self._empty_time_of_day_distribution()
                }

            activity_patterns = user_activity.get("activity_patterns", {})
            # Normalize patterns to ensure they're arrays
            activity_patterns = self._normalize_activity_patterns(activity_patterns)
            hourly_pattern = activity_patterns.get("hourly_pattern", [0] * 24)

            time_of_day_data = self.analyze_time_of_day_distribution(hourly_pattern)

            # Add contextual information
            most_active_hour = activity_patterns.get("most_active_hour", 0)
            time_of_day_data["most_active_hour"] = most_active_hour
            time_of_day_data["most_active_hour_period"] = self.categorize_hour_to_time_of_day(most_active_hour)

            return {
                "user_id": user_id,
                "guild_id": guild_id,
                "time_of_day_breakdown": time_of_day_data,
                "hourly_pattern": hourly_pattern
            }

        except Exception as e:
            self.logger.error(f"Error getting user time-of-day breakdown: {e}", exc_info=True)
            return {
                "user_id": user_id,
                "guild_id": guild_id,
                "error": str(e),
                "time_of_day_breakdown": self._empty_time_of_day_distribution()
            }

    async def get_guild_time_of_day_insights(self, guild_id: str, days: int = 7) -> Dict[str, Any]:
        """
        Get guild-wide time-of-day activity insights.

        Args:
            guild_id: Guild to analyze
            days: Number of days to look back

        Returns:
            Comprehensive time-of-day insights for the entire guild
        """
        try:
            cutoff_timestamp = int(time.time()) - (days * 24 * 60 * 60)

            # Fetch all active users' hourly patterns
            cursor = self.collection.find(
                {
                    "guild_id": guild_id,
                    "last_activity_timestamp": {"$gte": cutoff_timestamp}
                },
                {"activity_patterns.hourly_pattern": 1}
            )

            # Aggregate hourly patterns across all users
            guild_hourly_pattern = [0] * 24
            user_count = 0

            async for doc in cursor:
                activity_patterns = doc.get("activity_patterns", {})
                # Normalize patterns to handle legacy object format
                activity_patterns = self._normalize_activity_patterns(activity_patterns)
                hourly_pattern = activity_patterns.get("hourly_pattern", [])
                if len(hourly_pattern) == 24:
                    for i, count in enumerate(hourly_pattern):
                        guild_hourly_pattern[i] += count
                    user_count += 1

            if user_count == 0:
                return {
                    "guild_id": guild_id,
                    "period_days": days,
                    "active_users": 0,
                    "time_of_day_breakdown": self._empty_time_of_day_distribution()
                }

            # Analyze the aggregated pattern
            time_of_day_data = self.analyze_time_of_day_distribution(guild_hourly_pattern)

            # Find peak hour
            peak_hour = guild_hourly_pattern.index(max(guild_hourly_pattern))

            return {
                "guild_id": guild_id,
                "period_days": days,
                "active_users": user_count,
                "time_of_day_breakdown": time_of_day_data,
                "peak_hour": peak_hour,
                "peak_hour_period": self.categorize_hour_to_time_of_day(peak_hour),
                "hourly_pattern": guild_hourly_pattern,
                "generated_at": int(time.time())
            }

        except Exception as e:
            self.logger.error(f"Error getting guild time-of-day insights: {e}", exc_info=True)
            return {
                "guild_id": guild_id,
                "error": str(e),
                "time_of_day_breakdown": self._empty_time_of_day_distribution()
            }

    # Database Migration and Maintenance Methods

    async def migrate_patterns_to_arrays(self, dry_run: bool = True) -> Dict[str, Any]:
        """
        Migrate hourly_pattern and weekly_pattern from object format to array format.

        This method fixes documents that have patterns stored as objects:
        {"0": 3, "1": 5, ...} -> [3, 5, ...]

        Args:
            dry_run: If True, only report what would be changed without modifying data

        Returns:
            Migration statistics
        """
        if self.collection is None:
            self.logger.error("Cannot migrate: collection is not initialized.")
            return {"error": "Collection not initialized"}

        self.logger.info(f"Starting pattern migration (dry_run={dry_run})...")

        stats = {
            "total_documents": 0,
            "documents_needing_migration": 0,
            "hourly_pattern_migrated": 0,
            "weekly_pattern_migrated": 0,
            "errors": 0,
            "dry_run": dry_run
        }

        try:
            # Find all documents with activity patterns
            cursor = self.collection.find({"activity_patterns": {"$exists": True}})

            async for doc in cursor:
                stats["total_documents"] += 1
                doc_id = doc["_id"]
                needs_migration = False
                update_fields = {}

                activity_patterns = doc.get("activity_patterns", {})

                # Check hourly pattern
                hourly_pattern = activity_patterns.get("hourly_pattern")
                if hourly_pattern and isinstance(hourly_pattern, dict):
                    needs_migration = True
                    normalized_hourly = self._normalize_pattern_to_array(hourly_pattern, 24)
                    update_fields["activity_patterns.hourly_pattern"] = normalized_hourly
                    stats["hourly_pattern_migrated"] += 1
                    self.logger.debug(f"Document {doc_id}: hourly_pattern needs migration")

                # Check weekly pattern
                weekly_pattern = activity_patterns.get("weekly_pattern")
                if weekly_pattern and isinstance(weekly_pattern, dict):
                    needs_migration = True
                    normalized_weekly = self._normalize_pattern_to_array(weekly_pattern, 7)
                    update_fields["activity_patterns.weekly_pattern"] = normalized_weekly
                    stats["weekly_pattern_migrated"] += 1
                    self.logger.debug(f"Document {doc_id}: weekly_pattern needs migration")

                if needs_migration:
                    stats["documents_needing_migration"] += 1

                    if not dry_run:
                        try:
                            await self.collection.update_one(
                                {"_id": doc_id},
                                {"$set": update_fields}
                            )
                            self.logger.debug(f"✓ Migrated document {doc_id}")
                        except Exception as e:
                            stats["errors"] += 1
                            self.logger.error(f"✗ Error migrating document {doc_id}: {e}")

            self.logger.info(
                f"Migration {'simulation' if dry_run else 'complete'}:\n"
                f"  Total documents: {stats['total_documents']}\n"
                f"  Need migration: {stats['documents_needing_migration']}\n"
                f"  Hourly patterns: {stats['hourly_pattern_migrated']}\n"
                f"  Weekly patterns: {stats['weekly_pattern_migrated']}\n"
                f"  Errors: {stats['errors']}"
            )

            return stats

        except Exception as e:
            self.logger.error(f"Error during migration: {e}", exc_info=True)
            stats["error"] = str(e)
            return stats

    # Backward compatibility and utility methods

    async def cleanup_bot_data(self, bot: discord.Client):
        """
        One-time function to remove bot data from the activity collection.
        Maintains backward compatibility.
        """
        self.logger.info("Starting bot activity data cleanup...")
        if self.collection is None:
            self.logger.error("Cannot cleanup bot data: collection is not initialized.")
            return

        try:
            all_activity_cursor = self.collection.find({}, {"user_id": 1})
            all_activity = await all_activity_cursor.to_list(length=None)

            bots_found_and_deleted = 0
            for activity in all_activity:
                user_id = activity.get("user_id")
                if not user_id:
                    continue

                try:
                    user = await bot.fetch_user(int(user_id))
                    if user and user.bot:
                        self.logger.info(f"Found bot user {user_id} ({user.name}). Deleting from activity tracking.")
                        await self.collection.delete_one({"_id": activity["_id"]})
                        bots_found_and_deleted += 1

                except discord.NotFound:
                    self.logger.warning(f"User with ID {user_id} not found. Cannot determine if it is a bot. Skipping.")
                except Exception as e:
                    self.logger.error(f"An error occurred while processing user {user_id}: {e}", exc_info=True)

            self.logger.info(
                f"Bot activity data cleanup complete. Found and deleted {bots_found_and_deleted} bot entries.")

        except Exception as e:
            self.logger.error(f"An error occurred during bot data cleanup: {e}", exc_info=True)

    async def migrate_old_data(self):
        """Migrate data from old LastSeen format to new UserActivity format."""
        self.logger.info("Starting migration from old activity data format...")

        try:
            # Get old collection if it exists
            old_collection = get_collection(self.database_name, "LastSeen")

            if old_collection is None:
                self.logger.info("No old LastSeen collection found, skipping migration.")
                return

            # Count documents to migrate
            old_count = await old_collection.count_documents({})
            if old_count == 0:
                self.logger.info("No documents in old collection, skipping migration.")
                return

            self.logger.info(f"Found {old_count} documents to migrate.")

            # Migrate in batches
            batch_size = 100
            migrated = 0

            async for old_doc in old_collection.find():
                try:
                    # Create new format document
                    new_doc = {
                        "user_id": old_doc["user_id"],
                        "guild_id": old_doc["guild_id"],
                        "last_activity_timestamp": old_doc.get("last_seen_timestamp", 0),
                        "last_activity_type": old_doc.get("activity_type", "unknown"),
                        "last_activity_date": datetime.fromtimestamp(
                            old_doc.get("last_seen_timestamp", 0), tz=timezone.utc
                        ).strftime('%Y-%m-%d'),
                        "created_at": old_doc.get("last_seen_timestamp", 0),
                        "updated_at": int(time.time()),
                        "activity_summary": {
                            "total_activities": 1,
                            "message_count": 1 if old_doc.get("activity_type") == "message" else 0,
                            "voice_minutes": 0.0,
                            "reaction_count": 1 if old_doc.get("activity_type") == "reaction" else 0,
                            "first_activity_time": old_doc.get("last_seen_timestamp", 0),
                            "last_message_time": old_doc.get("last_seen_timestamp", 0) if old_doc.get(
                                "activity_type") == "message" else 0,
                            "last_voice_time": old_doc.get("last_seen_timestamp", 0) if old_doc.get(
                                "activity_type") == "voice" else 0,
                            "last_reaction_time": old_doc.get("last_seen_timestamp", 0) if old_doc.get(
                                "activity_type") == "reaction" else 0,
                            "unique_channels": [],
                            "peak_activity_hour": 12,
                            "total_voice_sessions": 0
                        },
                        "daily_stats": {},
                        "activity_patterns": {
                            "most_active_hour": 12,
                            "most_active_day": 0,
                            "activity_streak": 0,
                            "longest_streak": 0,
                            "last_streak_date": datetime.fromtimestamp(
                                old_doc.get("last_seen_timestamp", 0), tz=timezone.utc
                            ).strftime('%Y-%m-%d'),
                            "weekly_pattern": [0] * 7,
                            "hourly_pattern": [0] * 24
                        },
                        "quality_metrics": {
                            "avg_message_length": 0,
                            "emoji_usage": 0,
                            "link_shares": 0,
                            "attachment_shares": 0,
                            "thread_participation": 0
                        }
                    }

                    # Insert into new collection (use upsert to handle duplicates)
                    await self.collection.update_one(
                        {"user_id": new_doc["user_id"], "guild_id": new_doc["guild_id"]},
                        {"$setOnInsert": new_doc},
                        upsert=True
                    )

                    migrated += 1

                    if migrated % batch_size == 0:
                        self.logger.info(f"Migrated {migrated}/{old_count} documents...")

                except Exception as e:
                    self.logger.error(f"Error migrating document {old_doc.get('_id')}: {e}")
                    continue

            self.logger.info(f"Migration completed. Successfully migrated {migrated}/{old_count} documents.")

        except Exception as e:
            self.logger.error(f"Error during migration: {e}", exc_info=True)