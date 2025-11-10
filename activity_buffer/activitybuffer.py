import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Callable, Awaitable
from pathlib import Path
import aiosqlite
import discord
from dotenv import load_dotenv

from loggers.logger_setup import get_logger

load_dotenv()

# =============================================================================
# SECTION: Utility Functions
# =============================================================================

logger = get_logger("ActivityBuffer")


def _utc_now_ts() -> float:
    """Get current UTC timestamp as float."""
    return datetime.now(timezone.utc).timestamp()


def _safe_channel_name(channel: Optional[discord.abc.GuildChannel]) -> str:
    """
    Safely extract the channel name with fallback handling.

    Args:
        channel: Discord channel object or None

    Returns:
        Channel name, channel ID, or 'unknown' as fallback
    """
    try:
        if not channel:
            return "None"
        return getattr(channel, "name", str(channel.id))
    except Exception as e:
        logger.debug(f"Unknown - {e}")
        return "unknown"


def _ts(dt: Optional[datetime]) -> float:
    """
    Convert datetime to UTC timestamp with timezone handling.

    Args:
        dt: Datetime object (naive or aware)

    Returns:
        UTC timestamp as float, 0.0 on error
    """
    try:
        if not dt:
            return 0.0
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception as e:
        logger.debug(f"Unknown - {e}")
        return 0.0


# =============================================================================
# SECTION: Local Activity Database
# =============================================================================

class LocalActivityDatabase:
    """
    Local SQLite database manager for user activities with comprehensive analytics.

    Features:
    - User activity tracking with efficient indexing
    - Guild event logging
    - Statistical rollups and summaries
    - Performance-optimized queries
    - Data retention policies
    """

    def __init__(self, db_path: str = "data/activity.db"):
        """Initialize the local activity database.

        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialized = False

    async def initialize(self):
        """Initialize the database schema and indexes."""
        if self._initialized:
            return

        logger.info(
            "initializing_activity_database",
            extra={
                "event": "database_init_start",
                "db_path": str(self.db_path)
            }
        )

        try:
            async with aiosqlite.connect(self.db_path) as db:
                # =============================================================
                # SUBSECTION: User Activities Table
                # =============================================================
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS user_activities (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        guild_id TEXT NOT NULL,
                        user_id TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        timestamp REAL NOT NULL,
                        data TEXT NOT NULL,
                        date_created TEXT NOT NULL
                    )
                """)

                # =============================================================
                # SUBSECTION: User Activities Indexes
                # =============================================================
                await db.execute("""
                    CREATE INDEX IF NOT EXISTS idx_user_activities_guild_user
                    ON user_activities (guild_id, user_id)
                """)
                await db.execute("""
                    CREATE INDEX IF NOT EXISTS idx_user_activities_event_type
                    ON user_activities (event_type)
                """)
                await db.execute("""
                    CREATE INDEX IF NOT EXISTS idx_user_activities_timestamp
                    ON user_activities (timestamp)
                """)
                await db.execute("""
                    CREATE INDEX IF NOT EXISTS idx_user_activities_date_created
                    ON user_activities (date_created)
                """)

                # =============================================================
                # SUBSECTION: Guild Events Table
                # =============================================================
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS guild_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        guild_id TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        timestamp REAL NOT NULL,
                        data TEXT NOT NULL,
                        date_created TEXT NOT NULL
                    )
                """)

                # =============================================================
                # SUBSECTION: Guild Events Indexes
                # =============================================================
                await db.execute("""
                    CREATE INDEX IF NOT EXISTS idx_guild_events_guild_id
                    ON guild_events (guild_id)
                """)
                await db.execute("""
                    CREATE INDEX IF NOT EXISTS idx_guild_events_event_type
                    ON guild_events (event_type)
                """)
                await db.execute("""
                    CREATE INDEX IF NOT EXISTS idx_guild_events_timestamp
                    ON guild_events (timestamp)
                """)

                # =============================================================
                # SUBSECTION: User Statistics Table
                # =============================================================
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS user_activity_stats (
                        guild_id TEXT NOT NULL,
                        user_id TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        count INTEGER DEFAULT 0,
                        last_activity REAL NOT NULL,
                        PRIMARY KEY (guild_id, user_id, event_type)
                    )
                """)

                # =============================================================
                # SUBSECTION: Daily Rollups Table
                # =============================================================
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS user_activity_rollups (
                        guild_id TEXT NOT NULL,
                        user_id TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        date TEXT NOT NULL, -- YYYY-MM-DD format
                        count INTEGER DEFAULT 0,
                        PRIMARY KEY (guild_id, user_id, event_type, date)
                    )
                """)

                # =============================================================
                # SUBSECTION: Rollups Performance Indexes
                # =============================================================
                await db.execute("""
                    CREATE INDEX IF NOT EXISTS idx_rollups_guild_user_date
                    ON user_activity_rollups (guild_id, user_id, date)
                """)
                await db.execute("""
                    CREATE INDEX IF NOT EXISTS idx_rollups_date
                    ON user_activity_rollups (date)
                """)
                await db.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_rollups_guild_user_event_date
                    ON user_activity_rollups (guild_id, user_id, event_type, date)
                """)

                await db.commit()

            self._initialized = True
            logger.info(
                "activity_database_initialized",
                extra={
                    "event": "database_init_success",
                    "tables_created": 4,
                    "indexes_created": 10
                }
            )

        except Exception as e:
            logger.error(
                "database_initialization_failed",
                extra={
                    "event": "database_init_error",
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            raise

    async def store_events(self, events: List[Dict[str, Any]]):
        """
        Store events in the appropriate tables based on their type.

        Args:
            events: List of event dictionaries to store
        """
        if not events:
            logger.debug("no_events_to_store", extra={"event": "store_events_skip_empty"})
            return

        await self.initialize()

        user_events = []
        guild_events = []
        storage_stats = {
            "total_events": len(events),
            "user_events": 0,
            "guild_events": 0
        }

        # =====================================================================
        # SUBSECTION: Event Categorization
        # =====================================================================
        for event in events:
            event_type = event.get('type', 'unknown')
            timestamp = event.get('timestamp', _utc_now_ts())
            data = event.get('data', {})
            date_created = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime('%Y-%m-%d')

            if self._is_user_event(event_type, data):
                guild_id = str(data.get('guild_id', ''))
                user_id = str(data.get('user_id', ''))

                if guild_id and user_id:
                    user_events.append((
                        guild_id, user_id, event_type, timestamp,
                        json.dumps(data), date_created
                    ))
                    storage_stats["user_events"] += 1
            else:
                guild_id = str(data.get('guild_id', ''))
                if guild_id:
                    guild_events.append((
                        guild_id, event_type, timestamp,
                        json.dumps(data), date_created
                    ))
                    storage_stats["guild_events"] += 1

        # =====================================================================
        # SUBSECTION: Database Storage
        # =====================================================================
        try:
            async with aiosqlite.connect(self.db_path) as db:
                # Insert user events
                if user_events:
                    await db.executemany("""
                        INSERT INTO user_activities 
                        (guild_id, user_id, event_type, timestamp, data, date_created)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, user_events)

                    # Update statistics and rollups
                    await self._update_user_stats(db, user_events)
                    await self._update_daily_rollups(db, user_events)

                # Insert guild events
                if guild_events:
                    await db.executemany("""
                        INSERT INTO guild_events 
                        (guild_id, event_type, timestamp, data, date_created)
                        VALUES (?, ?, ?, ?, ?)
                    """, guild_events)

                await db.commit()

            logger.debug(
                "events_stored_successfully",
                extra={
                    "event": "store_events_success",
                    **storage_stats
                }
            )

        except Exception as e:
            logger.error(
                "event_storage_failed",
                extra={
                    "event": "store_events_error",
                    **storage_stats,
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            raise

    async def _update_daily_rollups(self, db: aiosqlite.Connection, user_events: List[tuple]):
        """
        Update per-day rollups for user activities.

        Args:
            db: Database connection
            user_events: List of user event tuples
        """
        rollup_updates = {}

        for guild_id, user_id, event_type, timestamp, _, date_created in user_events:
            key = (guild_id, user_id, event_type, date_created)
            rollup_updates[key] = rollup_updates.get(key, 0) + 1

        for (guild_id, user_id, event_type, date), count in rollup_updates.items():
            await db.execute("""
                INSERT INTO user_activity_rollups (guild_id, user_id, event_type, date, count)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (guild_id, user_id, event_type, date) 
                DO UPDATE SET count = count + excluded.count
            """, (guild_id, user_id, event_type, date, count))

    def _is_user_event(self, event_type: str, data: Dict[str, Any]) -> bool:
        """
        Determine if an event should be stored as a user-specific event.

        Args:
            event_type: Type of the event
            data: Event data payload

        Returns:
            True if the event is user-specific, False otherwise
        """
        user_event_types = {
            'message_create', 'message_edit', 'message_delete', 'bulk_message_delete',
            'reaction_add', 'reaction_remove', 'reaction_clear', 'reaction_clear_emoji',
            'voice_join', 'voice_leave', 'voice_switch', 'voice_session_start', 'voice_session_end',
            'member_join', 'member_remove', 'member_update', 'member_ban', 'member_unban',
            'typing_start', 'channel_create', 'channel_delete', 'guild_update',
            'level_up', 'achievement_earned', 'voice_state_accumulate'
        }

        return event_type in user_event_types or 'user_id' in data

    async def _update_user_stats(self, db: aiosqlite.Connection, user_events: List[tuple]):
        """
        Update user activity statistics.

        Args:
            db: Database connection
            user_events: List of user event tuples
        """
        stats_updates = {}

        for guild_id, user_id, event_type, timestamp, _, _ in user_events:
            key = (guild_id, user_id, event_type)
            if key not in stats_updates:
                stats_updates[key] = {'count': 0, 'last_activity': timestamp}
            stats_updates[key]['count'] += 1
            stats_updates[key]['last_activity'] = max(stats_updates[key]['last_activity'], timestamp)

        # Update or insert statistics
        for (guild_id, user_id, event_type), stats in stats_updates.items():
            await db.execute("""
                INSERT INTO user_activity_stats (guild_id, user_id, event_type, count, last_activity)
                VALUES (?, ?, ?, ?, ?) 
                ON CONFLICT (guild_id, user_id, event_type) 
                DO UPDATE SET
                    count = count + excluded.count,
                    last_activity = MAX(last_activity, excluded.last_activity)
            """, (guild_id, user_id, event_type, stats['count'], stats['last_activity']))

    # =========================================================================
    # SECTION: Analytics and Reporting
    # =========================================================================

    async def get_user_activity_summary(self, guild_id: str, user_id: str, days: int = 7) -> Dict[str, Any]:
        """
        Get a comprehensive activity summary for a specific user.

        Args:
            guild_id: The guild ID to filter by
            user_id: The user ID to get summary for
            days: Number of days to look back

        Returns:
            Dictionary with user activity summary
        """
        await self.initialize()

        cutoff_timestamp = _utc_now_ts() - (days * 24 * 60 * 60)

        try:
            async with aiosqlite.connect(self.db_path) as db:
                # Get recent activity counts by type
                cursor = await db.execute("""
                    SELECT event_type, COUNT(*) as count, MAX(timestamp) as last_activity
                    FROM user_activities
                    WHERE guild_id = ? AND user_id = ? AND timestamp >= ?
                    GROUP BY event_type
                    ORDER BY count DESC
                """, (guild_id, user_id, cutoff_timestamp))

                activity_types = {}
                async for row in cursor:
                    activity_types[row[0]] = {
                        'count': row[1],
                        'last_activity': datetime.fromtimestamp(row[2], tz=timezone.utc).isoformat()
                    }

                # Get total activity count
                cursor = await db.execute("""
                    SELECT COUNT(*)
                    FROM user_activities
                    WHERE guild_id = ? AND user_id = ? AND timestamp >= ?
                """, (guild_id, user_id, cutoff_timestamp))

                total_activities = (await cursor.fetchone())[0]

                return {
                    'guild_id': guild_id,
                    'user_id': user_id,
                    'period_days': days,
                    'total_activities': total_activities,
                    'activity_breakdown': activity_types,
                    'analysis_timestamp': datetime.now(timezone.utc).isoformat()
                }

        except Exception as e:
            logger.error(
                "user_activity_summary_error",
                extra={
                    "event": "analytics_error",
                    "guild_id": guild_id,
                    "user_id": user_id,
                    "days": days,
                    "error": str(e)
                }
            )
            raise

    async def get_guild_activity_overview(self, guild_id: str, days: int = 7) -> Dict[str, Any]:
        """
        Get activity overview for a guild with top users and activity types.

        Args:
            guild_id: The guild ID to analyze
            days: Number of days to look back

        Returns:
            Dictionary with guild activity overview
        """
        await self.initialize()

        cutoff_timestamp = _utc_now_ts() - (days * 24 * 60 * 60)

        try:
            async with aiosqlite.connect(self.db_path) as db:
                # Most active users
                cursor = await db.execute("""
                    SELECT user_id, COUNT(*) as activity_count
                    FROM user_activities
                    WHERE guild_id = ? AND timestamp >= ?
                    GROUP BY user_id
                    ORDER BY activity_count DESC
                    LIMIT 10
                """, (guild_id, cutoff_timestamp))

                top_users = []
                async for row in cursor:
                    top_users.append({
                        'user_id': row[0],
                        'activity_count': row[1]
                    })

                # Activity by type
                cursor = await db.execute("""
                    SELECT event_type, COUNT(*) as count
                    FROM user_activities
                    WHERE guild_id = ? AND timestamp >= ?
                    GROUP BY event_type
                    ORDER BY count DESC
                """, (guild_id, cutoff_timestamp))

                activity_types = {}
                async for row in cursor:
                    activity_types[row[0]] = row[1]

                # Total guild activities
                cursor = await db.execute("""
                    SELECT COUNT(*)
                    FROM user_activities
                    WHERE guild_id = ? AND timestamp >= ?
                """, (guild_id, cutoff_timestamp))

                total_activities = (await cursor.fetchone())[0]

                return {
                    'guild_id': guild_id,
                    'period_days': days,
                    'total_activities': total_activities,
                    'top_users': top_users,
                    'activity_by_type': activity_types,
                    'analysis_timestamp': datetime.now(timezone.utc).isoformat()
                }

        except Exception as e:
            logger.error(
                "guild_activity_overview_error",
                extra={
                    "event": "analytics_error",
                    "guild_id": guild_id,
                    "days": days,
                    "error": str(e)
                }
            )
            raise

    async def get_user_daily_trends(self, guild_id: str, user_id: str, days: int = 30) -> Dict[str, Dict[str, int]]:
        """
        Get per-day activity counts for a user grouped by event type.

        Args:
            guild_id: The guild ID to filter by
            user_id: The user ID to analyze
            days: Number of days to look back

        Returns:
            Dictionary with daily activity trends
        """
        await self.initialize()

        cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime('%Y-%m-%d')

        try:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute("""
                    SELECT date, event_type, count
                    FROM user_activity_rollups
                    WHERE guild_id = ? AND user_id = ? AND date >= ?
                    ORDER BY date ASC
                """, (guild_id, user_id, cutoff_date))

                trends: Dict[str, Dict[str, int]] = {}
                async for row in cursor:
                    date, event_type, count = row
                    if date not in trends:
                        trends[date] = {}
                    trends[date][event_type] = count

                return trends

        except Exception as e:
            logger.error(
                "user_daily_trends_error",
                extra={
                    "event": "analytics_error",
                    "guild_id": guild_id,
                    "user_id": user_id,
                    "days": days,
                    "error": str(e)
                }
            )
            raise

    # =========================================================================
    # SECTION: Database Maintenance
    # =========================================================================

    async def prune_old_events(self, days: int = 90):
        """
        Delete old raw events while preserving rollups for long-term analytics.

        Args:
            days: Delete events older than this many days
        """
        cutoff_timestamp = _utc_now_ts() - (days * 24 * 60 * 60)

        try:
            async with aiosqlite.connect(self.db_path) as db:
                # Get counts before deletion for logging
                user_count_cursor = await db.execute(
                    "SELECT COUNT(*) FROM user_activities WHERE timestamp < ?",
                    (cutoff_timestamp,)
                )
                guild_count_cursor = await db.execute(
                    "SELECT COUNT(*) FROM guild_events WHERE timestamp < ?",
                    (cutoff_timestamp,)
                )

                user_deleted = (await user_count_cursor.fetchone())[0]
                guild_deleted = (await guild_count_cursor.fetchone())[0]

                # Perform deletion
                await db.execute("DELETE FROM user_activities WHERE timestamp < ?", (cutoff_timestamp,))
                await db.execute("DELETE FROM guild_events WHERE timestamp < ?", (cutoff_timestamp,))
                await db.commit()

                logger.info(
                    "old_events_pruned",
                    extra={
                        "event": "prune_complete",
                        "user_events_deleted": user_deleted,
                        "guild_events_deleted": guild_deleted,
                        "retention_days": days
                    }
                )

        except Exception as e:
            logger.error(
                "prune_old_events_error",
                extra={
                    "event": "prune_error",
                    "days": days,
                    "error": str(e)
                }
            )
            raise


# =============================================================================
# SECTION: Activity Buffer
# =============================================================================

class ActivityBuffer:
    """
    Buffers activity events to reduce database writes and improve performance.

    Features:
    - Local SQLite database storage with efficient indexing
    - Configurable buffer sizes and flush intervals
    - High-watermark flushing for overflow prevention
    - Background periodic flushing with jitter
    - Graceful shutdown with final flush
    - Comprehensive analytics and reporting
    - Data retention policies
    """

    def __init__(
            self,
            flush_interval: int = 30,
            max_buffer_size: int = 1000,
            high_watermark: Optional[int] = None,
            flush_handler: Optional[Callable[[List[Dict[str, Any]]], Awaitable[None]]] = None,
            jitter_seconds: float = 5.0,
            drop_oldest_on_overflow: bool = True,
            shutdown_flush_timeout: float = 10.0,
            db_path: str = "data/activity.db",
    ):
        """
        Initialize the activity buffer.

        Args:
            flush_interval: Seconds between automatic flushes
            max_buffer_size: Maximum number of events in buffer
            high_watermark: Flush when buffer reaches this size (default: 80% of max)
            flush_handler: Async function to call during flush
            jitter_seconds: Random jitter to avoid synchronized flushes
            drop_oldest_on_overflow: Whether to drop the oldest events on overflow
            shutdown_flush_timeout: Timeout for a final flush during shutdown
            db_path: Path to SQLite database file
        """
        self.flush_interval = flush_interval
        self.max_buffer_size = max_buffer_size
        self.high_watermark = high_watermark if high_watermark is not None else max(1, int(max_buffer_size * 0.8))
        self.flush_handler = flush_handler
        self.jitter_seconds = max(0.0, float(jitter_seconds))
        self.drop_oldest_on_overflow = drop_oldest_on_overflow
        self.shutdown_flush_timeout = shutdown_flush_timeout

        # Internal state
        self.buffer: List[Dict[str, Any]] = []
        self.last_flush = _utc_now_ts()
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._is_running = False

        # Initialize local database
        self.local_db = LocalActivityDatabase(db_path)

        logger.info(
            "activity_buffer_initialized",
            extra={
                "event": "buffer_init",
                "flush_interval": flush_interval,
                "max_buffer_size": max_buffer_size,
                "high_watermark": self.high_watermark,
                "jitter_seconds": jitter_seconds,
                "drop_oldest_on_overflow": drop_oldest_on_overflow
            }
        )

    async def add_event(self, event_type: str, data: Dict[str, Any]):
        """
        Add an event to the buffer with overflow handling.

        Args:
            event_type: Type of the event
            data: Event data payload
        """
        event_timestamp = _utc_now_ts()

        async with self._lock:
            # =================================================================
            # SUBSECTION: Overflow Policy Enforcement
            # =================================================================
            if len(self.buffer) >= self.max_buffer_size:
                if self.drop_oldest_on_overflow and self.buffer:
                    # Drop the oldest event to make room
                    dropped_event = self.buffer.pop(0)
                    logger.warning(
                        "buffer_overflow_oldest_dropped",
                        extra={
                            "event": "buffer_overflow",
                            "dropped_event_type": dropped_event.get('type', 'unknown'),
                            "current_buffer_size": len(self.buffer),
                            "max_buffer_size": self.max_buffer_size
                        }
                    )
                else:
                    logger.warning(
                        "buffer_overflow_event_skipped",
                        extra={
                            "event": "buffer_overflow",
                            "event_type": event_type,
                            "buffer_size": len(self.buffer),
                            "max_buffer_size": self.max_buffer_size
                        }
                    )
                    return

            # Add new event
            self.buffer.append({
                'type': event_type,
                'timestamp': event_timestamp,
                'data': data
            })

            current_buffer_size = len(self.buffer)

            # =================================================================
            # SUBSECTION: High-Watermark Early Flush
            # =================================================================
            if current_buffer_size >= self.high_watermark:
                logger.debug(
                    "high_watermark_flush_triggered",
                    extra={
                        "event": "high_watermark_flush",
                        "buffer_size": current_buffer_size,
                        "high_watermark": self.high_watermark
                    }
                )
                # Schedule flush but don't wait for it
                asyncio.create_task(self._flush_buffer())

    async def _flush_buffer(self):
        """
        Flush buffered events to a local database and optional handler.

        This method handles the actual flushing process with proper error handling
        and event recovery in case of failures.
        """
        if not self.buffer:
            return

        events_to_flush: List[Dict[str, Any]] = []

        # =====================================================================
        # SUBSECTION: Buffer Swap (Minimize Lock Time)
        # =====================================================================
        async with self._lock:
            if not self.buffer:
                return
            events_to_flush = self.buffer
            self.buffer = []

        flush_start_time = _utc_now_ts()

        try:
            # =================================================================
            # SUBSECTION: Local Database Storage
            # =================================================================
            await self.local_db.store_events(events_to_flush)

            # =================================================================
            # SUBSECTION: Optional Flush Handler
            # =================================================================
            if self.flush_handler:
                await self.flush_handler(events_to_flush)
            else:
                logger.debug(
                    "buffer_flushed_to_database",
                    extra={
                        "event": "flush_success",
                        "event_count": len(events_to_flush),
                        "flush_duration": _utc_now_ts() - flush_start_time
                    }
                )

        except Exception as exc:
            # =================================================================
            # SUBSECTION: Flush Failure Recovery
            # =================================================================
            logger.exception(
                "buffer_flush_failed",
                extra={
                    "event": "flush_error",
                    "event_count": len(events_to_flush),
                    "error": str(exc),
                    "error_type": type(exc).__name__
                }
            )

            # Re-queue events at the front to avoid data loss
            async with self._lock:
                self.buffer = events_to_flush + self.buffer

            raise

        finally:
            self.last_flush = _utc_now_ts()

    async def periodic_flush(self):
        """
        Background task for periodic flushing with jitter and error handling.
        """
        self._is_running = True

        logger.info(
            "periodic_flush_started",
            extra={
                "event": "periodic_flush_start",
                "flush_interval": self.flush_interval,
                "jitter_seconds": self.jitter_seconds
            }
        )

        try:
            while self._is_running:
                # Calculate sleep time with jitter
                sleep_time = float(self.flush_interval)
                if self.jitter_seconds > 0:
                    import random
                    sleep_time += random.uniform(0, self.jitter_seconds)

                await asyncio.sleep(sleep_time)

                # Check if we should flush
                should_flush = (
                        self.buffer and
                        (_utc_now_ts() - self.last_flush >= self.flush_interval)
                )

                if should_flush:
                    await self._flush_buffer()

        except asyncio.CancelledError:
            logger.info(
                "periodic_flush_cancelled",
                extra={"event": "periodic_flush_cancelled"}
            )
            raise

        except Exception as e:
            logger.error(
                "periodic_flush_error",
                extra={
                    "event": "periodic_flush_error",
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            raise

        finally:
            self._is_running = False

    def start(self) -> None:
        """Start the background periodic flush task."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.periodic_flush(), name="activity-buffer-flush")
            logger.info(
                "background_flush_started",
                extra={"event": "background_flush_start"}
            )

    async def stop(self) -> None:
        """Stop the background task and perform a final flush."""
        logger.info(
            "activity_buffer_stopping",
            extra={"event": "buffer_stop_start"}
        )

        self._is_running = False
        task = self._task
        self._task = None

        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Ensure any remaining events are flushed
        if self.buffer:
            try:
                await asyncio.wait_for(self._flush_buffer(), timeout=self.shutdown_flush_timeout)
                logger.info(
                    "final_flush_completed",
                    extra={
                        "event": "final_flush_success",
                        "events_flushed": len(self.buffer)
                    }
                )
            except asyncio.TimeoutError:
                logger.error(
                    "final_flush_timeout",
                    extra={
                        "event": "final_flush_timeout",
                        "timeout": self.shutdown_flush_timeout,
                        "remaining_events": len(self.buffer)
                    }
                )
            except Exception as e:
                logger.error(
                    "final_flush_failed",
                    extra={
                        "event": "final_flush_error",
                        "error": str(e),
                        "remaining_events": len(self.buffer)
                    }
                )

        logger.info(
            "activity_buffer_stopped",
            extra={"event": "buffer_stop_complete"}
        )

    async def flush_now(self) -> None:
        """Force an immediate flush."""
        logger.debug(
            "manual_flush_triggered",
            extra={"event": "manual_flush"}
        )
        await self._flush_buffer()

    def get_buffer_size(self) -> int:
        """Get the current buffer size."""
        return len(self.buffer)

    def get_buffer_stats(self) -> Dict[str, Any]:
        """Get comprehensive buffer statistics."""
        return {
            "buffer_size": len(self.buffer),
            "max_buffer_size": self.max_buffer_size,
            "high_watermark": self.high_watermark,
            "last_flush": self.last_flush,
            "time_since_last_flush": _utc_now_ts() - self.last_flush,
            "is_running": self._is_running,
            "background_task_active": self._task is not None and not self._task.done()
        }

    # =========================================================================
    # SECTION: Analytics Proxy Methods
    # =========================================================================

    async def get_user_activity_summary(self, guild_id: str, user_id: str, days: int = 7) -> Dict[str, Any]:
        """Get an activity summary for a specific user from the database."""
        return await self.local_db.get_user_activity_summary(guild_id, user_id, days)

    async def get_guild_activity_overview(self, guild_id: str, days: int = 7) -> Dict[str, Any]:
        """Get activity overview for a guild from the database."""
        return await self.local_db.get_guild_activity_overview(guild_id, days)

    async def get_user_daily_trends(self, guild_id: str, user_id: str, days: int = 30) -> Dict[str, Dict[str, int]]:
        """Get per-day activity trends for a user from rollups."""
        return await self.local_db.get_user_daily_trends(guild_id, user_id, days)

    async def prune_old_events(self, days: int = 90):
        """Prune old raw events while preserving rollups."""
        return await self.local_db.prune_old_events(days)