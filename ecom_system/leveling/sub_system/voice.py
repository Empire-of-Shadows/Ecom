import logging
import time
import asyncio
from typing import Dict, Any, Tuple, Optional
from datetime import datetime, timezone

from discord import VoiceState

from ecom_system.Listeners.VoiceSessions import VoiceSession
from ecom_system.helpers.helpers import utc_now_ts, utc_today_key, utc_week_key, utc_month_key
from ecom_system.helpers.rate_limiter import rate_limiter
from ecom_system.helpers.daily_streak import check_and_update_streak, create_streak_update_data
from loggers.logger_setup import get_logger, log_performance, PerformanceLogger

logger = get_logger("VoiceLeveling", level=logging.DEBUG, json_format=False, colored_console=True)

# Import the VoiceSession class


class VoiceLevelingSystem:
    """
    Voice processing subsystem for the leveling system.
    Manages voice sessions and calculates rewards based on active participation.
    Integrated with the main leveling system structure.
    """

    def __init__(self, leveling_system):
        """Initialize with reference to parent LevelingSystem"""
        self.leveling_system = leveling_system
        self.logger = logger

        # Active voice sessions: (guild_id, user_id) -> VoiceSession
        self.voice_sessions: Dict[Tuple[str, str], VoiceSession] = {}

        # Session cleanup task
        self._cleanup_task = None

    async def initialize(self):
        """Initialize voice system and start cleanup task."""
        try:
            # Start background cleanup task
            self._cleanup_task = asyncio.create_task(self._periodic_cleanup())
            logger.info("✅ VoiceLevelingSystem initialized with periodic cleanup")
        except Exception as e:
            logger.error(f"❌ VoiceLevelingSystem initialization failed: {e}")

    async def shutdown(self):
        """Shutdown voice system and cleanup resources."""
        try:
            if self._cleanup_task:
                self._cleanup_task.cancel()
                try:
                    await self._cleanup_task
                except asyncio.CancelledError:
                    pass

            # Process any remaining active sessions
            await self._cleanup_all_sessions()
            logger.info("✅ VoiceLevelingSystem shutdown complete")
        except Exception as e:
            logger.error(f"❌ VoiceLevelingSystem shutdown error: {e}")

    @log_performance("process_voice_state_update")
    async def process_voice_state_update(
            self,
            user_id: str,
            guild_id: str,
            before: VoiceState,
            after: VoiceState,
    ):
        """
        Process a voice state update to manage sessions and calculate rewards.
        Integrated with main leveling system structure.
        """
        start_time = time.time()
        session_key = (guild_id, user_id)
        current_time = time.time()

        logger.info(f"🔄 Processing voice state update: G:{guild_id} U:{user_id}")

        try:
            # User joins a voice channel or moves between channels
            if after.channel is not None:
                await self._handle_voice_join(session_key, user_id, guild_id, after, current_time)

            # User leaves a voice channel
            elif before.channel is not None and after.channel is None:
                await self._handle_voice_leave(session_key, user_id, guild_id, current_time)

            # User state change within same channel
            elif before.channel is not None and after.channel is not None and before.channel == after.channel:
                await self._handle_voice_state_change(session_key, user_id, guild_id, after, current_time)

            processing_time = (time.time() - start_time) * 1000
            logger.debug(f"✅ Voice state update processed in {processing_time:.2f}ms")

        except Exception as e:
            logger.error(f"❌ Error processing voice state update: {e}", exc_info=True)

    async def _handle_voice_join(self, session_key: Tuple[str, str], user_id: str, guild_id: str,
                                 after: VoiceState, current_time: float):
        """Handle user joining a voice channel."""
        if session_key not in self.voice_sessions:
            # New session - user joins for the first time
            session = VoiceSession(
                start_time=current_time,
                is_muted=after.mute,
                is_deafened=after.deaf,
                is_self_muted=after.self_mute,
                is_self_deafened=after.self_deaf,
            )
            self.voice_sessions[session_key] = session
            logger.info(f"🎤 Voice session started for U:{user_id} in G:{guild_id} "
                        f"(channel: {after.channel.name})")

            # Check for daily streak on new session
            user_data = await self.leveling_system.get_user_data(user_id, guild_id)
            if user_data:
                new_streak, should_update_streak = check_and_update_streak(user_data)
                if should_update_streak:
                    streak_update_data = create_streak_update_data(new_streak)
                    await self.leveling_system.update_user_data(
                        user_id, guild_id, {"$set": streak_update_data}
                    )
                    logger.info(f"🔥 Daily streak updated to {new_streak} for U:{user_id} from voice activity.")
        else:
            # Existing session - user moved channels or reconnected
            session = self.voice_sessions[session_key]
            session.set_state(
                muted=after.mute,
                deafened=after.deaf,
                self_muted=after.self_mute,
                self_deafened=after.self_deaf,
                update_time=current_time,
            )
            logger.debug(f"🎤 Voice session updated for U:{user_id} in G:{guild_id}")

    async def _handle_voice_leave(self, session_key: Tuple[str, str], user_id: str, guild_id: str,
                                  current_time: float):
        """Handle user leaving a voice channel."""
        if session_key in self.voice_sessions:
            session = self.voice_sessions.pop(session_key)
            logger.info(f"🎤 Voice session ended for U:{user_id} in G:{guild_id}")
            await self._process_session_rewards(user_id, guild_id, session, current_time)

    async def _handle_voice_state_change(self, session_key: Tuple[str, str], user_id: str, guild_id: str,
                                         after: VoiceState, current_time: float):
        """Handle voice state changes within the same channel."""
        if session_key in self.voice_sessions:
            session = self.voice_sessions[session_key]
            session.set_state(
                muted=after.mute,
                deafened=after.deaf,
                self_muted=after.self_mute,
                self_deafened=after.self_deaf,
                update_time=current_time,
            )

            state_desc = session._get_state_description()
            logger.debug(f"🎤 Voice state changed for U:{user_id}: {state_desc}")

    async def _process_session_rewards(self, user_id: str, guild_id: str, session: VoiceSession, end_time: float):
        """
        Calculate and process rewards for a completed voice session.
        Integrated with main leveling system update structure.
        """
        try:
            logger.info(f"💰 Processing voice rewards: G:{guild_id} U:{user_id}")

            # Finalize session metrics
            session.update_state_times(end_time)
            metrics = session.compute_metrics(end_time)
            active_seconds = metrics.get("active_seconds", 0)

            # Anti-cheat: Minimum duration check
            if active_seconds <= 10:
                logger.info(f"🎤 Session too short ({active_seconds:.1f}s active). No rewards.")
                return

            # Rate limiting check
            if not await self._check_voice_rate_limit(user_id, guild_id):
                logger.warning(f"🚫 Voice rate limit exceeded: G:{guild_id} U:{user_id}")
                return

            # Load settings and user data
            settings = await self.leveling_system.get_guild_settings(guild_id)
            user_data = await self.leveling_system.get_user_data(user_id, guild_id)

            if not user_data:
                logger.info(f"🆕 Creating new user profile for voice: U:{user_id}")
                user_data = await self.leveling_system.create_enhanced_user_profile(user_id, guild_id)

            # Calculate rewards
            rewards = await self._calculate_voice_rewards(active_seconds, settings, user_data, metrics)

            # TODO: Implement voice caps from settings
            # rewards = self._apply_voice_caps(rewards, user_data, settings)

            if rewards["xp"] > 0 or rewards["embers"] > 0:
                result_data = await self._update_user_voice_stats(
                    user_id, guild_id, rewards, metrics, user_data, settings
                )

                logger.info(f"💰 Voice rewards awarded: {rewards['xp']} XP, "
                            f"{rewards['embers']} Embers for {active_seconds:.1f}s active")
                
                # =================================================================
                # Check for achievements
                # =================================================================
                if result_data and hasattr(self.leveling_system, 'achievement_system') and self.leveling_system.achievement_system:
                    logger.debug("Checking for voice achievements...")
                    try:
                        activity_data = {
                            "type": "voice",
                            "metrics": metrics,
                            "rewards": rewards,
                            "leveled_up": result_data.get("leveled_up", False),
                            "new_level": result_data.get("level_up", {}).get("new_level")
                        }
                        await self.leveling_system.achievement_system.check_and_update_achievements(
                            user_id, guild_id, activity_data
                        )
                        logger.debug("✅ Voice achievements checked.")
                    except Exception as e:
                        logger.error(f"❌ Voice achievement check failed: {e}", exc_info=True)

        except Exception as e:
            logger.error(f"❌ Error processing voice rewards: {e}", exc_info=True)

    async def _calculate_voice_rewards(self, active_seconds: float, settings: Dict[str, Any],
                                       user_data: Dict[str, Any], metrics: Dict[str, Any]) -> Dict[str, float]:
        """
        Calculate voice rewards based on active duration, settings, and engagement.
        """
        try:
            voice_cfg = settings.get("voice", {})
            base_xp_per_min = voice_cfg.get("xp_per_min", 6)
            base_embers_per_min = voice_cfg.get("embers_per_min", 4)

            active_minutes = active_seconds / 60.0

            # Engagement multiplier based on active percentage
            engagement_score = metrics.get("engagement_score", 0.5)
            engagement_multiplier = 0.5 + (engagement_score * 0.5)  # 0.5x to 1.0x

            # Streak bonus
            daily_streak = user_data.get("daily_streak", {}).get("count", 0)
            streak_bonus = 1.0 + (min(daily_streak, 30) * 0.01)  # Up to 30% bonus

            # Low level boost
            current_level = user_data.get("level", 1)
            if current_level < 15:
                level_multiplier = 1.5
            else:
                level_multiplier = 1.0

            # Calculate final rewards
            calculated_xp = base_xp_per_min * active_minutes * engagement_multiplier * streak_bonus * level_multiplier
            calculated_embers = base_embers_per_min * active_minutes * engagement_multiplier * streak_bonus * level_multiplier

            final_xp = max(1, round(calculated_xp))
            final_embers = max(1, round(calculated_embers))

            logger.debug(f"🎯 Voice reward calculation:")
            logger.debug(f"  • Base: {base_xp_per_min} XP/min, {base_embers_per_min} Embers/min")
            logger.debug(f"  • Active time: {active_minutes:.1f} minutes")
            logger.debug(f"  • Multipliers: engagement={engagement_multiplier:.2f}x, "
                         f"streak={streak_bonus:.2f}x, level={level_multiplier:.2f}x")
            logger.debug(f"  • Final: {final_xp} XP, {final_embers} Embers")

            return {
                "xp": float(final_xp),
                "embers": float(final_embers),
                "multipliers": {
                    "engagement": engagement_multiplier,
                    "streak": streak_bonus,
                    "level": level_multiplier
                }
            }

        except Exception as e:
            logger.error(f"❌ Voice reward calculation error: {e}")
            return {"xp": 0.0, "embers": 0.0}

    async def _update_user_voice_stats(self, user_id: str, guild_id: str, rewards: Dict[str, float],
                                       metrics: Dict[str, Any], user_data: Dict[str, Any], settings: Dict[str, Any]):
        """
        Update user voice statistics and process rewards.
        Uses the main leveling system's update structure.
        """
        try:
            current_xp = user_data.get("xp", 0)
            current_embers = user_data.get("embers", 0)
            current_level = user_data.get("level", 1)

            # Calculate new totals
            new_xp = current_xp + rewards.get("xp", 0)
            new_embers = current_embers + rewards.get("embers", 0)

            # Check for level up
            new_level, leveled_up = self.leveling_system.check_level_up(new_xp, current_level)

            # Prepare update data
            now = utc_now_ts()
            today_key = utc_today_key()
            week_key = utc_week_key()
            month_key = utc_month_key()

            update_data = {
                "$set": {
                    "xp": new_xp,
                    "embers": new_embers,
                    "level": new_level,
                    "updated_at": now,
                    "last_rewarded.voice": now,
                    "voice_stats.last_voice_activity": now,
                    "voice_stats.today_key": today_key,
                    "voice_stats.week_key": week_key,
                    "voice_stats.month_key": month_key,
                },
                "$inc": {
                    # Voice statistics
                    "voice_stats.voice_seconds": metrics.get("voice_seconds", 0),
                    "voice_stats.active_seconds": metrics.get("active_seconds", 0),
                    "voice_stats.muted_time": metrics.get("muted_time", 0),
                    "voice_stats.deafened_time": metrics.get("deafened_time", 0),
                    "voice_stats.self_muted_time": metrics.get("self_muted_time", 0),
                    "voice_stats.self_deafened_time": metrics.get("self_deafened_time", 0),
                    "voice_stats.voice_sessions": 1,

                    # Rewards tracking
                    "voice_stats.today_xp": rewards.get("xp", 0),
                    "voice_stats.today_embers": rewards.get("embers", 0),
                    "voice_stats.weekly_xp": rewards.get("xp", 0),
                    "voice_stats.weekly_embers": rewards.get("embers", 0),
                    "voice_stats.monthly_xp": rewards.get("xp", 0),
                    "voice_stats.monthly_embers": rewards.get("embers", 0),
                }
            }

            # Update session metrics
            total_sessions = user_data.get("voice_stats", {}).get("voice_sessions", 0) + 1
            total_active = user_data.get("voice_stats", {}).get("active_seconds", 0) + metrics.get("active_seconds", 0)

            if total_sessions > 0:
                update_data["$set"]["voice_stats.average_session_length"] = total_active / total_sessions

            # Add level up information if applicable
            result_data = {
                "status": "success",
                "rewards": {
                    "xp": rewards.get("xp", 0),
                    "embers": rewards.get("embers", 0)
                },
                "totals": {
                    "xp": new_xp,
                    "embers": new_embers,
                    "level": new_level
                },
                "leveled_up": leveled_up,
                "session_metrics": metrics,
                "multipliers": rewards.get("multipliers", {})
            }

            if leveled_up:
                result_data["level_up"] = {
                    "old_level": current_level,
                    "new_level": new_level
                }

                # TODO: Trigger level-up notifications and role updates
                logger.info(f"🎊 Voice level up: {current_level} → {new_level}")

            await self.leveling_system.update_user_data(user_id, guild_id, update_data)
            logger.debug(f"✅ Voice stats updated successfully: G:{guild_id} U:{user_id}")

            return result_data

        except Exception as e:
            logger.error(f"❌ Error updating voice stats: {e}")
            return None

    async def _check_voice_rate_limit(self, user_id: str, guild_id: str) -> bool:
        """Check voice activity rate limiting."""
        try:
            current_hour = int(time.time() // 3600)
            rate_limit_key = f"rate_limit:{guild_id}:{user_id}:voice:{current_hour}"
            recent_count = await rate_limiter.get_rate_limit_count(rate_limit_key)

            # Limit to 10 voice sessions per hour
            if recent_count >= 10:
                await self._log_anti_cheat_violation(
                    user_id, guild_id, "voice", "Rate limit exceeded"
                )
                return False

            await rate_limiter.increment_rate_limit_count(rate_limit_key)
            return True

        except Exception as e:
            logger.error(f"❌ Voice rate limit check error: {e}")
            return True  # Allow on error

    async def _log_anti_cheat_violation(self, user_id: str, guild_id: str, activity_type: str, reason: str):
        """Log anti-cheat violation."""
        logger.warning(f"🚫 Voice anti-cheat: U:{user_id} in G:{guild_id} - {reason}")

    async def _periodic_cleanup(self):
        """Periodically clean up stale voice sessions."""
        try:
            while True:
                await asyncio.sleep(300)  # Run every 5 minutes
                await self._cleanup_stale_sessions()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"❌ Voice session cleanup error: {e}")

    async def _cleanup_stale_sessions(self):
        """Clean up voice sessions that might have been missed due to disconnections."""
        try:
            current_time = time.time()
            stale_sessions = []

            for session_key, session in self.voice_sessions.items():
                guild_id, user_id = session_key

                # Consider sessions stale if no update in 10 minutes
                if current_time - session.last_update_time > 600:
                    stale_sessions.append((session_key, session))

            for session_key, session in stale_sessions:
                guild_id, user_id = session_key
                logger.warning(f"🧹 Cleaning up stale voice session: G:{guild_id} U:{user_id}")

                # Process rewards for the stale session
                await self._process_session_rewards(user_id, guild_id, session, current_time)

                # Remove from active sessions
                if session_key in self.voice_sessions:
                    del self.voice_sessions[session_key]

            if stale_sessions:
                logger.info(f"🧹 Cleaned up {len(stale_sessions)} stale voice sessions")

        except Exception as e:
            logger.error(f"❌ Stale session cleanup error: {e}")

    async def _cleanup_all_sessions(self):
        """Process all active sessions (used during shutdown)."""
        try:
            current_time = time.time()
            sessions_to_process = list(self.voice_sessions.items())

            for session_key, session in sessions_to_process:
                guild_id, user_id = session_key
                logger.info(f"🔄 Processing remaining session during shutdown: G:{guild_id} U:{user_id}")
                await self._process_session_rewards(user_id, guild_id, session, current_time)

            self.voice_sessions.clear()
            logger.info(f"✅ Cleaned up {len(sessions_to_process)} active voice sessions")

        except Exception as e:
            logger.error(f"❌ All sessions cleanup error: {e}")

    async def startup_voice_check(self, bot):
        """
        Check for users already in voice channels when bot starts.
        This should be called when the cog loads.
        """
        try:
            logger.info("🔍 Performing startup voice check...")

            for guild in bot.guilds:
                for voice_channel in guild.voice_channels:
                    for member in voice_channel.members:
                        if not member.bot:  # Ignore bots
                            session_key = (str(guild.id), str(member.id))
                            current_time = time.time()

                            if session_key not in self.voice_sessions:
                                session = VoiceSession(
                                    start_time=current_time,
                                    is_muted=member.voice.mute if member.voice else False,
                                    is_deafened=member.voice.deaf if member.voice else False,
                                    is_self_muted=member.voice.self_mute if member.voice else False,
                                    is_self_deafened=member.voice.self_deaf if member.voice else False,
                                )
                                self.voice_sessions[session_key] = session
                                logger.info(f"🎤 Startup: Voice session created for U:{member.id} in G:{guild.id}")

            logger.info(f"✅ Startup voice check complete. {len(self.voice_sessions)} active sessions found.")

        except Exception as e:
            logger.error(f"❌ Startup voice check failed: {e}")

    def get_active_sessions_count(self) -> int:
        """Get count of currently active voice sessions."""
        return len(self.voice_sessions)

    def get_user_session(self, user_id: str, guild_id: str) -> Optional[VoiceSession]:
        """Get a user's current voice session if it exists."""
        return self.voice_sessions.get((guild_id, user_id))