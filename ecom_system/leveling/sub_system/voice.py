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
from loggers.log_factory import log_performance, PerformanceLogger

logger = logging.getLogger(__name__)

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
            channel_id = str(after.channel.id) if after.channel else None

            # Count non-bot members in the channel
            participant_count = 0
            if after.channel:
                participant_count = len([m for m in after.channel.members if not m.bot])

            session = VoiceSession(
                start_time=current_time,
                channel_id=channel_id,
                participant_count=participant_count,
                is_muted=after.mute,
                is_deafened=after.deaf,
                is_self_muted=after.self_mute,
                is_self_deafened=after.self_deaf,
                is_streaming=after.self_stream,
                is_video=after.self_video,
            )
            self.voice_sessions[session_key] = session
            logger.info(f"🎤 Voice session started for U:{user_id} in G:{guild_id} "
                        f"(channel: {after.channel.name}, ID: {channel_id}, participants: {participant_count})")

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
            # Update channel_id if user moved to a different channel
            new_channel_id = str(after.channel.id) if after.channel else None
            if session.channel_id != new_channel_id:
                logger.debug(f"🚶 User moved channels: {session.channel_id} → {new_channel_id}")
                session.channel_id = new_channel_id

            session.set_state(
                muted=after.mute,
                deafened=after.deaf,
                self_muted=after.self_mute,
                self_deafened=after.self_deaf,
                streaming=after.self_stream,
                video=after.self_video,
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
                streaming=after.self_stream,
                video=after.self_video,
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

            # Calculate rewards (with channel-specific bonuses)
            channel_id = session.channel_id
            rewards = await self._calculate_voice_rewards(active_seconds, settings, user_data, metrics, channel_id)

            # Apply voice caps (daily/weekly/monthly limits)
            rewards = self._apply_voice_caps(rewards, user_data, settings)

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
                                       user_data: Dict[str, Any], metrics: Dict[str, Any],
                                       channel_id: str = None) -> Dict[str, float]:
        """
        Calculate voice rewards based on active duration, settings, and engagement.
        Includes channel-specific bonuses if configured.
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

            # Channel-specific bonuses
            channel_multiplier = 1.0
            channel_bonuses = voice_cfg.get("channel_bonuses", {})
            if channel_id and channel_id in channel_bonuses:
                bonus = channel_bonuses[channel_id]
                channel_multiplier = bonus
                logger.debug(f"  • Channel bonus applied: {bonus}x (channel: {channel_id})")

            # Streaming/Video bonuses (based on time spent streaming/with camera)
            streaming_multiplier = 1.0
            video_multiplier = 1.0

            # Apply screen share bonus if user was streaming during session
            streaming_time = metrics.get("streaming_time", 0)
            if streaming_time > 0:
                screen_share_bonus = voice_cfg.get("screen_share_bonus", 1.0)
                if screen_share_bonus > 1.0:
                    streaming_multiplier = screen_share_bonus
                    logger.debug(f"  • Screen share bonus applied: {screen_share_bonus}x ({streaming_time:.1f}s streaming)")

            # Apply camera bonus if user had video on during session
            video_time = metrics.get("video_time", 0)
            if video_time > 0:
                camera_bonus = voice_cfg.get("camera_bonus", 1.0)
                if camera_bonus > 1.0:
                    video_multiplier = camera_bonus
                    logger.debug(f"  • Camera bonus applied: {camera_bonus}x ({video_time:.1f}s with camera)")

            # Participant count bonus (social bonus for populated channels)
            participant_multiplier = 1.0
            participant_count = metrics.get("participant_count", 0)
            participant_bonus_enabled = voice_cfg.get("participant_bonus_enabled", False)

            if participant_bonus_enabled and participant_count > 0:
                # Anti-exploit: Require minimum active time before applying participant bonus
                min_time = voice_cfg.get("participant_min_time_seconds", 60)
                if active_seconds >= min_time:
                    threshold = voice_cfg.get("participant_bonus_threshold", 3)
                    bonus_per_person = voice_cfg.get("participant_bonus_per_person", 0.05)
                    max_bonus = voice_cfg.get("participant_bonus_max", 1.5)

                    # Apply bonus if above threshold
                    if participant_count >= threshold:
                        # Calculate bonus: 1.0 + (additional_people * bonus_per_person)
                        additional_people = participant_count - threshold
                        calculated_bonus = 1.0 + (additional_people * bonus_per_person)
                        participant_multiplier = min(calculated_bonus, max_bonus)
                        logger.debug(f"  • Participant bonus applied: {participant_multiplier:.2f}x "
                                   f"({participant_count} members, {additional_people} above threshold)")
                    else:
                        logger.debug(f"  • Participant count below threshold: {participant_count} < {threshold}")
                else:
                    logger.debug(f"  • Minimum time not met for participant bonus: "
                               f"{active_seconds:.1f}s < {min_time}s")

            # Calculate final rewards
            calculated_xp = base_xp_per_min * active_minutes * engagement_multiplier * streak_bonus * level_multiplier * channel_multiplier * streaming_multiplier * video_multiplier * participant_multiplier
            calculated_embers = base_embers_per_min * active_minutes * engagement_multiplier * streak_bonus * level_multiplier * channel_multiplier * streaming_multiplier * video_multiplier * participant_multiplier

            final_xp = max(1, round(calculated_xp))
            final_embers = max(1, round(calculated_embers))

            logger.debug(f"🎯 Voice reward calculation:")
            logger.debug(f"  • Base: {base_xp_per_min} XP/min, {base_embers_per_min} Embers/min")
            logger.debug(f"  • Active time: {active_minutes:.1f} minutes")
            if channel_id:
                logger.debug(f"  • Channel: {channel_id}")
            if participant_count > 0:
                logger.debug(f"  • Participants: {participant_count} members")
            logger.debug(f"  • Multipliers: engagement={engagement_multiplier:.2f}x, "
                         f"streak={streak_bonus:.2f}x, level={level_multiplier:.2f}x, "
                         f"channel={channel_multiplier:.2f}x, streaming={streaming_multiplier:.2f}x, "
                         f"video={video_multiplier:.2f}x, participants={participant_multiplier:.2f}x")
            logger.debug(f"  • Final: {final_xp} XP, {final_embers} Embers")

            return {
                "xp": float(final_xp),
                "embers": float(final_embers),
                "multipliers": {
                    "engagement": engagement_multiplier,
                    "streak": streak_bonus,
                    "level": level_multiplier,
                    "channel": channel_multiplier,
                    "streaming": streaming_multiplier,
                    "video": video_multiplier,
                    "participants": participant_multiplier
                }
            }

        except Exception as e:
            logger.error(f"❌ Voice reward calculation error: {e}")
            return {"xp": 0.0, "embers": 0.0}

    def _apply_voice_caps(self, rewards: Dict[str, float], user_data: Dict[str, Any],
                          settings: Dict[str, Any]) -> Dict[str, float]:
        """
        Apply daily/weekly/monthly caps to voice rewards.
        Reduces rewards if they would exceed configured limits.

        Args:
            rewards: Calculated rewards before caps
            user_data: Current user data with existing totals
            settings: Guild settings with cap configuration

        Returns:
            Adjusted rewards that respect caps
        """
        try:
            voice_cfg = settings.get("voice", {})
            voice_stats = user_data.get("voice_stats", {})

            # Get cap configuration
            daily_caps = voice_cfg.get("daily_caps", {})
            weekly_caps = voice_cfg.get("weekly_caps", {})
            monthly_caps = voice_cfg.get("monthly_caps", {})

            # Skip if no caps configured
            if not daily_caps and not weekly_caps and not monthly_caps:
                logger.debug("  ⚙️ No voice caps configured, skipping cap check")
                return rewards

            # Get current time keys
            current_today_key = utc_today_key()
            current_week_key = utc_week_key()
            current_month_key = utc_month_key()

            # Get stored time keys (for reset detection)
            stored_today_key = voice_stats.get("today_key")
            stored_week_key = voice_stats.get("week_key")
            stored_month_key = voice_stats.get("month_key")

            # Get current totals (reset if new period)
            today_xp = voice_stats.get("today_xp", 0) if stored_today_key == current_today_key else 0
            today_embers = voice_stats.get("today_embers", 0) if stored_today_key == current_today_key else 0

            weekly_xp = voice_stats.get("weekly_xp", 0) if stored_week_key == current_week_key else 0
            weekly_embers = voice_stats.get("weekly_embers", 0) if stored_week_key == current_week_key else 0

            monthly_xp = voice_stats.get("monthly_xp", 0) if stored_month_key == current_month_key else 0
            monthly_embers = voice_stats.get("monthly_embers", 0) if stored_month_key == current_month_key else 0

            # Start with original rewards
            final_xp = rewards.get("xp", 0)
            final_embers = rewards.get("embers", 0)
            original_xp = final_xp
            original_embers = final_embers

            cap_applied = False
            cap_reasons = []

            # Check daily caps
            if daily_caps:
                daily_xp_cap = daily_caps.get("xp", float('inf'))
                daily_embers_cap = daily_caps.get("embers", float('inf'))

                # Calculate remaining room
                xp_room = max(0, daily_xp_cap - today_xp)
                embers_room = max(0, daily_embers_cap - today_embers)

                # Apply cap if needed
                if final_xp > xp_room:
                    final_xp = xp_room
                    cap_applied = True
                    cap_reasons.append(f"daily XP cap ({daily_xp_cap})")

                if final_embers > embers_room:
                    final_embers = embers_room
                    cap_applied = True
                    cap_reasons.append(f"daily Embers cap ({daily_embers_cap})")

                # Warn when approaching cap (90%)
                if today_xp + final_xp >= daily_xp_cap * 0.9 and today_xp < daily_xp_cap * 0.9:
                    logger.info(f"  ⚠️ Approaching daily XP cap: {today_xp + final_xp:.0f}/{daily_xp_cap}")
                if today_embers + final_embers >= daily_embers_cap * 0.9 and today_embers < daily_embers_cap * 0.9:
                    logger.info(f"  ⚠️ Approaching daily Embers cap: {today_embers + final_embers:.0f}/{daily_embers_cap}")

            # Check weekly caps
            if weekly_caps:
                weekly_xp_cap = weekly_caps.get("xp", float('inf'))
                weekly_embers_cap = weekly_caps.get("embers", float('inf'))

                xp_room = max(0, weekly_xp_cap - weekly_xp)
                embers_room = max(0, weekly_embers_cap - weekly_embers)

                if final_xp > xp_room:
                    final_xp = min(final_xp, xp_room)
                    cap_applied = True
                    cap_reasons.append(f"weekly XP cap ({weekly_xp_cap})")

                if final_embers > embers_room:
                    final_embers = min(final_embers, embers_room)
                    cap_applied = True
                    cap_reasons.append(f"weekly Embers cap ({weekly_embers_cap})")

                if weekly_xp + final_xp >= weekly_xp_cap * 0.9 and weekly_xp < weekly_xp_cap * 0.9:
                    logger.info(f"  ⚠️ Approaching weekly XP cap: {weekly_xp + final_xp:.0f}/{weekly_xp_cap}")
                if weekly_embers + final_embers >= weekly_embers_cap * 0.9 and weekly_embers < weekly_embers_cap * 0.9:
                    logger.info(f"  ⚠️ Approaching weekly Embers cap: {weekly_embers + final_embers:.0f}/{weekly_embers_cap}")

            # Check monthly caps
            if monthly_caps:
                monthly_xp_cap = monthly_caps.get("xp", float('inf'))
                monthly_embers_cap = monthly_caps.get("embers", float('inf'))

                xp_room = max(0, monthly_xp_cap - monthly_xp)
                embers_room = max(0, monthly_embers_cap - monthly_embers)

                if final_xp > xp_room:
                    final_xp = min(final_xp, xp_room)
                    cap_applied = True
                    cap_reasons.append(f"monthly XP cap ({monthly_xp_cap})")

                if final_embers > embers_room:
                    final_embers = min(final_embers, embers_room)
                    cap_applied = True
                    cap_reasons.append(f"monthly Embers cap ({monthly_embers_cap})")

                if monthly_xp + final_xp >= monthly_xp_cap * 0.9 and monthly_xp < monthly_xp_cap * 0.9:
                    logger.info(f"  ⚠️ Approaching monthly XP cap: {monthly_xp + final_xp:.0f}/{monthly_xp_cap}")
                if monthly_embers + final_embers >= monthly_embers_cap * 0.9 and monthly_embers < monthly_embers_cap * 0.9:
                    logger.info(f"  ⚠️ Approaching monthly Embers cap: {monthly_embers + final_embers:.0f}/{monthly_embers_cap}")

            # Log if caps were applied
            if cap_applied:
                reduction_xp = original_xp - final_xp
                reduction_embers = original_embers - final_embers
                logger.warning(f"  🚫 Voice rewards capped: XP {original_xp:.0f}→{final_xp:.0f} "
                             f"(-{reduction_xp:.0f}), Embers {original_embers:.0f}→{final_embers:.0f} "
                             f"(-{reduction_embers:.0f})")
                logger.warning(f"  📊 Reason(s): {', '.join(cap_reasons)}")

            # Return adjusted rewards
            return {
                "xp": float(max(0, final_xp)),
                "embers": float(max(0, final_embers)),
                "multipliers": rewards.get("multipliers", {}),
                "capped": cap_applied,
                "original_xp": original_xp,
                "original_embers": original_embers
            }

        except Exception as e:
            logger.error(f"❌ Error applying voice caps: {e}", exc_info=True)
            return rewards  # Return original rewards on error

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

            # Get stored time keys for reset detection
            voice_stats = user_data.get("voice_stats", {})
            stored_today_key = voice_stats.get("today_key")
            stored_week_key = voice_stats.get("week_key")
            stored_month_key = voice_stats.get("month_key")

            # Determine if we need to reset counters (new period)
            reset_daily = stored_today_key != today_key
            reset_weekly = stored_week_key != week_key
            reset_monthly = stored_month_key != month_key

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
                }
            }

            # Handle period resets and increments
            if reset_daily:
                logger.debug(f"  🔄 Daily reset detected ({stored_today_key} → {today_key})")
                update_data["$set"]["voice_stats.today_xp"] = rewards.get("xp", 0)
                update_data["$set"]["voice_stats.today_embers"] = rewards.get("embers", 0)
            else:
                update_data["$inc"]["voice_stats.today_xp"] = rewards.get("xp", 0)
                update_data["$inc"]["voice_stats.today_embers"] = rewards.get("embers", 0)

            if reset_weekly:
                logger.debug(f"  🔄 Weekly reset detected ({stored_week_key} → {week_key})")
                update_data["$set"]["voice_stats.weekly_xp"] = rewards.get("xp", 0)
                update_data["$set"]["voice_stats.weekly_embers"] = rewards.get("embers", 0)
            else:
                update_data["$inc"]["voice_stats.weekly_xp"] = rewards.get("xp", 0)
                update_data["$inc"]["voice_stats.weekly_embers"] = rewards.get("embers", 0)

            if reset_monthly:
                logger.debug(f"  🔄 Monthly reset detected ({stored_month_key} → {month_key})")
                update_data["$set"]["voice_stats.monthly_xp"] = rewards.get("xp", 0)
                update_data["$set"]["voice_stats.monthly_embers"] = rewards.get("embers", 0)
            else:
                update_data["$inc"]["voice_stats.monthly_xp"] = rewards.get("xp", 0)
                update_data["$inc"]["voice_stats.monthly_embers"] = rewards.get("embers", 0)

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