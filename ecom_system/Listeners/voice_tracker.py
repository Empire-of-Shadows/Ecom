import discord
import logging
from discord.ext import commands

from ecom_system.helpers.helpers import utc_now_ts
from loggers.log_factory import log_performance


def _safe_channel_name(channel):
    """Safe utility function to get channel name with fallback."""
    try:
        if hasattr(channel, 'name'):
            return str(channel.name)
        elif hasattr(channel, 'recipient'):
            return f"DM with {channel.recipient.name}"
        else:
            return "Unknown Channel"
    except Exception:
        return "Unknown Channel"


class VoiceListener(commands.Cog):
    """
    Enhanced voice listener for comprehensive voice activity tracking.

    Features:
    - Voice state change detection and analysis
    - Activity system integration for detailed tracking
    - Activity buffer integration for batch processing
    - Leveling system integration for voice rewards
    - Comprehensive voice session analytics
    """

    def __init__(self, bot):
        """Initialize the voice listener with a bot instance."""
        self.bot = bot
        self.leveling_system = None
        self.activity_system = None
        self.logger = logging.getLogger(__name__)

    async def cog_load(self):
        """Initialize leveling system and perform startup checks when cog loads."""
        self.logger.info("🔄 Initializing Enhanced VoiceListener...")
        try:
            # Initialize leveling system
            if hasattr(self.bot, 'leveling_system') and self.bot.leveling_system:
                self.leveling_system = self.bot.leveling_system
                self.logger.info("✅ VoiceListener using shared leveling system from bot")

                # TODO: Call the `startup_voice_check` method on the voice_system
                # to handle users already in voice channels.
                # await self.leveling_system.voice_system.startup_voice_check(self.bot.guilds)

            else:
                self.logger.error("❌ No leveling system found on bot instance")
                raise ValueError("Leveling system not available on bot instance")

            # Initialize activity system
            if hasattr(self.bot, 'activity_system') and self.bot.activity_system:
                self.activity_system = self.bot.activity_system
                self.logger.info("✅ VoiceListener using shared activity system from bot")
            else:
                self.logger.warning("⚠️ Activity system not found on bot instance. Activity will not be tracked.")

            self.logger.info("✅ Enhanced VoiceListener fully initialized")

        except Exception as e:
            self.logger.error(f"❌ Failed to initialize VoiceListener: {e}", exc_info=True)
            raise

    def _analyze_voice_state_change(self, member: discord.Member, before: discord.VoiceState,
                                    after: discord.VoiceState) -> dict:
        """Analyze voice state changes to determine event type and details."""
        analysis = {
            "event_type": "unknown",
            "joined_channel": None,
            "left_channel": None,
            "channel_switch": False,
            "mute_change": False,
            "deaf_change": False,
            "self_mute_change": False,
            "self_deaf_change": False,
            "stream_change": False,
            "video_change": False,
            "is_join": False,
            "is_leave": False,
            "is_move": False,
            "timestamp": utc_now_ts()
        }

        # Channel changes
        before_channel = before.channel
        after_channel = after.channel

        if before_channel is None and after_channel is not None:
            # User joined voice
            analysis.update({
                "event_type": "join",
                "joined_channel": after_channel,
                "is_join": True
            })
        elif before_channel is not None and after_channel is None:
            # User left voice
            analysis.update({
                "event_type": "leave",
                "left_channel": before_channel,
                "is_leave": True
            })
        elif before_channel is not None and after_channel is not None and before_channel.id != after_channel.id:
            # User switched channels
            analysis.update({
                "event_type": "move",
                "left_channel": before_channel,
                "joined_channel": after_channel,
                "channel_switch": True,
                "is_move": True
            })
        else:
            # State change within same channel
            analysis["event_type"] = "state_change"

        # Audio state changes
        if before.mute != after.mute:
            analysis["mute_change"] = True
        if before.deaf != after.deaf:
            analysis["deaf_change"] = True
        if before.self_mute != after.self_mute:
            analysis["self_mute_change"] = True
        if before.self_deaf != after.self_deaf:
            analysis["self_deaf_change"] = True
        if before.self_stream != after.self_stream:
            analysis["stream_change"] = True
        if before.self_video != after.self_video:
            analysis["video_change"] = True

        return analysis

    @commands.Cog.listener()
    @log_performance("on_voice_state_update")
    async def on_voice_state_update(
            self,
            member: discord.Member,
            before: discord.VoiceState,
            after: discord.VoiceState,
    ):
        """
        Handle voice state updates with comprehensive tracking and analysis.
        """
        if member.bot or not member.guild:
            return

        user_id = str(member.id)
        guild_id = str(member.guild.id)

        try:
            # Analyze the voice state change
            analysis = self._analyze_voice_state_change(member, before, after)

            # Prepare comprehensive voice activity data
            voice_activity_data = {
                "event_type": analysis["event_type"],
                "user_name": str(member.name),
                "user_display_name": str(member.display_name),
                "guild_name": str(member.guild.name),
                "timestamp": analysis["timestamp"]
            }

            # Add channel information
            if analysis["joined_channel"]:
                voice_activity_data.update({
                    "joined_channel_id": str(analysis["joined_channel"].id),
                    "joined_channel_name": _safe_channel_name(analysis["joined_channel"]),
                    "joined_channel_type": str(type(analysis["joined_channel"]).__name__),
                    "joined_channel_user_limit": analysis["joined_channel"].user_limit,
                    "joined_channel_bitrate": analysis["joined_channel"].bitrate,
                    "joined_channel_members_count": len(analysis["joined_channel"].members)
                })

            if analysis["left_channel"]:
                voice_activity_data.update({
                    "left_channel_id": str(analysis["left_channel"].id),
                    "left_channel_name": _safe_channel_name(analysis["left_channel"]),
                    "left_channel_type": str(type(analysis["left_channel"]).__name__),
                    "left_channel_user_limit": analysis["left_channel"].user_limit,
                    "left_channel_bitrate": analysis["left_channel"].bitrate,
                    "left_channel_members_count": len(analysis["left_channel"].members)
                })

            # Add current voice states
            voice_activity_data.update({
                "before_state": {
                    "muted": before.mute,
                    "deafened": before.deaf,
                    "self_muted": before.self_mute,
                    "self_deafened": before.self_deaf,
                    "streaming": before.self_stream,
                    "video": before.self_video,
                    "channel_id": str(before.channel.id) if before.channel else None
                },
                "after_state": {
                    "muted": after.mute,
                    "deafened": after.deaf,
                    "self_muted": after.self_mute,
                    "self_deafened": after.self_deaf,
                    "streaming": after.self_stream,
                    "video": after.self_video,
                    "channel_id": str(after.channel.id) if after.channel else None
                },
                "state_changes": {
                    "mute_change": analysis["mute_change"],
                    "deaf_change": analysis["deaf_change"],
                    "self_mute_change": analysis["self_mute_change"],
                    "self_deaf_change": analysis["self_deaf_change"],
                    "stream_change": analysis["stream_change"],
                    "video_change": analysis["video_change"],
                    "channel_switch": analysis["channel_switch"]
                },
                "event_flags": {
                    "is_join": analysis["is_join"],
                    "is_leave": analysis["is_leave"],
                    "is_move": analysis["is_move"]
                }
            })

            self.logger.debug(f"Voice state update: {analysis['event_type']} for user {user_id} in guild {guild_id}")

            # =====================================================================
            # ACTIVITY SYSTEM INTEGRATION
            # =====================================================================
            if self.activity_system:
                try:
                    # Determine activity type based on event
                    if analysis["is_join"]:
                        activity_type = "voice_join"
                    elif analysis["is_leave"]:
                        activity_type = "voice_leave"
                    elif analysis["is_move"]:
                        activity_type = "voice_move"
                    else:
                        activity_type = "voice_state_change"

                    await self.activity_system.record_activity(
                        user_id=user_id,
                        guild_id=guild_id,
                        activity_type=activity_type,
                        activity_data=voice_activity_data
                    )

                    self.logger.debug(f"Voice activity recorded in activity system: {activity_type} for user {user_id}")

                except Exception as e:
                    self.logger.warning(f"Failed to record voice activity in activity system: {e}")

            # =====================================================================
            # LEVELING SYSTEM INTEGRATION
            # =====================================================================
            if self.leveling_system:
                try:
                    # Delegate to voice subsystem for leveling calculations
                    await self.leveling_system.voice_system.process_voice_state_update(
                        user_id=user_id,
                        guild_id=guild_id,
                        before=before,
                        after=after,
                    )

                    self.logger.debug(f"Voice state processed by leveling system for user {user_id}")

                except Exception as e:
                    self.logger.error(f"Error in leveling system voice processing: {e}", exc_info=True)

            # =====================================================================
            # ACHIEVEMENT SYSTEM INTEGRATION
            # =====================================================================
            # For voice join events, trigger achievement checking
            if analysis["is_join"] and hasattr(self.bot, 'leveling_system') and hasattr(self.bot.leveling_system, 'achievement_system'):
                try:
                    achievement_activity_data = {
                        "activity_type": "voice_join",
                        "channel_id": voice_activity_data.get("joined_channel_id"),
                        "channel_name": voice_activity_data.get("joined_channel_name"),
                        "channel_members_count": voice_activity_data.get("joined_channel_members_count", 0),
                        "timestamp": analysis["timestamp"]
                    }

                    await self.bot.leveling_system.achievement_system.check_and_update_achievements(
                        user_id=user_id,
                        guild_id=guild_id,
                        activity_data=achievement_activity_data
                    )

                    self.logger.debug(f"Voice join achievements checked for user {user_id}")

                except Exception as e:
                    self.logger.warning(f"Failed to process voice achievements: {e}")

            # Create a more descriptive log message
            log_message = f"🎙️ Voice event for {member.name} in {member.guild.name}: "
            details = []
            if analysis['is_join']:
                details.append(f"joined channel '{_safe_channel_name(after.channel)}'")
            elif analysis['is_leave']:
                details.append(f"left channel '{_safe_channel_name(before.channel)}'")
            elif analysis['is_move']:
                details.append(f"moved from '{_safe_channel_name(before.channel)}' to '{_safe_channel_name(after.channel)}'")
            
            state_changes = []
            if analysis['self_mute_change']:
                state_changes.append("self-muted" if after.self_mute else "self-unmuted")
            if analysis['self_deaf_change']:
                state_changes.append("self-deafened" if after.self_deaf else "self-undeafened")
            if analysis['stream_change']:
                state_changes.append("started streaming" if after.self_stream else "stopped streaming")
            if analysis['video_change']:
                state_changes.append("started video" if after.self_video else "stopped video")
            if analysis['mute_change']:
                state_changes.append("was server muted" if after.mute else "was server unmuted")
            if analysis['deaf_change']:
                state_changes.append("was server deafened" if after.deaf else "was server undeafened")

            if state_changes:
                details.append(f"has been {', '.join(state_changes)}")

            if not details:
                details.append(f"state changed in '{_safe_channel_name(after.channel)}'")

            log_message += ", ".join(details)
            self.logger.info(log_message)

        except Exception as e:
            self.logger.error(
                f"❌ Error processing voice state update for U:{user_id} in G:{guild_id}: {e}",
                exc_info=True
            )


async def setup(bot):
    """Setup function for discord.py cog loading."""
    await bot.add_cog(VoiceListener(bot))