import discord
from discord.ext import commands

from loggers.logger_setup import log_performance, get_logger

# TODO: Re-implement ActivityBuffer integration for detailed event logging.
# All the `activity_buffer.add_event` calls were removed for simplification.
# This includes logging for join, leave, switch, mute, deafen, etc.

class VoiceListener(commands.Cog):
    """
    Simple voice listener for leveling system integration.
    Handles voice state updates and delegates them to the voice leveling subsystem.
    """

    def __init__(self, bot):
        """Initialize the voice listener with a bot instance."""
        self.bot = bot
        self.leveling_system = None
        self.activity_system = None
        self.logger = get_logger("VoiceListener")

    async def cog_load(self):
        """Initialize leveling system and perform startup checks when cog loads."""
        self.logger.info("🔄 Initializing VoiceListener...")
        try:
            if hasattr(self.bot, 'leveling_system') and self.bot.leveling_system:
                self.leveling_system = self.bot.leveling_system
                self.logger.info("✅ VoiceListener using shared leveling system from bot.")

                # TODO: Call the `startup_voice_check` method on the voice_system
                # to handle users already in voice channels.
                # await self.leveling_system.voice_system.startup_voice_check(self.bot.guilds)

            else:
                self.logger.error("❌ No leveling system found on bot instance.")
                raise ValueError("Leveling system not available on bot instance.")

            if hasattr(self.bot, 'activity_system') and self.bot.activity_system:
                self.activity_system = self.bot.activity_system
                self.logger.info("✅ VoiceListener using shared activity system from bot")
            else:
                self.logger.warning("⚠️ Activity system not found on bot instance. Activity will not be tracked.")

        except Exception as e:
            self.logger.error(f"❌ Failed to initialize VoiceListener: {e}", exc_info=True)
            raise

    @commands.Cog.listener()
    @log_performance("on_voice_state_update")
    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ):
        """
        Handle voice state updates and pass them to the leveling system.
        """
        if member.bot or not member.guild:
            return

        user_id = str(member.id)
        guild_id = str(member.guild.id)

        try:
            # Check for user joining a voice channel to record activity
            is_join_event = before.channel is None and after.channel is not None
            if is_join_event and self.activity_system:
                self.logger.debug(f"User {user_id} joined voice channel in guild {guild_id}. Recording activity.")
                await self.activity_system.record_activity(
                    user_id=user_id,
                    guild_id=guild_id,
                    activity_type='voice'
                )

            # Delegate all logic to the voice subsystem for leveling
            await self.leveling_system.voice_system.process_voice_state_update(
                user_id=user_id,
                guild_id=guild_id,
                before=before,
                after=after,
            )
        except Exception as e:
            self.logger.error(
                f"❌ Error processing voice state update for U:{user_id} in G:{guild_id}: {e}",
                exc_info=True
            )


async def setup(bot):
    """Setup function for discord.py cog loading."""
    await bot.add_cog(VoiceListener(bot))