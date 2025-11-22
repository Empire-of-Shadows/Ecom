import discord
from discord.ext import commands, tasks
from loggers.logger_setup import get_logger

# Constants
CATEGORY_ID = 1364204791902896170
CHANNEL_PREFIX = "WAU: "

class ActivityTasks(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.logger = get_logger("ActivityTasks")
        self.update_active_users_channel.start()

    def cog_unload(self):
        self.update_active_users_channel.cancel()

    @tasks.loop(minutes=5)
    async def update_active_users_channel(self):
        """
        A background task that updates a voice channel with the number of weekly active users.
        """
        await self.bot.wait_until_ready()

        guild = self.bot.guilds[0] if self.bot.guilds else None
        if not guild:
            self.logger.warning("Bot is not in any guilds.")
            return

        category = guild.get_channel(CATEGORY_ID)
        if not isinstance(category, discord.CategoryChannel):
            self.logger.error(f"Category with ID {CATEGORY_ID} not found or is not a category channel.")
            return

        activity_system = self.bot.activity_system
        if not activity_system:
            self.logger.error("Activity system not found on bot instance.")
            return

        try:
            weekly_active_count = await activity_system.get_weekly_active_users_count(str(guild.id))
            
            target_channel = None
            for channel in category.voice_channels:
                if channel.name.startswith(CHANNEL_PREFIX):
                    target_channel = channel
                    break

            new_channel_name = f"{CHANNEL_PREFIX}{weekly_active_count}"

            if target_channel:
                if target_channel.name != new_channel_name:
                    await target_channel.edit(name=new_channel_name)
                    self.logger.info(f"Updated channel name to {new_channel_name}")
            else:
                # Create a new locked voice channel
                overwrites = {
                    guild.default_role: discord.PermissionOverwrite(connect=False)
                }
                await category.create_voice_channel(new_channel_name, overwrites=overwrites)
                self.logger.info(f"Created new channel: {new_channel_name}")

        except Exception as e:
            self.logger.error(f"An error occurred while updating active users channel: {e}", exc_info=True)


async def setup(bot):
    await bot.add_cog(ActivityTasks(bot))
