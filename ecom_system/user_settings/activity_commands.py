import discord
from discord import app_commands
from discord.ext import commands
import logging
from database.EconDataManager import econ_db_manager
from ecom_system.activity_system.activity_system import ActivitySystem

logger = logging.getLogger(__name__)

class ActivityCommands(commands.Cog):
    """
    Cog for user-facing commands related to activity.
    """

    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="activity", description="Shows your or another user's activity profile.")
    @app_commands.describe(user="The user to view activity for. Defaults to yourself.")
    async def activity(self, interaction: discord.Interaction, user: discord.Member = None):
        if user is None:
            user = interaction.user

        if user.bot:
            await interaction.response.send_message("Bots don't have activity profiles.", ephemeral=True)
            return

        # Access the activity_system from the bot instance.
        activity_system: ActivitySystem = self.bot.activity_system
        if not activity_system:
            await interaction.response.send_message("Activity system is not available.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        user_id = str(user.id)
        guild_id = str(interaction.guild.id)

        summary = await activity_system.get_user_activity_summary(user_id, guild_id)

        if not summary:
            await interaction.followup.send("No activity data found for this user.", ephemeral=True)
            return

        embed = self.create_activity_embed(user, summary)
        await interaction.followup.send(embed=embed, ephemeral=True)

    def create_activity_embed(self, user: discord.Member, summary: dict) -> discord.Embed:
        embed = discord.Embed(title=f"Activity Profile for {user.display_name}", color=user.color)
        embed.set_thumbnail(url=user.display_avatar.url)

        # Time-based analysis
        patterns = summary.get("activity_patterns", {})
        
        # Weekend vs Weekday
        weekend_vs_weekday = patterns.get("weekend_vs_weekday")
        if weekend_vs_weekday:
            embed.add_field(
                name="Activity Preference",
                value=(
                    f"**Weekend:** {weekend_vs_weekday['weekend_percentage']}% "
                    f"({weekend_vs_weekday['weekend_total']} activities)\n"
                    f"**Weekday:** {weekend_vs_weekday['weekday_percentage']}% "
                    f"({weekend_vs_weekday['weekday_total']} activities)"
                ),
                inline=False
            )

        # Time of Day Breakdown
        time_of_day = patterns.get("time_of_day_breakdown")
        if time_of_day:
            by_period = time_of_day.get("by_period", {})
            
            value = (
                f"**Morning (6-12):** {by_period.get('morning', 0)} activities\n"
                f"**Afternoon (12-18):** {by_period.get('afternoon', 0)} activities\n"
                f"**Evening (18-23):** {by_period.get('evening', 0)} activities\n"
                f"**Night (23-2):** {by_period.get('night', 0)} activities\n"
                f"**Overnight (2-6):** {by_period.get('overnight', 0)} activities"
            )
            embed.add_field(
                name="Time of Day Breakdown",
                value=value,
                inline=False
            )
            if time_of_day.get("most_active_period"):
                embed.add_field(
                    name="Most Active",
                    value=time_of_day['most_active_period'].capitalize(),
                    inline=True
                )
            if time_of_day.get("least_active_period"):
                embed.add_field(
                    name="Least Active",
                    value=time_of_day['least_active_period'].capitalize(),
                    inline=True
                )

        return embed

async def setup(bot):
    await bot.add_cog(ActivityCommands(bot))
    logger.info("ActivityCommands cog loaded.")
