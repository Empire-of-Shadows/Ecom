import discord
from discord import app_commands
from discord.ext import commands
import logging

from database.EconDataManager import econ_db_manager

logger = logging.getLogger(__name__)


class AdminCommands(commands.Cog):
    """
    Cog containing administrative commands for managing the economy system data.
    These commands are restricted to users with administrator permissions.
    """

    def __init__(self, bot):
        self.bot = bot

    # --- Command Groups ---
    reset_group = app_commands.Group(name="reset", description="Reset user or guild data.")
    nuke_group = app_commands.Group(name="nuke", description="Permanently delete user or guild data.")

    # --- Reset Commands ---

    @reset_group.command(name="user_stats", description="Reset a user's stats (XP, level) in this guild.")
    @app_commands.describe(user="The user whose stats you want to reset.")
    @app_commands.checks.has_permissions(administrator=True)
    async def reset_user_stats(self, interaction: discord.Interaction, user: discord.Member):
        await interaction.response.defer(ephemeral=True)
        try:
            await econ_db_manager.reset_user_stats(user_id=str(user.id), guild_id=str(interaction.guild.id))
            await interaction.followup.send(f"Successfully reset stats for {user.mention}.", ephemeral=True)
            logger.info(f"Admin {interaction.user} reset stats for user {user.id} in guild {interaction.guild.id}")
        except Exception as e:
            await interaction.followup.send(f"An error occurred while resetting stats: {e}", ephemeral=True)
            logger.error(f"Error resetting user stats: {e}")

    @reset_group.command(name="user_achievements", description="Reset a user's achievements in this guild.")
    @app_commands.describe(user="The user whose achievements you want to reset.")
    @app_commands.checks.has_permissions(administrator=True)
    async def reset_user_achievements(self, interaction: discord.Interaction, user: discord.Member):
        await interaction.response.defer(ephemeral=True)
        try:
            await econ_db_manager.reset_user_achievements(user_id=str(user.id), guild_id=str(interaction.guild.id))
            await interaction.followup.send(f"Successfully reset achievements for {user.mention}.", ephemeral=True)
            logger.info(f"Admin {interaction.user} reset achievements for user {user.id} in guild {interaction.guild.id}")
        except Exception as e:
            await interaction.followup.send(f"An error occurred while resetting achievements: {e}", ephemeral=True)
            logger.error(f"Error resetting user achievements: {e}")

    @reset_group.command(name="guild_achievements", description="Reset all achievements for everyone in this guild.")
    @app_commands.checks.has_permissions(administrator=True)
    async def reset_guild_achievements(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            await econ_db_manager.reset_guild_achievements(guild_id=str(interaction.guild.id))
            await interaction.followup.send(f"Successfully reset all achievements for this guild.", ephemeral=True)
            logger.info(f"Admin {interaction.user} reset all achievements for guild {interaction.guild.id}")
        except Exception as e:
            await interaction.followup.send(f"An error occurred while resetting guild achievements: {e}", ephemeral=True)
            logger.error(f"Error resetting guild achievements: {e}")


    # --- Nuke Commands (with confirmation) ---

    class NukeConfirmationView(discord.ui.View):
        def __init__(self, target_name: str, nuke_callback, user_to_check: discord.User):
            super().__init__(timeout=30.0)
            self.nuke_callback = nuke_callback
            self.target_name = target_name
            self.user_to_check = user_to_check
            self.value = None

        async def interaction_check(self, interaction: discord.Interaction) -> bool:
            if interaction.user.id != self.user_to_check.id:
                await interaction.response.send_message("You cannot interact with this confirmation.", ephemeral=True)
                return False
            return True

        @discord.ui.button(label="Confirm Nuke", style=discord.ButtonStyle.danger)
        async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
            self.value = True
            await self.nuke_callback(interaction)
            self.stop()

        @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
        async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
            self.value = False
            await interaction.response.edit_message(content="Nuke operation cancelled.", view=None)
            self.stop()


    @nuke_group.command(name="user_data", description="Permanently delete all of a user's data from this guild.")
    @app_commands.describe(user="The user whose data you want to delete.")
    @app_commands.checks.has_permissions(administrator=True)
    async def nuke_user_data(self, interaction: discord.Interaction, user: discord.Member):
        
        async def do_nuke(inter: discord.Interaction):
            await inter.response.edit_message(content=f"Nuking data for {user.mention}...", view=None)
            try:
                await econ_db_manager.delete_all_user_data(user_id=str(user.id), guild_id=str(inter.guild.id))
                await inter.edit_original_response(content=f"Successfully nuked all data for {user.mention}.", view=None)
                logger.warning(f"Admin {inter.user} nuked data for user {user.id} in guild {inter.guild.id}")
            except Exception as e:
                await inter.edit_original_response(content=f"An error occurred during nuke operation: {e}", view=None)
                logger.error(f"Error nuking user data: {e}")

        view = self.NukeConfirmationView(user.display_name, do_nuke, interaction.user)
        await interaction.response.send_message(
            f"**WARNING:** This is a destructive and irreversible action. Are you sure you want to permanently delete all data for {user.mention} in this guild?",
            view=view,
            ephemeral=True
        )

    @nuke_group.command(name="guild_data", description="Permanently delete ALL economy data for this entire guild.")
    @app_commands.checks.has_permissions(administrator=True)
    async def nuke_guild_data(self, interaction: discord.Interaction):
        
        async def do_nuke(inter: discord.Interaction):
            await inter.response.edit_message(content=f"Nuking ALL data for guild **{inter.guild.name}**...", view=None)
            try:
                await econ_db_manager.delete_all_guild_data(guild_id=str(inter.guild.id))
                await inter.edit_original_response(content=f"Successfully nuked all economy data for this guild.", view=None)
                logger.warning(f"Admin {inter.user} nuked all data for guild {inter.guild.id}")
            except Exception as e:
                await inter.edit_original_response(content=f"An error occurred during guild nuke operation: {e}", view=None)
                logger.error(f"Error nuking guild data: {e}")
        
        view = self.NukeConfirmationView(interaction.guild.name, do_nuke, interaction.user)
        await interaction.response.send_message(
            f"**EXTREME WARNING:** This will permanently delete ALL economy data for every user in **{interaction.guild.name}**. This action is irreversible. Are you sure?",
            view=view,
            ephemeral=True
        )


async def setup(bot):
    await bot.add_cog(AdminCommands(bot))
    logger.info("AdminCommands cog loaded.")
