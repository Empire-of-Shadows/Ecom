import discord
from discord import app_commands
from discord.ext import commands
import logging

from database.EconDataManager import econ_db_manager

logger = logging.getLogger(__name__)


# --- Modals and Views ---

class DeleteDataModal(discord.ui.Modal, title="Confirm Data Deletion"):
    """A modal to confirm permanent data deletion."""
    confirm_text = discord.ui.TextInput(
        label="Type 'delete' to confirm",
        placeholder="delete",
        style=discord.TextStyle.short,
        required=True,
        max_length=6
    )

    def __init__(self, user_id: str, guild_id: str):
        super().__init__()
        self.user_id = user_id
        self.guild_id = guild_id

    async def on_submit(self, interaction: discord.Interaction):
        if self.confirm_text.value.lower() == 'delete':
            await interaction.response.send_message("Processing your data deletion request...", ephemeral=True)
            try:
                await econ_db_manager.set_user_opt_out(self.user_id, self.guild_id, retain_data=False)
                await interaction.edit_original_response(content="You have successfully opted out and all your data has been deleted.")
                logger.warning(f"User {self.user_id} opted out and deleted their data in guild {self.guild_id}.")
            except Exception as e:
                await interaction.edit_original_response(content=f"An error occurred: {e}")
                logger.error(f"Error during user data deletion for {self.user_id}: {e}")
        else:
            await interaction.response.send_message("Incorrect confirmation text. Data deletion cancelled.", ephemeral=True)


class OptOutView(discord.ui.View):
    """A view to handle the opt-out process."""
    def __init__(self, user_id: str, guild_id: str):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.guild_id = guild_id

    @discord.ui.button(label="Retain Data (90 days)", style=discord.ButtonStyle.primary)
    async def retain_data(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        try:
            await econ_db_manager.set_user_opt_out(self.user_id, self.guild_id, retain_data=True)
            await interaction.followup.send("You have opted out. Your data will be automatically deleted in 90 days if you do not opt back in.", ephemeral=True)
            logger.info(f"User {self.user_id} opted out in guild {self.guild_id} with data retention.")
        except Exception as e:
            await interaction.followup.send(f"An error occurred: {e}", ephemeral=True)
            logger.error(f"Error during opt-out with retention for {self.user_id}: {e}")
        self.stop()

    @discord.ui.button(label="Delete All My Data", style=discord.ButtonStyle.danger)
    async def delete_data(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = DeleteDataModal(user_id=self.user_id, guild_id=self.guild_id)
        await interaction.response.send_modal(modal)
        self.stop()


class UserSettingsCommands(commands.Cog):
    """
    Cog for user-facing settings commands.
    """

    def __init__(self, bot):
        self.bot = bot

    settings_group = app_commands.Group(name="settings", description="Manage your economy system settings.")

    @settings_group.command(name="opt-out", description="Opt out of the economy system.")
    async def opt_out(self, interaction: discord.Interaction):
        is_opted_out = await econ_db_manager.get_user_opt_out_status(str(interaction.user.id), str(interaction.guild.id))
        if is_opted_out:
            await interaction.response.send_message("You have already opted out.", ephemeral=True)
            return

        view = OptOutView(user_id=str(interaction.user.id), guild_id=str(interaction.guild.id))
        await interaction.response.send_message(
            "**You are about to opt out of the economy system.**\n\n"
            "Choosing **Retain Data** means we will hold your data for 90 days, after which it will be deleted if you don't opt back in.\n"
            "Choosing **Delete All My Data** is permanent and cannot be undone.",
            view=view,
            ephemeral=True
        )

    @settings_group.command(name="opt-in", description="Opt back into the economy system.")
    async def opt_in(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            await econ_db_manager.set_user_opt_in(str(interaction.user.id), str(interaction.guild.id))
            await interaction.followup.send("You have successfully opted back into the economy system!", ephemeral=True)
            logger.info(f"User {interaction.user.id} opted back in in guild {interaction.guild.id}.")
        except Exception as e:
            await interaction.followup.send(f"An error occurred: {e}", ephemeral=True)
            logger.error(f"Error during opt-in for {interaction.user.id}: {e}")


async def setup(bot):
    await bot.add_cog(UserSettingsCommands(bot))
    logger.info("UserSettingsCommands cog loaded.")
