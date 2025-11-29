import logging
from database.EconDataManager import econ_db_manager

logger = logging.getLogger(__name__)

async def is_opted_out(user_id: str, guild_id: str) -> bool:
    """
    Checks if a user has opted out of the economy system in a specific guild.

    Args:
        user_id: The ID of the user to check.
        guild_id: The ID of the guild to check.

    Returns:
        True if the user has opted out, False otherwise.
    """
    try:
        return await econ_db_manager.get_user_opt_out_status(user_id, guild_id)
    except Exception as e:
        logger.error(f"Error checking opt-out status for user {user_id} in guild {guild_id}: {e}")
        # Fail-safe: If there's an error, assume the user has not opted out
        # to avoid interrupting their experience if the system is just malfunctioning.
        return False