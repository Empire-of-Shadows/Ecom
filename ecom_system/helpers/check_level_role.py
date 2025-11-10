import discord
from typing import Dict, List, Optional, Tuple
from loggers.logger_setup import get_logger

logger = get_logger("LevelRoleChecker", level=20, json_format=False, colored_console=True)


class LevelRoleResult:
    """Result object for level role operations."""

    def __init__(self):
        self.success: bool = False
        self.action_taken: str = "none"  # "added", "removed", "updated", "none"
        self.roles_added: List[discord.Role] = []
        self.roles_removed: List[discord.Role] = []
        self.target_role: Optional[discord.Role] = None
        self.user_level: int = 0
        self.error: Optional[str] = None
        self.reason: str = ""

    def __str__(self):
        return f"LevelRoleResult(action={self.action_taken}, added={len(self.roles_added)}, removed={len(self.roles_removed)})"


async def check_and_update_level_role(
        bot: discord.Client,
        guild_id: str,
        user_id: str,
        current_level: int,
        level_roles_config: Dict[str, int]
) -> LevelRoleResult:
    """
    Check and update a user's level roles based on their current level.

    Args:
        bot: Discord bot instance
        guild_id: Guild ID as string
        user_id: User ID as string
        current_level: User's current level
        level_roles_config: Dict mapping role_id (str) -> required_level (int)

    Returns:
        LevelRoleResult: Detailed result of the operation
    """
    result = LevelRoleResult()
    result.user_level = current_level

    try:
        logger.debug(f"🔍 Checking level roles for U:{user_id} (Level {current_level}) in G:{guild_id}")

        # Get guild
        guild = bot.get_guild(int(guild_id))
        if not guild:
            try:
                guild = await bot.fetch_guild(int(guild_id))
                logger.debug(f"✅ Fetched guild {guild_id} via API")
            except Exception as e:
                result.error = f"Failed to get guild: {e}"
                logger.error(f"❌ {result.error}")
                return result

        # Get member
        try:
            member = await guild.fetch_member(int(user_id))
            logger.debug(f"✅ Fetched member {member.display_name} ({user_id})")
        except Exception as e:
            result.error = f"Failed to get member: {e}"
            logger.error(f"❌ {result.error}")
            return result

        # Check bot permissions
        bot_member = guild.me
        if not bot_member.guild_permissions.manage_roles:
            result.error = "Bot lacks 'Manage Roles' permission"
            logger.error(f"❌ {result.error}")
            return result

        # Parse and validate level roles configuration
        level_roles = _parse_level_roles_config(guild, level_roles_config)
        if not level_roles:
            result.reason = "No valid level roles configured"
            logger.debug(f"ℹ️ {result.reason}")
            result.success = True
            return result

        # Determine target role based on current level
        target_role = _get_target_role_for_level(current_level, level_roles)
        result.target_role = target_role

        # Get user's current level roles
        current_level_roles = _get_user_current_level_roles(member, level_roles)

        logger.info(f"📊 Level Role Analysis for {member.display_name}:")
        logger.info(f"  • Current Level: {current_level}")
        logger.info(f"  • Target Role: {target_role.name if target_role else 'None'}")
        logger.info(f"  • Current Level Roles: {[r.name for r in current_level_roles]}")

        # Determine what action to take
        if target_role and target_role not in current_level_roles:
            # User should have target role but doesn't - add it and remove others
            await _update_user_level_roles(member, target_role, current_level_roles, result)

        elif not target_role and current_level_roles:
            # User shouldn't have any level roles but has some - remove them
            await _remove_all_level_roles(member, current_level_roles, result)

        elif target_role and len(current_level_roles) > 1:
            # User has multiple level roles - keep only the target one
            await _update_user_level_roles(member, target_role, current_level_roles, result)

        elif target_role and len(current_level_roles) == 1 and target_role in current_level_roles:
            # User has exactly the right role - no action needed
            result.success = True
            result.action_taken = "none"
            result.reason = "User already has correct level role"
            logger.info(f"✅ {result.reason}")

        else:
            # No action needed
            result.success = True
            result.action_taken = "none"
            result.reason = "No level role changes needed"
            logger.debug(f"ℹ️ {result.reason}")

        return result

    except Exception as e:
        result.error = f"Unexpected error: {e}"
        logger.error(f"❌ {result.error}", exc_info=True)
        return result


def _parse_level_roles_config(guild: discord.Guild, config: Dict[str, int]) -> List[Tuple[discord.Role, int]]:
    """
    Parse and validate the level roles configuration.

    Returns:
        List of tuples (role, required_level) sorted by required level descending
    """
    level_roles = []

    for role_id_str, required_level in config.items():
        try:
            role_id = int(role_id_str)
            role = guild.get_role(role_id)

            if not role:
                logger.warning(f"⚠️ Level role {role_id} not found in guild {guild.name}")
                continue

            if not isinstance(required_level, int) or required_level < 1:
                logger.warning(f"⚠️ Invalid required level {required_level} for role {role.name}")
                continue

            level_roles.append((role, required_level))
            logger.debug(f"✅ Registered level role: {role.name} (Level {required_level})")

        except (ValueError, TypeError) as e:
            logger.warning(f"⚠️ Invalid role ID {role_id_str}: {e}")
            continue

    # Sort by required level (highest first) so we can find the highest role the user qualifies for
    level_roles.sort(key=lambda x: x[1], reverse=True)

    logger.debug(f"📋 Parsed {len(level_roles)} valid level roles")
    return level_roles


def _get_target_role_for_level(current_level: int, level_roles: List[Tuple[discord.Role, int]]) -> Optional[
    discord.Role]:
    """
    Get the highest role the user qualifies for based on their level.

    Args:
        current_level: User's current level
        level_roles: List of (role, required_level) sorted by level descending

    Returns:
        The highest role the user qualifies for, or None if they don't qualify for any
    """
    for role, required_level in level_roles:
        if current_level >= required_level:
            logger.debug(f"🎯 Target role determined: {role.name} (requires Level {required_level})")
            return role

    logger.debug(f"🎯 No target role - user level {current_level} doesn't qualify for any level roles")
    return None


def _get_user_current_level_roles(member: discord.Member, level_roles: List[Tuple[discord.Role, int]]) -> List[
    discord.Role]:
    """
    Get all level roles that the user currently has.

    Args:
        member: Discord member
        level_roles: List of (role, required_level)

    Returns:
        List of level roles the user currently has
    """
    level_role_objects = [role for role, _ in level_roles]
    current_level_roles = [role for role in member.roles if role in level_role_objects]

    logger.debug(f"👤 User {member.display_name} currently has level roles: {[r.name for r in current_level_roles]}")
    return current_level_roles


async def _update_user_level_roles(
        member: discord.Member,
        target_role: discord.Role,
        current_level_roles: List[discord.Role],
        result: LevelRoleResult
):
    """
    Update user's level roles by adding target role and removing others.
    """
    try:
        # Roles to remove (all current level roles except the target)
        roles_to_remove = [role for role in current_level_roles if role != target_role]

        # Check if we need to add the target role
        needs_target_role = target_role not in current_level_roles

        logger.info(f"🔄 Updating level roles for {member.display_name}:")
        if needs_target_role:
            logger.info(f"  ➕ Adding: {target_role.name}")
        if roles_to_remove:
            logger.info(f"  ➖ Removing: {[r.name for r in roles_to_remove]}")

        # Check role hierarchy - bot must be able to manage these roles
        bot_top_role = member.guild.me.top_role

        # Check if we can manage the target role
        if needs_target_role and target_role >= bot_top_role:
            result.error = f"Cannot add {target_role.name} - role is higher than bot's top role"
            logger.error(f"❌ {result.error}")
            return

        # Check if we can manage roles to remove
        for role in roles_to_remove:
            if role >= bot_top_role:
                result.error = f"Cannot remove {role.name} - role is higher than bot's top role"
                logger.error(f"❌ {result.error}")
                return

        # Perform the role updates
        if needs_target_role:
            try:
                await member.add_roles(target_role, reason=f"Level role update: user reached level {result.user_level}")
                result.roles_added.append(target_role)
                logger.info(f"✅ Added role {target_role.name} to {member.display_name}")
            except Exception as e:
                result.error = f"Failed to add role {target_role.name}: {e}"
                logger.error(f"❌ {result.error}")
                return

        if roles_to_remove:
            try:
                await member.remove_roles(*roles_to_remove,
                                          reason=f"Level role update: user now level {result.user_level}")
                result.roles_removed.extend(roles_to_remove)
                logger.info(f"✅ Removed {len(roles_to_remove)} old level role(s) from {member.display_name}")
            except Exception as e:
                result.error = f"Failed to remove old roles: {e}"
                logger.error(f"❌ {result.error}")
                # Even if we can't remove old roles, adding the new one was successful
                if needs_target_role:
                    result.success = True
                    result.action_taken = "added"
                    result.reason = f"Added {target_role.name} but couldn't remove old roles"
                return

        # Success
        result.success = True
        if needs_target_role and roles_to_remove:
            result.action_taken = "updated"
            result.reason = f"Updated to {target_role.name}, removed {len(roles_to_remove)} old role(s)"
        elif needs_target_role:
            result.action_taken = "added"
            result.reason = f"Added {target_role.name}"
        elif roles_to_remove:
            result.action_taken = "removed"
            result.reason = f"Removed {len(roles_to_remove)} conflicting level role(s)"

    except Exception as e:
        result.error = f"Failed to update user roles: {e}"
        logger.error(f"❌ {result.error}", exc_info=True)


async def _remove_all_level_roles(
        member: discord.Member,
        current_level_roles: List[discord.Role],
        result: LevelRoleResult
):
    """
    Remove all level roles from a user.
    """
    try:
        logger.info(f"➖ Removing all level roles from {member.display_name}: {[r.name for r in current_level_roles]}")

        # Check role hierarchy
        bot_top_role = member.guild.me.top_role
        removable_roles = []
        non_removable_roles = []

        for role in current_level_roles:
            if role >= bot_top_role:
                non_removable_roles.append(role)
            else:
                removable_roles.append(role)

        if non_removable_roles:
            logger.warning(f"⚠️ Cannot remove roles higher than bot: {[r.name for r in non_removable_roles]}")

        if not removable_roles:
            result.error = "Cannot remove any level roles - all are higher than bot's role"
            logger.error(f"❌ {result.error}")
            return

        # Remove the roles
        await member.remove_roles(*removable_roles, reason=f"Level role cleanup: user level {result.user_level}")
        result.roles_removed.extend(removable_roles)

        result.success = True
        result.action_taken = "removed"
        result.reason = f"Removed {len(removable_roles)} level role(s)"
        logger.info(f"✅ Removed {len(removable_roles)} level role(s) from {member.display_name}")

        if non_removable_roles:
            result.reason += f" (couldn't remove {len(non_removable_roles)} high-hierarchy roles)"

    except Exception as e:
        result.error = f"Failed to remove level roles: {e}"
        logger.error(f"❌ {result.error}", exc_info=True)


# Convenience function for integration with the leveling system
async def update_level_role_on_levelup(
        bot: discord.Client,
        leveling_system,
        guild_id: str,
        user_id: str,
        new_level: int
) -> LevelRoleResult:
    """
    Convenience function to update level roles when a user levels up.
    Automatically fetches the level roles configuration from guild settings.

    Args:
        bot: Discord bot instance
        leveling_system: LevelingSystem instance
        guild_id: Guild ID as string
        user_id: User ID as string
        new_level: User's new level

    Returns:
        LevelRoleResult: Result of the operation
    """
    try:
        # Get guild settings
        settings = await leveling_system.get_guild_settings(guild_id)
        level_roles_config = settings.get("level_roles", {})

        if not level_roles_config:
            result = LevelRoleResult()
            result.success = True
            result.action_taken = "none"
            result.reason = "No level roles configured for this guild"
            result.user_level = new_level
            logger.debug(f"ℹ️ No level roles configured for guild {guild_id}")
            return result

        # Perform the role check and update
        return await check_and_update_level_role(
            bot=bot,
            guild_id=guild_id,
            user_id=user_id,
            current_level=new_level,
            level_roles_config=level_roles_config
        )

    except Exception as e:
        result = LevelRoleResult()
        result.error = f"Failed to update level role on levelup: {e}"
        result.user_level = new_level
        logger.error(f"❌ {result.error}", exc_info=True)
        return result


# Export main functions
__all__ = [
    "check_and_update_level_role",
    "update_level_role_on_levelup",
    "LevelRoleResult"
]