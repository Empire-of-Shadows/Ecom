import discord
import logging
from typing import Dict, Any
from datetime import timezone

from ecom_system.helpers.helpers import utc_now_ts

logger = logging.getLogger(__name__)

class ReactionLevelingSystem:
    """
    Handles processing reactions for the leveling system, including rewards and stats,
    based on detailed database settings.
    """

    def __init__(self, leveling_system):
        """Initialize with reference to parent LevelingSystem"""
        self.leveling_system = leveling_system
        self.logger = logger

    async def process_reaction(self, reaction: discord.Reaction, reactor: discord.User):
        """
        Processes a reaction event, incrementing stats and distributing rewards based on detailed settings.
        """
        message = reaction.message
        guild = message.guild
        message_owner = message.author

        if not guild or reactor.bot or message_owner.bot:
            return
            
        guild_id = str(guild.id)
        
        settings_full = await self.leveling_system.ls_reaction.find_one({"guild_id": guild_id})
        settings = settings_full.get("reaction", {}) if settings_full else {}

        # Self-reaction check from settings
        if settings.get("self_reaction_disabled", True) and reactor.id == message_owner.id:
            return

        # If reaction system is disabled, just increment stats and return
        if not settings.get("enabled", False):
            await self._increment_stats_only(str(reactor.id), str(message_owner.id), guild_id)
            return

        now = utc_now_ts()
        
        # Process rewards for both reactor and owner
        await self._process_reactor_rewards(reactor, guild_id, reaction, now, settings)
        await self._process_owner_rewards(message_owner, guild_id, reaction, now, settings)

    async def _increment_stats_only(self, reactor_id: str, message_owner_id: str, guild_id: str):
        """Increments reaction counts when the reward system is disabled."""
        # Ensure reactor profile exists and increment their count
        await self.leveling_system.get_enhanced_user_data(reactor_id, guild_id)
        await self.leveling_system.update_user_data(
            reactor_id, guild_id, {"$inc": {"message_stats.reacted_messages": 1}}
        )
        
        # Ensure owner profile exists and increment their count
        await self.leveling_system.get_enhanced_user_data(message_owner_id, guild_id)
        await self.leveling_system.update_user_data(
            message_owner_id, guild_id, {"$inc": {"message_stats.got_reactions": 1}}
        )
        self.logger.debug(f"Reaction stats updated for G:{guild_id} U:{reactor_id} and U:{message_owner_id} (rewards disabled)")

    async def _process_reactor_rewards(self, reactor: discord.User, guild_id: str, reaction: discord.Reaction, now: float, settings: Dict[str, Any]):
        """Calculates and applies rewards for the user who added the reaction."""
        reactor_id = str(reactor.id)
        reactor_settings = settings.get("reactor", {})
        
        reactor_data = await self.leveling_system.get_enhanced_user_data(reactor_id, guild_id)
        update_reactor = {"$inc": {"message_stats.reacted_messages": 1}}

        cooldown = reactor_settings.get("cooldown_seconds", 60)
        
        if reactor_data and (now - reactor_data.get("last_rewarded", {}).get("give_reaction", 0)) > cooldown:
            base_xp = reactor_settings.get("xp", 0)
            base_embers = reactor_settings.get("embers", 0)
            
            total_multiplier = 1.0

            # Fast reaction bonus (within 60 seconds of message)
            time_since_message = (discord.utils.utcnow().replace(tzinfo=timezone.utc) - reaction.message.created_at).total_seconds()
            if time_since_message < 60:
                total_multiplier *= settings.get("fast_reaction_bonus", 1.0)

            # Custom emoji bonus
            if isinstance(reaction.emoji, discord.Emoji):
                total_multiplier *= settings.get("custom_emoji_bonus", 1.0)
            
            # Unique emoji on this message bonus (first to use this emoji)
            if reaction.count == 1:
                total_multiplier *= settings.get("unique_emoji_bonus", 1.0)
            
            # Specific emoji bonuses
            emoji_str = str(reaction.emoji)
            if emoji_str in settings.get("emoji_bonuses", {}):
                total_multiplier *= settings["emoji_bonuses"][emoji_str]

            final_xp = base_xp * total_multiplier
            final_embers = base_embers * total_multiplier

            if final_xp > 0: update_reactor["$inc"]["xp"] = round(final_xp)
            if final_embers > 0: update_reactor["$inc"]["embers"] = round(final_embers)
            update_reactor.setdefault("$set", {})["last_rewarded.give_reaction"] = now
        
        await self.leveling_system.update_user_data(reactor_id, guild_id, update_reactor)
        self.logger.debug(f"Processed reactor rewards for G:{guild_id} U:{reactor_id}")

    async def _process_owner_rewards(self, owner: discord.User, guild_id: str, reaction: discord.Reaction, now: float, settings: Dict[str, Any]):
        """Calculates and applies rewards for the message owner."""
        owner_id = str(owner.id)
        owner_settings = settings.get("owner", {})
        
        owner_data = await self.leveling_system.get_enhanced_user_data(owner_id, guild_id)
        update_owner = {"$inc": {"message_stats.got_reactions": 1}}
        
        cooldown = owner_settings.get("cooldown_seconds", 60)

        if owner_data and (now - owner_data.get("last_rewarded", {}).get("got_reaction", 0)) > cooldown:
            base_xp = owner_settings.get("xp", 0)
            base_embers = owner_settings.get("embers", 0)

            total_multiplier = 1.0

            # Chain bonus (if more than one person has used this emoji)
            if reaction.count > 1:
                total_multiplier *= settings.get("chain_bonus", 1.0)
            
            # Diversity bonus (if more than one unique emoji is on the message)
            if len(reaction.message.reactions) > 1:
                total_multiplier *= settings.get("reaction_diversity_bonus", 1.0)
            
            final_xp = base_xp * total_multiplier
            final_embers = base_embers * total_multiplier
            
            if final_xp > 0: update_owner["$inc"]["xp"] = round(final_xp)
            if final_embers > 0: update_owner["$inc"]["embers"] = round(final_embers)
            update_owner.setdefault("$set", {})["last_rewarded.got_reaction"] = now
            
        await self.level_system.update_user_data(owner_id, guild_id, update_owner)
        self.logger.debug(f"Processed owner rewards for G:{guild_id} U:{owner_id}")