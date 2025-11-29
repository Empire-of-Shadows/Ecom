import logging
from collections import defaultdict, deque
from typing import List, Tuple, Dict

import discord
from discord.ext import commands

from ecom_system.helpers.helpers import utc_now_ts
from ecom_system.helpers.opt_out_helper import is_opted_out
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


class EnhancedReactionTracker(commands.Cog):
    """
    Enhanced reaction tracking with improved analytics and spam detection.

    Features:
    - Reaction spam detection and prevention
    - Comprehensive reaction analytics
    - Activity buffer integration for batch processing
    - Leveling system integration for reaction rewards
    - Activity system integration for comprehensive tracking
    """

    def __init__(self, bot):
        """Initialize the reaction tracker with required components."""
        self.bot = bot
        # self.leveling_system is now accessed via self.bot.leveling_system
        # self.tracker is removed and its functionality is moved to the leveling subsystem

        # =====================================================================
        # SECTION: System References
        # =====================================================================
        self.leveling_system = None
        self.activity_system = None

        # =====================================================================
        # SECTION: Logging Setup
        # =====================================================================
        self._log = logging.getLogger(__name__)
        self._log.info("reaction_tracker_initialized", extra={"event": "cog_init"})

        # =====================================================================
        # SECTION: Spam Detection State
        # =====================================================================
        # Track reaction rates per user per guild: (guild_id, user_id) -> deque of timestamps
        self.reaction_rates: Dict[Tuple[int, int], deque] = defaultdict(lambda: deque(maxlen=20))

    async def cog_load(self):
        """Initialize systems when cog loads."""
        self._log.info("🔄 Initializing Enhanced Reaction Tracker...")
        try:
            # Initialize leveling system
            if hasattr(self.bot, 'leveling_system') and self.bot.leveling_system:
                self.leveling_system = self.bot.leveling_system
                self._log.info("✅ ReactionTracker using shared leveling system from bot")
            else:
                self._log.error("❌ No leveling system found on bot instance")
                raise ValueError("Leveling system not available on bot instance")

            # Initialize activity system
            if hasattr(self.bot, 'activity_system') and self.bot.activity_system:
                self.activity_system = self.bot.activity_system
                self._log.info("✅ ReactionTracker using shared activity system from bot")
            else:
                self._log.warning("⚠️ Activity system not found on bot instance. Activity will not be tracked.")

            self._log.info("✅ Enhanced ReactionTracker fully initialized")

        except Exception as e:
            self._log.error(f"❌ Failed to initialize ReactionTracker: {e}")
            raise

    # =========================================================================
    # SECTION: Spam Detection Utilities
    # =========================================================================

    def _detect_reaction_spam(self, guild_id: int, user_id: int) -> bool:
        """
        Detect if user is spamming reactions based on rate limiting.

        Args:
            guild_id: The guild ID where the reaction occurred
            user_id: The user ID who added the reaction

        Returns:
            True if spam is detected, False otherwise
        """
        user_key = (guild_id, user_id)
        current_time = utc_now_ts()

        # Add current reaction timestamp
        self.reaction_rates[user_key].append(current_time)

        # Count reactions in the last 60 seconds
        time_threshold = current_time - 60
        recent_reactions = sum(
            1 for timestamp in self.reaction_rates[user_key]
            if timestamp > time_threshold
        )

        # More than 10 reactions per minute is considered spam
        spam_detected = recent_reactions > 10

        if spam_detected:
            self._log.warning(
                "reaction_spam_detected",
                extra={
                    "event": "reaction_spam",
                    "guild_id": guild_id,
                    "user_id": user_id,
                    "reactions_per_minute": recent_reactions,
                    "threshold": 10
                }
            )

        return spam_detected

    # =========================================================================
    # SECTION: Discord Reaction Event Listeners
    # =========================================================================

    @commands.Cog.listener()
    @log_performance("on_reaction_add")
    async def on_reaction_add(self, reaction: discord.Reaction, user: discord.User):
        """
        Handle reaction add events with comprehensive tracking and analysis.

        Args:
            reaction: The reaction object that was added
            user: The user who added the reaction
        """
        # =====================================================================
        # SUBSECTION: Initial Validation
        # =====================================================================
        self._log.debug(
            "reaction_add_event_received",
            extra={"event": "reaction_add_event_received"}
        )

        if getattr(user, "bot", False):
            self._log.debug(
                "ignoring_bot_reaction",
                extra={"event": "ignore", "user_id": getattr(user, "id", "unknown")}
            )
            return

        message = reaction.message
        if not message.guild:
            self._log.debug(
                "ignoring_dm_reaction",
                extra={"event": "ignore", "user_id": getattr(user, "id", "unknown")}
            )
            return

        guild_id_str = str(message.guild.id)
        user_id_str = str(user.id)

        if await is_opted_out(user_id_str, guild_id_str):
            return

        # =====================================================================
        # SUBSECTION: Data Extraction
        # =====================================================================
        guild_id = message.guild.id
        channel_id = message.channel.id
        reactor_id = user.id
        message_owner_id = message.author.id

        # =====================================================================
        # SUBSECTION: Spam Detection
        # =====================================================================
        is_spam = self._detect_reaction_spam(guild_id, reactor_id)

        # =====================================================================
        # SUBSECTION: Reaction Data Preparation
        # =====================================================================
        reaction_data = {
            "event": "reaction_add",
            "guild_id": guild_id,
            "channel_id": channel_id,
            "reactor_id": reactor_id,
            "message_owner_id": message_owner_id,
            "emoji": str(reaction.emoji),
            "emoji_id": getattr(reaction.emoji, "id", None),
            "emoji_animated": getattr(reaction.emoji, "animated", False),
            "message_id": int(message.id),
            "channel_name": _safe_channel_name(message.channel),
            "is_custom_emoji": hasattr(reaction.emoji, "id"),
            "is_spam": is_spam,
            "timestamp": utc_now_ts()
        }

        self._log.info("reaction_added", extra=reaction_data)

        # =====================================================================
        # SUBSECTION: Self-Reaction Check
        # =====================================================================
        if message_owner_id == reactor_id:
            self._log.info(
                "self_reaction_ignored",
                extra={
                    "event": "ignore_self_reaction",
                    "guild_id": guild_id,
                    "user_id": reactor_id
                }
            )
            return

        # =====================================================================
        # SUBSECTION: Processing (Skip if Spam Detected)
        # =====================================================================
        if is_spam:
            self._log.info(
                "spam_reaction_skipped",
                extra={
                    "event": "spam_skip_processing",
                    "guild_id": guild_id,
                    "user_id": reactor_id
                }
            )
            return

        # =====================================================================
        # SUBSECTION: Activity System Integration
        # =====================================================================
        if self.activity_system:
            try:
                # Prepare detailed activity data for the activity system
                activity_data = {
                    "channel_id": str(channel_id),
                    "channel_name": reaction_data["channel_name"],
                    "message_id": str(message.id),
                    "message_owner_id": str(message_owner_id),
                    "emoji": reaction_data["emoji"],
                    "emoji_id": str(reaction_data["emoji_id"]) if reaction_data["emoji_id"] else None,
                    "emoji_animated": reaction_data["emoji_animated"],
                    "is_custom_emoji": reaction_data["is_custom_emoji"],
                    "is_spam": is_spam,
                    "is_thread": isinstance(message.channel, discord.Thread),
                    "reaction_type": "add",
                    "created_at": message.created_at.timestamp() if message.created_at else None,
                    "reactor_name": str(user.name),
                    "reactor_display_name": str(user.display_name) if hasattr(user, 'display_name') else str(user.name),
                    "channel_type": str(type(message.channel).__name__)
                }

                await self.activity_system.record_activity(
                    user_id=str(reactor_id),
                    guild_id=str(guild_id),
                    activity_type='reaction',
                    activity_data=activity_data
                )

                self._log.debug(
                    "activity_system_reaction_recorded",
                    extra={
                        "event": "activity_system_success",
                        "guild_id": guild_id,
                        "user_id": reactor_id,
                        "message_id": int(message.id)
                    }
                )

            except Exception as e:
                self._log.warning(
                    "activity_system_reaction_failed",
                    extra={
                        "event": "activity_system_error",
                        "guild_id": guild_id,
                        "user_id": reactor_id,
                        "message_id": int(message.id),
                        "error": str(e)
                    }
                )

        try:
            # =============================================================
            # SUBSUBSECTION: Leveling System Integration
            # =============================================================
            if not hasattr(self.bot, "leveling_system"):
                self._log.error("Leveling system not available on bot object.")
                return

            await self.bot.leveling_system.reaction_system.process_reaction(
                reaction=reaction,
                reactor=user,
            )

            self._log.debug(
                "reaction_processing_completed",
                extra={
                    "event": "reaction_processed",
                    "guild_id": guild_id,
                    "reactor_id": reactor_id,
                    "message_owner_id": message_owner_id
                }
            )

        except Exception as e:
            self._log.exception(
                "reaction_processing_failed",
                extra={
                    "event": "error",
                    "guild_id": guild_id,
                    "reactor_id": reactor_id,
                    "message_owner_id": message_owner_id,
                    "error": str(e)
                },
            )

    @commands.Cog.listener()
    async def on_reaction_remove(self, reaction: discord.Reaction, user: discord.User):
        """
        Handle reaction removal events.

        Args:
            reaction: The reaction object that was removed
            user: The user who removed the reaction
        """
        try:
            # =================================================================
            # SUBSECTION: Initial Validation
            # =================================================================
            if getattr(user, "bot", False):
                self._log.debug(
                    "ignoring_bot_reaction_remove",
                    extra={"event": "ignore", "user_id": getattr(user, "id", "unknown")}
                )
                return

            message = reaction.message
            if not message.guild:
                self._log.debug(
                    "ignoring_dm_reaction_remove",
                    extra={"event": "ignore", "user_id": getattr(user, "id", "unknown")}
                )
                return

            # =================================================================
            # SUBSECTION: Data Preparation
            # =================================================================
            remove_data = {
                "event": "reaction_remove",
                "guild_id": int(message.guild.id),
                "channel_id": int(message.channel.id),
                "user_id": int(user.id),
                "message_owner_id": int(message.author.id),
                "emoji": str(reaction.emoji),
                "message_id": int(message.id),
                "channel_name": _safe_channel_name(message.channel),
                "timestamp": utc_now_ts()
            }

            self._log.info("reaction_removed", extra=remove_data)

            # =================================================================
            # SUBSECTION: Activity System Integration (For Remove Events)
            # =================================================================
            if self.activity_system:
                try:
                    # Prepare detailed activity data for reaction removal
                    activity_data = {
                        "channel_id": str(remove_data["channel_id"]),
                        "channel_name": remove_data["channel_name"],
                        "message_id": str(remove_data["message_id"]),
                        "message_owner_id": str(remove_data["message_owner_id"]),
                        "emoji": remove_data["emoji"],
                        "emoji_id": str(getattr(reaction.emoji, "id", None)) if hasattr(reaction.emoji, "id") else None,
                        "emoji_animated": getattr(reaction.emoji, "animated", False),
                        "is_custom_emoji": hasattr(reaction.emoji, "id"),
                        "is_thread": isinstance(message.channel, discord.Thread),
                        "reaction_type": "remove",
                        "created_at": message.created_at.timestamp() if message.created_at else None,
                        "reactor_name": str(user.name),
                        "reactor_display_name": str(user.display_name) if hasattr(user, 'display_name') else str(user.name),
                        "channel_type": str(type(message.channel).__name__)
                    }

                    await self.activity_system.record_activity(
                        user_id=str(remove_data["user_id"]),
                        guild_id=str(remove_data["guild_id"]),
                        activity_type='reaction_remove',
                        activity_data=activity_data
                    )

                    self._log.debug(
                        "activity_system_reaction_remove_recorded",
                        extra={
                            "event": "activity_system_success",
                            "guild_id": remove_data["guild_id"],
                            "user_id": remove_data["user_id"],
                            "message_id": remove_data["message_id"]
                        }
                    )

                except Exception as e:
                    self._log.warning(
                        "activity_system_reaction_remove_failed",
                        extra={
                            "event": "activity_system_error",
                            "guild_id": remove_data["guild_id"],
                            "user_id": remove_data["user_id"],
                            "message_id": remove_data["message_id"],
                            "error": str(e)
                        }
                    )

        except Exception as e:
            self._log.exception(
                "reaction_remove_processing_failed",
                extra={
                    "event": "error",
                    "error": str(e)
                }
            )

    @commands.Cog.listener()
    async def on_reaction_clear(self, message: discord.Message, reactions: List[discord.Reaction]):
        """
        Handle bulk reaction clearance from a message.

        Args:
            message: The message that had reactions cleared
            reactions: List of reactions that were cleared
        """
        try:
            # =================================================================
            # SUBSECTION: Initial Validation
            # =================================================================
            if not message.guild:
                self._log.debug(
                    "ignoring_dm_reaction_clear",
                    extra={"event": "ignore"}
                )
                return

            # =================================================================
            # SUBSECTION: Data Preparation
            # =================================================================
            reaction_count = len(reactions or [])
            clear_data = {
                "event": "reaction_clear",
                "guild_id": int(message.guild.id),
                "channel_id": int(message.channel.id),
                "message_id": int(message.id),
                "reaction_types_removed": reaction_count,
                "channel_name": _safe_channel_name(message.channel),
                "timestamp": utc_now_ts()
            }

            self._log.info("reactions_cleared", extra=clear_data)

        except Exception as e:
            self._log.exception(
                "reaction_clear_processing_failed",
                extra={
                    "event": "error",
                    "error": str(e)
                }
            )

    @commands.Cog.listener()
    async def on_reaction_clear_emoji(self, reaction: discord.Reaction):
        """
        Handle clearance of a specific emoji from all reactions on a message.

        Args:
            reaction: The reaction emoji that was cleared
        """
        try:
            # =================================================================
            # SUBSECTION: Initial Validation
            # =================================================================
            message = reaction.message
            if not message.guild:
                self._log.debug(
                    "ignoring_dm_reaction_clear_emoji",
                    extra={"event": "ignore"}
                )
                return

            # =================================================================
            # SUBSECTION: Data Preparation
            # =================================================================
            clear_emoji_data = {
                "event": "reaction_clear_emoji",
                "guild_id": int(message.guild.id),
                "channel_id": int(message.channel.id),
                "message_id": int(message.id),
                "emoji": str(reaction.emoji),
                "channel_name": _safe_channel_name(message.channel),
                "timestamp": utc_now_ts()
            }

            self._log.info("reaction_emoji_cleared", extra=clear_emoji_data)

        except Exception as e:
            self._log.exception(
                "reaction_clear_emoji_processing_failed",
                extra={
                    "event": "error",
                    "error": str(e)
                }
            )


async def setup(bot):
    """Setup function for discord.py cog loading."""
    await bot.add_cog(EnhancedReactionTracker(bot))