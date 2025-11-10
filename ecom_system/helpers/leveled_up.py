import discord
from discord import ui
from typing import Dict, Any, Optional
from datetime import datetime, timezone

from loggers.logger_setup import get_logger

logger = get_logger("LevelUpMessages")

class LevelUpMessages:
    """
    Handles level-up message creation and sending using Discord Components v2.
    Creates rich, interactive level-up notifications for different occasions.
    """

    def __init__(self, bot: discord.Client):
        """
        Initialize the LevelUpMessages handler.

        Args:
            bot: Discord bot instance
        """
        try:
            self.bot = bot
            logger.info("✅ LevelUpMessages handler initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize LevelUpMessages: {e}", exc_info=True)
            raise

    async def send_level_up_message(
            self,
            guild_id: str,
            user_id: str,
            old_level: int,
            new_level: int,
            channel_id: Optional[str] = None,
            reason: str = "standard",
            extra_data: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Send a level-up message to the configured notification channel.

        Args:
            guild_id: Guild ID where the level up occurred
            user_id: User ID who leveled up
            old_level: Previous level
            new_level: New level achieved
            channel_id: Optional channel ID override (uses guild settings if None)
            reason: Reason for level up ("standard", "prestige", "milestone", "first_level")
            extra_data: Additional data for the message (xp, embers, etc.)

        Returns:
            True if message was sent successfully, False otherwise
        """
        try:
            logger.debug(f"📨 Starting level-up message process for user {user_id} in guild {guild_id}")

            # Get guild with better error handling
            guild = self.bot.get_guild(int(guild_id))
            if not guild:
                logger.warning(f"⚠️ Guild {guild_id} not found in bot cache, attempting to fetch...")
                try:
                    guild = await self.bot.fetch_guild(int(guild_id))
                    logger.debug(f"✅ Fetched guild {guild_id} via API")
                except Exception as fetch_error:
                    logger.error(f"❌ Failed to fetch guild {guild_id}: {fetch_error}")
                    return False
            else:
                logger.debug(f"✅ Found guild {guild.name} ({guild_id}) in cache")

            # Get user
            try:
                user = await self.bot.fetch_user(int(user_id))
                logger.debug(f"✅ Fetched user {user.name} ({user_id})")
            except Exception as e:
                logger.error(f"❌ Failed to fetch user {user_id}: {e}")
                return False

            # Determine the channel to send to
            if not channel_id:
                logger.warning(f"⚠️ No notification channel configured for guild {guild_id}")
                return False

            try:
                channel = guild.get_channel(int(channel_id))
                if not channel:
                    logger.error(f"❌ Channel {channel_id} not found in guild {guild.name}")
                    return False
                logger.debug(f"✅ Found channel {channel.name} ({channel_id})")
            except Exception as e:
                logger.error(f"❌ Failed to get channel {channel_id}: {e}")
                return False

            # Check permissions
            try:
                permissions = channel.permissions_for(guild.me)
                if not permissions.send_messages:
                    logger.error(f"❌ No permission to send messages in channel {channel.name}")
                    return False

                # Check if we can attach files (needed for attachment:// protocol)
                if not permissions.attach_files:
                    logger.warning(f"⚠️ No permission to attach files in channel {channel.name}")
                    # We'll continue without file attachments

                logger.debug(f"✅ Have permissions to send in channel {channel.name}")
            except Exception as e:
                logger.error(f"❌ Failed to check permissions for channel {channel_id}: {e}")
                return False

            # Prepare file attachments if they exist and we have permission
            files = []
            try:
                import os

                # Define potential attachment files based on reason
                attachment_files = []

                if reason == "standard":
                    attachment_files = ["assets/strong/strong_512.png"]
                elif reason == "milestone":
                    attachment_files = ["assets/strong/strong_512.png"]
                elif reason == "prestige":
                    attachment_files = ["assets/strong/strong_512.png"]
                elif reason == "first_level":
                    attachment_files = ["assets/strong/strong_256.png"]

                # Check if we have attach permissions and files exist
                if channel.permissions_for(guild.me).attach_files:
                    for file_path in attachment_files:
                        if os.path.exists(file_path):
                            filename = os.path.basename(file_path)
                            files.append(discord.File(file_path, filename=filename))
                            logger.debug(f"✅ Prepared file attachment: {file_path} as {filename}")
                        else:
                            logger.debug(f"⚠️ File not found: {file_path}")
                else:
                    logger.debug("ℹ️ No attach_files permission, skipping file attachments")

            except Exception as e:
                logger.warning(f"⚠️ Could not prepare file attachments: {e}")
                files = []  # Continue without files

            # Create the appropriate message based on reason
            try:
                # Pass whether we have files available to the message creation methods
                has_attachments = len(files) > 0

                if reason == "first_level":
                    layout = self._create_first_level_message(user, new_level, extra_data, has_attachments)
                elif reason == "milestone":
                    layout = self._create_milestone_message(user, old_level, new_level, extra_data, has_attachments)
                elif reason == "prestige":
                    layout = self._create_prestige_message(user, new_level, extra_data, has_attachments)
                else:
                    layout = self._create_standard_level_up_message(user, old_level, new_level, extra_data,
                                                                    has_attachments)
                logger.debug(f"✅ Created level-up layout for reason: {reason} (attachments: {has_attachments})")
            except Exception as e:
                logger.error(f"❌ Failed to create level-up layout for reason '{reason}': {e}")
                return False

            # Send the message with or without files
            try:
                if files:
                    await channel.send(view=layout, files=files)
                    logger.debug(f"📎 Message sent with {len(files)} file attachment(s)")
                else:
                    await channel.send(view=layout)
                    logger.debug(f"📝 Message sent without attachments")

                logger.info(
                    f"✅ Level-up message sent: {user.name} ({user_id}) "
                    f"L{old_level}→L{new_level} in {guild.name} (reason: {reason})"
                )
                return True
            except Exception as e:
                logger.error(f"❌ Failed to send message to channel {channel_id}: {e}")
                return False

        except Exception as e:
            logger.error(f"❌ Failed to send level-up message: {e}", exc_info=True)
            return False

    def _create_standard_level_up_message(
            self,
            user: discord.User,
            old_level: int,
            new_level: int,
            extra_data: Optional[Dict[str, Any]] = None,
            has_attachments: bool = False
    ) -> ui.LayoutView:
        """
        Create a standard level-up message with Components v2.

        Args:
            user: Discord user object
            old_level: Previous level
            new_level: New level
            extra_data: Additional data (xp, embers, streak, etc.)
            has_attachments: Whether file attachments are available for attachment:// protocol

        Returns:
            LayoutView with the level-up message
        """
        try:
            layout = ui.LayoutView()

            # Hero section with user avatar
            try:
                hero_section = ui.Section(
                    accessory=ui.Thumbnail(media=str(user.display_avatar.url))
                )
            except Exception as e:
                logger.error(f"❌ Failed to create hero section: {e}")
                hero_section = ui.Section(
                    accessory=ui.Thumbnail(media=str(user.display_avatar.url)))

            # Main congratulations message
            try:
                hero_section.add_item(
                    ui.TextDisplay(f"# 🎊 Level Up!")
                )

                hero_section.add_item(
                    ui.TextDisplay(
                        f"**{user.mention}** has reached **Level {new_level}**! "
                        f"🎉\n\n"
                        f"Previous Level: **{old_level}** → New Level: **{new_level}**"
                    )
                )
            except Exception as e:
                logger.error(f"❌ Failed to add text to hero section: {e}")

            # Create hero container with accent color
            try:
                hero_container = ui.Container()
                hero_container.accent_color = discord.Color.gold().value
                hero_container.add_item(hero_section)
                layout.add_item(hero_container)
                layout.add_item(ui.Separator())
            except Exception as e:
                logger.error(f"❌ Failed to create hero container: {e}")

            # Stats section (if extra_data provided)
            try:
                if extra_data:
                    stats_container = ui.Container()

                    # Use attachment if available, otherwise use user avatar
                    if has_attachments:
                        stats_section = ui.Section(
                            accessory=ui.Thumbnail(media="attachment://strong_512.png")
                        )
                        logger.debug("📎 Using attachment for stats section thumbnail")
                    else:
                        stats_section = ui.Section(
                            accessory=ui.Thumbnail(media=str(user.display_avatar.url))
                        )
                        logger.debug("👤 Using user avatar for stats section thumbnail")

                    stats_section.add_item(ui.TextDisplay("## 📊 Stats"))

                    stats_lines = []

                    if "total_xp" in extra_data:
                        stats_lines.append(f"✨ **Total XP:** {extra_data['total_xp']:,}")

                    if "xp_to_next" in extra_data:
                        stats_lines.append(f"🎯 **XP to Next Level:** {extra_data['xp_to_next']:,}")

                    if "embers" in extra_data:
                        stats_lines.append(f"🔥 **Embers:** {extra_data['embers']:,}")

                    if "streak" in extra_data and extra_data["streak"] > 0:
                        stats_lines.append(f"🔥 **Daily Streak:** {extra_data['streak']} days")

                    # Add role update information if available
                    if "role_update" in extra_data:
                        role_info = extra_data["role_update"]
                        if role_info["action"] == "added" and role_info["new_role"]:
                            stats_lines.append(f"🎭 **New Role:** {role_info['new_role']}")
                        elif role_info["action"] == "updated" and role_info["new_role"]:
                            stats_lines.append(f"🎭 **Role Updated:** {role_info['new_role']}")

                    if stats_lines:
                        stats_section.add_item(ui.TextDisplay("\n".join(stats_lines)))

                    stats_container.add_item(stats_section)
                    layout.add_item(stats_container)
                    layout.add_item(ui.Separator())
            except Exception as e:
                logger.error(f"❌ Failed to create stats section: {e}")

            # Motivational footer
            try:
                motivational_quotes = [
                    "Keep up the amazing work! 🌟",
                    "Your dedication is paying off! 💪",
                    "You're on fire! Keep going! 🔥",
                    "Onwards and upwards! 🚀",
                    "Great progress! Keep it up! ⭐"
                ]

                import random
                footer_text = random.choice(motivational_quotes)
                layout.add_item(ui.TextDisplay(f"*{footer_text}*"))
            except Exception as e:
                logger.error(f"❌ Failed to add motivational footer: {e}")

            # Set overall accent color
            try:
                layout.accent_color = discord.Color.from_rgb(255, 215, 0)  # Gold
            except Exception as e:
                logger.error(f"❌ Failed to set accent color: {e}")

            return layout

        except Exception as e:
            logger.error(f"❌ Failed to create standard level-up message: {e}")
            # Return a basic layout as fallback
            fallback_layout = ui.LayoutView()
            try:
                fallback_section = ui.Section(
                    accessory=ui.Thumbnail(media=str(user.display_avatar.url)))
                fallback_section.add_item(ui.TextDisplay(f"🎊 {user.mention} leveled up to Level {new_level}!"))
                fallback_container = ui.Container()
                fallback_container.add_item(fallback_section)
                fallback_layout.add_item(fallback_container)
            except Exception:
                pass
            return fallback_layout

    def _create_first_level_message(
            self,
            user: discord.User,
            new_level: int,
            extra_data: Optional[Dict[str, Any]] = None,
            has_attachments: bool = False
    ) -> ui.LayoutView:
        """
        Create a special message for reaching level 2 (first level up).

        Args:
            user: Discord user object
            new_level: New level (should be 2)
            extra_data: Additional data
            has_attachments: Whether file attachments are available for attachment:// protocol

        Returns:
            LayoutView with the first level-up message
        """
        try:
            layout = ui.LayoutView()

            # Hero section with celebration theme
            try:
                hero_section = ui.Section(
                    accessory=ui.Thumbnail(media=str(user.display_avatar.url))
                )

                hero_section.add_item(
                    ui.TextDisplay(f"# 🎉 First Level Up!")
                )

                hero_section.add_item(
                    ui.TextDisplay(
                        f"**{user.mention}** just reached **Level {new_level}**! "
                        f"🌟\n\n"
                        f"Welcome to the leveling system! This is just the beginning of an amazing journey. "
                        f"Keep chatting, participating, and engaging to level up even more!"
                    )
                )

                hero_container = ui.Container()
                hero_container.accent_color = discord.Color.green().value
                hero_container.add_item(hero_section)

                layout.add_item(hero_container)
                layout.add_item(ui.Separator())
            except Exception as e:
                logger.error(f"❌ Failed to create hero section for first level: {e}")

            # Tips section
            try:
                tips_container = ui.Container()

                # Use attachment if available, otherwise use user avatar
                if has_attachments:
                    tips_section = ui.Section(
                        accessory=ui.Thumbnail(media="attachment://strong_256.png")
                    )
                    logger.debug("📎 Using attachment for tips section thumbnail")
                else:
                    tips_section = ui.Section(
                        accessory=ui.Thumbnail(media=str(user.display_avatar.url))
                    )
                    logger.debug("👤 Using user avatar for tips section thumbnail")

                tips_section.add_item(ui.TextDisplay("## 💡 Level Up Tips"))

                tips_section.add_item(
                    ui.TextDisplay(
                        "**1.** Stay active daily to maintain your streak 🔥\n"
                        "**2.** Quality messages earn more XP ✨\n"
                        "**3.** Engage with others through reactions 👍\n"
                        "**4.** Join voice channels to earn voice XP 🎙️"
                    )
                )

                tips_container.add_item(tips_section)
                layout.add_item(tips_container)
                layout.add_item(ui.Separator())
            except Exception as e:
                logger.error(f"❌ Failed to create tips section: {e}")

            try:
                layout.add_item(ui.TextDisplay("*Keep up the great work! We're excited to see you grow! 🚀*"))
                layout.accent_color = discord.Color.from_rgb(87, 242, 135)  # Green
            except Exception as e:
                logger.error(f"❌ Failed to set first level message styling: {e}")

            return layout

        except Exception as e:
            logger.error(f"❌ Failed to create first level message: {e}")
            fallback_layout = ui.LayoutView()
            try:
                fallback_section = ui.Section(
                    accessory=ui.Thumbnail(media=str(user.display_avatar.url)))
                fallback_section.add_item(
                    ui.TextDisplay(f"🎉 {user.mention} reached their first level! Welcome to Level {new_level}!"))
                fallback_container = ui.Container()
                fallback_container.add_item(fallback_section)
                fallback_layout.add_item(fallback_container)
            except Exception:
                pass
            return fallback_layout

    def _create_milestone_message(
            self,
            user: discord.User,
            old_level: int,
            new_level: int,
            extra_data: Optional[Dict[str, Any]] = None,
            has_attachments: bool = False
    ) -> ui.LayoutView:
        """
        Create a special milestone message (e.g., level 10, 25, 50, 100).

        Args:
            user: Discord user object
            old_level: Previous level
            new_level: New milestone level
            extra_data: Additional data
            has_attachments: Whether file attachments are available for attachment:// protocol

        Returns:
            LayoutView with the milestone message
        """
        try:
            layout = ui.LayoutView()

            # Determine milestone emoji
            try:
                milestone_emoji = "🏆"
                if new_level >= 100:
                    milestone_emoji = "👑"
                elif new_level >= 50:
                    milestone_emoji = "💎"
                elif new_level >= 25:
                    milestone_emoji = "⭐"
            except Exception as e:
                logger.error(f"❌ Failed to determine milestone emoji: {e}")
                milestone_emoji = "🏆"

            # Hero section with special styling
            try:
                hero_section = ui.Section(
                    accessory=ui.Thumbnail(media=str(user.display_avatar.url))
                )

                hero_section.add_item(
                    ui.TextDisplay(f"# {milestone_emoji} MILESTONE ACHIEVED! {milestone_emoji}")
                )

                hero_section.add_item(
                    ui.TextDisplay(
                        f"**{user.mention}** has reached the incredible milestone of "
                        f"**Level {new_level}**! 🎊\n\n"
                        f"This is a significant achievement that shows true dedication and engagement. "
                        f"Congratulations on this amazing accomplishment!"
                    )
                )

                hero_container = ui.Container()
                hero_container.accent_color = discord.Color.purple().value
                hero_container.add_item(hero_section)

                layout.add_item(hero_container)
                layout.add_item(ui.Separator())
            except Exception as e:
                logger.error(f"❌ Failed to create hero section for milestone: {e}")

            # Achievement stats
            try:
                if extra_data:
                    stats_container = ui.Container()

                    # Use attachment if available, otherwise use user avatar
                    if has_attachments:
                        stats_section = ui.Section(
                            accessory=ui.Thumbnail(media="attachment://strong_512.png")
                        )
                        logger.debug("📎 Using attachment for milestone stats section thumbnail")
                    else:
                        stats_section = ui.Section(
                            accessory=ui.Thumbnail(media=str(user.display_avatar.url))
                        )
                        logger.debug("👤 Using user avatar for milestone stats section thumbnail")

                    stats_section.add_item(ui.TextDisplay(f"## 📊 Journey Stats"))

                    stats_lines = [f"🎯 **Level:** {old_level} → **{new_level}**"]

                    if "total_xp" in extra_data:
                        stats_lines.append(f"✨ **Total XP Earned:** {extra_data['total_xp']:,}")

                    if "total_messages" in extra_data:
                        stats_lines.append(f"💬 **Messages Sent:** {extra_data['total_messages']:,}")

                    if "longest_streak" in extra_data and extra_data["longest_streak"] > 0:
                        stats_lines.append(f"🔥 **Longest Streak:** {extra_data['longest_streak']} days")

                    stats_section.add_item(ui.TextDisplay("\n".join(stats_lines)))
                    stats_container.add_item(stats_section)

                    layout.add_item(stats_container)
                    layout.add_item(ui.Separator())
            except Exception as e:
                logger.error(f"❌ Failed to create stats section for milestone: {e}")

            # Celebration footer
            try:
                layout.add_item(
                    ui.TextDisplay(
                        f"*{milestone_emoji} An extraordinary milestone! The community celebrates with you! {milestone_emoji}*"
                    )
                )

                layout.accent_color = discord.Color.from_rgb(138, 43, 226)  # Purple
            except Exception as e:
                logger.error(f"❌ Failed to set milestone message styling: {e}")

            return layout

        except Exception as e:
            logger.error(f"❌ Failed to create milestone message: {e}")
            fallback_layout = ui.LayoutView()
            try:
                fallback_section = ui.Section(
                    accessory=ui.Thumbnail(media=str(user.display_avatar.url)))
                fallback_section.add_item(
                    ui.TextDisplay(f"🏆 {user.mention} reached milestone Level {new_level}! Congratulations!"))
                fallback_container = ui.Container()
                fallback_container.add_item(fallback_section)
                fallback_layout.add_item(fallback_container)
            except Exception:
                pass
            return fallback_layout

    def _create_prestige_message(
            self,
            user: discord.User,
            new_level: int,
            extra_data: Optional[Dict[str, Any]] = None,
            has_attachments: bool = False
    ) -> ui.LayoutView:
        """
        Create a special prestige level-up message.

        Args:
            user: Discord user object
            new_level: New level
            extra_data: Additional data (should include prestige_level)
            has_attachments: Whether file attachments are available for attachment:// protocol

        Returns:
            LayoutView with the prestige message
        """
        try:
            layout = ui.LayoutView()

            try:
                prestige_level = extra_data.get("prestige_level", 1) if extra_data else 1
            except Exception as e:
                logger.error(f"❌ Failed to get prestige level: {e}")
                prestige_level = 1

            # Hero section with prestige theme
            try:
                hero_section = ui.Section(
                    accessory=ui.Thumbnail(media=str(user.display_avatar.url))
                )

                hero_section.add_item(
                    ui.TextDisplay(f"# ⚡ PRESTIGE LEVEL UP! ⚡")
                )

                hero_section.add_item(
                    ui.TextDisplay(
                        f"**{user.mention}** has ascended to **Prestige {prestige_level}**! 👑\n\n"
                        f"After mastering the regular levels, they've reset and begun the journey anew "
                        f"with enhanced rewards and exclusive benefits. This is true dedication!"
                    )
                )

                hero_container = ui.Container()
                hero_container.accent_color = discord.Color.from_rgb(255, 0, 255).value  # Magenta
                hero_container.add_item(hero_section)

                layout.add_item(hero_container)
                layout.add_item(ui.Separator())
            except Exception as e:
                logger.error(f"❌ Failed to create hero section for prestige: {e}")

            # Prestige benefits
            try:
                benefits_container = ui.Container()

                # Use attachment if available, otherwise use user avatar
                if has_attachments:
                    benefits_section = ui.Section(
                        accessory=ui.Thumbnail(media="attachment://strong_512.png")
                    )
                    logger.debug("📎 Using attachment for prestige benefits section thumbnail")
                else:
                    benefits_section = ui.Section(
                        accessory=ui.Thumbnail(media=str(user.display_avatar.url))
                    )
                    logger.debug("👤 Using user avatar for prestige benefits section thumbnail")

                benefits_section.add_item(ui.TextDisplay("## 🎁 Prestige Benefits"))

                benefits_section.add_item(
                    ui.TextDisplay(
                        f"⚡ **Prestige Level:** {prestige_level}\n"
                        f"✨ **XP Multiplier:** +{prestige_level * 10}%\n"
                        f"🔥 **Ember Bonus:** +{prestige_level * 5}%\n"
                        f"👑 **Exclusive Role:** Prestige {prestige_level}\n"
                        f"🌟 **Special Badge:** Displayed on profile"
                    )
                )

                benefits_container.add_item(benefits_section)
                layout.add_item(benefits_container)
                layout.add_item(ui.Separator())
            except Exception as e:
                logger.error(f"❌ Failed to create benefits section: {e}")

            try:
                layout.add_item(
                    ui.TextDisplay("*⚡ Elite status achieved! Continue to dominate! ⚡*")
                )

                layout.accent_color = discord.Color.from_rgb(255, 0, 255)  # Magenta
            except Exception as e:
                logger.error(f"❌ Failed to set prestige message styling: {e}")

            return layout

        except Exception as e:
            logger.error(f"❌ Failed to create prestige message: {e}")
            fallback_layout = ui.LayoutView()
            try:
                fallback_section = ui.Section(
                    accessory=ui.Thumbnail(media=str(user.display_avatar.url)))
                fallback_section.add_item(
                    ui.TextDisplay(f"⚡ {user.mention} reached Prestige Level {prestige_level}! Amazing!"))
                fallback_container = ui.Container()
                fallback_container.add_item(fallback_section)
                fallback_layout.add_item(fallback_container)
            except Exception:
                pass
            return fallback_layout

    @staticmethod
    def is_milestone_level(level: int) -> bool:
        """
        Check if a level is a milestone worth celebrating specially.

        Args:
            level: Level to check

        Returns:
            True if milestone, False otherwise
        """
        try:
            milestones = [5, 10, 25, 50, 75, 100, 150, 200, 250, 300, 500, 1000]
            return level in milestones
        except Exception as e:
            logger.error(f"❌ Failed to check milestone level {level}: {e}")
            return False

    @staticmethod
    def determine_reason(old_level: int, new_level: int, prestige_level: int = 0) -> str:
        """
        Determine the reason/type of level up for appropriate messaging.

        Args:
            old_level: Previous level
            new_level: New level
            prestige_level: Current prestige level

        Returns:
            Reason string ("first_level", "milestone", "prestige", "standard")
        """
        try:
            if old_level == 1 and new_level == 2:
                return "first_level"
            elif prestige_level > 0 and old_level < new_level:
                return "prestige"
            elif LevelUpMessages.is_milestone_level(new_level):
                return "milestone"
            else:
                return "standard"
        except Exception as e:
            logger.error(f"❌ Failed to determine level-up reason: {e}")
            return "standard"