import re
from typing import Dict, Any

import discord
from discord.ext import commands
from ecom_system.helpers.content_analyzer import ContentAnalyzer
from ecom_system.helpers.rate_limiter import rate_limiter

from loggers.logger_setup import log_performance, get_logger
from dotenv import load_dotenv

load_dotenv()


def _analyze_message(message: discord.Message) -> Dict[str, Any]:
    """
    Analyze message content for basic metrics.

    Args:
        message: Message to analyze

    Returns:
        Dict with analysis results
    """
    content = message.content or ""

    analysis = {
        'length': len(content),
        'word_count': len(content.split()),
        'has_attachments': len(message.attachments) > 0,
        'has_embeds': len(message.embeds) > 0,
        'mention_count': len(message.mentions),
        'is_reply': message.reference is not None,
        'is_thread': isinstance(message.channel, discord.Thread),
        'emoji_count': ContentAnalyzer.count_emojis(content),
        'link_count': ContentAnalyzer.count_links(content),
        'has_links': False,
    }

    analysis['has_links'] = analysis['link_count'] > 0

    return analysis


class MessageListener(commands.Cog):
    """
    Simple message listener for leveling system integration.
    Handles message events and processes them through the leveling system.
    """

    # Regex patterns for content analysis
    _URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
    _EMOJI_RE = re.compile(
        r"("
        r":[a-zA-Z0-9_~\-]+:"  # :custom_emoji:
        r"|<a?:\w+:\d+>"  # <a:name:id> or <:name:id> (custom)
        r"|[\U0001F300-\U0001F9FF]"  # Unicode emojis
        r"|[\u2600-\u27BF]"  # Misc symbols
        r")"
    )

    def __init__(self, bot):
        """Initialize the message listener with a bot and leveling system."""
        self.bot = bot
        self.leveling_system = None  # Will be set from bot instance
        self.logger = get_logger("MessageListener")

    async def cog_load(self):
        """Initialize a leveling system when cog loads."""
        self.logger.info("🔄 Initializing MessageListener...")
        try:
            # Use the leveling system from the bot instance instead of creating a new one
            if hasattr(self.bot, 'leveling_system') and self.bot.leveling_system:
                self.leveling_system = self.bot.leveling_system
                self.logger.info("✅ MessageListener using shared leveling system from bot")
            else:
                self.logger.error("❌ No leveling system found on bot instance")
                raise ValueError("Leveling system not available on bot instance")

        except Exception as e:
            self.logger.error(f"❌ Failed to initialize MessageListener: {e}")
            raise

    @commands.Cog.listener()
    @log_performance("on_message")
    async def on_message(self, message: discord.Message):
        """
        Handle message creation events for a leveling system.

        Args:
            message: The created message object
        """
        # Initial validation
        if message.author.bot:
            return  # Ignore bot messages

        if not message.guild:
            return  # Ignore DMs

        # Rate limiting check
        if not await rate_limiter.check_rate_limit(message):
            return  # Skip processing if rate limited

        try:
            # Extract message data
            guild_id = str(message.guild.id)
            user_id = str(message.author.id)
            channel_id = str(message.channel.id)
            message_content = message.content or ""
            message_length = len(message_content)

            # Analyze message content
            analysis = _analyze_message(message)

            # Process through a leveling system
            await self.leveling_system.message_system.process_message(
                user_id=user_id,
                guild_id=guild_id,
                message_content=message_content,
                channel_id=channel_id
            )

            self.logger.debug(
                f"✅ Message processed: {guild_id}:{user_id} "
                f"(len: {message_length}, emojis: {analysis['emoji_count']}, links: {analysis['link_count']})"
            )

        except Exception as e:
            self.logger.error(f"❌ Error processing message: {e}")


async def setup(bot):
    """Setup function for discord.py cog loading."""
    await bot.add_cog(MessageListener(bot))