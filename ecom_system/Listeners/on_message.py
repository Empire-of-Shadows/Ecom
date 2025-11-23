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
    Analyze message content for comprehensive metrics.

    Args:
        message: Message to analyze

    Returns:
        Dict with detailed analysis results
    """
    content = message.content or ""

    analysis = {
        'length': len(content),
        'word_count': len(content.split()) if content.strip() else 0,
        'character_count': len(content),
        'has_attachments': len(message.attachments) > 0,
        'attachment_count': len(message.attachments),
        'has_embeds': len(message.embeds) > 0,
        'embed_count': len(message.embeds),
        'mention_count': len(message.mentions),
        'role_mention_count': len(message.role_mentions),
        'channel_mention_count': len(message.channel_mentions),
        'is_reply': message.reference is not None,
        'is_thread': isinstance(message.channel, discord.Thread),
        'emoji_count': ContentAnalyzer.count_emojis(content),
        'link_count': ContentAnalyzer.count_links(content),
        'has_links': False,
        'has_code_blocks': '```' in content,
        'has_inline_code': '`' in content and '```' not in content,
        'is_command': content.startswith(('!', '/', '$', '?', '.')) if content else False,
        'has_mentions': len(message.mentions) > 0 or len(message.role_mentions) > 0,
        'is_caps': content.isupper() if content else False,
        'question_marks': content.count('?'),
        'exclamation_marks': content.count('!'),
    }

    analysis['has_links'] = analysis['link_count'] > 0
    analysis['total_mentions'] = (analysis['mention_count'] +
                                  analysis['role_mention_count'] +
                                  analysis['channel_mention_count'])

    # Calculate message quality score
    quality_score = 0
    if analysis['word_count'] >= 3:
        quality_score += 1
    if analysis['word_count'] >= 10:
        quality_score += 1
    if analysis['has_attachments']:
        quality_score += 1
    if analysis['emoji_count'] > 0 and analysis['emoji_count'] <= 3:
        quality_score += 1
    if analysis['is_reply']:
        quality_score += 1
    if not analysis['is_caps'] and analysis['word_count'] > 5:
        quality_score += 1

    analysis['quality_score'] = min(quality_score, 5)  # Max score of 5

    return analysis


class MessageListener(commands.Cog):
    """
    Enhanced message listener for leveling system and activity system integration.
    Handles message events and processes them through both systems with comprehensive tracking.
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
        """Initialize the message listener with a bot and systems."""
        self.bot = bot
        self.leveling_system = None  # Will be set from bot instance
        self.activity_system = None  # Will be set from bot instance
        self.achievement_system = None  # Will be set from bot instance
        self.activity_buffer = None  # Will be set from bot instance
        self.logger = get_logger("MessageListener")

    async def cog_load(self):
        """Initialize systems when cog loads."""
        self.logger.info("🔄 Initializing Enhanced MessageListener...")
        try:
            # Initialize leveling system
            if hasattr(self.bot, 'leveling_system') and self.bot.leveling_system:
                self.leveling_system = self.bot.leveling_system
                self.logger.info("✅ MessageListener using shared leveling system from bot")
            else:
                self.logger.error("❌ No leveling system found on bot instance")
                raise ValueError("Leveling system not available on bot instance")

            # Initialize activity system
            if hasattr(self.bot, 'activity_system') and self.bot.activity_system:
                self.activity_system = self.bot.activity_system
                self.logger.info("✅ MessageListener using shared activity system from bot")
            else:
                self.logger.warning("⚠️ Activity system not found on bot instance. Activity will not be tracked.")

            # Initialize achievement system
            if hasattr(self.bot, 'leveling_system') and hasattr(self.bot.leveling_system, 'achievement_system'):
                self.achievement_system = self.bot.leveling_system.achievement_system
                self.logger.info("✅ MessageListener using shared achievement system from bot")
            else:
                self.logger.warning("⚠️ Achievement system not found on bot instance.")

            self.logger.info("✅ Enhanced MessageListener fully initialized")

        except Exception as e:
            self.logger.error(f"❌ Failed to initialize MessageListener: {e}")
            raise

    @commands.Cog.listener()
    @log_performance("on_message")
    async def on_message(self, message: discord.Message):
        """
        Handle message creation events for leveling, activity, and achievement systems.

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

            # Perform comprehensive message analysis
            analysis = _analyze_message(message)

            # Process through leveling system first (for XP and level-ups)
            if self.leveling_system:
                await self.leveling_system.message_system.process_message(
                    user_id=user_id,
                    guild_id=guild_id,
                    message_content=message_content,
                    channel_id=channel_id,
                    is_thread=analysis['is_thread']
                )

            # Record comprehensive user activity in enhanced activity system
            if self.activity_system:
                # Prepare detailed activity data for the enhanced system
                activity_data = {
                    "channel_id": channel_id,
                    "channel_name": message.channel.name if hasattr(message.channel, 'name') else 'Unknown',
                    "message_length": message_length,
                    "word_count": analysis['word_count'],
                    "character_count": analysis['character_count'],
                    "has_attachments": analysis['has_attachments'],
                    "attachment_count": analysis['attachment_count'],
                    "has_embeds": analysis['has_embeds'],
                    "embed_count": analysis['embed_count'],
                    "mention_count": analysis['mention_count'],
                    "total_mentions": analysis['total_mentions'],
                    "emoji_count": analysis['emoji_count'],
                    "link_count": analysis['link_count'],
                    "has_links": analysis['has_links'],
                    "is_reply": analysis['is_reply'],
                    "is_thread": analysis['is_thread'],
                    "is_command": analysis['is_command'],
                    "has_code_blocks": analysis['has_code_blocks'],
                    "has_inline_code": analysis['has_inline_code'],
                    "quality_score": analysis['quality_score'],
                    "is_caps": analysis['is_caps'],
                    "question_marks": analysis['question_marks'],
                    "exclamation_marks": analysis['exclamation_marks'],
                    # Additional metadata
                    "message_id": str(message.id),
                    "author_name": str(message.author.name),
                    "author_display_name": str(message.author.display_name),
                    "created_at": message.created_at.timestamp(),
                    "channel_type": str(type(message.channel).__name__)
                }

                await self.activity_system.record_activity(
                    user_id=user_id,
                    guild_id=guild_id,
                    activity_type='message',
                    activity_data=activity_data
                )

            # Process achievements if available
            if self.achievement_system:
                try:
                    # Prepare activity data for achievement system
                    achievement_activity_data = {
                        "activity_type": "message",
                        "message_length": message_length,
                        "word_count": analysis['word_count'],
                        "has_attachments": analysis['has_attachments'],
                        "has_embeds": analysis['has_embeds'],
                        "emoji_count": analysis['emoji_count'],
                        "link_count": analysis['link_count'],
                        "is_thread": analysis['is_thread'],
                        "quality_score": analysis['quality_score'],
                        "channel_id": channel_id,
                        "timestamp": message.created_at.timestamp()
                    }

                    # Process achievements
                    await self.achievement_system.check_and_update_achievements(
                        user_id=user_id,
                        guild_id=guild_id,
                        activity_data=achievement_activity_data
                    )

                except Exception as e:
                    self.logger.warning(f"Failed to process achievements for message: {e}")

            # Log successful processing
            self.logger.debug(
                f"✅ Message processed: {guild_id}:{user_id} "
                f"(len: {message_length}, words: {analysis['word_count']}, "
                f"emojis: {analysis['emoji_count']}, links: {analysis['link_count']}, "
                f"quality: {analysis['quality_score']}/5)"
            )

            # Additional detailed logging for high-quality or special messages
            if (analysis['quality_score'] >= 4 or
                    analysis['has_attachments'] or
                    analysis['embed_count'] > 0 or
                    analysis['word_count'] > 50):
                self.logger.info(
                    f"🌟 High-quality message from {message.author.name} in {guild_id}: "
                    f"Quality {analysis['quality_score']}/5, "
                    f"{analysis['word_count']} words, "
                    f"{analysis['attachment_count']} attachments, "
                    f"{analysis['embed_count']} embeds"
                )

        except Exception as e:
            self.logger.error(f"❌ Error processing message from {message.author}: {e}", exc_info=True)


async def setup(bot):
    """Setup function for discord.py cog loading."""
    await bot.add_cog(MessageListener(bot))