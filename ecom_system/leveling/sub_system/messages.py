import asyncio
import logging
import time
import re
from typing import Optional, Dict, Any

from ecom_system.helpers.content_analyzer import ContentAnalyzer
from ecom_system.helpers.helpers import utc_now_ts, utc_today_key, utc_week_key, utc_month_key
from ecom_system.helpers.leveled_up import LevelUpMessages
from ecom_system.helpers.rate_limiter import rate_limiter
from ecom_system.helpers.daily_streak import (
    check_and_update_streak,
    get_streak_bonus,
    create_streak_update_data,
    log_streak_change
)
from loggers.log_factory import log_performance, PerformanceLogger

logger = logging.getLogger(__name__)


# =============================================================================
# TODO: UNIMPLEMENTED FEATURES FROM SETTINGS
# =============================================================================
#
# 2. QUALITY ANALYSIS SETTINGS (settings.message.quality_analysis.*)
#    Currently using hardcoded values, should use:
#    - emoji_bonus: 1.05 (currently hardcoded 0.05 per emoji)
#    - length_quality_threshold: 50
#    - length_quality_bonus: 1.1
#    - attachment_bonus: 1.08 (NOT IMPLEMENTED - need attachment detection)
#    - mention_penalty: 0.9 (NOT IMPLEMENTED - need mention count)
#    - caps_penalty: 0.8 (partially implemented in anti-cheat only)
#    - constructive_bonus: 1.15 (NOT IMPLEMENTED - need ML/sentiment analysis)
#    - link_bonus: 1.03 (currently hardcoded 0.08 per link)
#    - code_block_bonus: 1.07 (NOT IMPLEMENTED - need code block detection)
#    Location: analyze_message_content()
#
# 3. CHANNEL CONFIGURATIONS (settings.message.*)
#    - channel_bonuses: {} - Per-channel XP/Ember multipliers
#    - thread_bonus: 1.1 - Bonus for thread messages
#    - disabled_channels: [] - Channels to ignore
#    - premium_channels: {} - Special channel multipliers
#    - channel_caps: {} - Per-channel daily limits
#    Location: process_message() or calculate_message_rewards()
#    Implementation: Check channel_id against these settings
#
# =============================================================================


class MessageLevelingSystem:
    """
    Simplified message processing subsystem for the leveling system.
    Handles message rewards with basic quality analysis and anti-cheat.
    """

    # URL detection regex
    _URL_REGEX = re.compile(
        r"https?://[-\w.]+(?:[:\d]+)?(?:/[\w/_.]*(?:\?[\w&=%.]*)?(?:#[\w.]*)?)?",
        re.IGNORECASE
    )

    # Emoji detection regex
    _EMOJI_REGEX = re.compile(
        r"("
        r":[a-zA-Z0-9_~\-]+:"  # :custom_emoji:
        r"|<a?:\w+:\d+>"  # <a:name:id> or <:name:id> (custom)
        r"|[\U0001F300-\U0001F9FF]"  # Unicode emojis
        r"|[\u2600-\u27BF]"  # Misc symbols
        r")"
    )

    def __init__(self, leveling_system):
        """Initialize with reference to parent LevelingSystem"""
        self.leveling_system = leveling_system
        self.logger = logger

        # Initialize level-up message handler (will be set by bot)
        self.level_up_messages = None

    @log_performance("process_message")
    async def process_message(
            self,
            user_id: str,
            guild_id: str,
            message_content: str,
            channel_id: str = None,
            is_thread: bool = False
    ) -> Optional[dict]:
        """
        Process a message and calculate rewards with comprehensive logging.
        """
        start_time = time.time()

        # Initialize tracking variables for final summary
        processing_steps = {
            "anti_cheat": {"passed": False, "duration_ms": 0},
            "validation": {"passed": False, "duration_ms": 0, "reason": ""},
            "content_analysis": {"completed": False, "duration_ms": 0},
            "reward_calculation": {"completed": False, "duration_ms": 0},
            "database_update": {"completed": False, "duration_ms": 0}
        }

        with PerformanceLogger(logger, "process_message"):
            logger.info(f"🔄 Processing message: G:{guild_id} U:{user_id}")
            logger.debug(f"  📝 Message length: {len(message_content)} characters")
            if channel_id:
                logger.debug(f"  📍 Channel ID: {channel_id}")
            if is_thread:
                logger.debug("  🧵 Message is in a thread")

            # =================================================================
            # STEP 1: Anti-cheat validation
            # =================================================================
            step_start = time.time()
            logger.debug("Step 1: Running anti-cheat checks...")

            if not await self.check_anti_cheat(guild_id, user_id, message_content):
                processing_steps["anti_cheat"]["duration_ms"] = (time.time() - step_start) * 1000
                logger.warning(f"🚫 Message blocked by anti-cheat: G:{guild_id} U:{user_id}")
                self._log_final_summary(guild_id, user_id, processing_steps, start_time, success=False,
                                        is_thread=is_thread, reason="Anti-cheat violation")
                return None

            processing_steps["anti_cheat"]["passed"] = True
            processing_steps["anti_cheat"]["duration_ms"] = (time.time() - step_start) * 1000
            logger.debug(f"  ✅ Anti-cheat passed ({processing_steps['anti_cheat']['duration_ms']:.2f}ms)")

            # =================================================================
            # STEP 2: Load settings and user data
            # =================================================================
            step_start = time.time()
            logger.debug("Step 2: Loading guild settings and user data...")

            settings = await self.leveling_system.get_guild_settings(guild_id)
            user_data = await self.leveling_system.get_user_data(user_id, guild_id)

            if not user_data:
                logger.info(f"  🆕 Creating new user profile for U:{user_id}")
                user_data = await self.leveling_system.create_enhanced_user_profile(user_id, guild_id)
            else:
                current_level = user_data.get("level", 1)
                current_xp = user_data.get("xp", 0)
                logger.debug(f"  📊 Existing user: Level {current_level}, XP {current_xp}")

            data_load_time = (time.time() - step_start) * 1000
            logger.debug(f"  ✅ Data loaded ({data_load_time:.2f}ms)")

            # =================================================================
            # STEP 3: Message validation
            # =================================================================
            step_start = time.time()
            logger.debug("Step 3: Validating message...")

            message_length = len(message_content)
            validation_result = await self.validate_message(message_length, settings, user_data)

            processing_steps["validation"]["duration_ms"] = (time.time() - step_start) * 1000

            if not validation_result["valid"]:
                processing_steps["validation"]["reason"] = validation_result["reason"]
                logger.info(f"  ❌ Validation failed: {validation_result['reason']}")
                self._log_final_summary(guild_id, user_id, processing_steps, start_time, success=False,
                                        is_thread=is_thread, settings=settings, reason=validation_result["reason"])
                return validation_result.get("result")

            processing_steps["validation"]["passed"] = True
            logger.debug(f"  ✅ Validation passed ({processing_steps['validation']['duration_ms']:.2f}ms)")

            # =================================================================
            # STEP 4: Content analysis
            # =================================================================
            step_start = time.time()
            logger.debug("Step 4: Analyzing message content...")

            content_analysis = self.analyze_message_content(message_content, settings)

            processing_steps["content_analysis"]["completed"] = True
            processing_steps["content_analysis"]["duration_ms"] = (time.time() - step_start) * 1000

            logger.info(f"  📊 Content Analysis:")
            logger.info(f"    • Quality score: {content_analysis['score']:.2f}")
            logger.info(f"    • Word count: {content_analysis['word_count']}")
            logger.info(f"    • Emojis: {content_analysis['emoji_count']}")
            logger.info(f"    • Links: {content_analysis['link_count']}")
            if content_analysis['factors']:
                logger.info(f"    • Bonuses applied: {', '.join(content_analysis['factors'])}")
            logger.debug(f"  ✅ Analysis complete ({processing_steps['content_analysis']['duration_ms']:.2f}ms)")

            # =================================================================
            # STEP 5: Reward calculation
            # =================================================================
            step_start = time.time()
            logger.debug("Step 5: Calculating rewards...")

            rewards = await self.calculate_message_rewards(
                message_length, user_data, settings, content_analysis, channel_id, is_thread
            )

            processing_steps["reward_calculation"]["completed"] = True
            processing_steps["reward_calculation"]["duration_ms"] = (time.time() - step_start) * 1000

            logger.info(f"  💰 Calculated Rewards:")
            logger.info(f"    • XP: +{rewards.get('xp', 0)}")
            logger.info(f"    • Embers: +{rewards.get('embers', 0)}")

            multipliers = rewards.get('multipliers', {})
            if multipliers:
                logger.debug(f"    • Multipliers:")
                for mult_name, mult_value in multipliers.items():
                    logger.debug(f"      - {mult_name}: {mult_value:.2f}x")

            logger.debug(f"  ✅ Rewards calculated ({processing_steps['reward_calculation']['duration_ms']:.2f}ms)")

            # =================================================================
            # STEP 6: Process rewards and update database
            # =================================================================
            step_start = time.time()
            logger.debug("Step 6: Processing rewards and updating database...")

            result = await self.process_rewards(
                user_id, guild_id, rewards, user_data, settings, channel_id
            )

            processing_steps["database_update"]["completed"] = True
            processing_steps["database_update"]["duration_ms"] = (time.time() - step_start) * 1000

            if not result:
                logger.error("  ❌ Failed to process rewards")
                self._log_final_summary(guild_id, user_id, processing_steps, start_time, success=False,
                                        is_thread=is_thread, settings=settings, reason="Database update failed")
                return None

            logger.debug(f"  ✅ Database updated ({processing_steps['database_update']['duration_ms']:.2f}ms)")

            # =================================================================
            # STEP 7: Finalize result
            # =================================================================
            processing_time = (time.time() - start_time) * 1000
            self.leveling_system.daily_stats["processed_messages"] += 1

            if result:
                result["processing_time_ms"] = processing_time
                result["quality_score"] = content_analysis.get("score", 1.0)

            # Log comprehensive final summary
            self._log_final_summary(
                guild_id, user_id, processing_steps, start_time,
                success=True, is_thread=is_thread, settings=settings, result=result, rewards=rewards,
                content_analysis=content_analysis
            )

            # =================================================================
            # STEP 8: Check for achievements
            # =================================================================
            if hasattr(self.leveling_system, 'achievement_system') and self.leveling_system.achievement_system:
                logger.debug("Step 8: Checking for achievements...")
                step_start = time.time()
                try:
                    activity_data = {
                        "type": "message",
                        "content": message_content,
                        "channel_id": channel_id,
                        "is_thread": is_thread,
                        "rewards": rewards,
                        "leveled_up": result.get("leveled_up", False),
                        "new_level": result.get("level_up", {}).get("new_level")
                    }
                    await self.leveling_system.achievement_system.check_and_update_achievements(
                        user_id, guild_id, activity_data
                    )
                    logger.debug(f"  ✅ Achievements checked ({(time.time() - step_start) * 1000:.2f}ms)")
                except Exception as e:
                    logger.error(f"  ❌ Achievement check failed: {e}", exc_info=True)

            return result

    def _log_final_summary(
            self,
            guild_id: str,
            user_id: str,
            processing_steps: Dict[str, Any],
            start_time: float,
            success: bool,
            is_thread: bool,
            settings: Optional[Dict[str, Any]] = None,
            reason: str = None,
            result: Dict[str, Any] = None,
            rewards: Dict[str, Any] = None,
            content_analysis: Dict[str, Any] = None
    ):
        """
        Log a comprehensive final summary of message processing.

        Args:
            guild_id: Guild ID
            user_id: User ID
            processing_steps: Dictionary tracking each processing step
            start_time: Processing start timestamp
            success: Whether processing succeeded
            is_thread: Whether the message was in a thread
            settings: Guild settings
            reason: Failure reason if applicable
            result: Final result dictionary if successful
            rewards: Calculated rewards if applicable
            content_analysis: Content analysis results if applicable
        """
        total_time = (time.time() - start_time) * 1000

        logger.info("=" * 70)
        logger.info(f"📋 MESSAGE PROCESSING SUMMARY - G:{guild_id} U:{user_id}")
        logger.info("=" * 70)

        # Overall status
        if success:
            logger.info(f"✅ Status: SUCCESS")
        else:
            logger.info(f"❌ Status: FAILED - {reason}")

        logger.info(f"⏱️  Total Time: {total_time:.2f}ms")
        logger.info("")

        # Step-by-step breakdown
        logger.info("📊 Processing Steps:")
        logger.info("-" * 70)

        for step_name, step_data in processing_steps.items():
            step_display = step_name.replace("_", " ").title()
            duration = step_data.get("duration_ms", 0)
            percentage = (duration / total_time * 100) if total_time > 0 else 0

            if step_name == "anti_cheat":
                status = "✅ PASSED" if step_data.get("passed") else "❌ FAILED"
            elif step_name == "validation":
                if step_data.get("passed"):
                    status = "✅ PASSED"
                else:
                    status = f"❌ FAILED: {step_data.get('reason', 'Unknown')}"
            else:
                status = "✅ COMPLETED" if step_data.get("completed") else "⏭️  SKIPPED"

            logger.info(f"  {step_display:20} {status:30} {duration:6.2f}ms ({percentage:5.1f}%)")

        logger.info("-" * 70)

        # Success details
        if success and result:
            logger.info("")
            logger.info("🎉 Results:")
            logger.info(f"  • XP Gained: +{result['rewards']['xp']}")
            logger.info(f"  • Embers Gained: +{result['rewards']['embers']}")
            logger.info(f"  • New XP Total: {result['totals']['xp']}")
            logger.info(f"  • New Embers Total: {result['totals']['embers']}")
            logger.info(f"  • Current Level: {result['totals']['level']}")

            if result.get('leveled_up'):
                level_up = result.get('level_up', {})
                logger.info(f"  🎊 LEVEL UP! {level_up.get('old_level', '?')} → {level_up.get('new_level', '?')}")

            if content_analysis:
                logger.info("")
                logger.info("📈 Content Quality:")
                logger.info(f"  • Score: {content_analysis['score']:.2f}")
                logger.info(f"  • Length: {content_analysis['length']} chars")
                logger.info(f"  • Words: {content_analysis['word_count']}")
                if content_analysis.get('factors'):
                    logger.info(f"  • Quality Factors: {', '.join(content_analysis['factors'])}")

            if rewards and settings:
                msg_cfg = settings.get("message", {})
                base_xp = msg_cfg.get("base_xp", 0)
                base_embers = msg_cfg.get("base_embers", 0)
                multipliers = result.get("multipliers", {})

                logger.info("")
                logger.info("⚙️ Reward Calculation:")
                logger.info(f"  • Base Rewards: {base_xp} XP, {base_embers} Embers")
                logger.info(f"  • Multipliers:")
                logger.info(f"    - Quality: {multipliers.get('quality', 1.0):.2f}x")
                logger.info(f"    - Streak: {multipliers.get('streak', 1.0):.2f}x")
                logger.info(f"    - Length: {multipliers.get('length', 1.0):.2f}x")
                logger.info(f"    - Channel/Thread: {multipliers.get('channel', 1.0):.2f}x")
                logger.info(f"    - Low Level: {multipliers.get('low_level', 1.0):.2f}x")

            logger.info("")
            logger.info("📝 Message Context:")
            logger.info(f"  • In Thread: {'Yes' if is_thread else 'No'}")

        logger.info("=" * 70)

    async def check_anti_cheat(self, guild_id: str, user_id: str, content: str) -> bool:
        """
        Basic anti-cheat with rate limiting and pattern detection.
        """
        try:
            logger.debug(f"  🔍 Checking anti-cheat for U:{user_id}")
            now = time.time()
            current_minute = int(now // 60)

            # Rate limiting check
            rate_limit_key = f"rate_limit:{guild_id}:{user_id}:message:{current_minute}"
            recent_count = await rate_limiter.get_rate_limit_count(rate_limit_key)

            logger.debug(f"    • Rate limit: {recent_count}/20 messages this minute")

            if recent_count >= 20:  # 20 messages per minute limit
                logger.warning(f"    ⚠️  Rate limit exceeded: {recent_count} messages")
                await self.log_anti_cheat_violation(
                    user_id, guild_id, "message", "Rate limit exceeded"
                )
                return False

            # Pattern detection for spam
            if not self.check_message_patterns(content):
                logger.warning(f"    ⚠️  Spam pattern detected")
                await self.log_anti_cheat_violation(
                    user_id, guild_id, "message", "Spam pattern detected"
                )
                return False

            # Update rate limit counter
            await rate_limiter.increment_rate_limit_count(rate_limit_key)
            logger.debug(f"    ✅ Anti-cheat checks passed")
            return True

        except Exception as e:
            logger.error(f"❌ Anti-cheat check error: {e}")
            return True  # Allow on error

    def check_message_patterns(self, content: str) -> bool:
        """
        Check for spam patterns in message content.
        """
        if not content:
            return True

        # Check for repetitive content
        words = content.lower().split()
        unique_words = set(words)

        unique_ratio = len(unique_words) / max(1, len(words))
        logger.debug(f"    • Unique word ratio: {unique_ratio:.2f}")

        # Require at least 30% unique words
        if len(unique_words) < max(1, len(words) * 0.3):
            logger.debug(f"    ⚠️  Low unique word ratio: {unique_ratio:.2f}")
            return False

        return True

    def analyze_message_content(self, content: str, settings: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze message content for quality scoring.
        """
        quality_cfg = settings.get("message", {}).get("quality_analysis", {})

        analysis = {
            "score": 1.0,
            "length": len(content),
            "word_count": len(content.split()),
            "emoji_count": ContentAnalyzer.count_emojis(content),
            "link_count": ContentAnalyzer.count_links(content),
            "code_block_count": len(re.findall(r'```[\s\S]*?```|`[^`]+`', content)),
            "caps_ratio": sum(1 for c in content if c.isupper()) / len(content) if content else 0,
            "has_links": False,
            "bonuses": {},
            "penalties": {},
            "factors": []
        }

        # Length bonus
        length_threshold = quality_cfg.get("length_quality_threshold", 50)
        length_bonus_multiplier = quality_cfg.get("length_quality_bonus", 1.1)
        if analysis["length"] >= length_threshold:
            analysis["score"] *= length_bonus_multiplier
            analysis["bonuses"]["length"] = length_bonus_multiplier
            analysis["factors"].append("substantial_content")

        # Emoji bonus
        if analysis["emoji_count"] > 0:
            emoji_bonus_multiplier = quality_cfg.get("emoji_bonus", 1.05)
            analysis["score"] *= emoji_bonus_multiplier
            analysis["bonuses"]["emojis"] = emoji_bonus_multiplier
            analysis["factors"].append("emoji_usage")

        # Link bonus
        if analysis["link_count"] > 0:
            analysis["has_links"] = True
            link_bonus_multiplier = quality_cfg.get("link_bonus", 1.03)
            analysis["score"] *= link_bonus_multiplier
            analysis["bonuses"]["links"] = link_bonus_multiplier
            analysis["factors"].append("link_sharing")

        # Code block bonus
        if analysis["code_block_count"] > 0:
            code_block_bonus_multiplier = quality_cfg.get("code_block_bonus", 1.07)
            analysis["score"] *= code_block_bonus_multiplier
            analysis["bonuses"]["code_block"] = code_block_bonus_multiplier
            analysis["factors"].append("code_sharing")

        # Caps penalty
        if analysis["caps_ratio"] > 0.7:
            caps_penalty_multiplier = quality_cfg.get("caps_penalty", 0.8)
            analysis["score"] *= caps_penalty_multiplier
            analysis["penalties"]["excessive_caps"] = caps_penalty_multiplier
            analysis["factors"].append("penalty_caps")

        # Clamp final score
        analysis["score"] = max(0.5, min(analysis["score"], 2.0))

        return analysis

    async def validate_message(self, message_length: int, settings: Dict, user_data: Dict, channel_id: str = None) -> \
    Dict[str, Any]:
        """
        Basic message validation.
        """
        msg_cfg = settings.get("message", {})
        min_length = msg_cfg.get("min_length", 3)
        cooldown_seconds = msg_cfg.get("cooldown_seconds", 8)

        logger.debug(f"  🔍 Validation checks:")
        logger.debug(f"    • Min length required: {min_length}")
        logger.debug(f"    • Cooldown: {cooldown_seconds}s")

        # Check if channel is disabled
        disabled_channels = msg_cfg.get("disabled_channels", [])
        if channel_id and channel_id in disabled_channels:
            logger.debug(f"    ❌ Channel {channel_id} is disabled for leveling")
            return {"valid": False, "reason": "Channel disabled for leveling"}

        # Length validation
        if message_length < min_length:
            logger.debug(f"    ❌ Message too short: {message_length} < {min_length}")
            return {
                "valid": False,
                "reason": f"Message too short ({message_length} < {min_length})"
            }

        # Cooldown validation
        last_message_time = user_data.get("message_stats", {}).get("last_message_time", 0)
        now = utc_now_ts()
        time_since_last = now - last_message_time

        logger.debug(f"    • Time since last message: {time_since_last:.1f}s")

        if time_since_last < cooldown_seconds:
            remaining = cooldown_seconds - time_since_last
            logger.debug(f"    ❌ Cooldown active: {remaining:.1f}s remaining")
            return {
                "valid": False,
                "reason": f"Cooldown active ({remaining:.1f}s remaining)"
            }

        # Check if any caps are already met
        today_key = utc_today_key()
        week_key = utc_week_key()
        month_key = utc_month_key()

        user_today_key = user_data.get("message_stats", {}).get("today_key")
        user_week_key = user_data.get("message_stats", {}).get("week_key")
        user_month_key = user_data.get("message_stats", {}).get("month_key")

        # Daily caps
        daily_caps = msg_cfg.get("daily_caps", {})
        if daily_caps:
            today_xp = user_data.get("message_stats", {}).get("today_xp", 0) if user_today_key == today_key else 0
            if today_xp >= daily_caps.get("xp", float('inf')):
                return {"valid": False, "reason": "Daily XP cap reached"}
            today_embers = user_data.get("message_stats", {}).get("today_embers", 0) if user_today_key == today_key else 0
            if today_embers >= daily_caps.get("embers", float('inf')):
                return {"valid": False, "reason": "Daily Embers cap reached"}

        # Weekly caps
        weekly_caps = msg_cfg.get("weekly_caps", {})
        if weekly_caps:
            weekly_xp = user_data.get("message_stats", {}).get("weekly_xp", 0) if user_week_key == week_key else 0
            if weekly_xp >= weekly_caps.get("xp", float('inf')):
                return {"valid": False, "reason": "Weekly XP cap reached"}
            weekly_embers = user_data.get("message_stats", {}).get("weekly_embers", 0) if user_week_key == week_key else 0
            if weekly_embers >= weekly_caps.get("embers", float('inf')):
                return {"valid": False, "reason": "Weekly Embers cap reached"}

        # Monthly caps
        monthly_caps = msg_cfg.get("monthly_caps", {})
        if monthly_caps:
            monthly_xp = user_data.get("message_stats", {}).get("monthly_xp", 0) if user_month_key == month_key else 0
            if monthly_xp >= monthly_caps.get("xp", float('inf')):
                return {"valid": False, "reason": "Monthly XP cap reached"}
            monthly_embers = user_data.get("message_stats", {}).get("monthly_embers", 0) if user_month_key == month_key else 0
            if monthly_embers >= monthly_caps.get("embers", float('inf')):
                return {"valid": False, "reason": "Monthly Embers cap reached"}

        logger.debug(f"    ✅ All validation checks passed")
        return {"valid": True, "reason": "validation_passed"}

    async def calculate_message_rewards(
            self,
            message_length: int,
            user_data: Dict,
            settings: Dict,
            content_analysis: Dict,
            channel_id: str = None,
            is_thread: bool = False
    ) -> Dict[str, float]:
        """
        Calculate message rewards based on content and user stats.
        """
        try:
            logger.debug(f"  💰 Calculating rewards:")

            msg_cfg = settings.get("message", {})
            base_xp = msg_cfg.get("base_xp", 10)
            base_embers = msg_cfg.get("base_embers", 6)
            max_length = msg_cfg.get("max_length", 1200)

            logger.debug(f"    • Base XP: {base_xp}, Base Embers: {base_embers}")
            logger.debug(f"    • Max length: {max_length}")

            # Length factor - use a more reasonable scaling
            if message_length <= 100:
                length_factor = 1.0
            else:
                bonus_length = min(message_length - 100, max_length - 100)
                length_factor = 1.0 + (bonus_length / (max_length - 100)) * 0.5  # Up to 50% bonus

            logger.debug(f"    • Length factor: {length_factor:.2f} (message: {message_length} chars)")

            # Streak bonus
            daily_streak = user_data.get("daily_streak", {}).get("count", 0)
            streak_bonus = get_streak_bonus(daily_streak)

            # Quality multiplier
            quality_score = content_analysis.get("score", 1.0)
            logger.debug(f"    • Quality multiplier: {quality_score:.2f}x")

            # Low level boost
            current_level = user_data.get("level", 1)
            if current_level < 15:
                xp_multiplier = 2.0
                embers_multiplier = 1.5
                logger.debug(
                    f"    • Low-level boost active (Level {current_level}): XP {xp_multiplier}x, Embers {embers_multiplier}x")
            else:
                xp_multiplier = 1.0
                embers_multiplier = 1.0

            # Channel and thread bonuses
            channel_multiplier = 1.0
            
            channel_bonuses = msg_cfg.get("channel_bonuses", {})
            if channel_id and channel_id in channel_bonuses:
                bonus = channel_bonuses[channel_id]
                channel_multiplier *= bonus
                logger.debug(f"    • Channel bonus applied: {bonus}x")

            premium_channels = msg_cfg.get("premium_channels", {})
            if channel_id and channel_id in premium_channels:
                bonus = premium_channels[channel_id]
                channel_multiplier *= bonus
                logger.debug(f"    • Premium channel bonus applied: {bonus}x")

            if is_thread:
                thread_bonus = msg_cfg.get("thread_bonus", 1.0)
                if thread_bonus > 1.0:
                    channel_multiplier *= thread_bonus
                    logger.debug(f"    • Thread bonus applied: {thread_bonus}x")

            # Calculate final rewards
            calculated_xp = base_xp * length_factor * streak_bonus * quality_score * xp_multiplier * channel_multiplier
            calculated_embers = base_embers * length_factor * streak_bonus * quality_score * embers_multiplier * channel_multiplier

            final_xp = max(1, round(calculated_xp))
            final_embers = max(1, round(calculated_embers))

            logger.debug(
                f"    • Calculation: {base_xp} * {length_factor:.2f} * {streak_bonus:.2f} * {quality_score:.2f} * {xp_multiplier:.2f} * {channel_multiplier:.2f} = {calculated_xp:.2f}")
            logger.debug(f"    • Final: {final_xp} XP, {final_embers} Embers")

            return {
                "xp": float(final_xp),
                "embers": float(final_embers),
                "multipliers": {
                    "length": length_factor,
                    "streak": streak_bonus,
                    "quality": quality_score,
                    "low_level": xp_multiplier,
                    "channel": channel_multiplier
                }
            }

        except Exception as e:
            logger.error(f"❌ Reward calculation failed: {e}")
            return {"xp": 10.0, "embers": 6.0}

    async def process_rewards(
            self,
            user_id: str,
            guild_id: str,
            rewards: Dict,
            user_data: Dict,
            settings: Dict,
            channel_id: str = None
    ) -> Optional[Dict]:
        """
        Process rewards and update user data.

        - Check if adding rewards would exceed daily/weekly/monthly caps
        - Cap the rewards accordingly and log when caps are hit
        """
        try:
            logger.debug(f"  💾 Processing rewards and updating database:")

            current_xp = user_data.get("xp", 0)
            current_embers = user_data.get("embers", 0)
            current_level = user_data.get("level", 1)

            logger.debug(f"    • Current state: Level {current_level}, {current_xp} XP, {current_embers} Embers")

            msg_cfg = settings.get("message", {})

            # Apply channel-specific caps to rewards
            if channel_id:
                channel_caps_config = msg_cfg.get("channel_caps", {}).get(channel_id, {})
                if channel_caps_config:
                    logger.debug(f"    • Applying channel caps for channel {channel_id}")

                    # Get current cumulative stats for the channel
                    current_channel_xp = user_data.get("message_stats", {}).get("channel_xp", {}).get(channel_id, 0)
                    current_channel_embers = user_data.get("message_stats", {}).get("channel_embers", {}).get(channel_id, 0)

                    # Apply XP cap for channel
                    max_channel_xp = channel_caps_config.get("xp", float('inf'))
                    if rewards["xp"] > 0 and current_channel_xp + rewards["xp"] > max_channel_xp:
                        original_xp = rewards["xp"]
                        rewards["xp"] = max(0, max_channel_xp - current_channel_xp)
                        if rewards["xp"] < original_xp:
                            logger.warning(f"    ⚠️  XP capped due to channel {channel_id} limit (U:{user_id}, G:{guild_id}). Original: {original_xp}, Capped: {rewards['xp']}")

                    # Apply Embers cap for channel
                    max_channel_embers = channel_caps_config.get("embers", float('inf'))
                    if rewards["embers"] > 0 and current_channel_embers + rewards["embers"] > max_channel_embers:
                        original_embers = rewards["embers"]
                        rewards["embers"] = max(0, max_channel_embers - current_channel_embers)
                        if rewards["embers"] < original_embers:
                            logger.warning(f"    ⚠️  Embers capped due to channel {channel_id} limit (U:{user_id}, G:{guild_id}). Original: {original_embers}, Capped: {rewards['embers']}")

            # Apply global caps to rewards
            for cap_type in ["daily", "weekly", "monthly"]:
                caps = msg_cfg.get(f"{cap_type}_caps", {})
                if not caps:
                    continue

                # Get current cumulative stats for the cap type
                current_xp_stat = user_data.get("message_stats", {}).get(f"{cap_type}_xp", 0)
                current_embers_stat = user_data.get("message_stats", {}).get(f"{cap_type}_embers", 0)

                # Apply XP cap
                max_xp = caps.get("xp", float('inf'))
                if rewards["xp"] > 0 and current_xp_stat + rewards["xp"] > max_xp:
                    original_xp = rewards["xp"]
                    rewards["xp"] = max(0, max_xp - current_xp_stat)
                    if rewards["xp"] < original_xp:
                        logger.warning(f"    ⚠️  XP capped due to {cap_type} limit (U:{user_id}, G:{guild_id}). Original: {original_xp}, Capped: {rewards['xp']}")

                # Apply Embers cap
                max_embers = caps.get("embers", float('inf'))
                if rewards["embers"] > 0 and current_embers_stat + rewards["embers"] > max_embers:
                    original_embers = rewards["embers"]
                    rewards["embers"] = max(0, max_embers - current_embers_stat)
                    if rewards["embers"] < original_embers:
                        logger.warning(f"    ⚠️  Embers capped due to {cap_type} limit (U:{user_id}, G:{guild_id}). Original: {original_embers}, Capped: {rewards['embers']}")

            # Calculate new totals
            new_xp = current_xp + rewards.get("xp", 0)
            new_embers = current_embers + rewards.get("embers", 0)

            logger.debug(f"    • New totals: {new_xp} XP, {new_embers} Embers")

            # Check for level up
            new_level, leveled_up = self.leveling_system.check_level_up(new_xp, current_level)

            if leveled_up:
                logger.info(f"    🎊 LEVEL UP! {current_level} → {new_level}")

            # Check and update daily streak
            logger.debug(f"  🔥 Checking daily streak:")
            new_streak_count, should_update_streak = check_and_update_streak(user_data)
            old_streak_count = user_data.get("daily_streak", {}).get("count", 0)

            if should_update_streak:
                log_streak_change(user_id, guild_id, old_streak_count, new_streak_count)

            # Update database
            now = utc_now_ts()
            today_key = utc_today_key()
            week_key = utc_week_key()
            month_key = utc_month_key()

            update_data = {
                "$set": {
                    "xp": new_xp,
                    "embers": new_embers,
                    "level": new_level,
                    "updated_at": now,
                    "last_rewarded.message": now,
                    "message_stats.last_message_time": now,
                    "message_stats.today_key": today_key,
                    "message_stats.week_key": week_key,
                    "message_stats.month_key": month_key,
                },
                "$inc": {
                    "message_stats.messages": 1,
                    "message_stats.today_xp": rewards.get("xp", 0),
                    "message_stats.today_embers": rewards.get("embers", 0),
                    "message_stats.weekly_xp": rewards.get("xp", 0),
                    "message_stats.weekly_embers": rewards.get("embers", 0),
                    "message_stats.monthly_xp": rewards.get("xp", 0),
                    "message_stats.monthly_embers": rewards.get("embers", 0),
                }
            }

            if channel_id:
                update_data["$inc"][f"message_stats.channel_xp.{channel_id}"] = rewards.get("xp", 0)
                update_data["$inc"][f"message_stats.channel_embers.{channel_id}"] = rewards.get("embers", 0)

            # Update streak if needed
            if should_update_streak:
                streak_update = create_streak_update_data(new_streak_count)
                update_data["$set"].update(streak_update)

            await self.leveling_system.update_user_data(user_id, guild_id, update_data)

            if leveled_up:
                try:
                    # Import the level role checker
                    from ecom_system.helpers.check_level_role import update_level_role_on_levelup

                    # Check and update level roles
                    logger.debug(f"🎭 Checking level roles for new level {new_level}")
                    role_result = await update_level_role_on_levelup(
                        bot=self.leveling_system.bot,
                        leveling_system=self.leveling_system,
                        guild_id=guild_id,
                        user_id=user_id,
                        new_level=new_level
                    )

                    if role_result.success and role_result.action_taken != "none":
                        logger.info(f"🎭 Level role update: {role_result.reason}")
                        if role_result.roles_added:
                            logger.info(f"  ➕ Added roles: {[r.name for r in role_result.roles_added]}")
                        if role_result.roles_removed:
                            logger.info(f"  ➖ Removed roles: {[r.name for r in role_result.roles_removed]}")
                    elif role_result.error:
                        logger.warning(f"⚠️ Level role update failed: {role_result.error}")
                    else:
                        logger.debug(f"ℹ️ Level roles: {role_result.reason}")

                    if self.level_up_messages:
                        guild_settings = await self.leveling_system.get_guild_settings(guild_id)
                        notification_channel = guild_settings.get("notification_channel")

                        if notification_channel:
                            prestige_level = user_data.get("prestige_level", 0)
                            reason = LevelUpMessages.determine_reason(current_level, new_level, prestige_level)

                            extra_data = {
                                "total_xp": new_xp,
                                "xp_to_next": self.leveling_system.xp_to_next_level(new_level),
                                "embers": new_embers,
                                "streak": new_streak_count,
                                "total_messages": user_data.get("message_stats", {}).get("messages", 0) + 1,
                                "longest_streak": user_data.get("longest_streak", 0),
                                "prestige_level": prestige_level
                            }

                            # Add role information to extra_data if roles were updated
                            if role_result.success and role_result.action_taken != "none":
                                extra_data["role_update"] = {
                                    "action": role_result.action_taken,
                                    "new_role": role_result.target_role.name if role_result.target_role else None,
                                    "roles_added": [r.name for r in role_result.roles_added],
                                    "roles_removed": [r.name for r in role_result.roles_removed]
                                }

                            # Log BEFORE attempting to send
                            logger.debug(f"📨 Attempting to send level-up message to channel {notification_channel}")

                            # Send and wait for result
                            success = await self.level_up_messages.send_level_up_message(
                                guild_id=guild_id,
                                user_id=user_id,
                                old_level=current_level,
                                new_level=new_level,
                                channel_id=notification_channel,
                                reason=reason,
                                extra_data=extra_data
                            )

                            if success:
                                logger.info(f"✅ Level-up message successfully sent to channel {notification_channel}")
                            else:
                                logger.warning(f"⚠️ Level-up message failed to send to channel {notification_channel}")
                        else:
                            logger.debug(f"ℹ️ No notification channel configured for guild {guild_id}")
                    else:
                        logger.warning(f"⚠️ Level-up message handler not initialized")
                except Exception as e:
                    logger.error(f"❌ Failed to send level-up message: {e}", exc_info=True)

            result = {
                "status": "success",
                "rewards": {
                    "xp": rewards.get("xp", 0),
                    "embers": rewards.get("embers", 0)
                },
                "totals": {
                    "xp": new_xp,
                    "embers": new_embers,
                    "level": new_level
                },
                "leveled_up": leveled_up,
                "multipliers": rewards.get("multipliers", {}),
                "streak": {
                    "count": new_streak_count,
                    "updated": should_update_streak
                }
            }

            if leveled_up:
                result["level_up"] = {
                    "old_level": current_level,
                    "new_level": new_level
                }

            return result

        except Exception as e:
            logger.error(f"❌ Error processing rewards: {e}")
            return None

    async def log_anti_cheat_violation(self, user_id: str, guild_id: str, activity_type: str, reason: str):
        """Log anti-cheat violation - placeholder for shared helper"""
        logger.warning(f"🚫 Anti-cheat violation: U:{user_id} in G:{guild_id} - Type: {activity_type}, Reason: {reason}")