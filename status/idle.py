import random
import re
import logging
from collections import deque
from typing import Dict, List, Optional

import discord
from discord.ext import tasks

from dotenv import load_dotenv

from core.bot import bot
from loggers.log_factory import log_performance, log_context

load_dotenv()

logger = logging.getLogger(__name__)

# You can tweak these to your taste
ROTATE_MIN_SECONDS = 120  # 2 minutes
ROTATE_MAX_SECONDS = 600  # 10 minutes
NO_REPEAT_WINDOW = 5  # avoid repeating the same type within last N rotations

# Heuristic weights per activity type (higher = more likely).
# Types with no available phrases (or no streaming URL) are ignored at runtime.
STATUS_TYPE_WEIGHTS: Dict[str, int] = {
    "playing": 5,
    "watching": 4,
    "listening": 3,
    "competing": 2,
    "streaming": 1,  # keep streaming less frequent
}

# Optional basic check for a stream-like URL (Twitch/YouTube)
STREAM_URL_PATTERN = re.compile(r"(twitch\.tv|youtube\.com|youtu\.be)", re.IGNORECASE)

status_options: Dict[str, List[str] | Dict[str, List[str] | str]] = {
    "playing": [
        "with server stats ⚙️",
        "with uptime numbers ⏱️",
        "tag replies 🔖",
        "server data sync 🔄",
        "mention logs 🧾",
        "with activity reports 📈",
        "IP hunt 🔍",
        "port-checking puzzles 🧩",
        "auto-replies and help requests 🤖",
        "connection stability games 🔌",
        "ping pong with latency {latency_ms}ms 🏓",
        "across {guilds} servers 🌐",
    ],
    "watching": [
        "server activity 👀",
        "for mentions and replies 💬",
        "your status updates 🛰️",
        "the logs scroll endlessly 📜",
        "for API errors ⚠️",
        "network pings 📶",
        "free slots and user joins 👤",
        "your info requests 🧠",
        "the next command input ⌨️",
        "real-time usage graphs 📊",
        "the server pulse 💓",
        "over {users} users 🧮",
    ],
    "listening": [
        "for !help requests 🆘",
        "to server chatter 👂",
        "to the logs whisper 📝",
        "for @mentions 🔔",
        "command history talkbacks 🗣️",
        "uptime checks ⌚",
        "your support questions 🎧",
        "voice channel joins 🎙️",
        "admin alerts ⚡",
        "network heartbeat 🌐",
        "the rhythm of commands 🥁",
    ],
    "competing": [
        "in uptime races ⏳",
        "for fastest responses ⚡",
        "with latency {latency_ms}ms 🏁",
        "to help {users} users 🏆",
    ],
    "streaming": {
        "phrases": [
            "server info 24/7 📡",
            "live stats and data dashboards 📺",
            "mention responses on demand 🔁",
            "command output streams 📤",
            "real-time monitoring 🎯",
            "Discord bot diagnostics 🔍",
            "user activity logs 🖥️",
            "response time tracking 🕒",
            "uptime wars: bot vs lag ⏳",
            "server history playback 📽️",
            "info queries live feed 📡",
        ],
        "url": "https://twitch.tv/thegreateos"  # replace it with your stream URL if applicable
    }
}

_last_types = deque(maxlen=NO_REPEAT_WINDOW)


def _stream_url_ok(url: Optional[str]) -> bool:
    """Validate streaming URL and log validation result."""
    if not url:
        logger.debug("Streaming URL is None or empty")
        return False

    is_valid = bool(STREAM_URL_PATTERN.search(url))
    if is_valid:
        logger.debug(f"Streaming URL validation passed: {url}")
    else:
        logger.warning(f"Streaming URL validation failed - invalid format: {url}")
        logger.debug(f"URL must match pattern: {STREAM_URL_PATTERN.pattern}")

    return is_valid


@log_performance("runtime_placeholders_calculation")
def _runtime_placeholders() -> Dict[str, str]:
    """Calculate runtime placeholders with comprehensive logging."""
    logger.debug("Starting runtime placeholders calculation")

    # Safe accessors for dynamic values
    guilds = len(getattr(bot, "guilds", []) or [])
    logger.debug(f"Bot is connected to {guilds} guilds")

    users = 0
    try:
        if bot.guilds:
            guild_member_counts = []
            for guild in bot.guilds:
                member_count = guild.member_count or 0
                guild_member_counts.append((guild.name, guild.id, member_count))
                users += member_count

            logger.debug(f"Guild member count breakdown: {guild_member_counts}")
            logger.info(f"Total user count across all guilds: {users}")
        else:
            logger.warning("Bot is not connected to any guilds")
    except Exception as e:
        logger.error(f"Failed computing user count: {e}", exc_info=True)
        users = 0

    latency_ms = 0
    logger.debug(f"latency reset - {latency_ms}")
    try:
        raw_latency = bot.latency or 0
        latency_ms = int(raw_latency * 1000)

        # Log latency quality
        if latency_ms < 100:
            logger.debug(f"Excellent latency: {latency_ms}ms (raw: {raw_latency:.4f}s)")
        elif latency_ms < 300:
            logger.debug(f"Good latency: {latency_ms}ms (raw: {raw_latency:.4f}s)")
        elif latency_ms < 500:
            logger.info(f"Fair latency: {latency_ms}ms (raw: {raw_latency:.4f}s)")
        else:
            logger.warning(f"High latency detected: {latency_ms}ms (raw: {raw_latency:.4f}s)")

    except Exception as e:
        logger.error(f"Failed computing latency: {e}", exc_info=True)
        latency_ms = 0

    placeholders = {
        "guilds": str(guilds),
        "users": str(users),
        "latency_ms": str(latency_ms),
    }

    logger.info(f"Runtime placeholders generated: {placeholders}")
    return placeholders


def _format_phrase(phrase: str) -> str:
    """Format phrase with placeholders and comprehensive error handling."""
    logger.debug(f"Formatting phrase: '{phrase}'")

    if not phrase:
        logger.warning("Empty phrase provided for formatting")
        return ""

    try:
        placeholders = _runtime_placeholders()
        formatted = phrase.format(**placeholders)

        if formatted != phrase:
            logger.debug(f"Phrase formatting successful: '{phrase}' -> '{formatted}'")
        else:
            logger.debug(f"Phrase contains no placeholders: '{phrase}'")

        return formatted

    except KeyError as e:
        logger.error(f"Missing placeholder in phrase '{phrase}': {e}")
        logger.debug(f"Available placeholders: {list(_runtime_placeholders().keys())}")
        return phrase
    except Exception as e:
        logger.error(f"Unexpected error formatting phrase '{phrase}': {e}", exc_info=True)
        return phrase


@log_performance("status_type_selection")
def _choose_status_type() -> str:
    """Choose a status type with detailed candidate analysis."""
    logger.debug("Starting status type selection process")
    logger.debug(f"Recent types to avoid (last {NO_REPEAT_WINDOW}): {list(_last_types)}")

    # Build a candidate pool with weights, excluding recent types and invalid streaming
    candidates = []
    excluded_reasons = {}

    for status_type, weight in STATUS_TYPE_WEIGHTS.items():
        logger.debug(f"Evaluating status type: {status_type} (weight: {weight})")

        # Check if recently used
        if status_type in _last_types:
            excluded_reasons[status_type] = "recently used"
            logger.debug(f"Excluding {status_type}: recently used")
            continue

        # Special handling for streaming
        if status_type == "streaming":
            streaming_config = status_options.get("streaming", {})
            url = streaming_config.get("url") if isinstance(streaming_config, dict) else None
            phrases = streaming_config.get("phrases", []) if isinstance(streaming_config, dict) else []

            if not _stream_url_ok(url):
                excluded_reasons[status_type] = "invalid URL"
                logger.debug(f"Excluding {status_type}: invalid streaming URL")
                continue

            if not phrases:
                excluded_reasons[status_type] = "no phrases"
                logger.debug(f"Excluding {status_type}: no phrases available")
                continue

            logger.debug(f"Streaming type validated: URL={url}, phrases_count={len(phrases)}")
        else:
            # Regular status type validation
            phrases = status_options.get(status_type, [])
            if not phrases:
                excluded_reasons[status_type] = "no phrases"
                logger.debug(f"Excluding {status_type}: no phrases available")
                continue

            logger.debug(f"Status type {status_type} validated: phrases_count={len(phrases)}")

        candidates.append((status_type, weight))
        logger.debug(f"Added {status_type} to candidates pool")

    # Log exclusion summary
    if excluded_reasons:
        logger.info(f"Status types excluded from selection: {excluded_reasons}")

    # Fallback handling
    if not candidates:
        available_fallbacks = ["playing", "watching", "listening"]
        fallback = None

        for fb in available_fallbacks:
            if status_options.get(fb):
                fallback = fb
                break

        if not fallback:
            fallback = "playing"  # Last resort

        logger.warning(f"No valid candidates found! Using fallback: {fallback}")
        logger.debug(f"Fallback selection reason: no candidates available after filtering")
        return fallback

    # Weighted selection
    types, weights = zip(*candidates)
    total_weight = sum(weights)

    logger.debug(f"Candidate pool: {dict(candidates)} (total weight: {total_weight})")

    chosen = random.choices(types, weights=weights, k=1)[0]
    chosen_weight = dict(candidates)[chosen]
    selection_probability = (chosen_weight / total_weight) * 100

    logger.info(f"Status type selected: {chosen} (weight: {chosen_weight}, probability: {selection_probability:.1f}%)")
    logger.debug(f"Selection from candidates: {dict(candidates)}")

    return chosen


@log_performance("status_generation")
def get_random_status() -> Dict[str, str]:
    """Generate random status with comprehensive logging and validation."""
    logger.debug("=== Starting status generation ===")

    with log_context(logger, "status_type_selection"):
        status_type = _choose_status_type()

    logger.info(f"Selected status type: {status_type}")

    try:
        if status_type == "streaming":
            streaming_config = status_options["streaming"]
            if not isinstance(streaming_config, dict):
                logger.error(f"Invalid streaming configuration type: {type(streaming_config)}")
                raise ValueError("Invalid streaming configuration")

            phrases: List[str] = streaming_config["phrases"]
            url: str = streaming_config["url"]

            logger.debug(f"Streaming config - URL: {url}, available phrases: {len(phrases)}")

            if not phrases:
                logger.error("No streaming phrases available despite earlier validation")
                raise ValueError("No streaming phrases available")

            selected_phrase = random.choice(phrases)
            logger.debug(f"Raw streaming phrase selected: '{selected_phrase}'")

            name = _format_phrase(selected_phrase)

            result = {"type": status_type, "name": name, "url": url}
            logger.info(f"Generated streaming status: type={status_type}, name='{name}', url={url}")

            return result

        # Regular status types
        phrase_list = status_options.get(status_type)
        if not phrase_list:
            logger.error(f"No phrases found for status type: {status_type}")
            raise ValueError(f"No phrases available for {status_type}")

        if not isinstance(phrase_list, list):
            logger.error(f"Invalid phrase list type for {status_type}: {type(phrase_list)}")
            raise ValueError(f"Invalid phrase configuration for {status_type}")

        logger.debug(f"Available phrases for {status_type}: {len(phrase_list)}")

        selected_phrase = random.choice(phrase_list)
        logger.debug(f"Raw phrase selected for {status_type}: '{selected_phrase}'")

        formatted_phrase = _format_phrase(selected_phrase)

        result = {"type": status_type, "name": formatted_phrase}
        logger.info(f"Generated {status_type} status: '{formatted_phrase}'")

        return result

    except Exception as e:
        logger.error(f"Failed to generate status for type {status_type}: {e}", exc_info=True)
        logger.warning("Falling back to simple playing status")

        # Emergency fallback
        fallback_status = {"type": "playing", "name": "with server stats ⚙️"}
        logger.info(f"Emergency fallback status generated: {fallback_status}")
        return fallback_status


@log_performance("discord_activity_creation")
def _build_activity(random_status: Dict[str, str]) -> discord.BaseActivity:
    """Build Discord activity object with logging and validation."""
    status_type = random_status.get("type", "unknown")
    name = random_status.get("name", "")

    logger.debug(f"Building Discord activity: type={status_type}, name='{name}'")

    if not name:
        logger.warning(f"Empty activity name for type {status_type}")

    try:
        if status_type == "playing":
            activity = discord.Game(name=name)
            logger.debug(f"Created Game activity: {name}")

        elif status_type == "watching":
            activity = discord.Activity(type=discord.ActivityType.watching, name=name)
            logger.debug(f"Created watching activity: {name}")

        elif status_type == "listening":
            activity = discord.Activity(type=discord.ActivityType.listening, name=name)
            logger.debug(f"Created listening activity: {name}")

        elif status_type == "competing":
            activity = discord.Activity(type=discord.ActivityType.competing, name=name)
            logger.debug(f"Created competing activity: {name}")

        elif status_type == "streaming":
            url = random_status.get("url", "")
            if not url:
                logger.error(f"Missing URL for streaming activity: {random_status}")
                raise ValueError("Streaming activity requires URL")

            activity = discord.Streaming(name=name, url=url)
            logger.debug(f"Created streaming activity: {name} -> {url}")

        else:
            logger.warning(f"Unknown activity type '{status_type}', defaulting to Game")
            activity = discord.Game(name=name)

        logger.debug(f"Successfully built {type(activity).__name__} activity")
        return activity

    except Exception as e:
        logger.error(f"Failed to build activity for {status_type}: {e}", exc_info=True)
        logger.warning("Creating fallback Game activity")
        fallback_activity = discord.Game(name=name or "Bot Status")
        return fallback_activity


@log_performance("interval_randomization")
def _randomize_interval():
    """Randomize the next rotation interval with logging."""
    new_seconds = random.randint(ROTATE_MIN_SECONDS, ROTATE_MAX_SECONDS)

    logger.debug(f"Randomizing interval: min={ROTATE_MIN_SECONDS}s, max={ROTATE_MAX_SECONDS}s")
    logger.info(f"Next status rotation scheduled in {new_seconds}s ({new_seconds / 60:.1f} minutes)")

    try:
        old_interval = getattr(rotate_status, 'seconds', 'unknown')
        rotate_status.change_interval(seconds=new_seconds)

        logger.debug(f"Interval changed successfully: {old_interval}s -> {new_seconds}s")

        # Log interval distribution info
        range_size = ROTATE_MAX_SECONDS - ROTATE_MIN_SECONDS
        position_pct = ((new_seconds - ROTATE_MIN_SECONDS) / range_size) * 100
        logger.debug(f"Interval position within range: {position_pct:.1f}%")

    except Exception as e:
        logger.error(f"Failed to change loop interval to {new_seconds}s: {e}", exc_info=True)


@tasks.loop(seconds=ROTATE_MIN_SECONDS)
async def rotate_status():
    """Main status rotation task with comprehensive logging."""
    logger.debug("=== STATUS ROTATION CYCLE START ===")

    try:
        with log_context(logger, "status_rotation_cycle"):
            # Generate new status
            logger.debug("Step 1: Generating random status")
            random_status = get_random_status()

            logger.debug("Step 2: Building Discord activity")
            activity = _build_activity(random_status)

            logger.debug("Step 3: Updating bot presence")
            await bot.change_presence(status=discord.Status.online, activity=activity)
            logger.info(f"✅ Bot presence updated successfully")

            # Track last types to reduce repetition
            status_type = random_status["type"]
            _last_types.append(status_type)

            logger.info(f"Status rotation completed: {status_type} → '{random_status.get('name', '')}'")
            logger.debug(f"Recent types history: {list(_last_types)}")

            # Schedule next rotation
            logger.debug("Step 4: Scheduling next rotation")
            _randomize_interval()

        logger.debug("=== STATUS ROTATION CYCLE COMPLETE ===")

    except discord.HTTPException as e:
        logger.error(f"Discord HTTP error during status rotation: {e}", exc_info=True)
        logger.warning(f"HTTP Error details: status={e.status}, code={getattr(e, 'code', 'unknown')}")

    except Exception as e:
        logger.error(f"Unexpected error during status rotation: {e}", exc_info=True)
        logger.debug("Status rotation failed, but loop will continue")


@rotate_status.before_loop
async def _rotate_status_before_loop():
    """Pre-loop setup with enhanced logging."""
    logger.info("🔄 Status rotation system initializing...")
    logger.info("Waiting for bot to become ready...")

    logger.debug("Bot readiness check started")

    await bot.wait_until_ready()

    logger.info("✅ Bot is ready! Starting status rotation loop...")

    # Log initial bot state
    try:
        guild_count = len(bot.guilds) if bot.guilds else 0
        user_count = sum(g.member_count or 0 for g in bot.guilds) if bot.guilds else 0
        latency = int((bot.latency or 0) * 1000)

        logger.info(f"📊 Initial bot metrics:")
        logger.info(f"   • Guilds: {guild_count}")
        logger.info(f"   • Users: {user_count:,}")
        logger.info(f"   • Latency: {latency}ms")
        logger.info(f"   • Rotation interval: {ROTATE_MIN_SECONDS}-{ROTATE_MAX_SECONDS} seconds")
        logger.info(f"   • No-repeat window: {NO_REPEAT_WINDOW} rotations")
        logger.info(f"   • Available status types: {len(STATUS_TYPE_WEIGHTS)}")

        # Validate configuration
        total_phrases = sum(
            len(phrases) if isinstance(phrases, list)
            else len(phrases.get("phrases", [])) if isinstance(phrases, dict)
            else 0
            for phrases in status_options.values()
        )
        logger.info(f"   • Total status phrases available: {total_phrases}")

    except Exception as e:
        logger.warning(f"Failed to log initial bot metrics: {e}")

    logger.info("🚀 Status rotation system fully initialized and running!")