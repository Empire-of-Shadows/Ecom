
import time
from typing import Dict

import logging
import time
from typing import Dict

import discord

logger = logging.getLogger(__name__)



class RateLimiter:
    """
    Simple in-memory rate limiter for anti-cheat protection.
    In production, consider using Redis for distributed rate limiting.
    """

    def __init__(self):
        self._counters: Dict[str, int] = {}
        self._message_rates: Dict[tuple, list] = {}
        self._last_cleanup = time.time()
        self.SPAM_THRESHOLD = 10  # messages per minute

    async def get_rate_limit_count(self, key: str) -> int:
        """Get current rate limit count for a key"""
        now = time.time()
        self._cleanup_old_entries(now)
        return self._counters.get(key, 0)

    async def increment_rate_limit_count(self, key: str):
        """Increment rate limit count for a key"""
        now = time.time()
        self._counters[key] = self._counters.get(key, 0) + 1
        self._cleanup_old_entries(now)

    def _cleanup_old_entries(self, current_time: float):
        """Clean up entries older than 5 minutes"""
        if current_time - self._last_cleanup > 300:  # Cleanup every 5 minutes
            keys_to_remove = []
            for key in self._counters:
                # Key format: "rate_limit:guild:user:type:minute"
                parts = key.split(":")
                if len(parts) >= 5:
                    key_minute = int(parts[4])
                    current_minute = int(current_time // 60)
                    if current_minute - key_minute > 5:  # Older than 5 minutes
                        keys_to_remove.append(key)

            for key in keys_to_remove:
                del self._counters[key]

            self._last_cleanup = current_time

    async def check_rate_limit(self, message: discord.Message) -> bool:
        """
        Basic rate limiting to prevent spam.

        Args:
            message: Message to check

        Returns:
            bool: True if a message should be processed, False if rate limited
        """
        try:
            user_key = (message.guild.id, message.author.id)
            now = discord.utils.utcnow().timestamp()

            # Initialize or get a user's message history
            if user_key not in self._message_rates:
                self._message_rates[user_key] = []

            # Clean old entries (older than 1 minute)
            cutoff = now - 60
            self._message_rates[user_key] = [ts for ts in self._message_rates[user_key] if ts > cutoff]

            # Check if over limit
            if len(self._message_rates[user_key]) >= self.SPAM_THRESHOLD:
                logger.warning(
                    f"🚫 Rate limit exceeded: {message.author} in {message.guild.name} "
                    f"({len(self._message_rates[user_key])}/{self.SPAM_THRESHOLD} messages per minute)"
                )
                return False

            # Add the current message timestamp
            self._message_rates[user_key].append(now)
            return True

        except Exception as e:
            logger.error(f"❌ Rate limit check error: {e}")
            return True  # Allow on error


# Global instance
rate_limiter = RateLimiter()