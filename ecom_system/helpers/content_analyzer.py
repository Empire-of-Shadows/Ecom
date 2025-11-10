import re
from typing import Dict, Any


class ContentAnalyzer:
    """
    Shared content analysis utilities for message processing.
    """

    # URL detection regex
    URL_REGEX = re.compile(
        r"https?://(?:[-\w.])+(?:[:\d]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?(?:#(?:[\w.])*)?)?",
        re.IGNORECASE
    )

    # Emoji detection regex
    EMOJI_REGEX = re.compile(
        r"("
        r":[a-zA-Z0-9_~\-]+:"  # :custom_emoji:
        r"|<a?:\w+:\d+>"  # <a:name:id> or <:name:id> (custom)
        r"|[\U0001F300-\U0001F9FF]"  # Unicode emojis
        r"|[\u2600-\u27BF]"  # Misc symbols
        r")"
    )

    @classmethod
    def count_emojis(cls, content: str) -> int:
        """Count emojis in content."""
        if not content:
            return 0
        try:
            matches = cls.EMOJI_REGEX.findall(content)
            return len(matches)
        except Exception:
            return 0

    @classmethod
    def count_links(cls, content: str) -> int:
        """Count links in content."""
        if not content:
            return 0
        try:
            matches = cls.URL_REGEX.findall(content)
            return len(matches)
        except Exception:
            return 0

    @classmethod
    def check_spam_patterns(cls, content: str) -> bool:
        """
        Check for spam patterns in content.
        Returns True if content is valid, False if spam detected.
        """
        if not content:
            return True

        words = content.lower().split()
        unique_words = set(words)

        # Require at least 30% unique words
        if len(unique_words) < max(1, len(words) * 0.3):
            return False

        # Check for excessive caps
        caps_ratio = sum(1 for c in content if c.isupper()) / len(content)
        if caps_ratio > 0.7:  # 70% caps
            return False

        return True


# Global instance
content_analyzer = ContentAnalyzer()