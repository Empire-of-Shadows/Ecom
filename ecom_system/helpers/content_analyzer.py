import re
from typing import Dict
from spellchecker import SpellChecker

# Regular expression to find URLs
URL_REGEX = r'(https?://[^\s]+)'
# Regex to find standard Unicode emojis and custom Discord emojis
EMOJI_RE = re.compile(
    r'('
    r'<a?:\w+:\d+>'  # Custom Discord emojis
    r'|[\U0001F300-\U0001F9FF]'  # Common emoji block
    r'|[\U0001F600-\U0001F64F]'  # Emoticons
    r'|[\U0001F680-\U0001F6FF]'  # Transport & map symbols
    r'|[\u2600-\u27BF]'  # Miscellaneous Symbols
    r')'
)


class ContentAnalyzer:
    """
    Analyzes message content for various properties like word count,
    uniqueness, presence of links, emojis, etc.
    """
    spell = SpellChecker()

    @staticmethod
    def count_emojis(content: str) -> int:
        """Counts standard and custom emojis in a string."""
        return len(EMOJI_RE.findall(content))

    @staticmethod
    def count_links(content: str) -> int:
        """Counts URLs in a string."""
        return len(re.findall(URL_REGEX, content))

    @staticmethod
    def analyze_content(content: str) -> Dict[str, any]:
        """
        Analyzes the message content and returns a dictionary of metrics.

        Args:
            content: The text content of the message.

        Returns:
            A dictionary with analysis results.
        """
        tokens = re.findall(r'\b[a-zA-Z\'-]+\b', content.lower())

        # Filter out nonsense words
        known_words = ContentAnalyzer.spell.known(tokens)
        misspelled = ContentAnalyzer.spell.unknown(tokens)

        real_words = list(known_words)
        for word in misspelled:
            # If a word has candidates, it's likely a misspelling, not nonsense
            if ContentAnalyzer.spell.candidates(word):
                real_words.append(word)

        word_count = len(real_words)
        unique_words = len(set(real_words))
        character_count = len(content)

        # Link and emoji counting
        links = re.findall(URL_REGEX, content)
        emojis = EMOJI_RE.findall(content)

        return {
            "word_count": word_count,
            "unique_words": unique_words,
            "character_count": character_count,
            "has_links": len(links) > 0,
            "link_count": len(links),
            "emoji_count": len(emojis),
        }

