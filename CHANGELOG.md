# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased - 2025-12-01

### Added
-   **Comprehensive Testing Scenarios**: Created a detailed testing document (`docs/phase_9_test_scenarios.md`) covering all new reward calculation features from phases 1-8, including edge cases for attachments, emojis, links, threads, and all voice-related bonuses and caps.

### Changed
-   **Reward System Overhaul**: Completed a multi-phase overhaul of the XP and Ember reward calculation system to make it more fair, engaging, and resistant to farming. All new features are fully configurable via MongoDB settings.
    -   **Phase 1: Attachment Quality**: Implemented contextual rewards for messages with attachments. Messages with attachments and sufficient text get a bonus (`1.08x`), while attachment-only messages receive a penalty (`0.7x`).
    -   **Phase 2: Emoji Spam Penalties**: Introduced a progressive penalty system for emoji spam. A small bonus is retained for light emoji use, but messages with 10+ emojis receive a scaling penalty, capped at a `0.5x` multiplier. Emoji-only messages are also penalized.
    -   **Phase 3: Link Context Analysis**: The system now analyzes the context of shared links. Links with descriptive text receive a bonus (`1.03x`), while links with little or no context are penalized (`0.65x`). Link spam is also penalized.
    -   **Phase 4: Thread Bonuses**: Incentivized thread participation by adding a `1.15x` bonus for all messages in a thread and an additional `1.25x` bonus for the thread creator, which stack.
    -   **Phase 5: Voice Caps**: Implemented daily, weekly, and monthly caps for voice rewards (XP, Embers, and time) to ensure fair distribution and prevent abuse. The system includes warnings when approaching caps and handles period resets automatically.
    -   **Phase 6: Voice Channel Bonuses**: Admins can now configure specific voice channels to provide bonus rewards, allowing for designated high-value channels.
    -   **Phase 7: Streaming & Video Bonuses**: Users are now rewarded for more engaging voice participation. Screen sharing (`self_stream`) provides a `1.15x` bonus, and camera usage (`self_video`) provides a `1.10x` bonus. These bonuses stack.
    -   **Phase 8: Participant Count Bonus**: A new "social bonus" rewards users for being in populated voice channels. The bonus scales with the number of participants (starting after 3 members) and includes anti-exploit measures.

### Documentation
-   **Reward Calculation Improvement Plan**: Updated `docs/reward_calculation_improvement_plan.md` to mark all implementation phases (1-8) and the initial documentation phase (9.1, 9.2) as complete.

## Unreleased - 11-30-2025

### Added
-   **User Activity Command**: Implemented a new `/activity` command to display a user's activity profile.
    -   Located in `ecom_system/user_settings/activity_commands.py`.
    -   Displays comprehensive activity summary, including weekend vs. weekday activity preference and time-of-day breakdowns (morning, afternoon, evening, night, overnight).
    -   Allows viewing activity for self or another user.
    -   Ensures correct loading and access of the `ActivitySystem` instance from the bot.
-   **Reward Calculation Improvement Plan**: Created and finalized comprehensive planning document (`docs/reward_calculation_improvement_plan.md`) for enhancing XP/Ember reward calculations:
    -   **Completed Analysis**: Detailed review of current message and voice reward systems
    -   **Message Improvements Planned**:
        -   Attachment-only penalty: 0.7x (30% reduction)
        -   Progressive emoji penalties: Start at 10 emojis (0.75x), increase 0.05 per additional emoji
        -   Link context requirements: 10 words minimum (configurable)
        -   Thread participation: 15% bonus + creator engagement rewards
        -   Quality score stacking enabled (0.5x-2.0x range maintained)
    -   **Voice Improvements Planned**:
        -   Caps: 24-hour daily defaults (8640 XP, 5760 Embers, 1440 minutes)
        -   Channel-specific bonuses (fully configurable)
        -   Streaming bonus: 15%, Video bonus: 10% (stackable = 26.5%)
        -   Participant count bonus: 5% per person after 3, max 50%
    -   **Implementation Strategy**:
        -   9 phases defined with priorities (MUST/SHOULD/NICE-TO-HAVE)
        -   One phase at a time approach
        -   Complete database reset planned before rollout
        -   MongoDB settings with code defaults as fallback
    -   All 15 planning questions answered and decisions documented

### Changed
-   **Phase 1: Attachment Quality Analysis** - COMPLETED (`ecom_system/leveling/sub_system/messages.py`):
    -   Implemented contextual attachment rewards based on text content
    -   Attachment with good text (20+ words): 1.08x bonus (uses existing MongoDB setting)
    -   Attachment with short text (5-19 words): 0.85x penalty
    -   Attachment-only (<5 words): 0.7x penalty (30% reduction)
    -   All thresholds and multipliers configurable in MongoDB
    -   Enhanced logging shows attachment bonuses/penalties in content analysis
    -   Documentation: `docs/mongodb_settings_phase1_attachments.md`
-   **Phase 2: Emoji Spam Detection & Progressive Penalties** - COMPLETED (`ecom_system/leveling/sub_system/messages.py:529-568`):
    -   Implemented progressive penalty system for emoji spam
    -   Emoji-only messages (<3 words): 0.75x penalty
    -   Progressive penalties start at 10 emojis:
        - 10 emojis: 0.75x (25% reduction)
        - 11 emojis: 0.70x (30% reduction)
        - 12 emojis: 0.65x (35% reduction)
        - 15+ emojis: 0.50x floor (50% reduction)
    -   Penalty calculation: `base - (count - threshold) × increment`, floored at 0.5x
    -   Normal emoji usage (1-9 emojis with 3+ words): Still receives 1.05x bonus
    -   All thresholds configurable in MongoDB (threshold, base, increment, floor)
    -   Enhanced logging shows emoji penalties and counts
    -   Documentation: `docs/mongodb_settings_phase2_emojis.md`
-   **Phase 3: Link Context Analysis** - COMPLETED (`ecom_system/leveling/sub_system/messages.py:600-635`):
    -   Implemented contextual link scoring based on surrounding text
    -   Link spam detection (5+ links): 0.7x penalty
    -   Link-only messages (<10 words total): 0.65x penalty
    -   Links with good context (10+ words): 1.03x bonus (existing behavior)
    -   All thresholds configurable in MongoDB:
        - `link_bonus`: 1.03 (default)
        - `link_only_penalty`: 0.65
        - `link_context_word_threshold`: 10 words
        - `link_spam_threshold`: 5 links
        - `link_spam_penalty`: 0.7
    -   Enhanced logging shows link count, word count, and applied bonus/penalty
    -   Quality factors updated: `link_sharing`, `link_no_context`, `link_spam`
    -   Documentation: `docs/mongodb_settings_phase3_links.md`
-   **Phase 4: Thread Participation Bonuses** - COMPLETED (`ecom_system/leveling/sub_system/messages.py:817-828`):
    -   Updated default thread bonus from 1.0x (no bonus) to 1.15x (15% bonus)
    -   Implemented thread creator bonus detection
    -   Thread detection: Uses `isinstance(message.channel, discord.Thread)` (`ecom_system/Listeners/on_message.py:33-36`)
    -   Thread creator detection: Compares `message.channel.owner_id` with `message.author.id` (`ecom_system/Listeners/on_message.py:35-36`)
    -   Thread participation bonus: 1.15x multiplier (15% bonus) for any message in a thread
    -   Thread creator bonus: 1.25x multiplier (25% bonus) for thread creators posting in their own threads
    -   Both bonuses stack: Thread creators get 1.15 × 1.25 = 1.4375x total (43.75% bonus)
    -   All multipliers configurable in MongoDB:
        - `thread_bonus`: 1.15 (default) - Applied to all thread messages
        - `thread_starter_bonus`: 1.25 (default) - Applied to thread creators only
    -   Enhanced logging shows thread status and creator bonus in debug logs
    -   Thread creator status passed to achievement system for potential future achievements
    -   Encourages quality thread creation and community engagement
-   **Phase 5: Voice System Caps Implementation** - COMPLETED (`ecom_system/leveling/sub_system/voice.py:285-435, 455-519`):
    -   Implemented comprehensive voice reward cap system with daily/weekly/monthly limits
    -   Added `_apply_voice_caps()` method to enforce caps before rewards are granted (voice.py:285-435)
    -   Cap checking compares current totals against configured limits and reduces rewards if needed
    -   Automatic period detection and reset:
        - Tracks time keys (`today_key`, `week_key`, `month_key`) to detect period changes
        - Resets daily counters when date changes
        - Resets weekly counters when week changes
        - Resets monthly counters when month changes
    -   Smart reward reduction when approaching caps:
        - Reduces rewards to fit remaining cap room
        - Warns at 90% cap threshold
        - Prevents over-rewarding while maximizing eligible rewards
    -   All caps configurable in MongoDB (`voice.daily_caps`, `voice.weekly_caps`, `voice.monthly_caps`):
        - Each cap has `xp` and `embers` limits
        - Defaults allow ~24 hours daily activity if not configured
        - Can disable caps by not configuring them
    -   Enhanced logging:
        - Info messages when approaching 90% of any cap
        - Warning messages when caps are hit with reduction details
        - Debug messages showing period resets
    -   Database tracking includes separate counters:
        - `voice_stats.today_xp` / `voice_stats.today_embers`
        - `voice_stats.weekly_xp` / `voice_stats.weekly_embers`
        - `voice_stats.monthly_xp` / `voice_stats.monthly_embers`
    -   Prevents cap abuse and ensures fair reward distribution
-   **Phase 6: Voice Channel Bonuses** - COMPLETED (`ecom_system/leveling/sub_system/voice.py:268-274`, `ecom_system/Listeners/VoiceSessions.py:20`):
    -   Implemented channel-specific voice reward multipliers
    -   Added `channel_id` field to VoiceSession dataclass (VoiceSessions.py:20)
    -   Channel ID stored when session created and updated when user moves channels (voice.py:105, 132-135)
    -   Channel bonus lookup in reward calculation (voice.py:268-274):
        - Loads `channel_bonuses` dict from MongoDB settings
        - Applies multiplier if channel_id exists in configuration
        - Multiplies with other bonuses (engagement, streak, level)
    -   Fully configurable in MongoDB (`voice.channel_bonuses`):
        - Structure: `{"channel_id": multiplier}` (e.g., `{"123456789": 1.5}`)
        - No default bonuses - admins configure per channel
        - Can set different multipliers for different voice channels
    -   Enhanced logging shows channel ID and bonus when applied
    -   Channel multiplier included in reward calculation debug logs
    -   Enables rewarding premium/event voice channels more generously
-   **Phase 7: Voice Streaming/Video Bonuses** - COMPLETED (`ecom_system/leveling/sub_system/voice.py:282-300`, `ecom_system/Listeners/VoiceSessions.py:27-28, 35-36, 73-76`):
    -   Implemented screen sharing and camera bonuses for voice rewards
    -   Added streaming/video state tracking to VoiceSession (VoiceSessions.py:27-28, 35-36):
        - `is_streaming` flag for screen sharing (Discord's `self_stream`)
        - `is_video` flag for camera on (Discord's `self_video`)
        - `streaming_time` and `video_time` cumulative time tracking
    -   State tracking updated in real-time (VoiceSessions.py:73-76):
        - Tracks time spent streaming and with camera on
        - Updates when user toggles streaming/video during session
        - Included in session metrics with percentages
    -   Streaming and video states passed through voice state updates (voice.py:113-114, 142-143, 166-167)
    -   Bonus application in reward calculation (voice.py:282-300):
        - Loads `screen_share_bonus` from MongoDB (default: 1.15x = 15% bonus)
        - Loads `camera_bonus` from MongoDB (default: 1.1x = 10% bonus)
        - Applied only if user actually streamed/had camera on during session
        - Both bonuses stack: 1.15 × 1.1 = 1.265x (26.5% total bonus)
    -   Already configured in MongoDB (`voice.screen_share_bonus`: 1.15, `voice.camera_bonus`: 1.1)
    -   Enhanced logging shows streaming/video time and bonuses
    -   Multipliers included in reward calculation debug logs
    -   Encourages engaging voice sessions with screen sharing and video
-   **Phase 8: Voice Participant Count Bonus** - COMPLETED (`ecom_system/leveling/sub_system/voice.py:333-362`, `ecom_system/Listeners/VoiceSessions.py:21`, `ecom_system/Listeners/voice_tracker.py:71-73`):
    -   Implemented social bonus system that rewards users more when in populated voice channels
    -   Added participant count tracking to VoiceSession (VoiceSessions.py:21):
        - `participant_count` field tracks non-bot members in channel
        - Counted automatically when session starts (voice_tracker.py:71-73)
        - Stored in session for reward calculation
    -   Progressive bonus calculation (voice.py:333-362):
        - Opt-in system via `participant_bonus_enabled` (default: false)
        - Configurable threshold: minimum members needed before bonus applies
        - Bonus per person: additional multiplier per person above threshold
        - Formula: `1.0 + (additional_people × bonus_per_person)`
        - Maximum cap to prevent excessive rewards
    -   Anti-exploit protection:
        - Minimum active time requirement before bonus applies
        - Prevents channel-hopping to farm bonuses
        - Default: 60 seconds minimum active time required
        - Configurable via `participant_min_time_seconds`
    -   Fully configurable in MongoDB (`voice.participant_bonus_*`):
        - `participant_bonus_enabled`: false (opt-in, disabled by default)
        - `participant_bonus_threshold`: 3 (minimum members for bonus)
        - `participant_bonus_per_person`: 0.05 (5% bonus per additional person)
        - `participant_bonus_max`: 1.5 (maximum 50% bonus)
        - `participant_min_time_seconds`: 60 (anti-exploit minimum time)
    -   Example scaling (threshold 3, 0.05/person, max 1.5x):
        - 1-2 people: 1.0x (below threshold)
        - 3 people: 1.0x (threshold met, no bonus yet)
        - 5 people: 1.10x (10% bonus)
        - 8 people: 1.25x (25% bonus)
        - 13+ people: 1.50x (50% max bonus reached)
    -   Stacks with all other bonuses (engagement, streak, level, channel, streaming, video)
    -   Enhanced logging shows participant count and bonus in debug logs
    -   Bonus blocked message shown when minimum time not met (anti-exploit)
    -   Encourages social interaction and community building in voice channels
    -   Documentation: `docs/mongodb_settings_phase8_participants.md`

### Fixed
-   **Word Count Accuracy**: Fixed emojis and URLs being counted as words in word_count calculation (`ecom_system/leveling/sub_system/messages.py:472-504`):
    -   Custom Discord emojis (`<:name:id>` or `<a:name:id>`) no longer counted as words
    -   Emoji shortcodes (`:emoji_name:`) no longer counted as words
    -   **Unicode emojis** (🎙️ 🎧 💡 etc.) now properly removed before word counting
    -   **URLs** (https://example.com) now properly removed before word counting
    -   Added comprehensive Unicode emoji pattern matching (emoticons, symbols, transport, dingbats, variation selectors, ZWJ)
    -   Only actual text words are counted now
    -   Fixes issue where message with only emojis `🎙️ 🎧 💡 🔗 📢` showed as 5 words instead of 0
    -   Fixes issue where `"https://link1.com/ https://link2.com/ Check these!"` showed as 7 words instead of 2
    -   Emoji-only and link-only detection now works correctly
-   **CRITICAL: Settings Not Loading** - Fixed `get_guild_settings()` not loading Message/Voice/Reaction settings from MongoDB (`ecom_system/leveling/leveling.py:528-570`):
    -   Function was only querying Master collection, ignoring Message/Voice/Reaction collections
    -   Now loads and merges settings from all four collections:
        - Master: Guild-specific settings (guild_id, notification_channel, level_roles)
        - Message: Message rewards, quality analysis, caps, bonuses
        - Voice: Voice rewards, bonuses, caps
        - Reaction: Reaction rewards and bonuses
    -   Settings now properly load: base_xp (10), base_embers (6), quality_analysis settings, caps, etc.
    -   Added debug logging to confirm which settings loaded successfully
-   **Message Validation for Attachments**: Fixed messages with attachments (e.g., images) but no text being rejected by minimum length validation (`ecom_system/leveling/sub_system/messages.py`):
    -   Updated `validate_message()` to accept `has_attachments` parameter
    -   Messages with attachments now bypass minimum length requirement even with 0 text characters
    -   Users can now earn XP and embers for sending image-only messages

## Unreleased - 11-29-2025

### Added
-   **Time-of-Day Activity Tracking**: Added comprehensive time-of-day categorization to `ActivitySystem` (`ecom_system/activity_system/activity_system.py`):
    -   New static method `categorize_hour_to_time_of_day()` categorizes hours (0-23) into periods: morning (6-11), afternoon (12-17), evening (18-22), night (23-1), overnight (2-5)
    -   New static method `categorize_weekday()` categorizes days (0-6) into 'weekend' or 'weekday'
    -   New method `analyze_time_of_day_distribution()` analyzes hourly patterns and returns activity breakdown by time-of-day period with percentages and rankings
    -   New async method `get_user_time_of_day_breakdown()` provides detailed time-of-day analysis for individual users
    -   New async method `get_guild_time_of_day_insights()` provides guild-wide time-of-day activity insights
    -   Enhanced `_enhance_user_summary()` to automatically include time-of-day breakdowns in user activity summaries
    -   Enhanced `_enhance_user_summary()` to include weekend vs weekday breakdown with percentages and preference indicators
    -   Enhanced `get_activity_insights()` to include aggregated time-of-day data for entire guild
-   Added comprehensive test suite (`test_time_categorization.py`) for validating time-of-day and weekday categorization logic
-   **Activity Pattern Data Migration**: Added migration tools to fix activity pattern data structure:
    -   New static method `_normalize_pattern_to_array()` converts legacy object format `{"0": 3, ...}` to array format `[3, ...]`
    -   New method `_normalize_activity_patterns()` normalizes both hourly and weekly patterns
    -   New async method `migrate_patterns_to_arrays()` provides database-wide migration with dry-run support
    -   Added migration script `migrate_activity_patterns.py` for easy execution
-   Added documentation (`docs/activity_time_tracking_usage.md`) with usage examples and use cases for time-of-day tracking

### Fixed
-   **CRITICAL**: Fixed `hourly_pattern` and `weekly_pattern` being stored as objects instead of arrays in `ActivitySystem`:
    -   Fixed MongoDB WriteError conflict between `$setOnInsert` and `$inc` operations on array fields
    -   Added `_ensure_user_document_exists()` method that creates documents with proper array structure before increment operations (`ecom_system/activity_system/activity_system.py:61-116`)
    -   Modified `record_activity()` to use two-step approach: ensure document exists, then increment (`ecom_system/activity_system/activity_system.py:148`)
    -   Previous issue: patterns were stored as `{"0": 3, "1": 5}` instead of `[3, 5]` which broke time-of-day analysis
    -   Integrated normalization into all pattern-reading methods: `_enhance_user_summary()`, `get_user_time_of_day_breakdown()`, `get_guild_time_of_day_insights()`
    -   Backward compatibility maintained: existing object-format data is automatically converted to arrays when read
-   Fixed typo in `ecom_system/leveling/sub_system/reactions.py` where `self.level_system` should have been `self.leveling_system`, which prevented message owners from receiving `got_reactions` stat updates.
-   Added achievement system integration to `ecom_system/Listeners/reaction_tracker.py` to properly update achievement progress for both reactors (who give reactions) and message owners (who receive reactions).
-   Added missing condition type routes in `ecom_system/achievement_system/progress/achievement_progress.py`:
    -   `attachment_messages` now routes to message handler
    -   `quality_streak` now routes to message handler
    -   `prestige_level` now routes to level handler
-   Fixed SQLite database errors in `ecom_system/achievement_system/progress/db_time_tracker.py` by adding `_has_local_db()` check before attempting to query the local database for time-based achievements. Also prevents creation of a file named `"None"` when `local_db_path` is `None`.
-   Fixed handling of malformed achievements in `achievement_progress.py` by adding validation to skip achievements with missing `id`, `conditions`, or `type` fields.
-   Fixed field path parsing in `ecom_system/achievement_system/progress/reactions_tracker.py` to correctly handle dot-notation paths (e.g., `"message_stats.got_reactions"`). Previously treated the full path as a literal key, always returning 0.
-   Changed log level from WARNING to DEBUG for achievements showing 100% progress but not yet unlocked, as this is expected behavior when progress updates occur before achievement checks.

## Unreleased - 11-28-2025

### Added
-   Admin commands for resetting user stats (`/reset user_stats`).
-   Admin commands for resetting user achievements (`/reset user_achievements`).
-   Admin commands for resetting guild achievements (`/reset guild_achievements`).
-   Admin commands for permanently deleting user data (`/nuke user_data`).
-   Admin commands for permanently deleting guild data (`/nuke guild_data`).
-   User-facing commands for opting out of the economy system (`/settings opt-out`) with options for data retention or immediate deletion.
-   User-facing commands for opting back into the economy system (`/settings opt-in`).
-   `database/EconDataManager.py` module to centralize economy-related database operations.
-   `ecom_system/helpers/opt_out_helper.py` module providing a utility function to check user opt-out status.

### Added
-   Achievement conditions for tracking links sent and total attachments sent.
-   Enhanced word counting in `ContentAnalyzer` to filter out nonsense and include misspellings.
-   `ecom_system/helpers/content_analyzer.py`: New module for detailed message content analysis (links, word count, etc.).
-   `ecom_system/leveling/sub_system/messages.py`: New module for processing Discord messages, updating user message stats including link and attachment counts.

### Changed
-   Dependency: Added `pyspellchecker` to `requirements.txt` for enhanced word filtering.

### Fixed
-   Corrected database update logic in `EconDataManager.set_user_opt_in` to properly use MongoDB `$set` operator, resolving "update only works with $ operators" error.
-   Resolved duplicate variable definitions for `guild_id` and `user_id` in `ecom_system/Listeners/on_message.py` to prevent incorrect data extraction.
-   Addressed type inconsistency and duplicate definition of `guild_id` in `ecom_system/Listeners/reaction_tracker.py`.
-   Added missing `get_progress_summary()` and `get_detailed_progress()` methods to `ReactionsProgressTracker`, `StreakProgressTracker`, `TimeBasedProgressTracker`, and `DBTimeProgressTracker` in `ecom_system/achievement_system/progress/`.
-   Fixed achievement progress tracking for achievements in categories without direct handlers (e.g., "engagement") by implementing condition-type-based routing in `AchievementProgressSystem.update_progress()`.
-   Achievement progress now correctly routes based on condition type (`daily_streak`, `messages`, `voice_time`, etc.) when category doesn't have a dedicated handler.

### Changed
-   Integrated opt-out checks (`is_opted_out`) into the `on_message`, `on_reaction_add`, and `on_voice_state_update` listeners to skip processing for opted-out users.
-   Enhanced `EconDataManager`'s data deletion methods (`delete_all_user_data`, `delete_all_guild_data`) to include a placeholder for deletion of data from local SQLite databases, acknowledging the need for comprehensive data removal.