# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
