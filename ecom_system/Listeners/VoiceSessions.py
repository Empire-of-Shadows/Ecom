from dataclasses import dataclass, field
import time
import uuid
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger("VoiceSession")


@dataclass
class VoiceSession:
    """
    Comprehensive voice session tracking with detailed state management.
    Tracks time spent in various voice states for accurate reward calculation.
    """

    # Core session tracking
    start_time: float
    session_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    channel_id: Optional[str] = None  # Voice channel ID for channel-specific bonuses
    participant_count: int = 0  # Number of members in channel (excluding bots)

    # Current state flags
    is_muted: bool = False
    is_deafened: bool = False
    is_self_muted: bool = False
    is_self_deafened: bool = False
    is_streaming: bool = False  # Screen sharing (self_stream)
    is_video: bool = False      # Camera on (self_video)

    # Cumulative time tracking (in seconds)
    muted_time: float = 0.0
    deafened_time: float = 0.0
    self_muted_time: float = 0.0
    self_deafened_time: float = 0.0
    streaming_time: float = 0.0  # Time spent streaming
    video_time: float = 0.0      # Time spent with camera on

    # Timestamp management
    last_state_change: float = field(init=False)
    last_update_time: float = field(init=False)

    def __post_init__(self):
        """Initialize session with validation and default values."""
        current_time = time.time()

        # Validate start time isn't in the future
        if self.start_time > current_time:
            logger.warning(f"VoiceSession start_time is in the future, adjusting to current time")
            self.start_time = current_time

        # Initialize timestamps
        self.last_state_change = self.start_time
        self.last_update_time = self.start_time

        logger.debug(f"🎤 VoiceSession initialized: {self.session_id}")

    def update_state_times(self, current_time: float) -> None:
        """
        Update cumulative state times based on current flags.
        This should be called before any state changes or when getting metrics.
        """
        try:
            time_since_update = current_time - self.last_update_time

            if self.is_muted:
                self.muted_time += time_since_update
            if self.is_deafened:
                self.deafened_time += time_since_update
            if self.is_self_muted:
                self.self_muted_time += time_since_update
            if self.is_self_deafened:
                self.self_deafened_time += time_since_update
            if self.is_streaming:
                self.streaming_time += time_since_update
            if self.is_video:
                self.video_time += time_since_update

            self.last_update_time = current_time
            logger.debug(f"🔄 VoiceSession state times updated for {self.session_id}")

        except Exception as e:
            logger.error(f"❌ Error updating voice state times: {e}")

    def set_state(
            self,
            muted: bool,
            deafened: bool,
            self_muted: bool,
            self_deafened: bool,
            update_time: float,
            streaming: bool = False,
            video: bool = False
    ) -> None:
        """
        Update voice state flags and accumulate time for previous state.
        """
        try:
            # Update times for current state before changing flags
            self.update_state_times(update_time)

            # Check if state actually changed
            state_changed = (
                    self.is_muted != muted or
                    self.is_deafened != deafened or
                    self.is_self_muted != self_muted or
                    self.is_self_deafened != self_deafened or
                    self.is_streaming != streaming or
                    self.is_video != video
            )

            if state_changed:
                # Update state flags
                self.is_muted = muted
                self.is_deafened = deafened
                self.is_self_muted = self_muted
                self.is_self_deafened = self_deafened
                self.is_streaming = streaming
                self.is_video = video

                self.last_state_change = update_time

                state_desc = self._get_state_description()
                logger.debug(f"🎤 Voice state changed for {self.session_id}: {state_desc}")

        except Exception as e:
            logger.error(f"❌ Error setting voice state: {e}")

    def total_duration(self, current_time: float) -> float:
        """Calculate total session duration in seconds."""
        return current_time - self.start_time

    def active_duration(self, current_time: float) -> float:
        """
        Calculate time spent in active state (not muted and not deafened).
        This represents time when user can both speak and hear.
        """
        self.update_state_times(current_time)
        total = self.total_duration(current_time)

        # Active time = total time minus time spent muted or deafened
        inactive_time = max(self.muted_time, self.deafened_time)
        return max(0, total - inactive_time)

    def audible_duration(self, current_time: float) -> float:
        """
        Calculate time spent able to hear (not deafened).
        """
        self.update_state_times(current_time)
        total = self.total_duration(current_time)
        return max(0, total - self.deafened_time)

    def speakable_duration(self, current_time: float) -> float:
        """
        Calculate time spent able to speak (not muted).
        """
        self.update_state_times(current_time)
        total = self.total_duration(current_time)
        return max(0, total - self.muted_time)

    def is_active(self) -> bool:
        """Check if user is currently in an active state."""
        return not (self.is_muted or self.is_deafened)

    def snapshot(self) -> Dict[str, float]:
        """
        Capture current state durations without updating timestamps.
        Useful for periodic sampling without modifying state.
        """
        current_time = time.time()
        return {
            "total_duration": self.total_duration(current_time),
            "active_duration": self.active_duration(current_time),
            "audible_duration": self.audible_duration(current_time),
            "speakable_duration": self.speakable_duration(current_time),
            "muted_time": self.muted_time,
            "deafened_time": self.deafened_time,
            "self_muted_time": self.self_muted_time,
            "self_deafened_time": self.self_deafened_time,
        }

    def compute_metrics(self, current_time: float) -> Dict[str, Any]:
        """
        Compute comprehensive session metrics for reward calculation.
        """
        try:
            self.update_state_times(current_time)

            total_duration = self.total_duration(current_time)
            active_duration = self.active_duration(current_time)
            audible_duration = self.audible_duration(current_time)
            speakable_duration = self.speakable_duration(current_time)

            # Calculate percentages
            if total_duration > 0:
                active_percentage = (active_duration / total_duration) * 100
                audible_percentage = (audible_duration / total_duration) * 100
                speakable_percentage = (speakable_duration / total_duration) * 100
            else:
                active_percentage = audible_percentage = speakable_percentage = 0.0

            # Engagement score (0.0 to 1.0) based on active participation
            engagement_score = min(1.0, active_duration / max(1, total_duration))

            # Calculate streaming/video percentages
            if total_duration > 0:
                streaming_percentage = (self.streaming_time / total_duration) * 100
                video_percentage = (self.video_time / total_duration) * 100
            else:
                streaming_percentage = video_percentage = 0.0

            metrics = {
                # Core durations
                "voice_seconds": total_duration,
                "active_seconds": active_duration,
                "audible_seconds": audible_duration,
                "speakable_seconds": speakable_duration,

                # State durations
                "muted_time": self.muted_time,
                "deafened_time": self.deafened_time,
                "self_muted_time": self.self_muted_time,
                "self_deafened_time": self.self_deafened_time,
                "streaming_time": self.streaming_time,
                "video_time": self.video_time,

                # Percentages
                "total_active_percentage": active_percentage,
                "total_audible_percentage": audible_percentage,
                "total_speakable_percentage": speakable_percentage,
                "streaming_percentage": streaming_percentage,
                "video_percentage": video_percentage,

                # Engagement metrics
                "engagement_score": engagement_score,
                "is_currently_active": self.is_active(),

                # Social metrics
                "participant_count": self.participant_count,

                # Session metadata
                "session_id": self.session_id,
                "start_time": self.start_time,
                "end_time": current_time,
            }

            logger.debug(f"📊 Voice metrics computed for {self.session_id}: "
                         f"{active_duration:.1f}s active ({active_percentage:.1f}%)")

            return metrics

        except Exception as e:
            logger.error(f"❌ Error computing voice metrics: {e}")
            return {}

    def get_session_summary(self, current_time: float) -> Dict[str, Any]:
        """
        Generate a comprehensive session summary with metadata and analytics.
        """
        metrics = self.compute_metrics(current_time)

        summary = {
            "session_metadata": {
                "session_id": self.session_id,
                "start_time": self.start_time,
                "current_time": current_time,
                "duration_seconds": metrics.get("voice_seconds", 0),
            },
            "current_state": {
                "is_muted": self.is_muted,
                "is_deafened": self.is_deafened,
                "is_self_muted": self.is_self_muted,
                "is_self_deafened": self.is_self_deafened,
                "is_active": self.is_active(),
            },
            "time_analytics": metrics,
            "state_changes": {
                "last_state_change": self.last_state_change,
                "time_since_last_change": current_time - self.last_state_change,
            }
        }

        return summary

    def reset(self, new_start_time: float = None) -> None:
        """
        Reset session durations and states while preserving session ID.
        Useful for handling channel moves without creating new sessions.
        """
        current_time = time.time()
        self.start_time = new_start_time or current_time
        self.last_state_change = self.start_time
        self.last_update_time = self.start_time

        # Reset cumulative times
        self.muted_time = 0.0
        self.deafened_time = 0.0
        self.self_muted_time = 0.0
        self.self_deafened_time = 0.0

        logger.debug(f"🔄 VoiceSession reset: {self.session_id}")

    def _get_state_description(self) -> str:
        """Get human-readable description of current voice state."""
        states = []
        if self.is_muted: states.append("muted")
        if self.is_deafened: states.append("deafened")
        if self.is_self_muted: states.append("self-muted")
        if self.is_self_deafened: states.append("self-deafened")

        if not states:
            return "active"
        return ", ".join(states)

    def __str__(self) -> str:
        """Human-readable string representation."""
        current_time = time.time()
        duration = self.total_duration(current_time)
        active = self.active_duration(current_time)

        return (f"VoiceSession({self.session_id[:8]}): "
                f"{duration:.0f}s total, {active:.0f}s active, "
                f"state: {self._get_state_description()}")

    def __repr__(self) -> str:
        """Developer-friendly string representation."""
        return (f"VoiceSession(session_id='{self.session_id}', "
                f"start_time={self.start_time}, "
                f"is_active={self.is_active()}, "
                f"duration={self.total_duration(time.time()):.1f}s)")