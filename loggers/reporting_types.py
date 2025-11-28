from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
from typing import Dict, Optional, Any


class Severity(Enum):
	"""Error severity levels for better categorization"""
	CRITICAL = "🔴 CRITICAL"
	HIGH = "🟠 HIGH"
	MEDIUM = "🟡 MEDIUM"
	LOW = "🟢 LOW"
	INFO = "ℹ️ INFO"


class ErrorCategory(Enum):
	"""Error categories for better organization"""
	DATABASE = "💾 Database"
	DISCORD_API = "🤖 Discord API"
	COMMAND = "⚡ Command"
	SYSTEM = "🖥️ System"
	NETWORK = "🌐 Network"
	AUTHENTICATION = "🔐 Auth"
	PERMISSION = "🛡️ Permission"
	VALIDATION = "✅ Validation"
	UNKNOWN = "❓ Unknown"


@dataclass
class ErrorContext:
	"""Rich error context information"""
	timestamp: datetime
	severity: Severity
	category: ErrorCategory
	error_message: str
	guild_id: Optional[str] = None
	user_id: Optional[str] = None
	channel_id: Optional[str] = None
	command: Optional[str] = None
	stack_trace: Optional[str] = None
	additional_data: Optional[Dict[str, Any]] = None
