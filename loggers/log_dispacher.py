import asyncio
import os
import json
import traceback
from collections import Counter, defaultdict, deque
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass, asdict
from enum import Enum
import html
import re
import certifi, ssl, smtplib
from dotenv import load_dotenv

load_dotenv()

email = os.getenv("EMAIL")
password = os.getenv("PASSWORD")


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


class ErrorAnalyzer:
	"""Analyzes errors to determine severity and category"""

	@staticmethod
	def analyze_error(error_msg: str, stack_trace: Optional[str] = None) -> Tuple[Severity, ErrorCategory]:
		"""Analyze error message to determine severity and category"""
		error_lower = error_msg.lower()
		stack_lower = (stack_trace or "").lower()

		# Critical errors
		if any(keyword in error_lower for keyword in [
			'database connection lost', 'mongodb connection', 'fatal', 'critical',
			'out of memory', 'disk full', 'system crash'
		]):
			return Severity.CRITICAL, ErrorAnalyzer._get_category(error_lower, stack_lower)

		# High severity errors
		if any(keyword in error_lower for keyword in [
			'timeout', 'connection refused', 'authentication failed',
			'permission denied', 'rate limit', '429', '500', '503'
		]):
			return Severity.HIGH, ErrorAnalyzer._get_category(error_lower, stack_lower)

		# Medium severity errors
		if any(keyword in error_lower for keyword in [
			'400', '401', '403', '404', 'bad request', 'unauthorized',
			'validation', 'invalid', 'missing required'
		]):
			return Severity.MEDIUM, ErrorAnalyzer._get_category(error_lower, stack_lower)

		# Low severity (warnings and info)
		if any(keyword in error_lower for keyword in [
			'warning', 'deprecated', 'info', 'notice'
		]):
			return Severity.LOW, ErrorAnalyzer._get_category(error_lower, stack_lower)

		# Default to medium severity
		return Severity.MEDIUM, ErrorAnalyzer._get_category(error_lower, stack_lower)

	@staticmethod
	def _get_category(error_lower: str, stack_lower: str) -> ErrorCategory:
		"""Determine error category based on content"""
		if any(keyword in error_lower or keyword in stack_lower for keyword in [
			'mongodb', 'database', 'collection', 'query', 'cursor', 'transaction'
		]):
			return ErrorCategory.DATABASE

		if any(keyword in error_lower or keyword in stack_lower for keyword in [
			'discord', 'gateway', 'api', 'bot', 'guild', 'channel', 'message'
		]):
			return ErrorCategory.DISCORD_API

		if any(keyword in error_lower or keyword in stack_lower for keyword in [
			'command', 'slash', 'prefix', 'interaction'
		]):
			return ErrorCategory.COMMAND

		if any(keyword in error_lower or keyword in stack_lower for keyword in [
			'network', 'connection', 'timeout', 'http', 'request'
		]):
			return ErrorCategory.NETWORK

		if any(keyword in error_lower or keyword in stack_lower for keyword in [
			'auth', 'token', 'login', 'credential', 'permission'
		]):
			return ErrorCategory.AUTHENTICATION

		if any(keyword in error_lower or keyword in stack_lower for keyword in [
			'permission', 'forbidden', '403', 'unauthorized', '401'
		]):
			return ErrorCategory.PERMISSION

		if any(keyword in error_lower or keyword in stack_lower for keyword in [
			'validation', 'invalid', 'missing', 'required', 'format'
		]):
			return ErrorCategory.VALIDATION

		if any(keyword in error_lower or keyword in stack_lower for keyword in [
			'system', 'memory', 'cpu', 'disk', 'process'
		]):
			return ErrorCategory.SYSTEM

		return ErrorCategory.UNKNOWN


class EmailTemplate:
	"""Professional email templates with rich formatting"""

	@staticmethod
	def create_error_summary_html(
			errors: List[ErrorContext],
			stats: Dict[str, Any],
			period_start: datetime,
			period_end: datetime
	) -> str:
		"""Create a comprehensive HTML email template"""

		# Group errors by category and severity
		by_category = defaultdict(list)
		by_severity = defaultdict(list)

		for error in errors:
			by_category[error.category].append(error)
			by_severity[error.severity].append(error)

		html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Discord Bot Error Report</title>
            <style>
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    max-width: 900px;
                    margin: 0 auto;
                    padding: 20px;
                    background-color: #f5f5f5;
                }}
                .container {{
                    background: white;
                    border-radius: 10px;
                    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                    overflow: hidden;
                }}
                .header {{
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 30px;
                    text-align: center;
                }}
                .header h1 {{
                    margin: 0;
                    font-size: 2.5em;
                    font-weight: 300;
                }}
                .header p {{
                    margin: 10px 0 0;
                    opacity: 0.9;
                    font-size: 1.1em;
                }}
                .content {{
                    padding: 30px;
                }}
                .stats-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 20px;
                    margin-bottom: 30px;
                }}
                .stat-card {{
                    background: #f8f9fa;
                    border-left: 4px solid #007bff;
                    padding: 20px;
                    border-radius: 5px;
                }}
                .stat-card h3 {{
                    margin: 0 0 10px;
                    color: #495057;
                    font-size: 1.1em;
                }}
                .stat-value {{
                    font-size: 2em;
                    font-weight: bold;
                    color: #007bff;
                }}
                .section {{
                    margin-bottom: 30px;
                }}
                .section h2 {{
                    border-bottom: 2px solid #e9ecef;
                    padding-bottom: 10px;
                    color: #495057;
                }}
                .error-card {{
                    border: 1px solid #e9ecef;
                    border-radius: 8px;
                    margin-bottom: 15px;
                    overflow: hidden;
                }}
                .error-header {{
                    padding: 15px 20px;
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    font-weight: 500;
                }}
                .error-body {{
                    padding: 0 20px 20px;
                    font-family: 'Courier New', monospace;
                    font-size: 0.9em;
                    background-color: #f8f9fa;
                    border-top: 1px solid #e9ecef;
                }}
                .severity-critical {{ background-color: #f8d7da; color: #721c24; }}
                .severity-high {{ background-color: #ffeaa7; color: #856404; }}
                .severity-medium {{ background-color: #fff3cd; color: #856404; }}
                .severity-low {{ background-color: #d4edda; color: #155724; }}
                .severity-info {{ background-color: #cce5ff; color: #004085; }}
                .timestamp {{ color: #6c757d; font-size: 0.9em; }}
                .context-info {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                    gap: 10px;
                    margin: 10px 0;
                    font-size: 0.85em;
                }}
                .context-item {{
                    background: white;
                    padding: 8px 12px;
                    border-radius: 4px;
                    border: 1px solid #dee2e6;
                }}
                .context-label {{
                    font-weight: 600;
                    color: #495057;
                }}
                .footer {{
                    background: #f8f9fa;
                    padding: 20px 30px;
                    text-align: center;
                    border-top: 1px solid #e9ecef;
                    color: #6c757d;
                }}
                .chart-placeholder {{
                    height: 200px;
                    background: linear-gradient(45deg, #f8f9fa 25%, transparent 25%),
                               linear-gradient(-45deg, #f8f9fa 25%, transparent 25%),
                               linear-gradient(45deg, transparent 75%, #f8f9fa 75%),
                               linear-gradient(-45deg, transparent 75%, #f8f9fa 75%);
                    background-size: 20px 20px;
                    background-position: 0 0, 0 10px, 10px -10px, -10px 0px;
                    border: 2px dashed #dee2e6;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    color: #6c757d;
                    font-style: italic;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🤖 Discord Bot Error Report</h1>
                    <p>Period: {period_start.strftime('%Y-%m-%d %H:%M:%S')} - {period_end.strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>

                <div class="content">
                    <div class="stats-grid">
                        <div class="stat-card">
                            <h3>📊 Total Errors</h3>
                            <div class="stat-value">{stats.get('total_errors', 0)}</div>
                        </div>
                        <div class="stat-card">
                            <h3>🔴 Critical Issues</h3>
                            <div class="stat-value">{stats.get('critical_count', 0)}</div>
                        </div>
                        <div class="stat-card">
                            <h3>📈 Error Rate</h3>
                            <div class="stat-value">{stats.get('errors_per_hour', 0):.1f}/hr</div>
                        </div>
                        <div class="stat-card">
                            <h3>🎯 Most Affected</h3>
                            <div class="stat-value" style="font-size: 1.2em;">{stats.get('top_category', 'N/A')}</div>
                        </div>
                    </div>
        """

		# Add severity breakdown
		if by_severity:
			html_content += """
                    <div class="section">
                        <h2>📈 Error Breakdown by Severity</h2>
                        <div class="stats-grid">
            """
			for severity in Severity:
				count = len(by_severity.get(severity, []))
				if count > 0:
					html_content += f"""
                        <div class="stat-card">
                            <h3>{severity.value}</h3>
                            <div class="stat-value">{count}</div>
                        </div>
                    """
			html_content += """
                        </div>
                    </div>
            """

		# Add category breakdown
		if by_category:
			html_content += """
                    <div class="section">
                        <h2>🏷️ Error Breakdown by Category</h2>
                        <div class="stats-grid">
            """
			for category, category_errors in sorted(by_category.items(), key=lambda x: len(x[1]), reverse=True):
				count = len(category_errors)
				html_content += f"""
                    <div class="stat-card">
                        <h3>{category.value}</h3>
                        <div class="stat-value">{count}</div>
                    </div>
                """
			html_content += """
                        </div>
                    </div>
            """

		# Add detailed errors
		if errors:
			html_content += """
                    <div class="section">
                        <h2>🔍 Detailed Error Log</h2>
            """

			# Group and display errors
			for severity in [Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM, Severity.LOW, Severity.INFO]:
				severity_errors = by_severity.get(severity, [])
				if not severity_errors:
					continue

				html_content += f"""
                        <h3>{severity.value} ({len(severity_errors)} errors)</h3>
                """

				for error in severity_errors[:10]:  # Limit to 10 per severity
					severity_class = f"severity-{severity.name.lower()}"
					html_content += f"""
                        <div class="error-card">
                            <div class="error-header {severity_class}">
                                <span><strong>{error.category.value}</strong> - {severity.value}</span>
                                <span class="timestamp">{error.timestamp.strftime('%H:%M:%S')}</span>
                            </div>
                    """

					if any([error.guild_id, error.user_id, error.channel_id, error.command]):
						html_content += """
                            <div class="context-info">
                        """
						if error.guild_id:
							html_content += f"""
                                <div class="context-item">
                                    <span class="context-label">Guild:</span> {error.guild_id}
                                </div>
                            """
						if error.user_id:
							html_content += f"""
                                <div class="context-item">
                                    <span class="context-label">User:</span> {error.user_id}
                                </div>
                            """
						if error.channel_id:
							html_content += f"""
                                <div class="context-item">
                                    <span class="context-label">Channel:</span> {error.channel_id}
                                </div>
                            """
						if error.command:
							html_content += f"""
                                <div class="context-item">
                                    <span class="context-label">Command:</span> {error.command}
                                </div>
                            """
						html_content += """
                            </div>
                        """

					html_content += f"""
                            <div class="error-body">
                                <div><strong>Error:</strong> {html.escape(error.error_message)}</div>
                    """

					if error.stack_trace:
						# Show first few lines of stack trace
						stack_lines = error.stack_trace.split('\n')[:5]
						html_content += f"""
                                <div style="margin-top: 10px;"><strong>Stack Trace:</strong></div>
                                <pre style="margin: 5px 0; white-space: pre-wrap;">{html.escape('\n'.join(stack_lines))}{'...' if len(error.stack_trace.split('\n')) > 5 else ''}</pre>
                        """

					html_content += """
                            </div>
                        </div>
                    """

				if len(severity_errors) > 10:
					html_content += f"""
                        <p style="text-align: center; color: #6c757d; font-style: italic;">
                            ... and {len(severity_errors) - 10} more {severity.value.lower()} errors
                        </p>
                    """

			html_content += """
                    </div>
            """

		html_content += f"""
                </div>

                <div class="footer">
                    <p>Generated by Discord Bot Error Notifier at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                    <p>This is an automated message. Please review the errors above and take appropriate action.</p>
                </div>
            </div>
        </body>
        </html>
        """

		return html_content

	@staticmethod
	def create_text_summary(
			errors: List[ErrorContext],
			stats: Dict[str, Any],
			period_start: datetime,
			period_end: datetime
	) -> str:
		"""Create a plain text version for email clients that don't support HTML"""

		text_content = f"""
🤖 DISCORD BOT ERROR REPORT
════════════════════════════════════════

📅 Report Period: {period_start.strftime('%Y-%m-%d %H:%M:%S')} - {period_end.strftime('%Y-%m-%d %H:%M:%S')}

📊 SUMMARY STATISTICS
────────────────────
• Total Errors: {stats.get('total_errors', 0)}
• Critical Issues: {stats.get('critical_count', 0)}
• Error Rate: {stats.get('errors_per_hour', 0):.1f} errors/hour
• Most Affected Category: {stats.get('top_category', 'N/A')}

"""

		# Group errors by category and severity
		by_category = defaultdict(list)
		by_severity = defaultdict(list)

		for error in errors:
			by_category[error.category].append(error)
			by_severity[error.severity].append(error)

		# Severity breakdown
		if by_severity:
			text_content += "🔍 SEVERITY BREAKDOWN\n"
			text_content += "────────────────────\n"
			for severity in Severity:
				count = len(by_severity.get(severity, []))
				if count > 0:
					text_content += f"• {severity.value}: {count} errors\n"
			text_content += "\n"

		# Category breakdown
		if by_category:
			text_content += "🏷️ CATEGORY BREAKDOWN\n"
			text_content += "─────────────────────\n"
			for category, category_errors in sorted(by_category.items(), key=lambda x: len(x[1]), reverse=True):
				count = len(category_errors)
				text_content += f"• {category.value}: {count} errors\n"
			text_content += "\n"

		# Detailed errors
		if errors:
			text_content += "📋 DETAILED ERROR LOG\n"
			text_content += "═════════════════════\n\n"

			for i, error in enumerate(errors[:20], 1):  # Limit to 20 errors in text
				text_content += f"[{i:2d}] {error.severity.value} | {error.category.value}\n"
				text_content += f"     Time: {error.timestamp.strftime('%Y-%m-%d %H:%M:%S')}\n"

				if error.guild_id or error.user_id or error.channel_id or error.command:
					context_parts = []
					if error.guild_id:
						context_parts.append(f"Guild:{error.guild_id}")
					if error.user_id:
						context_parts.append(f"User:{error.user_id}")
					if error.channel_id:
						context_parts.append(f"Channel:{error.channel_id}")
					if error.command:
						context_parts.append(f"Command:{error.command}")
					text_content += f"     Context: {' | '.join(context_parts)}\n"

				text_content += f"     Error: {error.error_message}\n"

				if error.stack_trace:
					# Show first line of stack trace in text version
					first_line = error.stack_trace.split('\n')[0]
					text_content += f"     Stack: {first_line}\n"

				text_content += "\n"

			if len(errors) > 20:
				text_content += f"... and {len(errors) - 20} more errors (see HTML version for complete details)\n\n"

		text_content += f"""
────────────────────────────────────────
Generated by Discord Bot Error Notifier
{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

This is an automated message. Please review 
the errors above and take appropriate action.
        """

		return text_content.strip()


class EnhancedErrorNotifier:
	"""
    Significantly enhanced error notification system with:
    - Rich HTML email formatting
    - Error categorization and severity analysis
    - Advanced filtering and rate limiting
    - Comprehensive statistics and analytics
    - Attachment support for logs
    - Multiple notification channels
    - Performance monitoring
    - Error correlation and pattern detection
    """

	def __init__(
			self,
			email: str,
			app_password: str,
			interval: int = 300,
			max_errors_per_email: int = 100,
			enable_html: bool = True,
			enable_attachments: bool = True,
			severity_threshold: Severity = Severity.LOW
	):
		"""
        Enhanced initialization with comprehensive configuration

        :param email: Gmail address (you@gmail.com)
        :param app_password: Gmail App Password (NOT your normal password!)
        :param interval: How often to send error batches (seconds)
        :param max_errors_per_email: Maximum errors to include per email
        :param enable_html: Whether to send rich HTML emails
        :param enable_attachments: Whether to include log attachments
        :param severity_threshold: Minimum severity level to report
        """
		self.email = email
		self.app_password = app_password
		self.interval = interval
		self.max_errors_per_email = max_errors_per_email
		self.enable_html = enable_html
		self.enable_attachments = enable_attachments
		self.severity_threshold = severity_threshold

		# Error storage with rich context
		self.errors: List[ErrorContext] = []
		self.error_counter = Counter()

		# Rate limiting and spam prevention
		self.error_patterns = defaultdict(deque)  # Pattern detection
		self.last_sent = datetime.now()
		self.consecutive_failures = 0
		self.max_failures = 3

		# Statistics tracking
		self.stats = {
			'total_processed': 0,
			'total_sent': 0,
			'last_reset': datetime.now(),
			'uptime_start': datetime.now()
		}

		# Advanced features
		self.correlation_window = timedelta(minutes=5)
		self.pattern_threshold = 5  # Similar errors in window

		print(f"🚀 Enhanced Error Notifier initialized")
		print(f"   📧 Email: {email}")
		print(f"   ⏱️ Interval: {interval}s")
		print(f"   📊 Max errors per email: {max_errors_per_email}")
		print(f"   🎨 HTML enabled: {enable_html}")
		print(f"   📎 Attachments enabled: {enable_attachments}")
		print(f"   🔍 Severity threshold: {severity_threshold.value}")

	def log_error(
			self,
			error: str,
			guild_id: Optional[str] = None,
			user_id: Optional[str] = None,
			channel_id: Optional[str] = None,
			command: Optional[str] = None,
			stack_trace: Optional[str] = None,
			additional_data: Optional[Dict[str, Any]] = None,
			override_severity: Optional[Severity] = None,
			override_category: Optional[ErrorCategory] = None
	):
		"""
        Enhanced error logging with rich context and automatic analysis
        """
		try:
			# Analyze error if not overridden
			if override_severity and override_category:
				severity, category = override_severity, override_category
			else:
				severity, category = ErrorAnalyzer.analyze_error(error, stack_trace)

			# Skip if below threshold
			if self._severity_order(severity) < self._severity_order(self.severity_threshold):
				return

			# Create rich error context
			error_context = ErrorContext(
				timestamp=datetime.now(),
				severity=severity,
				category=category,
				error_message=error,
				guild_id=guild_id,
				user_id=user_id,
				channel_id=channel_id,
				command=command,
				stack_trace=stack_trace,
				additional_data=additional_data or {}
			)

			# Pattern detection for spam prevention
			pattern_key = self._generate_pattern_key(error_context)
			current_time = datetime.now()

			# Clean old patterns
			self._clean_old_patterns(current_time)

			# Add to pattern tracking
			self.error_patterns[pattern_key].append(current_time)

			# Check if this is a repeated pattern
			recent_count = len([
				t for t in self.error_patterns[pattern_key]
				if current_time - t <= self.correlation_window
			])

			if recent_count <= self.pattern_threshold:
				# Add error if not spam
				self.errors.append(error_context)
				self.error_counter[f"{category.value}: {error}"] += 1
				self.stats['total_processed'] += 1

				print(f"📝 Logged {severity.value} error: {category.value}")

				# Immediate send for critical errors
				if severity == Severity.CRITICAL:
					asyncio.create_task(self._send_immediate_alert(error_context))
			else:
				print(f"🚫 Suppressed repeated error pattern (count: {recent_count})")

		except Exception as e:
			print(f"❌ Failed to log error: {e}")

	def _severity_order(self, severity: Severity) -> int:
		"""Get numeric order for severity comparison"""
		order = {
			Severity.CRITICAL: 4,
			Severity.HIGH: 3,
			Severity.MEDIUM: 2,
			Severity.LOW: 1,
			Severity.INFO: 0
		}
		return order.get(severity, 0)

	def _generate_pattern_key(self, error_context: ErrorContext) -> str:
		"""Generate a key for pattern detection"""
		# Normalize error message for pattern matching
		normalized = re.sub(r'\d+', 'N', error_context.error_message)
		normalized = re.sub(r'[\'"][^\'\"]*[\'"]', 'STR', normalized)

		return f"{error_context.category.name}:{normalized}"

	def _clean_old_patterns(self, current_time: datetime):
		"""Clean old pattern tracking data"""
		cutoff = current_time - self.correlation_window

		for pattern_key in list(self.error_patterns.keys()):
			# Remove old timestamps
			while (self.error_patterns[pattern_key] and
				   self.error_patterns[pattern_key][0] < cutoff):
				self.error_patterns[pattern_key].popleft()

			# Remove empty deques
			if not self.error_patterns[pattern_key]:
				del self.error_patterns[pattern_key]

	async def _send_immediate_alert(self, error_context: ErrorContext):
		"""Send immediate alert for critical errors"""
		try:
			subject = f"🚨 CRITICAL ERROR ALERT - {error_context.category.value}"

			if self.enable_html:
				body = EmailTemplate.create_error_summary_html(
					[error_context],
					{'total_errors': 1, 'critical_count': 1, 'errors_per_hour': 0,
					 'top_category': error_context.category.value},
					error_context.timestamp - timedelta(seconds=1),
					error_context.timestamp
				)
			else:
				body = EmailTemplate.create_text_summary(
					[error_context],
					{'total_errors': 1, 'critical_count': 1, 'errors_per_hour': 0,
					 'top_category': error_context.category.value},
					error_context.timestamp - timedelta(seconds=1),
					error_context.timestamp
				)

			await asyncio.to_thread(self._send_email, subject, body)
			print(f"🚨 Sent immediate critical alert")

		except Exception as e:
			print(f"❌ Failed to send immediate alert: {e}")

	def _calculate_statistics(self, errors: List[ErrorContext]) -> Dict[str, Any]:
		"""Calculate comprehensive error statistics"""
		if not errors:
			return {
				'total_errors': 0,
				'critical_count': 0,
				'errors_per_hour': 0,
				'top_category': 'N/A',
				'top_severity': 'N/A',
				'error_rate_trend': 'stable',
				'unique_patterns': 0
			}

		# Basic counts
		total_errors = len(errors)
		critical_count = len([e for e in errors if e.severity == Severity.CRITICAL])
		high_count = len([e for e in errors if e.severity == Severity.HIGH])

		# Time-based analysis
		if errors:
			time_span = (errors[-1].timestamp - errors[0].timestamp).total_seconds() / 3600
			errors_per_hour = total_errors / max(time_span, 0.1)
		else:
			errors_per_hour = 0

		# Category analysis
		categories = Counter(e.category for e in errors)
		top_category = categories.most_common(1)[0][0].value if categories else 'N/A'

		# Severity analysis
		severities = Counter(e.severity for e in errors)
		top_severity = severities.most_common(1)[0][0].value if severities else 'N/A'

		# Pattern analysis
		patterns = set(self._generate_pattern_key(e) for e in errors)
		unique_patterns = len(patterns)

		return {
			'total_errors': total_errors,
			'critical_count': critical_count,
			'high_count': high_count,
			'errors_per_hour': errors_per_hour,
			'top_category': top_category,
			'top_severity': top_severity,
			'unique_patterns': unique_patterns,
			'categories': dict(categories),
			'severities': dict(severities),
			'time_span_hours': time_span if errors else 0
		}

	def _create_log_attachment(self, errors: List[ErrorContext]) -> Optional[str]:
		"""Create a detailed log file for attachment"""
		if not self.enable_attachments or not errors:
			return None

		try:
			log_content = []
			log_content.append(f"Discord Bot Error Log - {datetime.now().isoformat()}")
			log_content.append("=" * 80)
			log_content.append("")

			for i, error in enumerate(errors, 1):
				log_content.append(f"[{i:03d}] {error.timestamp.isoformat()}")
				log_content.append(f"Severity: {error.severity.value}")
				log_content.append(f"Category: {error.category.value}")
				log_content.append(f"Message: {error.error_message}")

				if error.guild_id:
					log_content.append(f"Guild ID: {error.guild_id}")
				if error.user_id:
					log_content.append(f"User ID: {error.user_id}")
				if error.channel_id:
					log_content.append(f"Channel ID: {error.channel_id}")
				if error.command:
					log_content.append(f"Command: {error.command}")

				if error.stack_trace:
					log_content.append("Stack Trace:")
					for line in error.stack_trace.split('\n'):
						log_content.append(f"  {line}")

				if error.additional_data:
					log_content.append("Additional Data:")
					log_content.append(f"  {json.dumps(error.additional_data, indent=2)}")

				log_content.append("-" * 80)
				log_content.append("")

			# Write to temporary file
			filename = f"error_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
			with open(filename, 'w', encoding='utf-8') as f:
				f.write('\n'.join(log_content))

			return filename

		except Exception as e:
			print(f"❌ Failed to create log attachment: {e}")
			return None

	def _send_email(self, subject: str, body: str, attachment_path: Optional[str] = None):
		"""Enhanced email sending with HTML support and attachments"""
		try:
			# Create message
			if self.enable_html and '<!DOCTYPE html>' in body:
				msg = MIMEMultipart('alternative')

				# Create text version from HTML (simplified)
				text_body = EmailTemplate.create_text_summary(
					self.errors[-10:] if self.errors else [],
					self._calculate_statistics(self.errors),
					self.last_sent,
					datetime.now()
				)

				# Add both versions
				msg.attach(MIMEText(text_body, 'plain', 'utf-8'))
				msg.attach(MIMEText(body, 'html', 'utf-8'))
			else:
				msg = MIMEText(body, 'plain', 'utf-8')

			msg["Subject"] = subject
			msg["From"] = self.email
			msg["To"] = self.email

			# Add attachment if provided
			if attachment_path and os.path.exists(attachment_path):
				try:
					with open(attachment_path, 'rb') as f:
						attach = MIMEBase('application', 'octet-stream')
						attach.set_payload(f.read())
						encoders.encode_base64(attach)
						attach.add_header(
							'Content-Disposition',
							f'attachment; filename= {os.path.basename(attachment_path)}'
						)

						if isinstance(msg, MIMEMultipart):
							msg.attach(attach)
						else:
							# Convert to multipart to add attachment
							new_msg = MIMEMultipart()
							new_msg["Subject"] = subject
							new_msg["From"] = self.email
							new_msg["To"] = self.email
							new_msg.attach(msg)
							new_msg.attach(attach)
							msg = new_msg

					print(f"📎 Added attachment: {attachment_path}")
				except Exception as e:
					print(f"❌ Failed to add attachment: {e}")

			# Send email with enhanced SSL context
			context = ssl.create_default_context(cafile=certifi.where())
			context.check_hostname = False
			context.verify_mode = ssl.CERT_NONE

			with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
				server.login(self.email, self.app_password)
				server.sendmail(self.email, self.email, msg.as_string())

			self.consecutive_failures = 0
			self.stats['total_sent'] += 1

			# Cleanup attachment file
			if attachment_path and os.path.exists(attachment_path):
				try:
					os.remove(attachment_path)
					print(f"🗑️ Cleaned up attachment: {attachment_path}")
				except Exception as e:
					print(f"⚠️ Failed to cleanup attachment: {e}")

		except Exception as e:
			self.consecutive_failures += 1
			print(f"❌ Failed to send email (attempt {self.consecutive_failures}): {e}")

			if self.consecutive_failures >= self.max_failures:
				print(f"🚫 Maximum email failures reached. Disabling email notifications temporarily.")

	async def start_loop(self):
		"""Enhanced background loop with comprehensive error processing"""
		print(f"🔄 Starting enhanced error notification loop (interval: {self.interval}s)")

		while True:
			try:
				await asyncio.sleep(self.interval)

				if not self.errors:
					continue

				print(f"📨 Processing {len(self.errors)} errors for email notification")

				# Skip if too many consecutive failures
				if self.consecutive_failures >= self.max_failures:
					print(f"⏭️ Skipping email send due to consecutive failures")
					self.errors.clear()
					self.error_counter.clear()
					continue

				# Prepare errors for email (limit quantity)
				errors_to_send = self.errors[:self.max_errors_per_email]
				period_start = self.last_sent
				period_end = datetime.now()

				# Calculate comprehensive statistics
				stats = self._calculate_statistics(errors_to_send)

				# Create subject with priority indicator
				critical_count = stats.get('critical_count', 0)
				high_count = stats.get('high_count', 0)
				total_count = stats.get('total_errors', 0)

				if critical_count > 0:
					priority = "🚨 CRITICAL"
				elif high_count > 0:
					priority = "⚠️ HIGH"
				else:
					priority = "📊"

				subject = f"{priority} Bot Errors Report - {total_count} errors"
				if critical_count > 0:
					subject += f" ({critical_count} critical)"

				# Generate email content
				if self.enable_html:
					body = EmailTemplate.create_error_summary_html(
						errors_to_send, stats, period_start, period_end
					)
				else:
					body = EmailTemplate.create_text_summary(
						errors_to_send, stats, period_start, period_end
					)

				# Create attachment if enabled
				attachment_path = None
				if self.enable_attachments and len(self.errors) > 10:
					attachment_path = self._create_log_attachment(self.errors)

				# Send email
				await asyncio.to_thread(self._send_email, subject, body, attachment_path)

				# Update tracking
				self.last_sent = period_end
				processed_count = len(self.errors)

				# Clear processed errors
				self.errors.clear()
				self.error_counter.clear()

				print(f"✅ Sent error report with {processed_count} errors")

				# Print statistics
				uptime = datetime.now() - self.stats['uptime_start']
				print(f"📊 Session stats: {self.stats['total_processed']} processed, "
					  f"{self.stats['total_sent']} emails sent, "
					  f"uptime: {str(uptime).split('.')[0]}")

			except asyncio.CancelledError:
				print("🛑 Error notification loop cancelled")
				break
			except Exception as e:
				print(f"❌ Error in notification loop: {e}")
				# Continue running despite errors in the loop itself

	def get_statistics(self) -> Dict[str, Any]:
		"""Get comprehensive notifier statistics"""
		uptime = datetime.now() - self.stats['uptime_start']

		return {
			'uptime_seconds': uptime.total_seconds(),
			'total_processed': self.stats['total_processed'],
			'total_sent': self.stats['total_sent'],
			'pending_errors': len(self.errors),
			'consecutive_failures': self.consecutive_failures,
			'active_patterns': len(self.error_patterns),
			'last_sent': self.last_sent.isoformat(),
			'severity_threshold': self.severity_threshold.value,
			'configuration': {
				'interval': self.interval,
				'max_errors_per_email': self.max_errors_per_email,
				'enable_html': self.enable_html,
				'enable_attachments': self.enable_attachments
			}
		}

	def reset_statistics(self):
		"""Reset all statistics counters"""
		self.stats.update({
			'total_processed': 0,
			'total_sent': 0,
			'last_reset': datetime.now()
		})
		print("📊 Statistics reset")

	def clear_errors(self):
		"""Manually clear all pending errors"""
		count = len(self.errors)
		self.errors.clear()
		self.error_counter.clear()
		print(f"🗑️ Cleared {count} pending errors")

	def set_severity_threshold(self, threshold: Severity):
		"""Change the severity threshold for notifications"""
		old_threshold = self.severity_threshold
		self.severity_threshold = threshold
		print(f"🔄 Changed severity threshold from {old_threshold.value} to {threshold.value}")


# For backward compatibility, create an alias
ErrorNotifier = EnhancedErrorNotifier


# Example usage and test function
async def test_enhanced_notifier():
	"""Test function demonstrating the enhanced features"""
	print("🧪 Testing Enhanced Error Notifier...")

	# Initialize with enhanced features
	notifier = EnhancedErrorNotifier(
		email=os.getenv("EMAIL"),
		app_password=os.getenv("PASSWORD"),
		interval=30,  # Shorter for testing
		enable_html=True,
		enable_attachments=True,
		severity_threshold=Severity.INFO
	)

	# Test various error types
	test_errors = [
		{
			'error': 'Database connection timeout after 30 seconds',
			'guild_id': '123456789',
			'command': 'balance',
			'additional_data': {'timeout': 30, 'retry_count': 3}
		},
		{
			'error': 'Discord API rate limit exceeded: 429 Too Many Requests',
			'user_id': '987654321',
			'channel_id': '555666777',
		},
		{
			'error': 'Invalid user permission for admin command',
			'guild_id': '123456789',
			'user_id': '111222333',
			'command': 'ban'
		},
		{
			'error': 'MongoDB connection lost - attempting reconnection',
			'override_severity': Severity.CRITICAL,
			'override_category': ErrorCategory.DATABASE
		},
		{
			'error': 'Command parsing failed: missing required argument',
			'command': 'shop buy',
			'additional_data': {'args': ['buy'], 'expected': ['buy', 'item_name']}
		}
	]

	# Log test errors
	for i, test_error in enumerate(test_errors):
		error_msg = test_error.pop('error')
		notifier.log_error(error_msg, **test_error)
		print(f"✅ Logged test error {i + 1}")
		await asyncio.sleep(0.1)  # Small delay between errors

	# Print statistics
	stats = notifier.get_statistics()
	print(f"📊 Current statistics: {json.dumps(stats, indent=2)}")

	print("🧪 Test complete - errors logged and ready for processing")

	return notifier


# if __name__ == "__main__":
# 	# Run test if executed directly
# 	async def main():
# 		notifier = await test_enhanced_notifier()
#
# 		# Start the notification loop for a short test
# 		print("🔄 Starting notification loop for 60 seconds...")
#
# 		try:
# 			# Run the loop for 1 minute for testing
# 			await asyncio.wait_for(notifier.start_loop(), timeout=60.0)
# 		except asyncio.TimeoutError:
# 			print("⏰ Test timeout - stopping")
# 		except KeyboardInterrupt:
# 			print("⌨️ Interrupted by user")
#
#
# 	asyncio.run(main())