from collections import defaultdict
import html
from datetime import datetime
from typing import List, Dict, Any

from loggers.reporting_types import ErrorContext, Severity


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