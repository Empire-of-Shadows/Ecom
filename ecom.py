import asyncio
import os
import signal
from contextlib import asynccontextmanager
from typing import Dict, Optional

import discord
from tabulate import tabulate

import http.server
import socketserver
import threading
import time
from dotenv import load_dotenv

from activity_buffer.activitybuffer import ActivityBuffer
from core.bot import bot, TOKEN
from core.sync import attach_databases, load_cogs
from database.DatabaseManager import db_manager
from ecom_system.leveling.leveling import LevelingSystem
from loggers.log_dispacher import EnhancedErrorNotifier, Severity
from loggers.logger_setup import setup_application_logging, EmailErrorHandler, log_performance, log_context
from status.idle import rotate_status
load_dotenv()

# Global activity buffer instance
activity_buffer: Optional[ActivityBuffer] = None


# Enhanced logging with performance tracking
logger = setup_application_logging(
    app_name="ecom",
    log_level=20,  # INFO level
    log_dir="logs",
    enable_performance_logging=True,
    max_file_size=20 * 1024 * 1024,  # 20 MB
    backup_count=10
)


# Enhanced usage with all features
notifier = EnhancedErrorNotifier(
    email=os.getenv("EMAIL"),
    app_password=os.getenv("PASSWORD"),
    interval=300,  # 5 minutes
    max_errors_per_email=50,
    enable_html=True,
    enable_attachments=True,
    severity_threshold=Severity.LOW
)


email_handler = EmailErrorHandler(notifier)
logger.addHandler(email_handler)

# Startup metrics
startup_metrics: Dict[str, Optional[float]] = {
    "start_time": None,
    "ready_time": None,
    "total_startup_time": None,
    "database_time": None,
    "cog_load_time": None,
    "sync_time": None
}




class HealthCheckHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"status": "healthy", "timestamp": ' + str(time.time()).encode() + b'}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Disable logging


def start_health_server(port=8090):
    """Start a health check server with better error handling"""
    try:
        with socketserver.TCPServer(("0.0.0.0", port), HealthCheckHandler) as httpd:
            print(f"✅ Health check server running on port {port}")
            httpd.serve_forever()
    except Exception as e:
        print(f"❌ Failed to start health server: {e}")
        # Don't exit - let the bot continue running


# Start a health server in a separate thread
def initialize_health_server():
    """Initialize the health server with delay to ensure the bot starts first"""

    def delayed_start():
        time.sleep(2)  # Wait 2 seconds for the bot to initialize
        start_health_server()

    health_thread = threading.Thread(target=delayed_start, daemon=True)
    health_thread.start()
    return health_thread


# In your main function, call this instead:
health_thread = initialize_health_server()

@asynccontextmanager
async def startup_phase(phase_name: str):
    """Context manager to track startup phase timing."""
    start_time = time.perf_counter()
    logger.info(f"🔄 Starting phase: {phase_name}")

    try:
        yield
        end_time = time.perf_counter()
        duration = end_time - start_time
        startup_metrics[f"{phase_name.lower().replace(' ', '_')}_time"] = duration
        logger.info(f"✅ Completed phase: {phase_name} in {duration:.4f}s")
    except Exception as e:
        end_time = time.perf_counter()
        duration = end_time - start_time
        logger.error(f"❌ Failed phase: {phase_name} after {duration:.4f}s - {str(e)}")
        raise


@log_performance("bot_ready_sequence")
async def on_ready():
    """
    Handles the bot's readiness state and performs initialization tasks when the bot is ready.
    Enhanced with performance logging and structured error handling.
    """
    global activity_buffer
    startup_metrics["ready_time"] = time.perf_counter()

    with log_context(logger, "Bot Ready Sequence", level=20):  # INFO level
        logger.info(f"🚀 Bot logged in as {bot.user}")
        logger.info(
            f"📊 Connected to {len(bot.guilds)} guilds with {sum(g.member_count or 0 for g in bot.guilds)} total members")

        # Database is now initialized before bot starts, so we skip database attachment here
        logger.info("✅ Database already initialized during startup")

        # Phase 1: Activity Buffer Initialization
        try:
            async with startup_phase("Activity Buffer Initialization"):
                activity_buffer = ActivityBuffer(
                    flush_interval=60,  # Flush every minute in production
                    max_buffer_size=2000,  # Larger buffer for busy servers
                    high_watermark=1000,  # Early flush at 1000 events
                    jitter_seconds=10.0,  # Add some randomization
                    db_path="data/activity.db"  # Store in a data directory
                )
                activity_buffer.start()

                # Make activity buffer accessible to cogs via bot instance
                bot.activity_buffer = activity_buffer

                logger.info("✅ Activity buffer started successfully")
        except Exception as activity_error:
            logger.warning(f"⚠️ Non-critical error during activity buffer initialization: {activity_error}",
                           exc_info=True)
            # Set None on the bot instance if initialization failed
            bot.activity_buffer = None

        # Phase 2: Cog Loading
        try:
            async with startup_phase("Cog Loading"):
                await load_cogs()
        except Exception as cog_error:
            logger.error(f"❌ Error during cog loading: {cog_error}", exc_info=True)

        # Phase 3: Command Synchronization
        try:
            async with startup_phase("Command Sync"):
                synced_global = await bot.tree.sync()
                logger.info(f"🔄 Resynced global commands: {len(synced_global)} commands registered.")
        except Exception as sync_error:
            logger.error(f"❌ Error during command sync: {sync_error}", exc_info=True)

        # Phase 4: Status and Finalization
        try:
            async with startup_phase("Status Setup"):
                await bot.change_presence(status=discord.Status.online)
                if not rotate_status.is_running():
                    rotate_status.start()
        except Exception as status_error:
            logger.error(f"❌ Error during status setup: {status_error}", exc_info=True)

        # Log startup completion metrics
        startup_metrics["total_startup_time"] = time.perf_counter() - startup_metrics["ready_time"]
        log_startup_summary()

        logger.info(f"🎉 Bot is fully online and operational!")

        # Log all commands (non-blocking)
        try:
            await log_all_commands()
        except Exception as cmd_log_error:
            logger.error(f"❌ Error logging commands: {cmd_log_error}")

@bot.event
async def on_error(event, *args, **kwargs):
    """Handle general bot errors and send email notifications."""
    error_msg = f"Error in event '{event}': {args} {kwargs}"
    logger.error(error_msg, exc_info=True)
    notifier.log_error(error_msg)

@bot.event
async def on_command_error(ctx, error):
    """Handle command errors and send email notifications."""
    error_msg = f"Command error in '{ctx.command}': {str(error)}"
    logger.error(error_msg, exc_info=True)
    notifier.log_error(error_msg)


def log_startup_summary():
    """Log a comprehensive startup performance summary."""
    total_time = startup_metrics.get("total_startup_time", 0)

    performance_data = [
        ["Phase", "Duration (s)", "Percentage"],
        ["Database Attachment", f"{startup_metrics.get('database_attachment_time', 0):.4f}",
         f"{(startup_metrics.get('database_attachment_time', 0) / total_time * 100):.1f}%" if total_time > 0 else "0%"],
        ["Auto-flush Init", f"{startup_metrics.get('auto-flush_initialization_time', 0):.4f}",
         f"{(startup_metrics.get('auto-flush_initialization_time', 0) / total_time * 100):.1f}%" if total_time > 0 else "0%"],
        ["Cog Loading", f"{startup_metrics.get('cog_loading_time', 0):.4f}",
         f"{(startup_metrics.get('cog_loading_time', 0) / total_time * 100):.1f}%" if total_time > 0 else "0%"],
        ["Command Sync", f"{startup_metrics.get('command_sync_time', 0):.4f}",
         f"{(startup_metrics.get('command_sync_time', 0) / total_time * 100):.1f}%" if total_time > 0 else "0%"],
        ["Status Setup", f"{startup_metrics.get('status_setup_time', 0):.4f}",
         f"{(startup_metrics.get('status_setup_time', 0) / total_time * 100):.1f}%" if total_time > 0 else "0%"],
        ["TOTAL", f"{total_time:.4f}", "100%"]
    ]

    performance_table = tabulate(performance_data[1:], headers=performance_data[0], tablefmt="fancy_grid")
    logger.info(f"📈 Startup Performance Summary:\n{performance_table}")


bot.event(on_ready)  # Register for the event


@log_performance("command_logging")
async def log_all_commands():
    """
    Logs all commands (prefix and slash) in a table format with performance tracking.
    """
    # Prepare data for prefix commands
    prefix_commands = [
        [cmd.name, cmd.help or "No description provided", ", ".join(cmd.aliases) or "None"]
        for cmd in bot.commands
    ]

    if prefix_commands:
        prefix_table = tabulate(
            prefix_commands, headers=["Prefix Command", "Description", "Aliases"], tablefmt="fancy_grid"
        )
        logger.info(f"📝 Registered Prefix Commands ({len(prefix_commands)}):\n{prefix_table}")
    else:
        logger.info("📝 No prefix commands registered")

    # Prepare data for slash commands
    slash_commands = [
        [cmd.name, cmd.description or "No description provided", cmd.parent.name if cmd.parent else "N/A"]
        for cmd in bot.tree.get_commands()
    ]

    if slash_commands:
        slash_table = tabulate(
            slash_commands, headers=["Slash Command", "Description", "Parent Command (Group)"], tablefmt="fancy_grid"
        )
        logger.info(f"⚡ Registered Slash Commands ({len(slash_commands)}):\n{slash_table}")
    else:
        logger.info("⚡ No slash commands registered")


@log_performance("graceful_shutdown")
async def shutdown_handler():
    """
    Handles the graceful shutdown of background tasks and the bot with performance logging.
    """
    logger.info("🛑 Initiating graceful shutdown...")

    shutdown_tasks = []

    # Stop the activity buffer first to flush remaining events
    try:
        if activity_buffer:
            logger.info("🔄 Stopping activity buffer...")
            await activity_buffer.stop()
            logger.info("✅ Activity buffer stopped and flushed")
    except Exception as e:
        logger.error(f"❌ Error stopping activity buffer: {e}")

    # Stop status rotation
    try:
        if rotate_status.is_running():
            rotate_status.cancel()
            logger.info("✅ Status rotation stopped")
    except Exception as e:
        logger.error(f"❌ Error stopping status rotation: {e}")

    # Close database connections (if applicable)
    try:
        # Add any database cleanup here if needed
        logger.info("✅ Database connections cleaned up")
    except Exception as e:
        logger.error(f"❌ Error during database cleanup: {e}")

    # Close the bot connection
    try:
        if not bot.is_closed():
            await bot.close()
            logger.info("✅ Bot connection closed")
    except Exception as shutdown_error:
        logger.error(f"❌ Error during bot shutdown: {shutdown_error}")

    logger.info("🏁 Graceful shutdown completed")

@log_performance("service_startup")
async def start_services(shutdown_event: asyncio.Event):
    """
    Starts the services required for the application and monitors a shutdown event.
    Enhanced with structured concurrency and better error handling.
    """
    startup_metrics["start_time"] = time.perf_counter()

    try:
        async def run_bot():
            try:
                await bot.start(TOKEN)
            except asyncio.CancelledError:
                logger.info("🔄 Bot task cancelled during shutdown")
                raise
            except Exception as e:
                logger.error(f"💥 Bot connection failed: {e}", exc_info=True)
                raise

        async def run_notifier():
            """Run the error notifier loop."""
            try:
                await notifier.start_loop()
            except asyncio.CancelledError:
                logger.info("🔄 Error notifier task cancelled during shutdown")
                raise
            except Exception as e:
                logger.error(f"💥 Error notifier failed: {e}", exc_info=True)
                raise

        # Use TaskGroup for structured concurrency (Python 3.11+)
        try:
            async with asyncio.TaskGroup() as tg:
                bot_task = tg.create_task(run_bot())
                notifier_task = tg.create_task(run_notifier())

                # Add other services here as needed
                # example: monitoring_task = tg.create_task(run_monitoring())

                # Wait for a shutdown signal
                await shutdown_event.wait()
                logger.info("🛑 Shutdown signal received, stopping services...")

        except* Exception as eg:  # Exception group handling
            for e in eg.exceptions:
                logger.error(f"💥 Service error: {e}", exc_info=True)
                raise

    except Exception as e:
        logger.error(f"💥 Critical error in services: {e}", exc_info=True)
        raise
    finally:
        await shutdown_handler()



def _install_signal_handlers(loop: asyncio.AbstractEventLoop, shutdown_event: asyncio.Event):
    """
    Install SIGINT/SIGTERM handlers to trigger a graceful shutdown.
    Enhanced with better logging and cross-platform compatibility.
    """

    def _signal_handler(sig_name: str):
        logger.info(f"📡 Received {sig_name} signal, initiating shutdown...")
        shutdown_event.set()

    # Handle different signal types based on a platform
    signals_to_handle = []

    if hasattr(signal, 'SIGINT'):
        signals_to_handle.append(signal.SIGINT)
    if hasattr(signal, 'SIGTERM'):
        signals_to_handle.append(signal.SIGTERM)

    for sig in signals_to_handle:
        try:
            loop.add_signal_handler(sig, _signal_handler, sig.name)
            logger.debug(f"📡 Signal handler registered for {sig.name}")
        except NotImplementedError:
            # Windows doesn't support signal handlers in event loops
            logger.debug(f"⚠️ Signal handlers not supported on this platform for {sig.name}")
            pass
        except Exception as e:
            logger.warning(f"⚠️ Failed to register signal handler for {sig.name}: {e}")


@log_performance("application_main")
def main():
    """
    Main entry point for the application with comprehensive error handling and logging.
    """
    logger.info("🚀 Starting Ecom Discord Bot...")
    logger.info(f"🐍 Python version: {__import__('sys').version}")
    logger.info(f"🤖 Discord.py version: {discord.__version__}")

    shutdown_event = asyncio.Event()

    try:
        # Run the async main function
        asyncio.run(_async_main(shutdown_event))
    except KeyboardInterrupt:
        logger.info("⌨️ Keyboard interrupt received, shutting down...")
    except Exception as e:
        logger.error(f"💥 Fatal error occurred: {e}", exc_info=True)
        raise
    finally:
        logger.info("👋 Application shutdown complete")


@log_performance("async_main_execution")
async def _async_main(shutdown_event: asyncio.Event):
    """
    Async main function that sets up signal handlers and starts services.
    """
    loop = asyncio.get_running_loop()

    # Install signal handlers
    _install_signal_handlers(loop, shutdown_event)

    # Initialize database manager BEFORE starting bot services
    try:
        logger.info("🔄 Initializing database manager...")
        await db_manager.initialize()
        logger.info("✅ Database manager initialized successfully")

        # Optional: Export mappings for reference
        mappings_file = await db_manager.export_mappings()
        logger.info(f"📁 Database mappings exported to: {mappings_file}")

    except Exception as e:
        logger.critical(f"💥 Failed to initialize database manager: {e}")
        raise

    try:
        logger.info("Settings up leveling system")
        leveling_system = LevelingSystem()
        await leveling_system.initialize()  # This should NOT call set_bot anymore
        leveling_system.set_bot(bot)  # Call this with the actual bot instance

        # Make the leveling system available to the bot so cogs can access it
        bot.leveling_system = leveling_system
        logger.info("✅ Leveling system attached to bot")
    except Exception as e:
        logger.error(f"{e}")

    try:
        logger.info("Setting up Activity system")
        from ecom_system.activity_system.activity_system import ActivitySystem
        activity_system = ActivitySystem(db_manager=db_manager)
        await activity_system.initialize()
        bot.activity_system = activity_system
        logger.info("✅ Activity system attached to bot")
    except Exception as e:
        logger.error(f"Failed to setup Activity system: {e}")

    # Start all services
    await start_services(shutdown_event)


if __name__ == "__main__":
    main()