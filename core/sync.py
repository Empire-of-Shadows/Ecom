import os
from pathlib import Path
from typing import List, Tuple

from discord.ext import commands
from tabulate import tabulate

from dotenv import load_dotenv

from core.bot import bot
import logging
from loggers.log_factory import log_performance

load_dotenv()

logger = logging.getLogger(__name__)

# Directories to scan for cogs (Python packages/modules)
COG_DIRECTORIES: List[str] = ["ecom_system/Listeners", "ecom_system/activity_system/tasks"]


@bot.command(name="load_cogs", help="Loads all cogs in the COG_DIRECTORIES list.")
@commands.is_owner()
async def load_cogs_command(ctx: commands.Context) -> None:
	"""
    Owner-only command to reload all cogs from COG_DIRECTORIES.
    """
	logger.info(f"Load cogs command invoked by {ctx.author} ({ctx.author.id})")
	await ctx.send("Loading cogs...")

	try:
		await load_cogs()
		await ctx.send("Cogs loaded successfully.")
		logger.info("Load cogs command completed successfully")
	except Exception as e:
		logger.error(f"Load cogs command failed: {e}")
		await ctx.send(f"Error loading cogs: {e}")


def log_command_details(guild_name: str, commands_list) -> None:
	"""
    Pretty-logs a guild's command details.
    """
	logger.debug(f"Logging command details for guild: {guild_name}")

	try:
		command_data = [
			[cmd.name, cmd.description or "No description provided.", cmd.type.name]
			for cmd in commands_list
		]
		command_table = tabulate(
			command_data, headers=["Command Name", "Description", "Type"], tablefmt="fancy_grid"
		)
		logger.info(f"Commands for {guild_name}:\n{command_table}")

	except Exception as e:
		logger.error(f"Failed to log command details for {guild_name}: {e}")


@log_performance("attach_databases")
async def attach_databases() -> None:
	"""
    Attaches runtime resources (e.g., channels) to the bot and logs status.
    Grouped logs show ✅ successes and ❌ failures.
    """
	logger.info("Starting runtime attachment process...")
	success_logs = []
	failed_logs = []

	try:
		# Rewards / utility channels (replace with your IDs or config)
		channel_specs: List[Tuple[str, int]] = [
			("rewards_channel", 1395467283589107712),
			("suggest_channel", 1371239792888516719),
			("admin_channel", 1265125266519494727),
		]

		logger.debug(f"Attempting to attach {len(channel_specs)} channels")

		for attr_name, channel_id in channel_specs:
			try:
				logger.debug(f"Attempting to attach channel: {attr_name} (ID: {channel_id})")
				channel = bot.get_channel(channel_id)
				setattr(bot, attr_name, channel)

				if channel is None:
					error_msg = f"Channel {attr_name} not found (ID={channel_id})"
					failed_logs.append(error_msg)
					logger.warning(error_msg)
				else:
					success_msg = f"Successfully attached {attr_name}: {channel}"
					success_logs.append(success_msg)
					logger.info(success_msg)

			except Exception as e:
				error_msg = f"Error attaching {attr_name}: {e}"
				failed_logs.append(error_msg)
				logger.error(error_msg)

	except Exception as e:
		critical_error = f"Critical error during runtime attachment: {e}"
		failed_logs.append(critical_error)
		logger.critical(critical_error)

	# Summary logging
	if success_logs:
		logger.info(f"Successfully attached {len(success_logs)} channels")
	if failed_logs:
		logger.warning(f"Failed to attach {len(failed_logs)} channels")

	logger.info("Attachment process completed")

@log_performance("load_cogs")
async def load_cogs() -> None:
	"""
    Discover and load all cogs from COG_DIRECTORIES.
    Skips already loaded modules and logs grouped results.
    """
	logger.info("Starting cog loading process...")

	discovered_modules = _discover_cog_modules(COG_DIRECTORIES)
	logger.info(f"Discovered {len(discovered_modules)} potential cog modules")

	success_count = 0
	failure_count = 0
	skipped_count = 0
	ignored_count = 0  # New counter for "no setup" modules

	for module_name in discovered_modules:
		if module_name in bot.extensions:
			logger.debug(f"Skipping already loaded cog: {module_name}")
			skipped_count += 1
			continue

		success, reason = await safely_load_cog(module_name)
		if success:
			success_count += 1
		elif reason == "no_setup":
			ignored_count += 1
		else:
			failure_count += 1

	logger.info(f"Cog loading completed - Loaded: {success_count}, Failed: {failure_count}, Skipped: {skipped_count}, Ignored (no setup): {ignored_count}")


def _discover_cog_modules(base_dirs: List[str]) -> List[str]:
	"""
    Walks configured directories and yielded module names for .py files (non-dunder).
    """
	logger.debug(f"Discovering cog modules in directories: {base_dirs}")

	modules: List[str] = []
	total_files_scanned = 0

	for base_dir in base_dirs:
		if not os.path.exists(base_dir):
			logger.warning(f"Directory does not exist: {base_dir}")
			continue

		logger.debug(f"Scanning directory: {base_dir}")
		dir_file_count = 0

		for root, _, files in os.walk(base_dir):
			for file in files:
				total_files_scanned += 1

				if not file.endswith(".py"):
					continue
				if file.startswith("__"):
					logger.debug(f"Skipping dunder file: {file}")
					continue

				module_name = generate_cog_module_name(root, file)
				modules.append(module_name)
				dir_file_count += 1

		logger.debug(f"Found {dir_file_count} potential cog files in {base_dir}")

	logger.info(f"Module discovery completed - Scanned {total_files_scanned} files, found {len(modules)} cog modules")
	return modules


async def safely_load_cog(module: str) -> Tuple[bool, str]:
	"""
    Load a cog module by name, returning success status and reason.
    Returns: (success: bool, reason: str)
    - (True, "loaded") - Successfully loaded
    - (False, "no_setup") - Module has no setup function (expected, should be ignored)
    - (False, "error") - Actual error occurred
    """
	try:
		logger.debug(f"Loading cog: {module}")
		await bot.load_extension(module)
		logger.info(f"Successfully loaded cog: {module}")
		return True, "loaded"
	except Exception as e:
		error_msg = str(e).lower()
		if "has no 'setup' function" in error_msg:
			logger.debug(f"Skipping cog {module}: no setup function (expected)")
			return False, "no_setup"
		else:
			logger.error(f"Failed to load cog {module}: {e}")
			return False, "error"

def generate_cog_module_name(root: str, file: str) -> str:
	"""
    Convert a filesystem path to a Python module path.
    """
	try:
		relative_path = os.path.relpath(os.path.join(root, file), start=str(Path("."))).replace("\\", "/")
		module_name = relative_path.replace("/", ".").removesuffix(".py")
		logger.debug(f"Generated module name for {file}: {module_name}")
		return module_name
	except Exception as e:
		logger.error(f"Error generating module name for {file}: {e}")
		# Fallback to a simple conversion
		return f"{root.replace('/', '.').replace('\\', '.')}.{file.removesuffix('.py')}"


def log_prefix_commands(commands_list) -> None:
	"""
    Pretty-logs all prefix commands.
    """
	logger.debug(f"Logging {len(commands_list)} prefix commands")

	try:
		command_data = [
			[cmd.name, cmd.help or "No description", ", ".join(cmd.aliases) or "None"]
			for cmd in commands_list
		]
		command_table = tabulate(
			command_data, headers=["Command", "Description", "Aliases"], tablefmt="fancy_grid"
		)
		logger.info(f"Prefix Commands:\n{command_table}")

	except Exception as e:
		logger.error(f"Failed to log prefix commands: {e}")