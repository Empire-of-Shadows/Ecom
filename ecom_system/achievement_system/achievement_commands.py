import discord
from discord.ext import commands
from discord import app_commands
import asyncio
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
import math


class AchievementCommands(commands.Cog):
	"""Discord cog for achievement system interactions"""

	def __init__(self, bot: commands.Bot, leveling_system):
		self.bot = bot
		self.leveling_system = leveling_system
		self.achievement_system = leveling_system.achievement_system
		self.logger = logging.getLogger(__name__)

	@app_commands.command(
		name="achievements",
		description="View your achievements or another user's achievements"
	)
	@app_commands.describe(
		user="User to view achievements for (defaults to yourself)",
		category="Filter by achievement category",
		show_locked="Show locked achievements as well"
	)
	async def view_achievements(
			self,
			interaction: discord.Interaction,
			user: Optional[discord.Member] = None,
			category: Optional[str] = None,
			show_locked: bool = False
	):
		"""Display user's achievements with progress"""
		await interaction.response.defer()

		target_user = user or interaction.user
		guild_id = str(interaction.guild_id)
		user_id = str(target_user.id)

		try:
			# Get user achievement stats
			stats = await self.achievement_system.get_user_achievement_stats(user_id, guild_id)

			if "error" in stats:
				embed = discord.Embed(
					title="❌ Error",
					description=f"Failed to load achievements: {stats['error']}",
					color=discord.Color.red()
				)
				await interaction.followup.send(embed=embed, ephemeral=True)
				return

			# Get user's unlocked achievements
			user_achievements = await self.achievement_system._get_user_achievements(user_id, guild_id)
			unlocked_ids = user_achievements.get("unlocked", [])

			# Get all achievement definitions
			all_achievements = await self.achievement_system.list_achievements(
				category=category, enabled_only=True
			)

			if not all_achievements:
				embed = discord.Embed(
					title="🏆 No Achievements Available",
					description="No achievements are currently configured for this server.",
					color=discord.Color.orange()
				)
				await interaction.followup.send(embed=embed)
				return

			# Separate unlocked and locked achievements
			unlocked_achievements = [ach for ach in all_achievements if ach["id"] in unlocked_ids]
			locked_achievements = [ach for ach in all_achievements if ach["id"] not in unlocked_ids]

			# Create embeds
			embeds = []

			# Main stats embed
			main_embed = discord.Embed(
				title=f"🏆 {target_user.display_name}'s Achievements",
				color=discord.Color.gold()
			)

			main_embed.add_field(
				name="📊 Progress Overview",
				value=(
					f"**Unlocked:** {stats['total_unlocked']}/{stats['total_available']}\n"
					f"**Completion:** {stats['completion_percentage']}%\n"
					f"**Total Rewards:** {stats['total_rewards_earned']['xp']:,} XP, "
					f"{stats['total_rewards_earned']['embers']:,} Embers"
				),
				inline=False
			)

			# Add category breakdown
			if stats.get("by_category"):
				category_text = []
				for cat, count in stats["by_category"].items():
					category_text.append(f"**{cat.title()}:** {count}")

				main_embed.add_field(
					name="📂 By Category",
					value="\n".join(category_text) if category_text else "None",
					inline=True
				)

			# Add rarity breakdown
			if stats.get("by_rarity"):
				rarity_text = []
				rarity_emojis = {
					"common": "⚪",
					"uncommon": "🟢",
					"rare": "🔵",
					"epic": "🟣",
					"legendary": "🟡"
				}

				for rarity, count in stats["by_rarity"].items():
					emoji = rarity_emojis.get(rarity.lower(), "⚪")
					rarity_text.append(f"{emoji} **{rarity.title()}:** {count}")

				main_embed.add_field(
					name="✨ By Rarity",
					value="\n".join(rarity_text) if rarity_text else "None",
					inline=True
				)

			embeds.append(main_embed)

			# Unlocked achievements embed
			if unlocked_achievements:
				unlocked_embed = discord.Embed(
					title="🎉 Unlocked Achievements",
					color=discord.Color.green()
				)

				# Group by rarity for better display
				by_rarity = {}
				for ach in unlocked_achievements:
					rarity = ach.get("rarity", "common")
					if rarity not in by_rarity:
						by_rarity[rarity] = []
					by_rarity[rarity].append(ach)

				# Display achievements by rarity
				for rarity in ["legendary", "epic", "rare", "uncommon", "common"]:
					if rarity in by_rarity:
						rarity_emojis = {
							"common": "⚪",
							"uncommon": "🟢",
							"rare": "🔵",
							"epic": "🟣",
							"legendary": "🟡"
						}

						emoji = rarity_emojis.get(rarity, "⚪")
						achievements_text = []

						for ach in by_rarity[rarity][:5]:  # Limit to avoid embed limits
							rewards = ach.get("rewards", {})
							reward_text = ""
							if rewards.get("xp", 0) > 0 or rewards.get("embers", 0) > 0:
								reward_text = f" (+{rewards.get('xp', 0)} XP, +{rewards.get('embers', 0)} Embers)"

							achievements_text.append(
								f"**{ach['name']}**{reward_text}\n*{ach.get('description', 'No description')[:60]}...*"
							)

						if achievements_text:
							unlocked_embed.add_field(
								name=f"{emoji} {rarity.title()} ({len(by_rarity[rarity])})",
								value="\n\n".join(achievements_text),
								inline=False
							)

				embeds.append(unlocked_embed)

			# Next achievements embed (progress towards locked ones)
			if stats.get("next_achievements") and show_locked:
				next_embed = discord.Embed(
					title="🎯 Next Achievements",
					description="Achievements you're making progress towards",
					color=discord.Color.blue()
				)

				for next_ach in stats["next_achievements"]:
					ach = next_ach["achievement"]
					progress = next_ach["progress_percentage"]

					# Create progress bar
					bar_length = 10
					filled = int(progress / 10)
					bar = "█" * filled + "░" * (bar_length - filled)

					rewards = ach.get("rewards", {})
					reward_text = ""
					if rewards.get("xp", 0) > 0 or rewards.get("embers", 0) > 0:
						reward_text = f"\n💰 Rewards: {rewards.get('xp', 0)} XP, {rewards.get('embers', 0)} Embers"

					next_embed.add_field(
						name=f"{self._get_rarity_emoji(ach.get('rarity', 'common'))} {ach['name']}",
						value=(
							f"*{ach.get('description', 'No description')[:80]}...*\n"
							f"`{bar}` {progress:.1f}%{reward_text}"
						),
						inline=False
					)

				embeds.append(next_embed)

			# Send embeds with pagination if multiple
			if len(embeds) == 1:
				await interaction.followup.send(embed=embeds[0])
			else:
				await self._send_paginated_embeds(interaction, embeds)

		except Exception as e:
			self.logger.error(f"Error in view_achievements: {e}", exc_info=True)
			embed = discord.Embed(
				title="❌ Error",
				description="An error occurred while loading achievements. Please try again later.",
				color=discord.Color.red()
			)
			await interaction.followup.send(embed=embed, ephemeral=True)

	@app_commands.command(
		name="achievement_progress",
		description="View detailed progress towards specific achievements"
	)
	@app_commands.describe(
		category="Filter by achievement category",
		user="User to check progress for (defaults to yourself)"
	)
	async def achievement_progress(
			self,
			interaction: discord.Interaction,
			category: Optional[str] = None,
			user: Optional[discord.Member] = None
	):
		"""Show detailed progress towards achievements"""
		await interaction.response.defer()

		target_user = user or interaction.user
		guild_id = str(interaction.guild_id)
		user_id = str(target_user.id)

		try:
			# Get user data for progress calculations
			user_data = await self.leveling_system._get_enhanced_user_data(user_id, guild_id)
			if not user_data:
				embed = discord.Embed(
					title="❌ No Data",
					description="No user data found. Try being active in the server first!",
					color=discord.Color.red()
				)
				await interaction.followup.send(embed=embed, ephemeral=True)
				return

			# Get user achievements
			user_achievements = await self.achievement_system._get_user_achievements(user_id, guild_id)
			unlocked_ids = user_achievements.get("unlocked", [])

			# Get all achievements
			all_achievements = await self.achievement_system.list_achievements(
				category=category, enabled_only=True
			)

			# Filter to locked achievements only
			locked_achievements = [ach for ach in all_achievements if ach["id"] not in unlocked_ids]

			if not locked_achievements:
				embed = discord.Embed(
					title="🎉 All Achievements Unlocked!",
					description="You've unlocked all available achievements in this category!" if category else "You've unlocked all available achievements!",
					color=discord.Color.gold()
				)
				await interaction.followup.send(embed=embed)
				return

			# Calculate progress for each locked achievement
			progress_data = []
			for ach in locked_achievements:
				progress = await self.achievement_system._calculate_achievement_progress(ach, user_data)
				if progress >= 0:  # Include all achievements, even 0% progress
					# New schema: conditions.data.threshold
					cond = ach.get("conditions", {}) or {}
					cond_type = cond.get("type") or ach.get("condition_type")
					cond_data = cond.get("data", {}) if isinstance(cond.get("data", {}), dict) else {}
					required_value = cond_data.get("threshold", cond.get("threshold", ach.get("threshold", 1)))

					# For time-based and pattern-based conditions we cannot get a raw current value from user_data.
					# Approximate from progress percent to make the UI intuitive.
					time_like_types = {"day_of_week", "day_of_month", "weekday_weekend", "weekend_activity", "time_pattern"}
					if cond_type in time_like_types:
						current_value = int(math.floor((progress / 100.0) * max(1, float(required_value))))
					else:
						current_value = await self._get_current_value_for_achievement(ach, user_data)

					progress_data.append({
						"achievement": ach,
						"progress": progress,
						"current_value": current_value,
						"required_value": required_value
					})

			# Sort by progress (highest first)
			progress_data.sort(key=lambda x: x["progress"], reverse=True)

			# Create embed
			embed = discord.Embed(
				title=f"🎯 {target_user.display_name}'s Achievement Progress",
				description=f"Progress towards {len(progress_data)} locked achievements" +
							(f" in **{category}**" if category else ""),
				color=discord.Color.blue()
			)

			# Add progress fields
			for i, data in enumerate(progress_data[:10]):  # Limit to 10 to avoid embed limits
				ach = data["achievement"]
				progress = data["progress"]
				current = data["current_value"]
				required = data["required_value"]

				# Create progress bar
				bar_length = 20
				filled = int(progress / 5)  # 5% per character
				bar = "█" * filled + "░" * (bar_length - filled)

				# Format current/required values
				if current >= 1000:
					current_str = f"{current:,}"
				else:
					current_str = str(current)

				if required >= 1000:
					required_str = f"{required:,}"
				else:
					required_str = str(required)

				# Get rewards info
				rewards = ach.get("rewards", {})
				reward_parts = []
				if rewards.get("xp", 0) > 0:
					reward_parts.append(f"{rewards['xp']} XP")
				if rewards.get("embers", 0) > 0:
					reward_parts.append(f"{rewards['embers']} Embers")

				reward_text = f"\n💰 **Rewards:** {', '.join(reward_parts)}" if reward_parts else ""

				embed.add_field(
					name=f"{self._get_rarity_emoji(ach.get('rarity', 'common'))} {ach['name']}",
					value=(
						f"*{ach.get('description', 'No description')[:100]}...*\n"
						f"`{bar}` **{progress:.1f}%**\n"
						f"📊 **Progress:** {current_str}/{required_str}{reward_text}"
					),
					inline=False
				)

				if i >= 9:  # Stop at 10 achievements
					if len(progress_data) > 10:
						embed.add_field(
							name="📝 Note",
							value=f"Showing top 10 of {len(progress_data)} achievements. Use category filters to see more specific progress.",
							inline=False
						)
					break

			await interaction.followup.send(embed=embed)

		except Exception as e:
			self.logger.error(f"Error in achievement_progress: {e}", exc_info=True)
			embed = discord.Embed(
				title="❌ Error",
				description="An error occurred while loading progress data. Please try again later.",
				color=discord.Color.red()
			)
			await interaction.followup.send(embed=embed, ephemeral=True)

	# ────────────────────────────────────────────────────────────────────────────────
	# Achievement Information Command – updated to support the new time‑based
	# conditions that live in `AchievementConditionSystem`.
	# ────────────────────────────────────────────────────────────────────────────────
	@app_commands.command(
		name="achievement_info",
		description="Get detailed information about a specific achievement"
	)
	@app_commands.describe(
		achievement_name="Name of the achievement to look up"
	)
	async def achievement_info(
			self,
			interaction: discord.Interaction,
			achievement_name: str
	):
		"""Show detailed information about a specific achievement"""
		await interaction.response.defer()

		try:
			# --------------------------------------------------------------------
			# 1️⃣  Load all enabled achievements
			# --------------------------------------------------------------------
			all_achievements = await self.achievement_system.list_achievements(enabled_only=True)

			# --------------------------------------------------------------------
			# 2️⃣  Find the achievement that matches the supplied name
			# --------------------------------------------------------------------
			matching_achievements = [
				ach for ach in all_achievements
				if achievement_name.lower() in ach['name'].lower()
			]

			if not matching_achievements:
				embed = discord.Embed(
					title="❌ Achievement Not Found",
					description=f"No achievement found matching '{achievement_name}'",
					color=discord.Color.red()
				)
				# Optional: list a few popular achievements for user help
				if len(achievement_name) > 2:
					suggestions = [ach['name'] for ach in all_achievements[:5]]
					if suggestions:
						embed.add_field(
							name="💡 Available Achievements",
							value="\n".join(f"• {name}" for name in suggestions),
							inline=False
						)
				await interaction.followup.send(embed=embed, ephemeral=True)
				return

			# If multiple matches – pick an exact match if available, otherwise the first one
			achievement = next(
				(ach for ach in matching_achievements if ach['name'].lower() == achievement_name.lower()),
				matching_achievements[0]
			)

			# --------------------------------------------------------------------
			# 3️⃣  Gather basic information
			# --------------------------------------------------------------------
			guild_id = str(interaction.guild_id)
			user_id = str(interaction.user.id)
			user_achievements = await self.achievement_system._get_user_achievements(user_id, guild_id)
			has_achievement = achievement["id"] in user_achievements.get("unlocked", [])

			# --------------------------------------------------------------------
			# 4️⃣  Build the embed
			# --------------------------------------------------------------------
			embed = discord.Embed(
				title=f"{self._get_rarity_emoji(achievement.get('rarity', 'common'))} {achievement['name']}",
				description=achievement.get('description', 'No description available'),
				color=self._get_rarity_color(achievement.get('rarity', 'common'))
			)

			# 📂  Basic details
			embed.add_field(
				name="📂 Details",
				value=(
					f"**Category:** {achievement.get('category', 'Unknown').title()}\n"
					f"**Rarity:** {achievement.get('rarity', 'common').title()}\n"
					f"**Status:** {'✅ Unlocked' if has_achievement else '🔒 Locked'}"
				),
				inline=True
			)

			# 💰  Rewards
			rewards = achievement.get("rewards", {})
			reward_parts = []
			if rewards.get("xp", 0) > 0:
				reward_parts.append(f"**{rewards['xp']:,}** XP")
			if rewards.get("embers", 0) > 0:
				reward_parts.append(f"**{rewards['embers']:,}** Embers")
			if rewards.get("title"):
				reward_parts.append(f"**Title:** {rewards['title']}")

			embed.add_field(
				name="💰 Rewards",
				value="\n".join(reward_parts) if reward_parts else "No rewards specified",
				inline=True
			)

			# 🎯  Requirements – supports new condition types in `conditions.data`
			conditions_obj = achievement.get('conditions', {}) or {}
			condition_type = conditions_obj.get('type') or achievement.get('condition_type', 'unknown')
			cond_data = conditions_obj.get('data', {}) if isinstance(conditions_obj.get('data', {}), dict) else {}
			threshold = cond_data.get('threshold', conditions_obj.get('threshold', 1))
			comparison = cond_data.get('comparison', conditions_obj.get('comparison', 'gte'))
			min_activity_per_day = cond_data.get('min_activity_per_day', conditions_obj.get('min_activity_per_day', 1))

			# Helper to turn a comparison token into a human‑readable phrase
			cmp_str = {
				'gte': 'at least',
				'gt': 'more than',
				'lte': 'at most',
				'lt': 'less than',
				'eq': 'exactly'
			}.get(comparison, comparison)

			# Render the requirement string for each new type
			if condition_type == "level":
				requirement_text = f"Reach **Level {threshold}**"
			elif condition_type == "messages":
				requirement_text = f"Send **{threshold:,}** messages"
			elif condition_type == "voice_time":
				hours = float(threshold) / 3600.0
				requirement_text = f"Spend **{hours:.1f} hours** in voice channels"
			elif condition_type == "voice_sessions":
				requirement_text = f"Join voice channels **{threshold:,}** times"
			elif condition_type == "daily_streak":
				requirement_text = f"Maintain a **{threshold}**‑day activity streak"
			elif condition_type == "reactions_given":
				requirement_text = f"Give **{threshold:,}** reactions"
			elif condition_type == "got_reactions":
				requirement_text = f"Receive **{threshold:,}** reactions"
			elif condition_type == "attachment_messages":
				requirement_text = f"Send **{threshold:,}** messages with attachments"
			elif condition_type == "links_sent":
				requirement_text = f"Send **{threshold:,}** links"
			elif condition_type == "attachments_sent":
				requirement_text = f"Send **{threshold:,}** attachments"
			elif condition_type == "time_pattern":
				tr = cond_data.get("time_range", conditions_obj.get("time_range", {})) or {}
				start = tr.get("start", "?")
				end = tr.get("end", "?")
				requirement_text = (
					f"Be active between **{start}–{end}** on **{cmp_str} {threshold}** days"
				)

			# -------- New time‑based conditions --------------------------------
			elif condition_type == "day_of_week":
				days = cond_data.get('days', conditions_obj.get('days', [])) or []
				days_str = ", ".join(days)
				requirement_text = (
					f"Be active on {days_str} with {cmp_str} {min_activity_per_day} "
					f"activities per day for **{threshold:,}** days"
				)
			elif condition_type == "day_of_month":
				days_of_month = cond_data.get('days_of_month', conditions_obj.get('days_of_month', [])) or []
				days_str = ", ".join(str(d) for d in days_of_month)
				requirement_text = (
					f"Be active on the {days_str} day(s) of each month with {cmp_str} "
					f"{min_activity_per_day} activities per day for **{threshold:,}** months"
				)
			elif condition_type == "weekday_weekend":
				day_type = (cond_data.get('day_type', conditions_obj.get('day_type', 'weekday')) or 'weekday').capitalize()
				requirement_text = (
					f"Be active on {day_type}s with {cmp_str} {min_activity_per_day} activities "
					f"per day for **{threshold:,}** {day_type.lower()}s"
				)
			elif condition_type == "weekend_activity":
				min_per_weekend = cond_data.get('min_activity_per_weekend',
												conditions_obj.get('min_activity_per_weekend', 10))
				requirement_text = (
					f"Engage in activity on weekends with {cmp_str} {min_per_weekend} "
					f"actions per weekend for **{threshold:,}** weekends"
				)
			elif condition_type == "prestige_level":
				requirement_text = f"Reach **Prestige Level {threshold}**"
			else:
				requirement_text = "Requirement details not available"

			embed.add_field(
				name="🎯 Requirements",
				value=requirement_text,
				inline=False
			)

			# --------------------------------------------------------------------
			# 5️⃣  If the achievement isn't unlocked yet – show a progress bar
			# --------------------------------------------------------------------
			if not has_achievement:
				try:
					user_data = await self.leveling_system._get_enhanced_user_data(user_id, guild_id)
					if user_data:
						progress = await self.achievement_system._calculate_achievement_progress(
							achievement, user_data
						)

						# For time-like conditions, estimate current from progress and threshold
						time_like_types = {"day_of_week", "day_of_month", "weekday_weekend", "weekend_activity", "time_pattern"}
						if condition_type in time_like_types:
							current_value = int(math.floor((progress / 100.0) * max(1, float(threshold))))
						else:
							current_value = await self._get_current_value_for_achievement(achievement, user_data)

						if progress > 0:
							bar_length = 20
							filled = int(progress / 5)  # 20‑step bar
							bar = "█" * filled + "░" * (bar_length - filled)

							embed.add_field(
								name="📊 Your Progress",
								value=(
									f"`{bar}` **{progress:.1f}%**\n"
									f"**Current:** {current_value:,}/{int(threshold):,}"
								),
								inline=False
							)
				except Exception as e:
					self.logger.error(f"Error calculating progress for achievement info: {e}")

			# --------------------------------------------------------------------
			# 6️⃣  Footer & send the embed
			# --------------------------------------------------------------------
			embed.set_footer(text=f"Achievement ID: {achievement['id']}")
			await interaction.followup.send(embed=embed)

		except Exception as e:
			self.logger.error(f"Error in achievement_info: {e}", exc_info=True)
			embed = discord.Embed(
				title="❌ Error",
				description="An error occurred while loading achievement information. Please try again later.",
				color=discord.Color.red()
			)
			await interaction.followup.send(embed=embed, ephemeral=True)

	@app_commands.command(
		name="achievement_leaderboard",
		description="View the server's achievement leaderboard"
	)
	@app_commands.describe(
		category="Filter by achievement category",
		limit="Number of users to show (default: 10)"
	)
	async def achievement_leaderboard(
			self,
			interaction: discord.Interaction,
			category: Optional[str] = None,
			limit: int = 10
	):
		"""Show achievement leaderboard for the server"""
		await interaction.response.defer()

		guild_id = str(interaction.guild_id)
		limit = max(1, min(limit, 20))  # Clamp between 1 and 20

		try:
			# Get all user achievements for this guild
			cursor = self.leveling_system.user_achievements.find({"guild_id": guild_id})
			all_user_achievements = await cursor.to_list(None)

			if not all_user_achievements:
				embed = discord.Embed(
					title="📊 Achievement Leaderboard",
					description="No achievement data found for this server yet!",
					color=discord.Color.orange()
				)
				await interaction.followup.send(embed=embed)
				return

			# Get achievement definitions for category filtering
			all_achievements = await self.achievement_system.list_achievements(
				category=category, enabled_only=True
			)

			if category:
				# Filter achievements by category
				category_achievement_ids = {ach["id"] for ach in all_achievements}

			# Calculate leaderboard data
			leaderboard_data = []
			for user_ach in all_user_achievements:
				user_id = user_ach["user_id"]
				unlocked = user_ach.get("unlocked", [])

				if category:
					# Count only achievements in the specified category
					unlocked_in_category = [ach_id for ach_id in unlocked if ach_id in category_achievement_ids]
					achievement_count = len(unlocked_in_category)
				else:
					achievement_count = len(unlocked)

				if achievement_count > 0:  # Only include users with achievements
					leaderboard_data.append({
						"user_id": user_id,
						"achievement_count": achievement_count,
						"total_unlocked": len(unlocked)
					})

			if not leaderboard_data:
				embed = discord.Embed(
					title="📊 Achievement Leaderboard",
					description=f"No achievements found{f' in category **{category}**' if category else ''}!",
					color=discord.Color.orange()
				)
				await interaction.followup.send(embed=embed)
				return

			# Sort by achievement count
			leaderboard_data.sort(key=lambda x: x["achievement_count"], reverse=True)

			# Create embed
			embed = discord.Embed(
				title=f"🏆 Achievement Leaderboard{f' - {category.title()}' if category else ''}",
				description=f"Top {min(limit, len(leaderboard_data))} users by achievements unlocked",
				color=discord.Color.gold()
			)

			# Add leaderboard entries
			medal_emojis = ["🥇", "🥈", "🥉"]
			leaderboard_text = []

			for i, data in enumerate(leaderboard_data[:limit]):
				try:
					user = self.bot.get_user(int(data["user_id"]))
					if user:
						display_name = user.display_name
					else:
						# Try to fetch user if not in cache
						user = await self.bot.fetch_user(int(data["user_id"]))
						display_name = user.display_name if user else f"User {data['user_id']}"
				except:
					display_name = f"User {data['user_id']}"

				position = i + 1
				emoji = medal_emojis[i] if i < 3 else f"{position}."

				count_text = f"{data['achievement_count']}"
				if category and data['total_unlocked'] != data['achievement_count']:
					count_text += f" ({data['total_unlocked']} total)"

				leaderboard_text.append(f"{emoji} **{display_name}** - {count_text} achievements")

			embed.description += f"\n\n{chr(10).join(leaderboard_text)}"

			# Add server stats
			total_users = len(all_user_achievements)
			total_achievements_unlocked = sum(len(ua.get("unlocked", [])) for ua in all_user_achievements)
			total_available = len(await self.achievement_system.list_achievements(enabled_only=True))

			embed.add_field(
				name="📈 Server Stats",
				value=(
					f"**Total Users:** {total_users:,}\n"
					f"**Achievements Unlocked:** {total_achievements_unlocked:,}\n"
					f"**Available Achievements:** {total_available}"
				),
				inline=True
			)

			if total_available > 0:
				avg_completion = (total_achievements_unlocked / (total_users * total_available)) * 100
				embed.add_field(
					name="🎯 Completion Rate",
					value=f"{avg_completion:.1f}% average completion",
					inline=True
				)

			await interaction.followup.send(embed=embed)

		except Exception as e:
			self.logger.error(f"Error in achievement_leaderboard: {e}", exc_info=True)
			embed = discord.Embed(
				title="❌ Error",
				description="An error occurred while loading the leaderboard. Please try again later.",
				color=discord.Color.red()
			)
			await interaction.followup.send(embed=embed, ephemeral=True)

	# Autocomplete for achievement categories
	@view_achievements.autocomplete("category")
	@achievement_progress.autocomplete("category")
	@achievement_leaderboard.autocomplete("category")
	async def achievement_category_autocomplete(
			self,
			interaction: discord.Interaction,
			current: str
	) -> List[app_commands.Choice[str]]:
		"""Provide autocomplete for achievement categories"""
		try:
			all_achievements = await self.achievement_system.list_achievements(enabled_only=True)
			categories = list(set(ach.get("category", "unknown") for ach in all_achievements))
			categories.sort()

			# Filter based on current input
			if current:
				categories = [cat for cat in categories if current.lower() in cat.lower()]

			return [
				app_commands.Choice(name=cat.title(), value=cat)
				for cat in categories[:25]  # Discord limit
			]
		except:
			return []

	@achievement_info.autocomplete("achievement_name")
	async def achievement_name_autocomplete(
			self,
			interaction: discord.Interaction,
			current: str
	) -> List[app_commands.Choice[str]]:
		"""Provide autocomplete for achievement names"""
		try:
			all_achievements = await self.achievement_system.list_achievements(enabled_only=True)

			# Filter based on current input
			matching_achievements = []
			if current:
				current_lower = current.lower()
				for ach in all_achievements:
					if current_lower in ach["name"].lower():
						matching_achievements.append(ach)
			else:
				matching_achievements = all_achievements[:25]

			# Sort by relevance (exact matches first, then partial)
			if current:
				def sort_key(ach):
					name_lower = ach["name"].lower()
					current_lower = current.lower()
					if name_lower.startswith(current_lower):
						return 0  # Exact prefix match
					elif current_lower in name_lower:
						return 1  # Contains match
					else:
						return 2  # Other match

				matching_achievements.sort(key=sort_key)

			return [
				app_commands.Choice(
					name=f"{self._get_rarity_emoji(ach.get('rarity', 'common'))} {ach['name']}"[:100],
					value=ach["name"]
				)
				for ach in matching_achievements[:25]
			]
		except:
			return []

	# Helper methods
	def _get_rarity_emoji(self, rarity: str) -> str:
		"""Get emoji for achievement rarity"""
		rarity_emojis = {
			"common": "⚪",
			"uncommon": "🟢",
			"rare": "🔵",
			"epic": "🟣",
			"legendary": "🟡"
		}
		return rarity_emojis.get(rarity.lower(), "⚪")

	def _get_rarity_color(self, rarity: str) -> discord.Color:
		"""Get Discord color for achievement rarity"""
		rarity_colors = {
			"common": discord.Color.light_grey(),
			"uncommon": discord.Color.green(),
			"rare": discord.Color.blue(),
			"epic": discord.Color.purple(),
			"legendary": discord.Color.gold()
		}
		return rarity_colors.get(rarity.lower(), discord.Color.light_grey())

	def _get_nested_value(self, data: Dict, field_path: str, default=0):
		"""Get nested dictionary value using dot notation (e.g., 'message_stats.messages')"""
		try:
			keys = field_path.split('.')
			value = data
			for key in keys:
				if isinstance(value, dict) and key in value:
					value = value[key]
				else:
					return default
			return value if value is not None else default
		except Exception as e:
			self.logger.error(f"Error getting nested value for path '{field_path}': {e}")
			return default

	async def _get_current_value_for_achievement(self, achievement: Dict, user_data: Dict) -> int:
		"""Get the current value for an achievement's progress tracking"""
		# Prefer new schema, fall back to legacy
		conditions_obj = achievement.get("conditions", {}) or {}
		condition_type = conditions_obj.get("type", achievement.get("condition_type", "simple"))

		if condition_type == "level":
			return user_data.get("level", 1)
		elif condition_type == "messages":
			return user_data.get("message_stats", {}).get("messages", 0)
		elif condition_type == "voice_time":
			return user_data.get("voice_stats", {}).get("total_time", 0)
		elif condition_type == "voice_sessions":
			return user_data.get("voice_stats", {}).get("sessions", 0)
		elif condition_type == "daily_streak":
			field = conditions_obj.get('data', {}).get('field', 'daily_streak.count')
			return self._get_nested_value(user_data, field)
		elif condition_type == "reactions_given":
			return user_data.get("message_stats", {}).get("reacted_messages", 0)
		elif condition_type == "got_reactions":
			return user_data.get("message_stats", {}).get("got_reactions", 0)
		elif condition_type == "attachment_messages":
			return user_data.get("message_stats", {}).get("attachment_messages", 0)
		elif condition_type == "links_sent":
			return user_data.get("message_stats", {}).get("links_sent", 0)
		elif condition_type == "attachments_sent":
			return user_data.get("message_stats", {}).get("attachments_sent", 0)
		elif condition_type == "prestige_level":
			# Reasonable default; adjust if your schema differs
			return user_data.get("prestige", 0)
		else:
			# For time-based conditions, raw "current" isn't trivially available from user_data here
			return 0

	async def _send_paginated_embeds(self, interaction: discord.Interaction, embeds: List[discord.Embed]):
		"""Send embeds with pagination using buttons"""
		if len(embeds) == 1:
			await interaction.followup.send(embed=embeds[0])
			return

		class PaginationView(discord.ui.View):
			def __init__(self, embeds: List[discord.Embed]):
				super().__init__(timeout=300)
				self.embeds = embeds
				self.current_page = 0
				self.update_buttons()

			def update_buttons(self):
				self.previous_button.disabled = self.current_page == 0
				self.next_button.disabled = self.current_page == len(self.embeds) - 1

			@discord.ui.button(label="◀️ Previous", style=discord.ButtonStyle.secondary)
			async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
				if self.current_page > 0:
					self.current_page -= 1
					self.update_buttons()
					await interaction.response.edit_message(embed=self.embeds[self.current_page], view=self)

			@discord.ui.button(label="▶️ Next", style=discord.ButtonStyle.secondary)
			async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
				if self.current_page < len(self.embeds) - 1:
					self.current_page += 1
					self.update_buttons()
					await interaction.response.edit_message(embed=self.embeds[self.current_page], view=self)

		# Update first embed with pagination info
		embeds[0].set_footer(text=f"Page 1 of {len(embeds)}")
		for i, embed in enumerate(embeds[1:], 2):
			embed.set_footer(text=f"Page {i} of {len(embeds)}")

		view = PaginationView(embeds)
		await interaction.followup.send(embed=embeds[0], view=view)


async def setup(bot: commands.Bot):
	"""Setup function for the cog - ensures a LevelingSystem exists on the bot."""
	# Reuse existing instance if available; otherwise create and attach one
	leveling_system = getattr(bot, "leveling_system", None)
	if leveling_system is None:
		# Lazy import to avoid circulars at module import time
		try:
			from ecom_system.leveling.leveling import LevelingSystem  # type: ignore
		except Exception:
			# Fallback import path if your package layout differs
			from ecom_system.leveling.leveling import LevelingSystem  # type: ignore

		leveling_system = LevelingSystem()
		setattr(bot, "leveling_system", leveling_system)
		logging.getLogger(__name__).info("LevelingSystem instance created and attached to bot in AchievementCommands.setup()")

	await bot.add_cog(AchievementCommands(bot, leveling_system))