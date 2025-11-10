# Using Components v2 in `discord.py`

This guide explains how to use **Components v2** in `discord.py`, covering new layouts, UI elements, interaction handling, and migration tips.

---

## Table of Contents

1. [What changed in v2](#1-what-changed-in-v2)
2. [Key component types](#2-key-component-types-in-v2)
3. [LayoutView vs View](#3-layoutview-vs-view)
4. [Component limits & rules](#4-component-limits--rules)
5. [Building a message](#5-building-messages-with-v2-components)
6. [Handling interactions](#6-handling-interactions--callbacks)
7. [Editing & updating](#7-editing--updating-components)
8. [Dynamic items](#8-dynamic-items--advanced-usage)
9. [Migration from v1](#9-migrating-from-v1-to-v2)
10. [Common pitfalls](#10-common-pitfalls--tips)
11. [Full examples](#11-full-working-examples)
12. [References](#12-reference--links)

---

## 1. What changed in v2

- Discord expanded layouts beyond button rows.
- `discord.py` introduced `` for manual layouting.
- Old `ui.View` still works for backwards compatibility.
- New components include: `Section`, `TextDisplay`, `Thumbnail`, `MediaGallery`, `FileComponent`, `Separator`, and `Container`.
- Added `DynamicItem` support for custom dynamic layouts.

---

## 2. Key component types in v2

| Component Type  | Class in `discord.ui` | Purpose                                        |
| --------------- | --------------------- | ---------------------------------------------- |
| Action Row      | `ActionRow`           | Holds up to 5 child items (buttons, selects)   |
| Section         | `Section`             | Displays header-style text                     |
| Text Display    | `TextDisplay`         | Shows static text                              |
| Thumbnail       | `Thumbnail`           | Displays an image thumbnail                    |
| Media Gallery   | `MediaGallery`        | Displays multiple media (images, video, audio) |
| File Component  | `File`                | Displays or attaches a file                    |
| Separator       | `Separator`           | Visual divider line                            |
| Container       | `Container`           | Higher-level container for grouping            |
| Button / Select | `Button`, `Select`    | Interactive components (from v1, usable in v2) |

All derive from `Component` in [`discord/components.py`](https://github.com/Rapptz/discord.py/blob/master/discord/components.py).

---

## 3. LayoutView vs View

- ``: Automatic row layout (old system).
- ``: Manual, more powerful layouting.

### Key methods

- `layout = ui.LayoutView()`
- `layout.add_item(component)`
- `layout.clear_items()`
- `layout.remove_item(component)`
- `layout.children` → list of all child components

Use `LayoutView` for fine control, especially with v2-only components.

---

## 4. Component limits & rules

- Max 25 components per message (including nested containers).
- Buttons: max 5 per row (`ActionRow`).
- Select menus: 1 per row.
- A message can contain up to 5 `ActionRow`s.
- Containers can nest other components but must respect Discord’s structural rules.
- Ephemeral responses can still use components but disappear when the message does.

---

## 5. Building messages with v2 components

### Steps:

1. Create a `LayoutView`.
2. Add components (`Section`, `Thumbnail`, `ActionRow`, etc.).
3. Send the message with `ctx.respond(..., view=layout)` or `interaction.response.send_message(..., view=layout)`.

### Example:

```python
from discord import ui, ButtonStyle
from discord.ui import LayoutView, Section, Thumbnail, ActionRow, Button

async def send_custom(ctx):
    layout = LayoutView()

    # Header
    layout.add_item(Section("This is a v2 demo header"))

    # Thumbnail
    layout.add_item(Thumbnail(url="https://example.com/thumb.png"))

    # Buttons
    btn1 = Button(label="Yes", style=ButtonStyle.green, custom_id="yes")
    btn2 = Button(label="No", style=ButtonStyle.red, custom_id="no")
    layout.add_item(ActionRow(btn1, btn2))

    await ctx.respond("Here’s the UI v2 message", view=layout)
```

---

## 6. Handling interactions / callbacks

Attach async callbacks to interactive components:

```python
btn = ui.Button(label="Click me", custom_id="click_me")

async def on_click(interaction):
    await interaction.response.send_message(f"You clicked, {interaction.user.display_name}!", ephemeral=True)

btn.callback = on_click
```

- Always handle **ephemeral responses** for user-specific actions.
- Use `interaction.checks` or `interaction.user` to validate access.

---

## 7. Editing & updating components

- Use `await message.edit(view=new_layout)` to update a layout.
- Rebuild layouts with `clear_items()`.
- Dynamic updates (like toggles) can be handled by swapping button labels/styles and re-editing the message.

---

## 8. Dynamic items & advanced usage

`discord.ui.DynamicItem` allows creating items where state is encoded in `custom_id`.

Example:

```python
class CounterButton(ui.DynamicItem[ui.Button]):
    def __init__(self, value: int):
        super().__init__(ui.Button(label=f"Count: {value}", custom_id=f"counter:{value}"))

    async def callback(self, interaction: discord.Interaction):
        value = int(self.item.custom_id.split(":")[1]) + 1
        new_button = CounterButton(value)
        view = ui.LayoutView()
        view.add_item(new_button)
        await interaction.response.edit_message(view=view)
```

This enables custom state machines inside components.

---

## 9. Migrating from v1 to v2

- Replace `View` with `LayoutView` for new layouts.
- Wrap buttons/selects into `ActionRow` explicitly.
- Use `Section` and `Separator` instead of embeds for lightweight formatting.
- `MediaGallery` replaces multiple image embeds.
- Existing `View` subclasses still work, but won’t support v2-only components.

---

## 10. Common pitfalls & tips

- **Forgetting ActionRow**: Buttons/selects must be grouped in rows.
- **Timeouts**: Views expire by default; use `timeout=None` for persistent.
- **Overusing embeds**: Prefer sections/thumbnails for structured layouts.
- **Mobile rendering**: Always test on mobile, layouts can differ.
- **Custom IDs**: Must be unique per button/select in a view.

---

## 11. Full working examples

### Basic demo

```python
import discord
from discord.ext import commands
from discord.ui import LayoutView, Section, ActionRow, Button

bot = commands.Bot(command_prefix="!")

@bot.slash_command(name="v2demo")
async def v2demo(ctx: discord.ApplicationContext):
    layout = LayoutView()
    layout.add_item(Section("Welcome to Components v2!"))

    yes_btn = Button(label="Yes", style=discord.ButtonStyle.green, custom_id="yes")
    no_btn = Button(label="No", style=discord.ButtonStyle.red, custom_id="no")

    async def yes_cb(inter):
        await inter.response.send_message("You clicked Yes!", ephemeral=True)

    async def no_cb(inter):
        await inter.response.send_message("You clicked No!", ephemeral=True)

    yes_btn.callback = yes_cb
    no_btn.callback = no_cb

    layout.add_item(ActionRow(yes_btn, no_btn))
    await ctx.respond("Here’s your Components v2 demo:", view=layout)

bot.run("YOUR_TOKEN")
```

### Media gallery demo

```python
from discord.ui import LayoutView, MediaGallery, Thumbnail

async def gallery_demo(ctx):
    layout = LayoutView()
    gallery = MediaGallery()
    gallery.append_item(Thumbnail(url="https://placekitten.com/200/200"))
    gallery.append_item(Thumbnail(url="https://placekitten.com/300/300"))
    layout.add_item(gallery)

    await ctx.respond("Gallery demo:", view=layout)
```

---

## 12. Reference & Links

- [Bot UI Kit / Interactions](https://discordpy.readthedocs.io/en/latest/interactions/api.html#bot-ui-kit)
- [LayoutView docs](https://discordpy.readthedocs.io/en/latest/interactions/api.html#layoutview)
- [Components source](https://github.com/Rapptz/discord.py/blob/master/discord/components.py)
- [Types definition](https://github.com/Rapptz/discord.py/blob/master/discord/types/components.py)
- [What’s New in discord.py](https://discordpy.readthedocs.io/en/latest/whats_new.html)


# Example components v2 message

```python
# =============================================================================
# MAIN BUMP MESSAGE CREATION
# =============================================================================

async def create_bump_message(guild: discord.Guild, settings: Dict[str, Any]) -> Dict[str, Any]:
	"""
    Create a comprehensive bump advertisement message with rich components.

    This function creates an interactive Discord message using Components v2,
    featuring server banners, statistics, interactive buttons, and fallback
    mechanisms for error handling.

    Args:
        guild: Discord guild object to create advertisement for
        settings: Dictionary containing bump settings (custom_ad, invite_link, etc.)

    Returns:
        Dictionary containing message content, layout, and files for Discord
    """
	try:
		# Extract settings
		custom_ad = settings.get("bump_ad", "")
		invite_link = settings.get("invite_link", "")

		# Initialize layout and file containers
		layout_view = discord.ui.LayoutView()
		files = []

		# =================================================================
		# BANNER PROCESSING
		# =================================================================

		# Determine banner URL (custom or Discord server banner)
		custom_banner_url = settings.get("custom_banner_url")
		banner_url = custom_banner_url or (str(guild.banner.url) if guild.banner else None)

		# Process banner if available
		if banner_url:
			try:
				banner_file = await resize_banner_to_file(banner_url, target_width=600, target_height=400)
				if banner_file:
					files.append(banner_file)
					# Add media gallery with attachment
					layout_view.add_item(discord.ui.Separator())
					media_gallery = discord.ui.MediaGallery()
					media_gallery.add_item(media=f"attachment://{banner_file.filename}")
					layout_view.add_item(media_gallery)
					layout_view.add_item(discord.ui.Separator())
					logger.debug(f"Using resized banner file attachment: {banner_file.filename}")
				else:
					# Fallback to original URL
					layout_view.add_item(discord.ui.Separator())
					media_gallery = discord.ui.MediaGallery()
					media_gallery.add_item(media=banner_url)
					layout_view.add_item(media_gallery)
					layout_view.add_item(discord.ui.Separator())
			except Exception as e:
				logger.warning(f"Failed to process banner: {e}")

		# =================================================================
		# HERO SECTION
		# =================================================================

		# Create hero section with server icon
		hero_accessory = discord.ui.Thumbnail(media=str(guild.icon.url)) if guild.icon else None
		hero_section = discord.ui.Section(accessory=hero_accessory)

		# Add custom description or default welcome message
		desc_text = custom_ad or (
			"🎉 Welcome to an amazing Discord community! "
			"Join us for great conversations, events, and connections. "
			"We're excited to meet you!"
		)
		hero_section.add_item(discord.ui.TextDisplay(desc_text))

		# Create hero container with dynamic accent color
		hero_container = discord.ui.Container()

		# Set accent color based on server tier/features
		if guild.premium_tier > 0:
			hero_container.accent_color = discord.Color.gold().value
		elif "VERIFIED" in guild.features or "PARTNERED" in guild.features:
			hero_container.accent_color = discord.Color.blue().value
		else:
			hero_container.accent_color = discord.Color.from_rgb(88, 101, 242).value

		hero_container.add_item(hero_section)

		# Add action buttons (Join/Explore and Report)
		button_row = discord.ui.ActionRow()

		if invite_link:
			join_button = discord.ui.Button(
				style=discord.ButtonStyle.primary,
				label=f"🚀 Join {guild.name}",
				emoji="✨",
				url=invite_link
			)
		else:
			join_button = discord.ui.Button(
				style=discord.ButtonStyle.secondary,
				label="📊 Explore Server",
				emoji="🔍",
				custom_id=f"basic_info_{guild.id}"
			)

		report_button = discord.ui.Button(
			style=discord.ButtonStyle.danger,
			label="🚨 Report",
			emoji="⚠️",
			custom_id=f"report_{guild.id}"
		)

		button_row.add_item(join_button)
		button_row.add_item(report_button)
		hero_container.add_item(button_row)

		layout_view.add_item(hero_container)
		layout_view.add_item(discord.ui.Separator())

		# =================================================================
		# SERVER DETAILS SECTION
		# =================================================================

		details_container = discord.ui.Container()
		details_container.add_item(discord.ui.TextDisplay("## 🏠 Server Overview"))

		# Build server details
		details_lines = []

		# Member information with activity indicators
		member_info = f"👥 **{guild.member_count:,} members strong**"
		if getattr(guild, 'approximate_presence_count', None):
			online_percentage = int((guild.approximate_presence_count / guild.member_count) * 100)
			activity_emoji = "🔥" if online_percentage > 30 else "🟢" if online_percentage > 15 else "💤"
			member_info += f" • {activity_emoji} **{guild.approximate_presence_count:,}** active now"

		member_info += f" • 📅 Established <t:{int(guild.created_at.timestamp())}:D>"
		details_lines.append(member_info)

		# Channel information
		text_channels = len([c for c in guild.channels if isinstance(c, discord.TextChannel)])
		voice_channels = len([c for c in guild.channels if isinstance(c, discord.VoiceChannel)])
		if text_channels or voice_channels:
			channel_info = []
			if text_channels:
				channel_info.append(f"**{text_channels}** text channels")
			if voice_channels:
				channel_info.append(f"**{voice_channels}** voice channels")
			details_lines.append(f"💬 {' • '.join(channel_info)}")

		# Roles and emoji information
		role_count = len(guild.roles) - 1
		emoji_count = len(guild.emojis)
		role_emoji_info = []
		if role_count > 0:
			role_emoji_info.append(f"🏷️ **{role_count}** unique roles")
		if emoji_count >= 10:
			role_emoji_info.append(f"😊 **{emoji_count}** custom emojis")
		if role_emoji_info:
			details_lines.append(" • ".join(role_emoji_info))

		# Boost information
		if guild.premium_tier > 0:
			boost_emoji = BOOST_EMOJIS.get(guild.premium_tier, "💎")
			boost_text = f"{boost_emoji} **Level {guild.premium_tier}** Nitro Boosted"
			if guild.premium_subscription_count > 0:
				boost_text += f" ({guild.premium_subscription_count} boosts)"
			details_lines.append(boost_text)

		details_container.add_item(discord.ui.TextDisplay("\n".join(details_lines)))

		# Add detail buttons
		detail_button_row = discord.ui.ActionRow()
		detail_button_row.add_item(discord.ui.Button(
			style=discord.ButtonStyle.secondary,
			label="📊 Full Stats",
			emoji="📈",
			custom_id=f"server_info_{guild.id}"
		))

		# Add emoji button if server has interesting emojis
		interesting_emojis = [e for e in guild.emojis if e.animated][:8]
		if not interesting_emojis:
			interesting_emojis = guild.emojis[:8]

		if interesting_emojis:
			detail_button_row.add_item(discord.ui.Button(
				style=discord.ButtonStyle.secondary,
				label="😊 See All Emojis",
				emoji="🎨",
				custom_id=f"all_emojis_{guild.id}"
			))

		details_container.add_item(detail_button_row)
		layout_view.add_item(details_container)
		layout_view.add_item(discord.ui.Separator())

		# =================================================================
		# FEATURES SECTION
		# =================================================================

		# Process and sort guild features by priority
		guild_features = []
		for feature in guild.features:
			if feature in BUMP_DISPLAY_FEATURE_MAP:
				guild_features.append((
					BUMP_DISPLAY_FEATURE_MAP[feature]["priority"],
					BUMP_DISPLAY_FEATURE_MAP[feature]["display"]
				))

		guild_features.sort(key=lambda x: x[0])  # Sort by priority
		features = [f[1] for f in guild_features]

		if features:
			features_section = discord.ui.Section(
				accessory=discord.ui.Button(
					style=discord.ButtonStyle.secondary,
					label="✨ Learn More",
					emoji="ℹ️",
					custom_id=f"features_info_{guild.id}"
				)
			)
			features_section.add_item(discord.ui.TextDisplay("## 🌟 What Makes Us Special"))

			# Format features for display
			feature_text = []
			for i, feature in enumerate(features[:6], 1):
				feature_text.append(f"**{i}.** {feature}")

			features_section.add_item(discord.ui.TextDisplay("\n".join(feature_text)))
			layout_view.add_item(features_section)
			layout_view.add_item(discord.ui.Separator())

		# =================================================================
		# FOOTER
		# =================================================================

		footer_text = (
			"✨ *Powered by Imperial Bumps* • Ready to join? Click the button above!"
			if invite_link else
			"✨ *Powered by Imperial Bumps* • Click 'Explore Server' to learn more!"
		)
		layout_view.add_item(discord.ui.TextDisplay(footer_text))
		layout_view.accent_color = discord.Color.from_rgb(114, 137, 218)

		logger.debug(f"Created enhanced rich bump message for guild {guild.id} ({guild.name})")

		return {
			"content": None,
			"layout": layout_view,
			"files": files
		}

	except Exception as e:
		logger.error(f"Error creating rich bump message for guild {guild.id}: {e}", exc_info=True)
```