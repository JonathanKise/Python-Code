import os
import shutil
import asyncio
from collections import deque
import discord
from discord.ext import commands
from discord import app_commands
import yt_dlp
from dotenv import load_dotenv
import concurrent.futures
from urllib.parse import urlparse, parse_qs
import time
import random
from mutagen.mp3 import MP3
import requests
from bs4 import BeautifulSoup


# ====================
# ENV / CONFIG
# ====================
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN missing in .env")
DELETE_AFTER_PLAY = os.getenv("DELETE_AFTER_PLAY", "false").lower() == "true"
ARCHIVE_FOLDER = os.getenv("ARCHIVE_FOLDER", "./Archive")
MAX_PLAYLIST_SIZE = int(os.getenv("MAX_PLAYLIST_SIZE", "50"))
MUSIC_DOWNLOAD_FOLDER = os.getenv("MUSIC_DOWNLOAD_FOLDER", "./Queue")
os.makedirs(MUSIC_DOWNLOAD_FOLDER, exist_ok=True)
os.makedirs(ARCHIVE_FOLDER, exist_ok=True)

# ====================
# DISCORD SETUP
# ====================
intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True

class MusicBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=intents)
        self.queues: dict[int, deque[str]] = {}

    async def setup_hook(self):
        await self.tree.sync()
        print("✅ Slash commands synced")

bot = MusicBot()

# ====================
# BUTTON CONTROLS (Previous button removed)
# ====================
class MusicControls(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="⏸ Pause", style=discord.ButtonStyle.gray, custom_id="pause_resume", row=0)
    async def pause_resume(self, interaction: discord.Interaction, button: discord.ui.Button):
        voice = interaction.guild.voice_client
        if not voice:
            await interaction.response.send_message("❌ Not connected", ephemeral=True)
            return
        if voice.is_paused():
            voice.resume()
            button.label = "⏸ Pause"
            button.style = discord.ButtonStyle.gray
            await interaction.response.edit_message(view=self)
            await interaction.followup.send("▶ Resumed", ephemeral=True)
        else:
            voice.pause()
            button.label = "▶ Resume"
            button.style = discord.ButtonStyle.green
            await interaction.response.edit_message(view=self)
            await interaction.followup.send("⏸ Paused", ephemeral=True)

    @discord.ui.button(label="⏭ Skip", style=discord.ButtonStyle.blurple, row=0)
    async def skip(self, interaction: discord.Interaction, _):
        voice = interaction.guild.voice_client
        if voice:
            voice.stop()
            await interaction.response.send_message("⏭ Skipped", ephemeral=True)

    @discord.ui.button(label="🔀 Shuffle", style=discord.ButtonStyle.green, row=1)
    async def shuffle(self, interaction: discord.Interaction, _):
        voice = interaction.guild.voice_client
        if not voice:
            await interaction.response.send_message("❌ Not connected", ephemeral=True)
            return
        q = get_guild_queue(interaction.guild.id)
        if not q:
            await interaction.response.send_message("❌ Queue is empty", ephemeral=True)
            return
        shuffled = list(q)
        random.shuffle(shuffled)
        q.clear()
        q.extend(shuffled)
        await interaction.response.send_message("🔀 Queue shuffled!", ephemeral=True)

    @discord.ui.button(label="⏹ Stop", style=discord.ButtonStyle.red, row=1)
    async def stop(self, interaction: discord.Interaction, _):
        voice = interaction.guild.voice_client
        if voice:
            voice.stop()
            await voice.disconnect()
            q = get_guild_queue(interaction.guild.id)
            q.clear()
            # Clear queue folder
            for file in os.listdir(MUSIC_DOWNLOAD_FOLDER):
                try:
                    os.remove(os.path.join(MUSIC_DOWNLOAD_FOLDER, file))
                except:
                    pass
            # Delete now playing message on stop
            if hasattr(voice, 'now_playing_msg'):
                try:
                    await voice.now_playing_msg.delete()
                except:
                    pass
            await interaction.response.send_message("⏹ Stopped & left", ephemeral=True)

# ====================
# NOW PLAYING HELPERS
# ====================
def create_progress_bar(position_ms: int, duration_ms: int, length: int = 15) -> str:
    if duration_ms == 0:
        return "🌿" + "─" * (length - 1)
    progress = min(position_ms / duration_ms, 1.0)
    filled = int(progress * length)
    bar = "─" * filled + "🌿" + "─" * (length - filled - 1)
    return bar

def format_time(ms: int) -> str:
    seconds = int(ms // 1000)
    minutes = seconds // 60
    seconds %= 60
    return f"{minutes}:{seconds:02d}"

async def update_progress_bar(voice: discord.VoiceClient):
    if not hasattr(voice, 'now_playing_msg') or not voice.now_playing_msg:
        return
    while voice.is_playing() or voice.is_paused():
        await asyncio.sleep(5)
        if not voice.is_connected():
            break
        position = (time.time() - voice.start_time - voice.paused_time) * 1000
        if position > voice.duration + 2000:  # small buffer
            break
        progress = create_progress_bar(position, voice.duration)
        time_display = f"{format_time(position)} {progress} {format_time(voice.duration)}"
        title = os.path.basename(voice.current_path).replace('.mp3', '')
        embed = discord.Embed(
            title="🎵 Now Playing 🌿",
            description=f"**{title}**\n{time_display}",
            color=discord.Color.green()
        )
        try:
            await voice.now_playing_msg.edit(embed=embed, view=MusicControls())
        except discord.NotFound:
            # Message was deleted → stop updating
            break
        except discord.HTTPException as e:
            if e.code == 50027:  # Invalid Webhook Token / message too old
                break
            print(f"Edit failed: {e}")
    # Clean up when track ends / stopped
    if hasattr(voice, 'now_playing_msg'):
        try:
            await voice.now_playing_msg.delete()
        except:
            pass
        del voice.now_playing_msg

# ====================
# LINK PARSER (unchanged)
# ====================
def get_search_query(query: str) -> str:
    if "music.apple.com" in query or "itunes.apple.com" in query or "spotify.com" in query:
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(query, headers=headers, timeout=8)
            soup = BeautifulSoup(response.text, 'html.parser')
            title = soup.find('title')
            if title:
                song_info = title.text.replace('‎', '').replace(' on Apple Music', '').replace(' | Spotify', '')
                return f"ytsearch:{song_info.strip()}"
        except Exception as e:
            print(f"Link parse error: {e}")
    return query

# ====================
# YT-DLP HELPERS
# ====================
def download_mp3(url: str, output_folder: str = MUSIC_DOWNLOAD_FOLDER) -> str:
    os.makedirs(output_folder, exist_ok=True)
    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": f"{output_folder}/%(title)s.%(ext)s",
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }
        ],
        "quiet": True,
        "noplaylist": True,
        "ignorewarnings": True,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        filename = ydl.prepare_filename(info)
        if filename.endswith(".webm"):
            filename = filename.replace(".webm", ".mp3")
        elif filename.endswith(".m4a"):
            filename = filename.replace(".m4a", ".mp3")
        return filename

async def async_download_mp3(url: str, output_folder: str = MUSIC_DOWNLOAD_FOLDER) -> str | None:
    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        try:
            return await loop.run_in_executor(pool, download_mp3, url, output_folder)
        except yt_dlp.utils.DownloadError as e:
            if "Video unavailable" in str(e):
                print(f"Skipping unavailable video: {url}")
            else:
                print("Download error:", e)
            return None
        except Exception as e:
            print("Unexpected download error:", e)
            return None

def get_playlist_urls(url: str) -> list[str]:
    """Return list of video URLs from a playlist or single URL."""
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)
    if 'list' in query_params:
        playlist_id = query_params['list'][0]
        extraction_url = f"https://www.youtube.com/playlist?list={playlist_id}"
    else:
        extraction_url = url

    ydl_opts = {
        "quiet": True,
        "extract_flat": True,
        "skip_download": True,
        "ignorewarnings": True,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            info = ydl.extract_info(extraction_url, download=False)
        except yt_dlp.utils.DownloadError:
            return []

        if info.get("_type") == "playlist" and "entries" in info:
            urls = []
            for entry in info["entries"]:
                if not entry:
                    continue
                video_id = entry.get("id")
                if video_id:
                    urls.append(f"https://www.youtube.com/watch?v={video_id}")
            return urls

        # Single video or search result fallback
        return [url]

# ====================
# QUEUE / PLAYER LOGIC
# ====================
def get_guild_queue(guild_id: int) -> deque[str]:
    if guild_id not in bot.queues:
        bot.queues[guild_id] = deque()
    return bot.queues[guild_id]

def enqueue_tracks(guild_id: int, paths: list[str]) -> int:
    q = get_guild_queue(guild_id)
    for p in paths:
        q.append(p)
    return len(paths)

async def play_next_in_queue(guild: discord.Guild):
    voice = guild.voice_client
    if not voice:
        return
    q = get_guild_queue(guild.id)
    if not q:
        return
    next_path = q.popleft()
    print(f"▶ Playing: {next_path}")
    # Reset / set attributes
    voice.current_path = next_path
    try:
        voice.duration = MP3(next_path).info.length * 1000
    except:
        voice.duration = 0
    voice.start_time = time.time()
    voice.paused_time = 0.0
    voice.pause_start = None
    # Create / update now playing message
    progress = create_progress_bar(0, voice.duration)
    time_display = f"{format_time(0)} {progress} {format_time(voice.duration)}"
    title = os.path.basename(next_path).replace('.mp3', '')
    embed = discord.Embed(
        title="🎵 Now Playing 🌿",
        description=f"**{title}**\n{time_display}",
        color=discord.Color.green()
    )
    text_channel = getattr(voice, 'text_channel', guild.text_channels[0] if guild.text_channels else None)
    if text_channel:
        try:
            # If we already have a message, edit it
            if hasattr(voice, 'now_playing_msg') and voice.now_playing_msg:
                await voice.now_playing_msg.edit(embed=embed, view=MusicControls())
            else:
                # First time → send new
                msg = await text_channel.send(embed=embed, view=MusicControls())
                voice.now_playing_msg = msg
            # Start background progress updater
            bot.loop.create_task(update_progress_bar(voice))
        except Exception as e:
            print(f"Now playing send/edit error: {e}")
    def after_play(err=None):
        if err:
            print("FFmpeg error:", err)
        try:
            if DELETE_AFTER_PLAY and os.path.exists(next_path):
                os.remove(next_path)
            elif os.path.exists(next_path):
                shutil.move(next_path, os.path.join(ARCHIVE_FOLDER, os.path.basename(next_path)))
        except:
            pass
        asyncio.run_coroutine_threadsafe(play_next_in_queue(guild), bot.loop)
    source = discord.FFmpegPCMAudio(next_path, executable="ffmpeg")
    voice.play(source, after=after_play)

# ====================
# SLASH COMMANDS (only showing changed /play for brevity)
# ====================
@bot.tree.command(name="play", description="Play a YouTube video or playlist URL")
@app_commands.describe(query="YouTube video or playlist URL")
async def play(interaction: discord.Interaction, query: str):
    await interaction.response.defer()
    if not interaction.user.voice or not interaction.user.voice.channel:
        await interaction.followup.send("❌ You must be in a voice channel.", ephemeral=True)
        return
    voice = interaction.guild.voice_client
    if not voice:
        try:
            voice = await interaction.user.voice.channel.connect()
        except Exception as e:
            await interaction.followup.send(f"❌ Failed to connect: `{e}`", ephemeral=True)
            return
    voice.text_channel = interaction.channel
    parsed_query = get_search_query(query)
    if parsed_query is None:
        await interaction.followup.send("❌ Playlists not supported from Spotify/Apple. Use YouTube or song name.", ephemeral=True)
        return
    try:
        urls = get_playlist_urls(parsed_query)
    except Exception as e:
        await interaction.followup.send(f"❌ Failed to parse: `{e}`", ephemeral=True)
        return
    if not urls:
        await interaction.followup.send("❌ No valid tracks found.", ephemeral=True)
        return
    is_search = not query.startswith(('http', 'https'))
    if is_search:
        urls = urls[:1]
    if len(urls) > MAX_PLAYLIST_SIZE:
        urls = urls[:MAX_PLAYLIST_SIZE]
    await interaction.followup.send(f"📥 Downloading {len(urls)} track(s)...")
    tasks = [async_download_mp3(u) for u in urls]
    paths = await asyncio.gather(*tasks)
    downloaded_paths = [p for p in paths if p is not None]
    if not downloaded_paths:
        await interaction.followup.send("❌ Failed to download any tracks.", ephemeral=True)
        return
    added = enqueue_tracks(interaction.guild.id, downloaded_paths)
    msg = f"✅ Added **{added}** track{'s' if added > 1 else ''} to the queue."
    await interaction.followup.send(msg)
    if not voice.is_playing():
        await play_next_in_queue(interaction.guild)

@bot.tree.command(name="skip", description="Skip the current song")
async def skip(interaction: discord.Interaction):
    voice: discord.VoiceClient | None = interaction.guild.voice_client
    if not voice or not voice.is_connected():
        await interaction.response.send_message("❌ Not connected.", ephemeral=True)
        return
    if not voice.is_playing():
        await interaction.response.send_message("❌ Nothing is playing.", ephemeral=True)
        return
    voice.stop()
    await interaction.response.send_message("⏭ Skipped.", ephemeral=True)

@bot.tree.command(name="stop", description="Stop playback and clear the queue")
async def stop(interaction: discord.Interaction):
    voice: discord.VoiceClient | None = interaction.guild.voice_client
    if voice and voice.is_connected():
        voice.stop()
        await voice.disconnect()
    q = get_guild_queue(interaction.guild.id)
    q.clear()
    # Clear queue folder
    for file in os.listdir(MUSIC_DOWNLOAD_FOLDER):
        try:
            os.remove(os.path.join(MUSIC_DOWNLOAD_FOLDER, file))
        except Exception as e:
            print(f"Error deleting file {file}: {e}")
    await interaction.response.send_message("⏹ Stopped and cleared the queue.", ephemeral=True)

@bot.tree.command(name="queue", description="Show the current queue")
async def queue_cmd(interaction: discord.Interaction):
    q = get_guild_queue(interaction.guild.id)
    if not q:
        await interaction.response.send_message("🎵 Queue is empty.", ephemeral=True)
        return
    lines = []
    for i, path in enumerate(list(q)[:10], start=1):
        name = os.path.basename(path)
        lines.append(f"{i}. {name}")
    if len(q) > 10:
        lines.append(f"... and {len(q) - 10} more")
    await interaction.response.send_message("🎵 **Current queue:**\n" + "\n".join(lines), ephemeral=True)

@bot.tree.command(name="pause", description="Pause the current song")
async def pause(interaction: discord.Interaction):
    voice: discord.VoiceClient | None = interaction.guild.voice_client
    if not voice or not voice.is_connected():
        await interaction.response.send_message("❌ Not connected.", ephemeral=True)
        return
    if not voice.is_playing():
        await interaction.response.send_message("❌ Nothing is playing.", ephemeral=True)
        return
    voice.pause()
    await interaction.response.send_message("⏸ Paused.", ephemeral=True)

@bot.tree.command(name="resume", description="Resume the current song")
async def resume(interaction: discord.Interaction):
    voice: discord.VoiceClient | None = interaction.guild.voice_client
    if not voice or not voice.is_connected():
        await interaction.response.send_message("❌ Not connected.", ephemeral=True)
        return
    if not voice.is_paused():
        await interaction.response.send_message("❌ Not paused.", ephemeral=True)
        return
    voice.resume()
    await interaction.response.send_message("▶ Resumed.", ephemeral=True)

# ====================
# RUN BOT
# ====================
if __name__ == "__main__":
    print("🚀 Starting bot...")
    bot.run(TOKEN)