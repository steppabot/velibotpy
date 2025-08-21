import discord
from discord import app_commands, channel
from discord.ui import Modal, TextInput, Button, View, Select
from discord.app_commands import AppCommandError, CheckFailure
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageOps, ImageChops
from datetime import datetime, timedelta, timezone
from collections import Counter
from dotenv import load_dotenv
from psycopg2 import sql
from copy import deepcopy
from discord import PartialEmoji
from typing import Optional
from bidi.algorithm import get_display
from io import BytesIO
import io
import os
import re
import psycopg2
import unicodedata
import string
import stripe
import requests
import asyncio
import emoji
import regex
import contextlib
import arabic_reshaper

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
stripe.api_key = os.getenv("STRIPE_SECRET_KEY")
# Load database URL from environment
DATABASE_URL = os.getenv("DATABASE_URL")

@contextlib.contextmanager
def get_safe_cursor():
    global conn
    try:
        # ping to detect a dead connection
        with conn.cursor() as ping:
            ping.execute("SELECT 1")
    except (psycopg2.InterfaceError, psycopg2.OperationalError):
        # reconnect if needed
        print("🔁 Reconnecting to database...")
        conn, _ = init_db()

    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

def init_db():
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        cursor = conn.cursor()

        # ─── veil_messages ─────────────────────────────────────────────────────
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS veil_messages (
                id SERIAL PRIMARY KEY,
                message_id BIGINT UNIQUE NOT NULL,
                channel_id BIGINT NOT NULL,
                author_id BIGINT NOT NULL,
                content TEXT NOT NULL,
                timestamp TIMESTAMPTZ DEFAULT NOW()
            )
        ''')
        cursor.execute("""
            SELECT column_name
              FROM information_schema.columns
             WHERE table_name = 'veil_messages'
        """)
        cols = {row[0] for row in cursor.fetchall()}
        if 'guess_count' not in cols:
            cursor.execute("ALTER TABLE veil_messages ADD COLUMN guess_count INTEGER DEFAULT 0")
        if 'is_unveiled' not in cols:
            cursor.execute("ALTER TABLE veil_messages ADD COLUMN is_unveiled BOOLEAN DEFAULT FALSE")
        if 'veil_number' not in cols:
            cursor.execute("ALTER TABLE veil_messages ADD COLUMN veil_number INTEGER")

        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_vm_channel_veilno
            ON veil_messages(channel_id, veil_number)
        """)

        # simple per-channel counter; we store the *last assigned* number
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS veil_channel_counters (
                channel_id BIGINT PRIMARY KEY,
                current_number INTEGER NOT NULL DEFAULT 0
            )
        """)

        # ─── veil_guesses ──────────────────────────────────────────────────────
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS veil_guesses (
                id SERIAL PRIMARY KEY,
                message_id BIGINT NOT NULL,
                guesser_id  BIGINT NOT NULL,
                guessed_user_id BIGINT NOT NULL,
                timestamp TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (message_id, guesser_id)
            )
        ''')

        # Ensure is_correct column exists
        cursor.execute("""
            SELECT column_name
              FROM information_schema.columns
             WHERE table_name = 'veil_guesses'
        """)
        guess_cols = {row[0] for row in cursor.fetchall()}
        if 'is_correct' not in guess_cols:
            cursor.execute(
                "ALTER TABLE veil_guesses ADD COLUMN is_correct BOOLEAN NOT NULL DEFAULT FALSE"
            )

        # ─── latest_veil_messages, veil_users, veil_channels, etc. ─────────────
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS latest_veil_messages (
                channel_id BIGINT PRIMARY KEY,
                message_id BIGINT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS veil_users (
                user_id BIGINT NOT NULL,
                guild_id BIGINT NOT NULL,
                coins INTEGER DEFAULT 0,
                veils_unveiled INTEGER DEFAULT 0,
                last_reset TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (user_id, guild_id)
            )
        ''')
        cursor.execute("""
            SELECT column_name
              FROM information_schema.columns
             WHERE table_name = 'veil_users'
        """)
        user_cols = {row[0] for row in cursor.fetchall()}
        if 'last_refill' not in user_cols:
            cursor.execute("ALTER TABLE veil_users ADD COLUMN last_refill TIMESTAMPTZ")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS veil_channels (
                guild_id BIGINT PRIMARY KEY,
                channel_id BIGINT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS veil_admin_channels (
                guild_id BIGINT PRIMARY KEY,
                channel_id BIGINT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS veil_subscriptions (
                guild_id BIGINT PRIMARY KEY,
                tier TEXT NOT NULL DEFAULT 'free',
                subscribed_at TIMESTAMPTZ DEFAULT NOW(),
                renews_at TIMESTAMPTZ,
                payment_failed BOOLEAN DEFAULT FALSE
            )
        ''')
        cursor.execute("""
            SELECT column_name
              FROM information_schema.columns
             WHERE table_name = 'veil_subscriptions'
        """)
        sub_cols = {row[0] for row in cursor.fetchall()}
        if 'subscription_id' not in sub_cols:
            cursor.execute("ALTER TABLE veil_subscriptions ADD COLUMN subscription_id TEXT")

        # ─── coin_checkout_sessions (for editing ephemeral after purchase) ───
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS coin_checkout_sessions (
                stripe_session_id TEXT PRIMARY KEY,
                interaction_token TEXT NOT NULL,
                application_id   BIGINT NOT NULL,
                user_id          BIGINT NOT NULL,
                guild_id         BIGINT NOT NULL,
                coins            INTEGER NOT NULL,
                created_at       TIMESTAMPTZ DEFAULT NOW()
            )
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_coin_checkout_sessions_created_at
            ON coin_checkout_sessions (created_at)
        ''')

        conn.commit()
        return conn, cursor

    except Exception as e:
        print(f"Database error: {e}")
        return None, None

ZWS = "\u200B"

APPLICATION_EMOJIS = {
    "veilsuccess": 1403948454589235201,
    "veilcaution": 1403948435895488592,
    "veilincorrect": 1403948415498584135,
    "veiladd": 1403948390819299440,
    "unveilemoji": 1403948376059412562,
    "veilcoin": 1403948316429123675,
    "veilemoji": 1403948357222924410,
    "veilstore": 1404977462638805117,
    "1st": 1404977588161482783,
    "2nd": 1404977637171920927,
    "3rd": 1404977679152709804,
    "4th": 1404977715039440987, 
    "5th": 1404977749868806195,
    "6th": 1404977759943397477, 
    "7th": 1404977769699606733,
    "8th": 1404977785268867092,
    "9th": 1404977796459004104,
    "10th": 1404977804621123697
}

intents = discord.Intents.default()
intents.messages = True
intents.guilds = True
intents.members = True  # ✅ You need this!
intents.message_content = True

conn, _ = init_db()

client = discord.AutoShardedClient(intents=intents)   # one process, many shards
tree = app_commands.CommandTree(client)

@client.event
async def on_shard_ready(shard_id: int):
    lat = client.shards[shard_id].latency * 1000
    print(f"✅ Shard {shard_id} ready ({lat:.0f} ms)")

@client.event
async def on_shard_resumed(shard_id: int):
    print(f"🔄 Shard {shard_id} resumed")

@client.event
async def on_shard_disconnect(shard_id: int):
    print(f"⚠️ Shard {shard_id} disconnected")

@client.event
async def on_ready():
    print(f"logged in as {client.user} with {client.shard_count} shard(s)")

    # Load emojis
    client.app_emojis = {
        name: discord.PartialEmoji(name=name, id=eid)
        for name, eid in APPLICATION_EMOJIS.items()
    }
    print("✅ Loaded application emojis")

    # Background tasks
    client.loop.create_task(notify_failed_payments())
    
    # Sync commands
    await tree.sync()
    print("✅ Command Tree Synced")

    # Persistent view(s)
    client.add_view(WelcomeView())
    client.add_view(StoreView())
    await hydrate_latest_views()



OWNER_IDS = {568583831985061918}  # <-- your Discord user ID(s)
SUPPORT_SERVER_ID = 1394932709394087946  # Your support server ID
SUPPORT_CHANNEL_ID = 1399973286649008158  # The channel where webhook posts
DISCORD_API_BASE = "https://discord.com/api/v10"
UPGRADE_RE = re.compile(
    r"guild\s+(\d+)\s+upgraded\s+to\s+\*{0,2}([A-Za-z]+)\*{0,2}\s+tier!?",
    re.IGNORECASE,
)
COIN_RE = re.compile(
    r"^\[COIN_TOPUP\]\s+session_id=(\S+)\s+user_id=(\d+)\s+guild_id=(\d+)\s+coins=(\d+)\s*$"
)

emoji_patternz = re.compile(r"<a?:([a-zA-Z0-9_]+):(\d+)>")  # matches <:name:id> and <a:name:id>
emoji_sequence_pattern = regex.compile(r'\X', regex.UNICODE)


def fmt(n) -> str:
    try: return f"{int(n):,}"
    except: return str(n)
    
# 🪙 Coin allocations by tier
COINS_BY_TIER = {
    'free': 100,
    'basic': 250,
    'premium': 1000,
    'elite': None  # Unlimited
}

STRIPE_PRICE_IDS = {
    "basic": "price_1RuT1sADYgCtNnMoWMzdQ7YI",     
    "premium": "price_1RuT34ADYgCtNnModSx70nr1",
    "elite": "price_1RuT3ZADYgCtNnMopSZon3vt"}

COIN_PRICE_IDS = {
    100:  "price_1RuT5IADYgCtNnMorF0zsMRK",  # $1
    250:  "price_1RuT5dADYgCtNnMoNY5O0cuc",  # $2
    500:  "price_1RuT5yADYgCtNnMoWTUR4XMC",  # $3
    1000: "price_1RuT6KADYgCtNnMoKwM3iw9H",  # $5 
    }

COIN_PACKS = [
    (100, 100),    # (coins, price_cents)
    (250, 200),
    (500, 300),
    (1000, 500),
]

CUSTOM_EMOJI = re.compile(r'<a?:\w+:(\d+)>')
CUSTOM_EMOJI_RE = re.compile(r"<a?:\w{1,32}:\d{17,20}>")
EMOJI_LIMIT = 50
MAX_VISUAL = 200
discord_emoji_pattern = re.compile(r"<a?:\w+:(\d+)>", re.IGNORECASE)
TWEMOJI_BASE = "https://cdn.jsdelivr.net/gh/twitter/twemoji@latest/assets/72x72"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # folder where veilbot.py is
PNG_EMOJI_DIR = os.path.join(BASE_DIR, "png")
ARABIC_RE = re.compile(r'[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]')
CJK_RE = re.compile(r'[\u3400-\u4DBF\u4E00-\u9FFF\uF900-\uFAFF]')
DEVANAGARI_RE = re.compile(r'[\u0900-\u097F]')
MENTION_RE = re.compile(r"<(@!?|@&|#)(\d+)>")

FONT_MAP = {
    "latin": "ariblk.ttf",        # English + Latin
    "arabic": "arabic2.ttf",       # NotoNaskhArabic
    "cjk": "chinese3.ttf",         # NotoSansSC/TC
    "devanagari": "indian.ttf",   # NotoSansDevanagari
}

def _safe_display_name(member):
    return member.display_name if member else None

async def normalize_mentions(text: str, guild: discord.Guild, client: discord.Client) -> str:
    """Turn <@id>, <@!id>, <@&id>, <#id> into @name / @role / #channel (no pings)."""
    if not text or not guild:
        return text

    replacements = {}

    for kind, raw_id in MENTION_RE.findall(text):
        full_tag = f"<{kind}{raw_id}>"
        if full_tag in replacements:
            continue

        _id = int(raw_id)

        if kind.startswith("@"):  # user mention <@id> or <@!id>
            name = None
            m = guild.get_member(_id)
            if m:
                name = _safe_display_name(m)
            else:
                # fallback to global user cache / fetch (non-blocking if cached)
                u = client.get_user(_id)
                if not u:
                    try:
                        u = await client.fetch_user(_id)
                    except Exception:
                        u = None
                name = (u.global_name or u.name) if u else f"user:{_id}"
            replacements[full_tag] = f"@{name}"

        elif kind == "@&":  # role mention
            role = guild.get_role(_id)
            replacements[full_tag] = f"@{role.name}" if role else f"@role:{_id}"

        elif kind == "#":   # channel mention
            ch = guild.get_channel(_id)
            replacements[full_tag] = f"#{ch.name}" if ch and hasattr(ch, "name") else f"#chan:{_id}"

    # apply replacements
    if not replacements:
        return text

    def sub_fn(m):
        kind, raw_id = m.groups()
        return replacements.get(f"<{kind}{raw_id}>", m.group(0))

    return MENTION_RE.sub(sub_fn, text)

def detect_script(text: str) -> str:
    if ARABIC_RE.search(text):
        return "arabic"
    if CJK_RE.search(text):
        return "cjk"
    if DEVANAGARI_RE.search(text):
        return "devanagari"
    return "latin"

def get_render_text_and_font(text: str):
    script = detect_script(text)

    if script == "arabic":
        render_text = shape_rtl(text)
        font_file = FONT_MAP["arabic"]
    elif script == "cjk":
        render_text = text  # no shaping
        font_file = FONT_MAP["cjk"]
    elif script == "devanagari":
        render_text = text  # no shaping
        font_file = FONT_MAP["devanagari"]
    else:
        render_text = text
        font_file = FONT_MAP["latin"]

    return render_text, font_file

def shape_rtl(s: str) -> str:
    # reshape Arabic letters into contextual forms, then reorder visually (RTL)
    reshaped = arabic_reshaper.reshape(s)
    return get_display(reshaped)

def count_emojis_all(text: str) -> int:
    custom = len(CUSTOM_EMOJI_RE.findall(text))
    # emoji.emoji_list finds full emoji sequences (ZWJ, flags, keycaps) like iPhone sends
    unicode_count = len(emoji.emoji_list(text))
    return custom + unicode_count

def visual_length(text: str) -> int:
    parts = regex.findall(r"(<a?:\w{1,32}:\d{17,20}>|\X)", text)
    return len(parts)

def _format_price(cents: int) -> str:
    return f"${cents/100:.2f}"

def _format_coins(n: int) -> str:
    return f"{n:,}"

def get_local_emoji(token: str, size: int):
    """Look up emoji from local PNGs, handling multi-codepoint emojis."""
    # Remove variation selector (FE0F)
    codepoints = [f"{ord(c):x}" for c in token if ord(c) != 0xfe0f]
    filename = "-".join(codepoints) + ".png"
    local_path = os.path.join(PNG_EMOJI_DIR, filename)

    if os.path.exists(local_path):
        return Image.open(local_path).convert("RGBA").resize((size, size), Image.LANCZOS)
    return None

def split_long_word(word: str, max_chars: int = 12) -> list[str]:
    return [word[i:i + max_chars] for i in range(0, len(word), max_chars)]

def draw_text_with_shadow(image, position, text, font, fill,
                          shadow_color=(0, 0, 0, 60), offset=(2, 2), blur_radius=2):
    """
    Draws text with a soft blurred shadow on the image.
    """
    x, y = position

    # Create a transparent layer for the shadow
    shadow_layer = Image.new("RGBA", image.size, (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow_layer)

    # Draw shadow text
    shadow_draw.text((x + offset[0], y + offset[1]), text, font=font, fill=shadow_color)

    # Blur the shadow
    blurred_shadow = shadow_layer.filter(ImageFilter.GaussianBlur(blur_radius))

    # Composite shadow onto the original image
    image.alpha_composite(blurred_shadow)

    # Draw actual text on top
    draw = ImageDraw.Draw(image)
    draw.text((x, y), text, font=font, fill=fill)

def trim_emoji(img: Image.Image) -> Image.Image:
    bbox = img.getbbox()
    return img.crop(bbox) if bbox else img

def clean_unicode_emoji(token: str) -> str:
    # Only strip FE0F (variation selector), keep ZWJ so sequences stay intact
    return token.replace("\uFE0F", "")

def unicode_to_twemoji_url(cluster: str) -> str:
    # Keep full sequence, join by '-' for each codepoint (skip FE0F)
    codepoints = "-".join(f"{ord(c):x}" for c in cluster if ord(c) != 0xfe0f)
    return f"https://cdnjs.cloudflare.com/ajax/libs/twemoji/14.0.2/72x72/{codepoints}.png"

def tokenize_message_for_wrap(text: str):
    """Tokenize text for wrapping, keeping words intact and handling emojis properly."""
    tokens = []
    current_word = ""

    # 1️⃣ Separate Discord custom emojis from words (adds spaces around them)
    text = re.sub(r'(?<=\w)(<a?:\w+:\d+>)', r' \1', text)  # add space before
    text = re.sub(r'(<a?:\w+:\d+>)(?=\w)', r'\1 ', text)    # add space after

    # 2️⃣ Capture either a custom emoji OR a grapheme cluster
    pattern = regex.compile(r"(<a?:\w+:\d+>|\X)", regex.UNICODE)
    graphemes = pattern.findall(text)

    def flush_word():
        nonlocal current_word
        if current_word:
            tokens.append(current_word.upper())
            current_word = ""

    for g in graphemes:
        if discord_emoji_pattern.fullmatch(g):
            # ✅ Custom emoji <:name:id>
            flush_word()
            tokens.append(g)
        elif emoji.is_emoji(g):
            # ✅ Unicode emoji (single or ZWJ sequence)
            flush_word()
            tokens.append(g)
        elif g.isspace():
            # ✅ Space
            flush_word()
            tokens.append(" ")
        else:
            # ✅ Normal character → build word
            current_word += g

    flush_word()
    return tokens

async def render_emojis(draw, image, tokens, x_start, y, font, emoji_size, emoji_padding, color):
    for token in tokens:
        raw_token = token
        stripped = token.strip()
        em_img = None
        emoji_offset_y = 0

        # 1) Custom Discord emoji via CDN
        m = discord_emoji_pattern.fullmatch(stripped)
        if m:
            emoji_id = m.group(1)
            url = f"https://cdn.discordapp.com/emojis/{emoji_id}.png?size=96"
            try:
                resp = requests.get(url, timeout=5)
                if resp.status_code == 200:
                    em_img = (
                        Image.open(io.BytesIO(resp.content))
                        .convert("RGBA")
                        .resize((emoji_size, emoji_size), Image.LANCZOS)
                    )
                    emoji_offset_y = 17
                else:
                    print(f"⚠️ custom emoji {emoji_id} returned HTTP {resp.status_code}")
            except Exception as e:
                print(f"⚠️ custom emoji fetch error for {emoji_id}: {e}")

        # 2) Unicode emoji (iPhone/Twemoji) → local, then CDN
        if em_img is None and stripped:
            parts = emoji.emoji_list(stripped)  # robust: catches ZWJ, flags, keycaps, skin tones
            if parts:
                # Use the first emoji span inside this token
                span = parts[0]
                em_sub = stripped[span["match_start"]:span["match_end"]]

                cps_full = [f"{ord(c):x}" for c in em_sub]              # includes fe0f/200d if present
                with_fe0f    = "-".join(cps_full)
                without_fe0f = "-".join(cp for cp in cps_full if cp != "fe0f")
                candidates = [with_fe0f] if with_fe0f == without_fe0f else [with_fe0f, without_fe0f]

                tried_local, tried_cdn = [], []

                # Try local first
                for codepoints in candidates:
                    local_file = os.path.join(PNG_EMOJI_DIR, f"{codepoints}.png")
                    if os.path.exists(local_file):
                        try:
                            em_img = (
                                Image.open(local_file)
                                .convert("RGBA")
                                .resize((emoji_size, emoji_size), Image.LANCZOS)
                            )
                            emoji_offset_y = 17
                            break
                        except Exception as e:
                            print(f"⚠️ local twemoji load error for {codepoints}: {e}")
                    else:
                        tried_local.append(local_file)

                # Then CDN
                if em_img is None:
                    for codepoints in candidates:
                        tw_url = f"{TWEMOJI_BASE}/{codepoints}.png"
                        try:
                            resp = requests.get(tw_url, timeout=5)
                            if resp.status_code == 200:
                                em_img = (
                                    Image.open(io.BytesIO(resp.content))
                                    .convert("RGBA")
                                    .resize((emoji_size, emoji_size), Image.LANCZOS)
                                )
                                emoji_offset_y = 17
                                break
                            else:
                                tried_cdn.append((resp.status_code, tw_url))
                        except Exception as e:
                            tried_cdn.append((f"error:{e}", tw_url))

                # Still missing? Print one consolidated debug line
                if em_img is None:
                    hex_token = " ".join(f"U+{ord(c):04X}" for c in stripped)
                    hex_emoji = " ".join(f"U+{ord(c):04X}" for c in em_sub)
                    print(
                        "❌ Missing Unicode emoji image\n"
                        f"   token: {repr(stripped)}   ({hex_token})\n"
                        f"   emoji: {repr(em_sub)}     ({hex_emoji})\n"
                        f"   tried local: {tried_local or '—'}\n"
                        f"   tried CDN:   { [f'{st} {u}' for st,u in tried_cdn] or '—' }"
                    )

        # 3) Draw either the emoji or fallback to text
        if em_img:
            # soft shadow
            shadow_layer = Image.new("RGBA", em_img.size, (0, 0, 0, 0))
            sd = ImageDraw.Draw(shadow_layer)
            sd.bitmap((0, 0), em_img, fill=(0, 0, 0, 100))
            shadow_blur = shadow_layer.filter(ImageFilter.GaussianBlur(2))
            image.alpha_composite(shadow_blur, (int(x_start + 2), int(y + emoji_offset_y + 2)))

            # emoji
            image.alpha_composite(em_img, (int(x_start), int(y + emoji_offset_y)))
            x_start += emoji_size + emoji_padding
        else:
            # text with soft shadow
            draw_text_with_shadow(
                image,
                (x_start, y),
                raw_token,
                font,
                fill=color,
                shadow_color=(0, 0, 0, 60),
                offset=(2, 2),
                blur_radius=3
            )
            x_start += draw.textlength(raw_token, font=font)

    return x_start
    
def build_wrapped_lines(tokens, font, box_width, draw, emoji_size=48, emoji_padding=4):
    lines = []
    current_line = []
    current_width = 0

    def token_width(token):
        if discord_emoji_pattern.fullmatch(token) or emoji.is_emoji(token.strip()):
            return emoji_size + emoji_padding
        return draw.textlength(token, font=font)

    for token in tokens:
        width = token_width(token)

        # Ignore leading spaces
        if token.isspace() and not current_line:
            continue

        # Check if token is longer than the box
        if width > box_width:
            if current_line:
                lines.append(current_line)
                current_line = []
                current_width = 0

            # Force split word by pixel width
            split_parts = []
            current_part = ""
            for char in token:
                test_part = current_part + char
                if draw.textlength(test_part, font=font) > box_width and current_part:
                    split_parts.append(current_part)
                    current_part = char
                else:
                    current_part = test_part
            if current_part:
                split_parts.append(current_part)

            for part in split_parts:
                lines.append([part])
            continue

        # Normal wrapping
        if current_width + width > box_width:
            lines.append(current_line)
            current_line = [] if token.isspace() else [token]
            current_width = 0 if token.isspace() else width
        else:
            current_line.append(token)
            current_width += width

    if current_line:
        lines.append(current_line)

    return lines

def calculate_line_y(line_tokens, font, base_y):
    """Adjust baseline if line is mostly emoji (iOS/multi-emoji fix)."""
    ascent, _ = font.getmetrics()
    emoji_count = sum(
        1 for t in line_tokens
        if discord_emoji_pattern.fullmatch(t) or emoji.is_emoji(t.strip())
    )
    if line_tokens and emoji_count >= len(line_tokens) * 0.7:
        return base_y + int(ascent * 0.35)  # shift down for emoji alignment
    return base_y

def is_visually_blank(name):
    stripped = name.strip()
    if not stripped:
        return True

    # Remove common invisible or formatting characters
    stripped = ''.join(c for c in stripped if not unicodedata.category(c).startswith('C'))

    # If it's only punctuation or symbols, consider it blank
    only_symbols = all(c in string.punctuation or unicodedata.category(c).startswith('S') for c in stripped)

    return not any(c.isalnum() for c in stripped) or only_symbols

def get_display_name_safe(member):
    name = member.display_name
    return member.name if is_visually_blank(name) else name

def is_latest_veil(channel_id, message_id):
    latest = get_latest_message_id(channel_id)
    return latest == message_id

def get_latest_message_id(channel_id):
    try:
        with get_safe_cursor() as cur:
            cur.execute("SELECT message_id FROM latest_veil_messages WHERE channel_id = %s", (channel_id,))
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as e:
        print(f"Error fetching latest message: {e}")
        return None

def set_latest_message_id(channel_id, message_id):
    try:
        with get_safe_cursor() as cur:
            cur.execute("""
                INSERT INTO latest_veil_messages (channel_id, message_id)
                VALUES (%s, %s)
                ON CONFLICT (channel_id)
                DO UPDATE SET message_id = EXCLUDED.message_id
            """, (channel_id, message_id))
            conn.commit()
    except Exception as e:
        print(f"Error saving latest message: {e}")

def ensure_user_entry(user_id, guild_id):
    with get_safe_cursor() as cur:
        cur.execute("""
            INSERT INTO veil_users (user_id, guild_id)
            VALUES (%s, %s)
            ON CONFLICT (user_id, guild_id) DO NOTHING
        """, (user_id, guild_id))
        conn.commit()

def get_user_coins(user_id, guild_id):
    with get_safe_cursor() as cur:
        cur.execute("""
            SELECT coins FROM veil_users WHERE user_id = %s AND guild_id = %s
        """, (user_id, guild_id))
        result = cur.fetchone()
        return result[0] if result else 0

def add_user_coins(user_id, guild_id, amount):
    with get_safe_cursor() as cur:
        cur.execute("""
            UPDATE veil_users
            SET coins = coins + %s
            WHERE user_id = %s AND guild_id = %s
        """, (amount, user_id, guild_id))
        # If user entry doesn't exist, create one
        if cur.rowcount == 0:
            cur.execute("""
                INSERT INTO veil_users (user_id, guild_id, coins)
                VALUES (%s, %s, %s)
            """, (user_id, guild_id, amount))
        conn.commit()

def deduct_user_coins(user_id, guild_id, amount):
    with get_safe_cursor() as cur:
        cur.execute("""
            UPDATE veil_users SET coins = coins - %s
            WHERE user_id = %s AND guild_id = %s AND coins >= %s
        """, (amount, user_id, guild_id, amount))
        conn.commit()
        return cur.rowcount > 0  # True if deduction succeeded

def increment_unveiled_count(user_id, guild_id):
    with get_safe_cursor() as cur:
        cur.execute("""
            UPDATE veil_users SET veils_unveiled = veils_unveiled + 1
            WHERE user_id = %s AND guild_id = %s
        """, (user_id, guild_id))
        conn.commit()

def build_frozen_view(msg_id: int, guild: discord.Guild):
    try:
        with get_safe_cursor() as cur:
            cur.execute("""
                SELECT channel_id, guess_count, is_unveiled, author_id, veil_number
                FROM veil_messages
                WHERE message_id = %s
            """, (msg_id,))
            row = cur.fetchone()

        if not row:
            return None

        channel_id, guess_count, is_unveiled, author_id, veil_number = row
        author_member = guild.get_member(author_id)

        # Build the base view with the correct per-channel number
        view = VeilView(veil_number=veil_number)

        # Update labels / disablements
        for child in list(view.children):
            if isinstance(child, discord.ui.Button):
                if child.custom_id == "guess_count":
                    child.label = f"Guesses {guess_count}/3"
                elif child.custom_id == "submitted_by":
                    if is_unveiled and author_member:
                        display_name = get_display_name_safe(author_member)
                        child.label = f"Submitted by {display_name.capitalize()}"
                elif child.custom_id == "guess_btn":
                    child.disabled = is_unveiled or guess_count >= 3
                elif child.custom_id == "new_btn":
                    # ✅ Use the *channel* to check latest
                    if not is_latest_veil(channel_id, msg_id):
                        view.remove_item(child)

        return view

    except Exception as e:
        print(f"❌ Failed to build frozen view: {e}")
        return None

def set_veil_channel(guild_id, channel_id):
    with get_safe_cursor() as cur:
        cur.execute('''
            INSERT INTO veil_channels (guild_id, channel_id)
            VALUES (%s, %s)
            ON CONFLICT (guild_id) DO UPDATE SET channel_id = EXCLUDED.channel_id
        ''', (guild_id, channel_id))
        conn.commit()

def get_veil_channel(guild_id):
    with get_safe_cursor() as cur:
        cur.execute('SELECT channel_id FROM veil_channels WHERE guild_id = %s', (guild_id,))
        result = cur.fetchone()
        return result[0] if result else None
    
def set_veil_admin_channel(guild_id, channel_id):
    with get_safe_cursor() as cur:
        cur.execute('''
            INSERT INTO veil_admin_channels (guild_id, channel_id)
            VALUES (%s, %s)
            ON CONFLICT (guild_id) DO UPDATE SET channel_id = EXCLUDED.channel_id
        ''', (guild_id, channel_id))
        conn.commit()

def get_veil_admin_channel(guild_id):
    with get_safe_cursor() as cur:
        cur.execute('SELECT channel_id FROM veil_admin_channels WHERE guild_id = %s', (guild_id,))
        result = cur.fetchone()
        return result[0] if result else None

def get_subscription_tier(guild_id):
    with get_safe_cursor() as cur:
        cur.execute("SELECT tier FROM veil_subscriptions WHERE guild_id = %s", (guild_id,))
        result = cur.fetchone()
        return result[0] if result else "free"

def set_subscription_tier(guild_id, tier, renews_at=None):
    with get_safe_cursor() as cur:
        cur.execute('''
            INSERT INTO veil_subscriptions (guild_id, tier, renews_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (guild_id) DO UPDATE
            SET tier = EXCLUDED.tier,
                renews_at = EXCLUDED.renews_at,
                subscribed_at = NOW()
        ''', (guild_id, tier, renews_at))
        conn.commit()

def refill_user_coins(user_id, guild_id):
    with get_safe_cursor() as cur:
        # Get user record
        cur.execute("""
            SELECT coins, last_refill FROM veil_users
            WHERE user_id = %s AND guild_id = %s
        """, (user_id, guild_id))
        result = cur.fetchone()

        if not result:
            return  # No user record

        current_coins, last_refill = result

        # Get subscription tier
        cur.execute("""
            SELECT tier FROM veil_subscriptions WHERE guild_id = %s
        """, (guild_id,))
        sub_result = cur.fetchone()
        tier = sub_result[0] if sub_result else 'free'

        # Elite tier = skip refill
        if tier == 'elite':
            return

        # Decide coin amount by tier
        refill_amount = {
            "free": 100,
            "basic": 250,
            "premium": 1000,
            "elite": None
        }.get(tier, 100)

        # Check if 30 days have passed
        now = datetime.now(timezone.utc)
        if not last_refill or now - last_refill >= timedelta(days=30):
            new_coins = current_coins + refill_amount

            # Update user record
            cur.execute("""
                UPDATE veil_users
                SET coins = %s,
                    last_refill = %s
                WHERE user_id = %s AND guild_id = %s
            """, (new_coins, now, user_id, guild_id))
            conn.commit()

def add_microtransaction_coins(user_id, guild_id, coins_to_add):
    with get_safe_cursor() as cur:
        cur.execute("""
            UPDATE veil_users
            SET coins = coins + %s
            WHERE user_id = %s AND guild_id = %s
        """, (coins_to_add, user_id, guild_id))
        conn.commit()

def is_admin_or_owner(member: discord.Member) -> bool:
    return member.guild_permissions.administrator or member == member.guild.owner

def create_coin_checkout_session(user_id: int, guild_id: int, coins: int) -> stripe.checkout.Session | None:
    price_id = COIN_PRICE_IDS.get(coins)
    if not price_id:
        return None

    # warm the webhook dyno (optional)
    try:
        requests.get("https://veilstripewebhook-5062fc7c0b88.herokuapp.com/", timeout=3)
    except Exception:
        pass

    # build success/cancel redirect to Veil channel (same as your tier flow)
    with get_safe_cursor() as cur:
        cur.execute("SELECT channel_id FROM veil_channels WHERE guild_id = %s", (guild_id,))
        row = cur.fetchone()
    veil_channel_id = row[0] if row else None
    redirect_url = f"https://discord.com/channels/{guild_id}/{veil_channel_id}" if veil_channel_id else f"https://discord.com/channels/{guild_id}"

    # create session
    session = stripe.checkout.Session.create(
        mode="payment",
        payment_method_types=["card"],
        line_items=[{"price": price_id, "quantity": 1}],
        success_url=redirect_url,
        cancel_url=redirect_url,
        client_reference_id=str(user_id),
        metadata={
            "type": "coins",
            "price_id": price_id,
            "coins": str(coins),
            "guild_id": str(guild_id),
        },
    )
    return session

def save_coin_checkout_mapping(session_id: str, interaction: discord.Interaction, coins: int):
    # create table once if missing
    try:
        with get_safe_cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS coin_checkout_sessions (
                    stripe_session_id TEXT PRIMARY KEY,
                    interaction_token TEXT NOT NULL,
                    application_id   BIGINT NOT NULL,
                    user_id          BIGINT NOT NULL,
                    guild_id         BIGINT NOT NULL,
                    coins            INTEGER NOT NULL,
                    created_at       TIMESTAMPTZ DEFAULT NOW()
                )
            """)
    except Exception as e:
        print("⚠️ Could not ensure coin_checkout_sessions table:", e)

    # insert mapping
    try:
        with get_safe_cursor() as cur:
            cur.execute("""
                INSERT INTO coin_checkout_sessions
                    (stripe_session_id, interaction_token, application_id, user_id, guild_id, coins)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (stripe_session_id) DO NOTHING
            """, (
                session_id,
                interaction.token,                         # needed to edit the ephemeral later
                interaction.client.application_id,         # or client.application_id
                interaction.user.id,
                interaction.guild.id,
                coins
            ))
    except Exception as e:
        print("❌ Failed to save coin checkout mapping:", e)

def create_checkout_session(user_id, guild_id, tier):
    price_id = STRIPE_PRICE_IDS.get(tier)
    if not price_id:
        return None  # invalid tier

    try:
        res = requests.get("https://veilstripewebhook-5062fc7c0b88.herokuapp.com/")
        print(f"🟢 Webhook warmed: {res.status_code}")
    except Exception as e:
        print("⚠️ Failed to warm webhook:", e)

    try:
        # ✅ Fetch the veil channel from DB
        with get_safe_cursor() as cur:
            cur.execute("SELECT channel_id FROM veil_channels WHERE guild_id = %s", (guild_id,))
            row = cur.fetchone()

        veil_channel_id = row[0] if row else None

        # ✅ Build success and cancel URLs
        if veil_channel_id:
            redirect_url = f"https://discord.com/channels/{guild_id}/{veil_channel_id}"
        else:
            # Fallback to server root if channel not found
            redirect_url = f"https://discord.com/channels/{guild_id}"

        session = stripe.checkout.Session.create(
            success_url=redirect_url,
            cancel_url=redirect_url,
            payment_method_types=["card"],
            mode="subscription",
            line_items=[{
                "price": price_id,
                "quantity": 1
            }],
            client_reference_id=str(user_id),  # Send user ID
            metadata={
                "guild_id": str(guild_id),     # Send guild ID
                "price_id": price_id           # Optional but helpful
            }
        )
        return session.url
    except Exception as e:
        print(f"❌ Failed to create Stripe session: {e}")
        return None

async def notify_failed_payments():
    await client.wait_until_ready()
    while not client.is_closed():
        try:
            conn = psycopg2.connect(DATABASE_URL, sslmode='require')
            cur = conn.cursor()
            cur.execute("""
                SELECT s.guild_id, c.channel_id
                FROM veil_subscriptions s
                JOIN veil_channels c ON s.guild_id = c.guild_id
                WHERE s.payment_failed = TRUE
            """)
            rows = cur.fetchall()

            for guild_id, channel_id in rows:
                channel = client.get_channel(channel_id)
                if channel:
                    embed = discord.Embed(
                        title="❌ Payment Failed",
                        description="Your guild’s payment failed. You’ve been reverted to the **Free Tier**.",
                        color=0x992d22
                    )
                    await channel.send(embed=embed)

            # Reset the flags
            cur.execute("UPDATE veil_subscriptions SET payment_failed = FALSE WHERE payment_failed = TRUE")
            conn.commit()
            cur.close()
            conn.close()

        except Exception as e:
            print("❌ Error notifying failed payments:", e)

        await asyncio.sleep(900)  # every 15 minutes

def count_incorrect_guesses_for_guild(guesser_id: int, guild: discord.Guild) -> int:
    """# of incorrect guesses this user made in THIS guild."""
    channel_ids = tuple(c.id for c in guild.text_channels)
    if not channel_ids:
        return 0

    with get_safe_cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)
            FROM veil_guesses g
            JOIN veil_messages m ON m.message_id = g.message_id
            WHERE g.guesser_id = %s
              AND g.is_correct = FALSE
              AND m.channel_id IN %s
        """, (guesser_id, channel_ids))
        row = cur.fetchone()
    return int(row[0]) if row else 0

async def build_bot_info_embed(guild: discord.Guild, tier: str = "free") -> tuple[discord.Embed, Optional[View]]:   
    bot_user = guild.me
    joined_at = bot_user.joined_at.strftime("%B %d, %Y") if bot_user.joined_at else "Unknown"

    with get_safe_cursor() as cur:
        # Subscription info
        cur.execute("SELECT tier, renews_at FROM veil_subscriptions WHERE guild_id = %s", (guild.id,))
        sub = cur.fetchone()
        tiername = sub[0] if sub else tier
        renew_date = sub[1].strftime("%B %d, %Y") if sub and sub[1] else "N/A"

        # ✅ Get all text channel IDs in the guild
        channel_ids = tuple(c.id for c in guild.text_channels)
        if not channel_ids:
            veils_sent = 0
            veils_unveiled = 0
        else:
            # Veils sent
            cur.execute(
                f"SELECT COUNT(*) FROM veil_messages WHERE channel_id IN %s",
                (channel_ids,)
            )
            veils_sent = cur.fetchone()[0]

            # Veils unveiled
            cur.execute(f"""
                SELECT COUNT(*) 
                FROM veil_guesses 
                WHERE is_correct = TRUE 
                AND message_id IN (
                    SELECT message_id 
                    FROM veil_messages 
                    WHERE channel_id IN %s
                )
            """, (channel_ids,))
            veils_unveiled = cur.fetchone()[0]

        # Bot channel
        cur.execute("SELECT channel_id FROM veil_channels WHERE guild_id = %s", (guild.id,))
        bot_channel_id = cur.fetchone()
        bot_channel = guild.get_channel(bot_channel_id[0]) if bot_channel_id else None

    # ✅ Build embed
    embed = discord.Embed(
        title=f"VeilBot Info for {guild.name}",
        color=0xeeac00
    )
    embed.set_thumbnail(url=guild.icon.url if guild.icon else bot_user.display_avatar.url)

    embed.add_field(name="Guild", value=f"`{guild.name}`", inline=True)
    embed.add_field(name="Members", value=f"`{guild.member_count}`", inline=True)
    embed.add_field(name="Bot Joined", value=f"`{joined_at}`", inline=True)
    embed.add_field(name="Bot Channel", value=f"{bot_channel.mention if bot_channel else '`Not Set`'}", inline=True)

    embed.add_field(name="Current Tier", value=f"`{tiername.title()}`", inline=True)
    embed.add_field(name="Renewal Date", value=f"`{renew_date}`", inline=True)
    embed.add_field(name="Veils Sent", value=f"`{veils_sent}`", inline=True)
    embed.add_field(name="Veils Unveiled", value=f"`{veils_unveiled}`", inline=True)

    embed.set_footer(text="VeilBot • Every message wears a mask")

    view = InfoView(tiername)
    return embed, view

def build_store_embed() -> discord.Embed:
    embed = discord.Embed(
        title="Veil Store",
        description="Pick a Veil Coin pack below.\n*Purchases apply instantly after checkout.*",
        color=0xeeac00
    )
    embed.set_thumbnail(url="attachment://veilstore.png")
    return embed

def make_accuracy_bar_image(
    correct: int,
    incorrect: int,
    *,
    width: int = 180,
    height: int = 16,
    pad: int = 0,
    bar_h: int = 16,
    top: int = 0,
    fill: str = "#e5a41a",
    # semi-neutral outline that works on light & dark; RGBA allowed
    border=(168, 168, 168, 200),
    border_w: int = 2,
):
    total = max(correct + incorrect, 1)
    pct = correct / total

    img = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)

    x0, y0 = pad, top
    x1, y1 = width - pad, y0 + bar_h
    radius = max(4, bar_h // 2)

    # Transparent track: outline only (no fill)
    d.rounded_rectangle(
        (x0, y0, x1, y1),
        radius=radius,
        fill=None,
        outline=border,
        width=border_w
    )

    # Filled portion
    fill_w = int((x1 - x0) * pct)
    if fill_w > 0:
        # When very short, shrink radius to avoid weird corners
        fill_radius = min(radius, max(1, min(fill_w // 2, bar_h // 2)))
        d.rounded_rectangle(
            (x0, y0, x0 + fill_w, y1),
            radius=fill_radius,
            fill=fill
        )

    bio = BytesIO()
    img.save(bio, "PNG")
    bio.seek(0)
    return discord.File(bio, filename="accuracy.png")

def build_user_stats_embed_and_file(guild: discord.Guild, user: discord.Member) -> tuple[discord.Embed, discord.File | None]:
    user_id = user.id
    guild_id = guild.id

    # Ensure row exists + perform monthly refill if due
    ensure_user_entry(user_id, guild_id)
    refill_user_coins(user_id, guild_id)

    tier = (get_subscription_tier(guild_id) or "free").lower()
    coins = get_user_coins(user_id, guild_id) or 0

    # Pull cached unveil count + last_refill (FAST)
    with get_safe_cursor() as cur:
        cur.execute("""
            SELECT veils_unveiled, last_refill
            FROM veil_users
            WHERE user_id=%s AND guild_id=%s
        """, (user_id, guild_id))
        row = cur.fetchone()

    unveiled_count = (row[0] if row else 0) or 0
    last_refill    = row[1] if row else None  # TIMESTAMPTZ or None

    # Incorrect guesses (scoped to this guild via channel_id)
    incorrect_count = count_incorrect_guesses_for_guild(user_id, guild)

    # Monthly refill amounts by tier
    REFILL_BY_TIER = {
        "free":    100,
        "basic":   250,
        "premium": 1000,
        "elite":   None,  # unlimited
    }
    refill_amt = REFILL_BY_TIER.get(tier)

    # Compute next refill as +30d from last_refill (for tiers with refills)
    if refill_amt is not None and last_refill:
        if last_refill.tzinfo is None:
            last_refill = last_refill.replace(tzinfo=timezone.utc)
        next_refill_dt = last_refill + timedelta(days=30)
        ts = int(next_refill_dt.timestamp())
        next_refill_display = f"<t:{ts}:d>"
    else:
        next_refill_display = "—" if tier != "elite" else "N/A"

    # Pretty bits
    veilcoin  = str(getattr(client, "app_emojis", {}).get("veilcoin", "🪙"))
    maskemoji = str(getattr(client, "app_emojis", {}).get("veilemoji", "🎭"))
    incorrectmoji = str(client.app_emojis["veilincorrect"])
    pfp = user.display_avatar.url
    username = get_display_name_safe(user).capitalize()

    coins_display            = "♾️" if tier == "elite" else f"{int(coins):,}"
    unveiled_display         = f"{int(unveiled_count):,}"
    incorrect_display        = f"{int(incorrect_count):,}"
    monthly_refill_display   = "♾️ Unlimited" if tier == "elite" else f"{(refill_amt or 0):,} / month"

    # === embed layout (matches your demo) ===
    embed = discord.Embed(title=f"{username}'s Veil Stats", color=0xeeac00)
    embed.set_thumbnail(url=pfp)

    # Row 1: Veil Coins + spacers
    embed.add_field(name="Veil Coins", value=f"{veilcoin} `{coins_display}`", inline=True)
    embed.add_field(name=ZWS, value=ZWS, inline=True)
    embed.add_field(name=ZWS, value=ZWS, inline=True)

    # Row 2: Monthly Refill, Next Refill, spacer
    embed.add_field(name="Monthly Refill", value=f"`{monthly_refill_display}`", inline=True)
    if next_refill_display in ("—", "N/A"):
        embed.add_field(name="Next Refill", value=f"`{next_refill_display}`", inline=True)
    else:
        embed.add_field(name="Next Refill", value=next_refill_display, inline=True)
    embed.add_field(name=ZWS, value=ZWS, inline=True)

    # Row 3: Msgs Unveiled, Incorrect Guesses, spacer
    embed.add_field(name="Msgs Unveiled", value=f"{maskemoji} `{unveiled_display}`", inline=True)
    embed.add_field(name="Incorrect Guesses", value=f"{incorrectmoji} `{incorrect_display}`", inline=True)
    embed.add_field(name=ZWS, value=ZWS, inline=True)

    # Accuracy header + bar image
    embed.add_field(name="Accuracy", value="", inline=False)
    file = make_accuracy_bar_image(correct=unveiled_count, incorrect=incorrect_count)
    embed.set_image(url="attachment://accuracy.png")

    return embed, file

def build_help_embed(guild: discord.Guild):
    tier = get_subscription_tier(guild.id) or "free"
    maskemoji = str(client.app_emojis["veilemoji"])
    veilcoinemoji = str(client.app_emojis["veilcoin"])

    is_premium = tier in ("premium", "elite")
    is_elite = tier == "elite"

    # Header + basics
    desc = (
        f"{maskemoji} **Veil — Anonymous Messages, With Receipts**\n\n"
        "**How it works**\n"
        "• Use `/veil <message>` to post anonymously into your linked Veil channel\n"
        "• Others try to **Unveil** the author — correct guesses are tracked\n"
        "• Server subs unlock perks (coins, logs, leaderboards)\n\n"
        "**Message rules**\n"
        "• Up to **200 visual characters**\n"
        "• Keep it respectful; server rules still apply\n"
    )

    embed = discord.Embed(
        title="📖 Veil Help & Commands",
        description=desc,
        color=0xeeac00
    )

    # User Commands
    embed.add_field(
        name="🙋 User Commands",
        value=(
            "• `/veil <message>` — Send a message behind a veil\n"
            "• `/user [@user]` — Your Veil stats (coins, unveils)\n"
            "• `/store` — Open the Veil Coin store"
        ),
        inline=False
    )

    # Premium features
    embed.add_field(
        name=f"🏆 Premium Features{'' if is_premium else ' (locked)'}",
        value=("• `/leaderboard` — Top unveil users in this server"
               + ("" if is_premium else "\n   └ Upgrade with `/upgrade` to unlock")),
        inline=False
    )

    # Admin commands
    admin_lines = [
        "• `/setup` — Drop a setup message anywhere I can post",
        "• `/configure` — Link the Veil channel",
        "• `/upgrade` — View/upgrade your server tier",
        "• `/remove` — Remove a Veil for violating TOS",
        "• `/info` — Server plan & bot info",
    ]
    if is_elite:
        admin_lines.append("• Elite: configure **Admin Logs** via `/configure`")

    embed.add_field(
        name="🛠️ Admin Commands",
        value="\n".join(admin_lines),
        inline=False
    )

    # Current tier badge
    TIER_LABELS = {
        "free":    "Free — 100 coins/mo",
        "basic":   "Basic — 250 coins/mo",
        "premium": "Premium — 1,000 coins/mo + Leaderboard",
        "elite":   "Elite — Unlimited coins + Admin Logs",
    }
    embed.add_field(
        name="💼 Server Tier",
        value=f"{veilcoinemoji} **{TIER_LABELS.get(tier, tier.title())}**",
        inline=False
    )

    embed.set_footer(text="Pro tip: /configure must be set before /veil can post.")
    return embed

def build_upgrade_panel(guild_id: int, user_id: int):
    veilcoinemoji = str(client.app_emojis["veilcoin"])

    COLOR_BY_TIER = {
        "free":    0xA0A0A0,
        "basic":   0xF8AF3F,
        "premium": 0x69A7D9,
        "elite":   0xEC8195,
    }

    # current tier
    with get_safe_cursor() as cur:
        cur.execute("SELECT tier FROM veil_subscriptions WHERE guild_id = %s", (guild_id,))
        row = cur.fetchone()
        current_tier = (row[0] if row else "free").lower()

    # Elite “you’re already elite”
    if current_tier == "elite":
        embed = discord.Embed(
            title="You're on the Elite Tier 🧠",
            description=(
                f"{veilcoinemoji} • Unlimited coins for all users\n"
                "🗃️ • Admin-only logging access\n"
                "🥇 • Unveiling leaderboard\n"
                "💎 • Early access to new features"
            ),
            color=COLOR_BY_TIER["elite"]
        )
        return embed, None  # no view needed

    desc_map = {
        "free": (
            "You're currently on the **Free Tier** ✨\n\n"
            f"{veilcoinemoji} • **100** coins per month\n"
            "🔄 • Refills every month\n\n"
            "Upgrade for more coins, leaderboards, admin logging and early access to new features! 🚀",
            ["basic", "premium", "elite"]
        ),
        "basic": (
            "You're on the **Basic Tier** 🌟\n\n"
            f"{veilcoinemoji} • **250** coins per month\n"
            "🔄 • Refills every month\n"
            "🔍 • Earn coins by unveiling\n\n"
            "Upgrade for more coins, leaderboards, admin logging and early access to new features! 🚀",
            ["premium", "elite"]
        ),
        "premium": (
            "You're on the **Premium Tier** 💎\n\n"
            f"{veilcoinemoji} • **1,000** coins per month\n"
            "🔄 • Refills every month\n"
            "🔍 • Earn coins by unveiling\n"
            "🥇 • Unveiling leaderboard\n\n"
            "Upgrade for unlimited coins, admin logging, and early access to new features 🚀!",
            ["elite"]
        ),
    }

    embed = discord.Embed(
        title="Upgrade Your VeilBot Tier",
        description=desc_map[current_tier][0],
        color=COLOR_BY_TIER.get(current_tier, 0xA0A0A0)
    )

    # ⬇️ Your existing view that shows UpgradeTierButton(s)
    view = UpgradeMenuView(current_tier, user_id, guild_id)
    return embed, view

def ensure_free_subscription(guild_id):
    try:
        with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    INSERT INTO veil_subscriptions (guild_id, tier)
                    VALUES (%s, 'free')
                    ON CONFLICT (guild_id) DO NOTHING
                ''', (guild_id,))
                conn.commit()
                print(f"✅ Initialized free tier for guild {guild_id}")
    except Exception as e:
        print(f"❌ Error inserting free tier for guild {guild_id}:", e)

def edit_ephemeral_original(application_id: int, interaction_token: str, title: str, desc: str, color: int = 0xe7ad22):
    url = f"{DISCORD_API_BASE}/webhooks/{application_id}/{interaction_token}/messages/@original"
    payload = {"embeds": [{"title": title, "description": desc, "color": color}], "components": []}
    r = requests.patch(url, json=payload, timeout=6)
    return r.status_code, r.text

def claim_next_veil_number(channel_id: int) -> int:
    with get_safe_cursor() as cur:
        cur.execute("""
            INSERT INTO veil_channel_counters (channel_id, current_number)
            VALUES (%s, 1)
            ON CONFLICT (channel_id)
            DO UPDATE SET current_number = veil_channel_counters.current_number + 1
            RETURNING current_number
        """, (channel_id,))
        row = cur.fetchone()
    return int(row[0]) if row else 1

async def hydrate_latest_views():
    if not conn:
        return
    with get_safe_cursor() as cur:
        cur.execute("SELECT DISTINCT channel_id FROM latest_veil_messages")
        channels = [row[0] for row in cur.fetchall()]

    for channel_id in channels:
        latest_id = get_latest_message_id(channel_id)
        if not latest_id:
            continue

        channel = client.get_channel(channel_id)
        if not channel:
            continue

        try:
            msg = await channel.fetch_message(latest_id)
            # Use the DB-aware view so guesses/submitted-by/veil # are correct
            v = build_frozen_view(latest_id, channel.guild)
            await msg.edit(view=(v or VeilView()))
        except discord.NotFound:
            print(f"⚠️ Latest veil {latest_id} in channel {channel_id} not found.")
        except discord.HTTPException as e:
            print(f"⚠️ Failed to restore latest veil {latest_id}: {e}")

async def send_veil_message(interaction, text, channel, unveiled=False, return_file=False, veil_msg_id=None):
    channel_id = get_veil_channel(interaction.guild.id)
    channel = interaction.guild.get_channel(channel_id)

    base_img = "unveilfinal_black2.png" if unveiled else "veilfinal_gold2.png"
    color = "#e5a41a" if unveiled else "#a65e00"

    # Load base image
    image = Image.open(base_img).convert("RGBA")
    draw = ImageDraw.Draw(image)

    # Author fallback
    author_user = interaction.user

    # 🔹 Handle unveiled overlay with avatar fade
    if unveiled:
        target_msg_id = veil_msg_id or (interaction.message.id if interaction.message else None)
        if target_msg_id:
            with get_safe_cursor() as cur:
                cur.execute("SELECT author_id FROM veil_messages WHERE message_id=%s", (target_msg_id,))
                row = cur.fetchone()
            if row:
                member = interaction.guild.get_member(row[0])
                if member:
                    author_user = member

        try:
            avatar_url = str(author_user.display_avatar.with_size(512))
            resp = requests.get(avatar_url, timeout=8)
            resp.raise_for_status()
            pfp = Image.open(io.BytesIO(resp.content)).convert("RGBA")
        except Exception as e:
            print(f"⚠️ Avatar fetch failed, using transparent fill: {e}")
            pfp = Image.new("RGBA", (492, 482), (0, 0, 0, 0))
        # Resize & grayscale
        pfp = pfp.resize((492, 482), Image.LANCZOS)
        pfp = ImageOps.grayscale(pfp).convert("RGBA")

        # Round bottom-left corner
        corner_radius = 40
        mask = Image.new("L", pfp.size, 255)
        corner = Image.new("L", (corner_radius*2, corner_radius*2), 0)
        draw_corner = ImageDraw.Draw(corner)
        draw_corner.ellipse((0, 0, corner_radius*2, corner_radius*2), fill=255)
        mask.paste(
            corner.crop((0, corner_radius, corner_radius, corner_radius*2)),
            (0, pfp.height - corner_radius)
        )
        pfp.putalpha(mask)

        # Horizontal gradient fade
        grad_width, grad_height = pfp.size
        gradient = Image.new("L", (grad_width, grad_height))
        for x in range(grad_width):
            opacity = int(255 * (1 - x / grad_width))
            gradient.paste(opacity, (x, 0, x+1, grad_height))

        alpha = pfp.getchannel("A")
        alpha = ImageChops.multiply(alpha, gradient)
        alpha = alpha.point(lambda x: int(x * 0.2))  # 20% opacity
        pfp.putalpha(alpha)

        # Paste to box area
        inner_x, inner_y = 30, 156
        image.paste(pfp, (inner_x, inner_y), pfp)

    # Text box settings
    box_x, box_y = 45, 145
    box_width, box_height = 1200, 440
    line_spacing = 10
    emoji_size = 48
    emoji_padding = 4

    # 🔹 normalize mentions
    text = await normalize_mentions(text, interaction.guild, interaction.client)
    
    render_text, font_file = get_render_text_and_font(text)
    tokens = tokenize_message_for_wrap(render_text)
        
    for font_size in range(56, 24, -2):
        font = ImageFont.truetype(font_file, font_size)
        ascent, descent = font.getmetrics()
        line_height = max(emoji_size, ascent + descent)
        lines = build_wrapped_lines(tokens, font, box_width, draw, emoji_size, emoji_padding)
        total_height = len(lines) * line_height + line_spacing * (len(lines) - 1)
        if total_height <= box_height:
            break

    # Vertical centering
    y = box_y + (box_height - total_height) // 2 + 50

    # Center single-line emoji messages perfectly
    total_emoji_count = sum(
        1 for t in tokens
        if discord_emoji_pattern.fullmatch(t) or emoji.is_emoji(t.strip())
    )
    if len(lines) == 1 and total_emoji_count == len(tokens):
        y = box_y + (box_height - emoji_size) // 2

    # Render each line centered
    for line_tokens in lines:
        line_width = sum(
            emoji_size + emoji_padding if (
                discord_emoji_pattern.fullmatch(t) or emoji.is_emoji(t.strip())
            ) else draw.textlength(t, font=font)
            for t in line_tokens
        )
        x_start = box_x + (box_width - line_width) // 2
        line_y = calculate_line_y(line_tokens, font, y)
        await render_emojis(draw, image, line_tokens, x_start, line_y, font, emoji_size, emoji_padding, color)
        y += line_height + line_spacing

    # Save buffer
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    img_bytes = buffer.getvalue()

    if return_file:
        # return a Discord File so the callback can re-send it correctly
        return discord.File(io.BytesIO(img_bytes), filename="veil.png")

    # 1️⃣ Remove "New Veil" from previous latest message
    prev_msg_id = get_latest_message_id(channel.id)
    if prev_msg_id:
        try:
            old_msg = await channel.fetch_message(prev_msg_id)
            old_view = build_frozen_view(prev_msg_id, interaction.guild)  # ✅ keeps correct state
            if old_view:
                for child in list(old_view.children):
                    if isinstance(child, discord.ui.Button) and child.custom_id == "new_btn":
                        old_view.remove_item(child)
                await old_msg.edit(view=old_view)
        except discord.NotFound:
            print("⚠️ Old veil message not found, maybe deleted.")
        except discord.HTTPException as e:
            print(f"⚠️ Failed to edit old veil: {e}")

    # 2️⃣ Send the new Veil message
    veil_no = claim_next_veil_number(channel.id)
    view = VeilView(veil_number=veil_no)
    file_main = discord.File(io.BytesIO(img_bytes), filename="veil.png")
    msg = await channel.send(file=file_main, view=view)

    # 3️⃣ Insert into DB & update latest veil
    try:
        if conn:
            with get_safe_cursor() as cur:
                # Insert veil message
                cur.execute(
                    """
                    INSERT INTO veil_messages (message_id, channel_id, author_id, content, veil_number, guess_count, is_unveiled)
                    VALUES (%s, %s, %s, %s, %s, 0, FALSE)
                    ON CONFLICT (message_id) DO NOTHING
                    """,
                    (msg.id, msg.channel.id, interaction.user.id, text, veil_no)
                )
                cur.execute("""
                    INSERT INTO latest_veil_messages (channel_id, message_id)
                    VALUES (%s, %s)
                    ON CONFLICT (channel_id) DO UPDATE SET message_id = EXCLUDED.message_id
                """, (channel.id, msg.id))
                conn.commit()
                
    except Exception as e:
        print(f"❌ DB insert failed: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # ADMIN LOGS: if this guild is elite, send a copy of the veil to their log channel
    with get_safe_cursor() as cur:
        cur.execute(
            "SELECT tier FROM veil_subscriptions WHERE guild_id = %s",
            (interaction.guild.id,)
        )
        tier_row = cur.fetchone()
    if tier_row and tier_row[0] == "elite":
        with get_safe_cursor() as cur:
            cur.execute(
                "SELECT channel_id FROM veil_admin_channels WHERE guild_id = %s",
                (interaction.guild.id,)
            )
            log_row = cur.fetchone()

        if log_row:
            log_chan = interaction.guild.get_channel(log_row[0])
            if log_chan:
                author_member = interaction.guild.get_member(interaction.user.id)
                display_name = get_display_name_safe(author_member).capitalize()

                embed = discord.Embed(title="🗃️ New Veil Submitted")
                embed.set_image(url="attachment://veil.png")

                admin_view = discord.ui.View(timeout=None)
                submitted_btn = discord.ui.Button(
                    label=f"Submitted by {display_name}",
                    style=discord.ButtonStyle.grey,
                    custom_id="submitted_by_admin",
                    disabled=True
                )
                admin_view.add_item(submitted_btn)

                # re-use img_bytes for the admin log
                file_log = discord.File(io.BytesIO(img_bytes), filename="veil.png")
                await log_chan.send(embed=embed, file=file_log, view=admin_view)
    # ─────────────────────────────────────────────────────────────────────────

    return msg
    

# 🔶 MODAL
class VeilModal(Modal, title="New Veil"):
    # allow longer raw input; we enforce MAX_VISUAL ourselves
    message = TextInput(
        label="Send your message behind a veil.",
        style=discord.TextStyle.paragraph,
        max_length=200,  # was 200
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        incorrectmoji = str(client.app_emojis["veilincorrect"])
        maskemoji = str(client.app_emojis["veilemoji"])

        # Defer to avoid modal timeout
        await interaction.response.defer(ephemeral=True, thinking=True)

        text = self.message.value

        # 🚫 Emoji limit (custom + unicode/Twemoji)
        total_emojis = count_emojis_all(text)
        if total_emojis > EMOJI_LIMIT:
            return await interaction.followup.send(
                embed=discord.Embed(
                    title=f"{incorrectmoji} Too Many Emojis",
                    description=(
                        f"You can use up to **{EMOJI_LIMIT} emojis** per veil "
                        f"(custom & animated included). You used **{total_emojis}**."
                    ),
                    color=0x992d22
                ),
                ephemeral=True
            )

        # 🚫 Visual-length limit (grapheme-aware, emojis count as 1)
        vlen = visual_length(text)
        if vlen > MAX_VISUAL:
            return await interaction.followup.send(
                embed=discord.Embed(
                    title=f"{incorrectmoji} Message Too Long",
                    description=(
                        f"Your veil exceeds **{MAX_VISUAL} visual characters** "
                        f"(emojis count as 1). Current: **{vlen}**."
                    ),
                    color=0x992d22
                ),
                ephemeral=True
            )

        # ✅ Check configured channel
        channel_id = get_veil_channel(interaction.guild.id)
        channel = interaction.guild.get_channel(channel_id) if channel_id else None
        if not channel:
            return await interaction.followup.send(
                embed=discord.Embed(
                    title=f"{incorrectmoji} Channel Not Linked",
                    description="This server hasn’t linked a Veil channel yet.\nUse **/configure** to set one.",
                    color=0x992d22
                ),
                ephemeral=True
            )

        # 🖼️ Send the actual veil
        await send_veil_message(interaction, text, channel)

        # ✅ Success confirmation
        await interaction.followup.send(
            embed=discord.Embed(
                title=f"{maskemoji} Veil Sent",
                description="Your message has been sent under a veil!",
                color=0x43b581
            ),
            ephemeral=True
        )

# 🔘 NEW VEIL BUTTON
class NewVeilButton(Button):
    def __init__(self):
        newveilemoji = client.app_emojis['veiladd']
        super().__init__(label="New Veil", style=discord.ButtonStyle.grey, custom_id="new_btn", emoji=newveilemoji)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(VeilModal())

# 🎨 COMBINED VIEW FOR VEIL MESSAGE
class VeilView(discord.ui.View):
    def __init__(self, veil_number: int | None = None):
        super().__init__(timeout=None)
        unveil = client.app_emojis['unveilemoji']

        self.add_item(discord.ui.Button(style=discord.ButtonStyle.gray, label="Unveil", custom_id="guess_btn", emoji=unveil))
        self.add_item(NewVeilButton())
        self.add_item(discord.ui.Button(label="Guesses 0/3", style=discord.ButtonStyle.gray, disabled=True, custom_id="guess_count"))
        self.add_item(discord.ui.Button(label="Submitted by █████", style=discord.ButtonStyle.gray, disabled=True, custom_id="submitted_by"))

        # New: “Veil #N” badge
        num_label = f"Veil #{veil_number}" if veil_number else "Veil #–"
        self.add_item(discord.ui.Button(label=num_label, style=discord.ButtonStyle.gray, disabled=True, custom_id="veil_number"))

# UNVEIL DROPDOWN WHEN BUTTON CLICKED
class UnveilDropdown(discord.ui.Select):
    def __init__(self, message_id, author_id, options):
        super().__init__(placeholder="Guess who wrote this...", min_values=1, max_values=1, options=options)
        self.message_id = message_id
        self.author_id = author_id

    async def callback(self, interaction: discord.Interaction):
        incorrectmoji  = str(client.app_emojis["veilincorrect"])
        veilcoinemoji  = str(client.app_emojis["veilcoin"])
        maskemoji      = str(client.app_emojis["veilemoji"])

        guesser_id = interaction.user.id
        guessed_user_id = int(self.values[0])
        guild_id = interaction.guild.id
        tier = get_subscription_tier(guild_id)
        is_elite = (tier == "elite")

        # 1) ACK fast: swap the ephemeral into a "processing" state (no view)
        try:
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title="Checking Guess",
                    description="Checking your guess and updating the veil.",
                    color=0xeeac00
                ),
                view=None
            )
        except Exception:
            # If the ephemeral was already edited somehow, we’ll still proceed
            pass

        # 2) Coins (skip for Elite)
        if not is_elite:
            ensure_user_entry(guesser_id, guild_id)
            refill_user_coins(guesser_id, guild_id)
            if get_user_coins(guesser_id, guild_id) < 5:
                veilcoinemoji = str(client.app_emojis["veilcoin"])

                # show a one-button view to pop the store
                view = discord.ui.View()
                view.add_item(StoreButton())  # re-use your existing button class

                return await interaction.edit_original_response(
                    embed=discord.Embed(
                        title=f"{incorrectmoji} Not Enough Coins",
                        description=(
                            "You don't have enough **Veil Coins** to make a guess.\n"
                            f"Each guess costs **5** {veilcoinemoji}.\n\n"
                            "Need more coins? **Open the Store** below."),
                        color=0x992d22
                    ),
                    view=view
                )
            if not deduct_user_coins(guesser_id, guild_id, 5):
                return await interaction.edit_original_response(
                    embed=discord.Embed(
                        title=f"{incorrectmoji} Transaction Failed",
                        description="Failed to deduct **Veil Coins**. Please try again later.",
                        color=0x992d22
                    ),
                    view=None
                )


        # 3) DB work (race-safe)
        with get_safe_cursor() as cur:
            cur.execute(
                "SELECT guess_count, author_id, is_unveiled FROM veil_messages WHERE message_id = %s",
                (self.message_id,)
            )
            row = cur.fetchone()
            if not row:
                return await interaction.edit_original_response(
                    embed=discord.Embed(
                        title=f"{incorrectmoji} Message Not Found",
                        description="The message you're trying to unveil doesn't exist or was deleted.",
                        color=0x992d22
                    ),
                    view=None
                )

            guess_count, real_author_id, is_unveiled = row

            if is_unveiled or guess_count >= 3:
                return await interaction.edit_original_response(
                    embed=discord.Embed(
                        title=f"{incorrectmoji} No More Guesses",
                        description="This Veil has already been unveiled or reached the maximum of 3 guesses.",
                        color=0x992d22
                    ),
                    view=None
                )

            is_correct = (guessed_user_id == real_author_id)

            # ✅ Prevent duplicate (message_id, guesser_id) with ON CONFLICT
            cur.execute("""
                INSERT INTO veil_guesses (message_id, guesser_id, guessed_user_id, is_correct)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (message_id, guesser_id) DO NOTHING
                RETURNING 1
            """, (self.message_id, guesser_id, guessed_user_id, is_correct))
        
            inserted = (cur.fetchone() is not None)
        
            if not inserted:
                # User already guessed this veil — don't charge another 5 coins, don't bump guess_count
                # If you already deducted coins above, optionally refund here:
                if not is_elite:
                    add_user_coins(guesser_id, guild_id, 5)
                conn.commit()
                return await interaction.edit_original_response(
                    embed=discord.Embed(
                        title=f"{incorrectmoji} You Already Guessed",
                        description="You’ve already made a guess on this veil.",
                        color=0x992d22
                    ),
                    view=None
                )

            guess_count += 1

            won = False
            if is_correct:
                cur.execute("""
                    UPDATE veil_messages
                    SET guess_count = %s, is_unveiled = TRUE
                    WHERE message_id = %s AND is_unveiled = FALSE
                    RETURNING 1
                """, (guess_count, self.message_id))
                won = (cur.fetchone() is not None)

                if won:
                    increment_unveiled_count(guesser_id, guild_id)
                else:
                    cur.execute("""
                        UPDATE veil_guesses
                        SET is_correct = FALSE
                        WHERE message_id = %s AND guesser_id = %s
                    """, (self.message_id, guesser_id))
                    cur.execute("""
                        UPDATE veil_messages
                        SET guess_count = %s
                        WHERE message_id = %s
                    """, (guess_count, self.message_id))
            else:
                cur.execute("""
                    UPDATE veil_messages
                    SET guess_count = %s
                    WHERE message_id = %s
                """, (guess_count, self.message_id))

            conn.commit()

        # 4) Update the public veil message UI
        msg = await interaction.channel.fetch_message(self.message_id)

        with get_safe_cursor() as cur:
            cur.execute("SELECT veil_number FROM veil_messages WHERE message_id=%s", (self.message_id,))
            row = cur.fetchone()
        veil_no = row[0] if row else None

        view = VeilView(veil_number=veil_no)

        if not is_latest_veil(interaction.channel.id, self.message_id):
            for child in list(view.children):
                if isinstance(child, discord.ui.Button) and child.custom_id == "new_btn":
                    view.remove_item(child)

        for child in view.children:
            if isinstance(child, discord.ui.Button):
                if child.custom_id == "guess_count":
                    child.label = f"Guesses {guess_count}/3"
                    child.disabled = True
                elif child.custom_id == "guess_btn" and ((is_correct and won) or guess_count >= 3):
                    child.disabled = True

        # 5) Outcome flows
        if is_correct and won:
            # fetch original text for unveiled image
            with get_safe_cursor() as cur:
                cur.execute("SELECT content FROM veil_messages WHERE message_id=%s", (self.message_id,))
                r = cur.fetchone()
            original_text = r[0] if r else "(missing)"

            file = await send_veil_message(
                interaction,
                original_text,
                interaction.channel,
                unveiled=True,
                return_file=True,
                veil_msg_id=self.message_id
            )

            reward_line = ""
            if not is_elite:
                reward_map = {"basic": 10, "premium": 15}
                reward = reward_map.get(tier, 0)
                if reward > 0:
                    add_user_coins(guesser_id, guild_id, reward)
                    reward_line = f"\n\n**{reward} Veil Coins** have been added to your total. {veilcoinemoji}"

            author_member = interaction.guild.get_member(real_author_id)
            for child in view.children:
                if isinstance(child, discord.ui.Button) and child.custom_id == "submitted_by" and author_member:
                    display_name = get_display_name_safe(author_member)
                    child.label = f"Submitted by {display_name.capitalize()}"

            await msg.edit(attachments=[file], view=view, embed=None)

            return await interaction.edit_original_response(
                embed=discord.Embed(
                    title=f"{maskemoji} Veil Removed",
                    description=f"The veil has been removed from this message!{reward_line}",
                    color=0xeeac00
                ),
                view=None
            )

        if is_correct and not won:
            try:
                await msg.edit(view=view)
                return await interaction.edit_original_response(
                    embed=discord.Embed(
                        title=f"{incorrectmoji} Too Late",
                        description="Someone else unveiled this message just before you.",
                        color=0x992d22
                    ),
                    view=None
                )
            except Exception as e:
                print(f"❌ Failed to update 'too late' state: {e}")
                return

        if guess_count >= 3:
            try:
                await msg.edit(view=view)
                return await interaction.edit_original_response(
                    embed=discord.Embed(
                        title=f"{incorrectmoji} 3 Guesses Used",
                        description="The veil on this message remains.",
                        color=0x992d22
                    ),
                    view=None
                )
            except Exception as e:
                print(f"❌ Failed to update after 3 guesses: {e}")
                return

        # incorrect but still attempts left
        try:
            await msg.edit(view=view)
            return await interaction.edit_original_response(
                embed=discord.Embed(
                    title=f"{incorrectmoji} Incorrect Guess",
                    description="That guess is not correct!",
                    color=0x992d22
                ),
                view=None
            )
        except Exception as e:
            print(f"❌ Failed to update guess count: {e}")
            return

class UnveilView(discord.ui.View):
    def __init__(self, message_id: int, author_id: int, interaction: discord.Interaction):
        super().__init__(timeout=None)

        guild = interaction.guild
        channel_id = interaction.channel.id

        # --- Build candidate pools ---
        # Everyone except bots & the real author
        all_members = [m for m in guild.members if not m.bot and m.id != author_id]

        # Recent posters in THIS channel (ids only -> members)
        with get_safe_cursor() as cur:
            cur.execute("""
                SELECT author_id
                FROM veil_messages
                WHERE channel_id = %s
                GROUP BY author_id
                ORDER BY MAX(timestamp) DESC
                LIMIT 50
            """, (channel_id,))
            recent_ids = [row[0] for row in cur.fetchall() if row[0] != author_id]

        recent_members = [guild.get_member(uid) for uid in recent_ids]
        recent_members = [m for m in recent_members if m and not m.bot and m.id != author_id]

        # --- Random selection logic ---
        import random

        # Cap how many we *try* to take from "recent" to keep variety
        RECENT_CAP = 12
        take_recent = min(RECENT_CAP, len(recent_members))
        recent_pick = random.sample(recent_members, k=take_recent) if take_recent else []

        # Fill the rest from everyone else (excluding the ones we already took)
        remaining_slots = max(0, 24 - len(recent_pick))  # 24 + author = 25 max
        excluded_ids = {m.id for m in recent_pick}
        others_pool = [m for m in all_members if m.id not in excluded_ids]

        if remaining_slots and others_pool:
            others_pick = random.sample(others_pool, k=min(remaining_slots, len(others_pool)))
        else:
            others_pick = []

        # Combine + guarantee the real author is present
        final_members = recent_pick + others_pick
        author_member = guild.get_member(author_id)
        if author_member and not author_member.bot:
            # If we somehow ran out of space, bump one random entry to ensure author presence
            if len(final_members) >= 24:
                final_members.pop(random.randrange(len(final_members)))
            final_members.append(author_member)

        # De-dupe just in case, then hard-cap to 25
        seen = set()
        unique_members = []
        for m in final_members:
            if m.id in seen:
                continue
            seen.add(m.id)
            unique_members.append(m)
            if len(unique_members) >= 25:
                break

        # Shuffle so the order is NOT alphabetical and the author position is random too
        random.shuffle(unique_members)

        # Build menu options (use only the username)
        used_labels = set()
        options = []
        for member in unique_members:
            label = member.name  # actual Discord username, no discriminator
            if label in used_labels:
                # if dupes, make it unique by tacking on last 4 digits of ID
                label = f"{label} · {str(member.id)[-4:]}"
            used_labels.add(label)
        
            options.append(discord.SelectOption(label=label[:100], value=str(member.id)))
        
        # Attach the dropdown
        self.add_item(UnveilDropdown(message_id, author_id, options))

class CreateChannelButton(Button):
    def __init__(self):
        super().__init__(label="Create Channel", style=discord.ButtonStyle.success, custom_id="create_channel", emoji="📝")

    async def callback(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.administrator:
            incorrectmoji = str(client.app_emojis["veilincorrect"]) 
            return await interaction.response.send_message(
                embed=discord.Embed(
                    title=f"{incorrectmoji} Permission Denied",
                    description="Only admins can create the channel.",
                    color=0x992d22
                ),
                ephemeral=True
            )

        guild = interaction.guild

        # ✅ Check DB for existing veil channel
        with get_safe_cursor() as cur:
            cur.execute("SELECT channel_id FROM veil_channels WHERE guild_id = %s", (guild.id,))
            result = cur.fetchone()

        if result:
            existing_channel = guild.get_channel(result[0])
            if existing_channel:
                cautionemoji = str(client.app_emojis["veilcaution"]) 
                await interaction.response.send_message(
                    embed=discord.Embed(
                        title=f"{cautionemoji} Channel Already Exists",
                        description=f"A veil channel already exists: {existing_channel.mention}",
                        color=0xeeac00
                    ),
                    ephemeral=True
                )
                try:
                    await interaction.message.delete()
                except Exception as e:
                    print(f"❌ Failed to delete setup message: {e}")
                return
            else:
                # Channel was deleted, remove from DB
                with get_safe_cursor() as cur:
                    cur.execute("DELETE FROM veil_channels WHERE guild_id = %s", (guild.id,))
                    conn.commit()

        # ✅ Create the new channel
        channel = await guild.create_text_channel(name="🎭・veil")
        maskemoji = str(client.app_emojis["veilemoji"]) 
        veilcoinemoji = str(client.app_emojis["veilcoin"])

        # 🔒 Save new channel to DB
        set_veil_channel(guild.id, channel.id)

        # ✅ Fetch current tier (default to free)
        with get_safe_cursor() as cur:
            cur.execute("SELECT tier FROM veil_subscriptions WHERE guild_id = %s", (guild.id,))
            row = cur.fetchone()
            tier = row[0] if row else "free"

        # 🎨 Dynamic welcome description
        desc_map = {
            "free": (
                "You're currently on the **Free Tier** ✨\n\n"
                f"{veilcoinemoji} • **100** coins per month\n"
                "🔄 • Refills every month\n\n"
                "Upgrade for more coins, leaderboards, admin logging and early access to new features! 🚀"
            ),
            "basic": (
                "You're on **Basic Tier** 🌟\n\n"
                f"{veilcoinemoji} • **250** coins per month\n"
                "🔄 • Refills every month\n"
                "🔍 • Earning coins by unveiling\n\n"
                "Upgrade for more coins, leaderboards, admin logging and early access to new features! 🚀"
            ),
            "premium": (
                "You're on **Premium Tier** 💎\n\n"
                f"{veilcoinemoji} • **1,000** coins per month\n"
                "🔄 • Refills every month\n"
                "🔍 • Earning coins by unveiling\n"
                "🥇 • Unveiling leaderboard\n\n"
                "Upgrade for unlimited coins, admin logging, and early access to new features 🚀!"
            ),
            "elite": (
                "You're on **Elite Tier** 🧠\n\n"
                f"{veilcoinemoji} • **Unlimited** coins per user\n"
                "🔄 • Refills every month\n"
                "🔍 • Earning coins by unveiling\n"
                "🥇 • Unveiling leaderboard\n"
                "🗃️ • Admin-only logging\n"
                "💎 • Early access to new features\n"
            ),
        }

        # ✅ Build welcome embed based on tier
        welcome_embed = discord.Embed(
            title="Welcome to Veil",
            description=f"**Veil** {maskemoji} — Where every message wears a mask.\n\n"
                        "Use the slash command `/veil` to send your first message or click the **New Veil** button below.\n\n"
                        + desc_map.get(tier, desc_map["free"]),
            color=0xeeac00
        )

        await interaction.response.send_message(
            embed=discord.Embed(
                title=f"{maskemoji} Channel Created",
                description=f"New channel {channel.mention} was created and linked to the bot.",
                color=0x43b581
            ),
            ephemeral=True
        )

        try:
            await interaction.message.delete()
        except Exception as e:
            print(f"❌ Failed to delete setup message: {e}")

        view = WelcomeView()
        try:
            msg = await channel.send(embed=welcome_embed, view=view)
            await msg.pin()
        except Exception as e:
            print(f"❌ Failed to send welcome message to {channel.name}: {e}")

class WelcomeView(View):
    def __init__(self):
        super().__init__(timeout=None)  # Persistent view

class AdminLog(View):
    def __init__(self):
        super().__init__(timeout=None)  # Persistent view
        self.add_item(ConfigureAdminLogsButton())

        # Add the persistent buttons
        self.add_item(NewVeilButton())  # Ensure this button has a custom_id in its class

class ConfigureButton(Button):
    def __init__(self):
        super().__init__(label="Configure", style=discord.ButtonStyle.grey, custom_id="configure", emoji="🛠️")

    async def callback(self, interaction: discord.Interaction):
        incorrectmoji = str(client.app_emojis["veilincorrect"]) 
        if not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message(
                embed=discord.Embed(
                    title=f"{incorrectmoji} Admin Only",
                    description="Only admins can configure the bot.",
                    color=0x992d22
                ),
                ephemeral=True
            )

        embed = discord.Embed(
            title="Bot Configuration 🛠️",
            description="A new channel will be created for the bot.\nClick below to continue.",
            color=0xeeac00
        )
        view = View()
        view.add_item(CreateChannelButton())
        await interaction.response.edit_message(embed=embed, view=view)

class ConfigureAdminLogsButton(Button):
    def __init__(self):
        super().__init__(
            label="Configure Admin Logs",
            style=discord.ButtonStyle.grey,
            custom_id="configureadminlogs",
            emoji="🧾"
        )

    async def callback(self, interaction: discord.Interaction):
        incorrectmoji = str(client.app_emojis["veilincorrect"]) 
        if not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message(
                embed=discord.Embed(
                    title=f"{incorrectmoji} Admin Only",
                    description="Only admins can configure the bot.",
                    color=0x992d22
                ),
                ephemeral=True
            )

        embed = discord.Embed(
            title="Admin Logs Configuration 🛠️",
            description="A new channel will be created for **admin logs**.\nClick below to continue.",
            color=0xeeac00
        )

        view = View()
        view.add_item(CreateAdminChannelButton())
        await interaction.response.edit_message(embed=embed, view=view)

class CreateAdminChannelButton(Button):
    def __init__(self):
        super().__init__(
            label="Create Admin Log Channel",
            style=discord.ButtonStyle.success,
            custom_id="create_admin_channel",
            emoji="🧾"
        )

    async def callback(self, interaction: discord.Interaction):
        incorrectmoji = str(client.app_emojis["veilincorrect"]) 
        if not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message(
                embed=discord.Embed(
                    title=f"{incorrectmoji} Permission Denied",
                    description="Only admins can create the channel.",
                    color=0x992d22
                ),
                ephemeral=True
            )

        guild = interaction.guild

        # ✅ Check DB for existing admin log channel
        with get_safe_cursor() as cur:
            cur.execute("SELECT channel_id FROM veil_admin_channels WHERE guild_id = %s", (guild.id,))
            result = cur.fetchone()

        if result:
            existing_channel = guild.get_channel(result[0])
            if existing_channel:
                cautionemoji = str(client.app_emojis["veilcaution"]) 
                await interaction.response.send_message(
                    embed=discord.Embed(
                        title=f"{cautionemoji} Admin Channel Exists",
                        description=f"Admin logs channel already exists: {existing_channel.mention}",
                        color=0xeeac00
                    ),
                    ephemeral=True
                )
                try:
                    await interaction.message.delete()
                except Exception as e:
                    print(f"❌ Failed to delete setup message: {e}")
                return
            else:
                # Channel was deleted, remove from DB
                with get_safe_cursor() as cur:
                    cur.execute("DELETE FROM veil_admin_channels WHERE guild_id = %s", (guild.id,))
                    conn.commit()

        # ✅ Create the new channel
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.me: discord.PermissionOverwrite(view_channel=True)
        }
        channel = await guild.create_text_channel(name="🗃️・veil-logs", overwrites=overwrites)

        # 🔒 Save new channel to DB
        set_veil_admin_channel(guild.id, channel.id)

        await interaction.response.send_message(
            embed=discord.Embed(
                title="🧾 Admin Logs Channel Created",
                description=f"Admin logs will be sent to {channel.mention}.",
                color=0x43b581
            ),
            ephemeral=True
        )

        try:
            await interaction.message.delete()
        except Exception as e:
            print(f"❌ Failed to delete setup message: {e}")

class SetupView(View):
    def __init__(self, user: discord.Member):
        super().__init__(timeout=None)
        if user and is_admin_or_owner(user):
            self.add_item(ConfigureButton())

class ConfigureDropdown(Select):
    def __init__(self, guild_channels):
        options = [
            discord.SelectOption(label=channel.name, value=str(channel.id))
            for channel in guild_channels if isinstance(channel, discord.TextChannel)
        ]
        super().__init__(placeholder="Select a channel to link...", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        selected_channel_id = int(self.values[0])
        set_veil_channel(interaction.guild.id, selected_channel_id)

        channel = interaction.guild.get_channel(selected_channel_id)
        maskemoji = str(client.app_emojis["veilemoji"]) 

        await interaction.response.edit_message(
            embed=discord.Embed(
                title=f"{maskemoji} Channel Linked",
                description=f"Veil is now linked to {channel.mention}.",
                color=0x43b581
            ),
            view=None
        )

class ConfigureView(View):
    def __init__(self, channels):
        super().__init__(timeout=None)
        self.add_item(ConfigureDropdown(channels))

class ConfigureVeilButton(Button):
    def __init__(self, guild_channels: list[discord.TextChannel]):
        super().__init__(
            label="Configure Veil",
            style=discord.ButtonStyle.grey,
            custom_id="configure_veil",
            emoji="🛠️"
        )
        self.guild_channels = guild_channels

    async def callback(self, interaction: discord.Interaction):
        # only admins
        if not interaction.user.guild_permissions.administrator:
            incorrectmoji = str(client.app_emojis["veilincorrect"]) 
            return await interaction.response.send_message(
                embed=discord.Embed(
                    title=f"{incorrectmoji} Permission Denied",
                    description="Only admins can configure the bot.",
                    color=0x992d22
                ),
                ephemeral=True
            )

        embed = discord.Embed(
            title="🛠️ Configure Veil",
            description="Select a channel from the dropdown below to link it to Veil.",
            color=0xeeac00
        )
        # reuse your existing ConfigureView
        view = ConfigureView(self.guild_channels)
        await interaction.response.edit_message(embed=embed, view=view)

class EliteConfigureView(View):
    def __init__(self, guild_channels: list[discord.TextChannel]):
        super().__init__(timeout=None)
        # Veil linkage
        self.add_item(ConfigureVeilButton(guild_channels))
        # Admin‐logs linkage
        self.add_item(ConfigureAdminLogsButton())

class UpgradeMenuView(View):
    def __init__(self, current_tier, user_id, guild_id):
        super().__init__()
        tiers = {"basic": "🌟 Basic", "premium": "💎 Premium", "elite": "🧠 Elite"}
        available = {
            "free": ["basic", "premium", "elite"],
            "basic": ["premium", "elite"],
            "premium": ["elite"]
        }[current_tier]
        for tier in available:
            self.add_item(UpgradeTierButton(tier, user_id, guild_id))

class UpgradeTierButton(Button):
    COLOR_BY_TIER = {
        "basic":   0xF8AF3F,  # f7ef8b
        "premium": 0x69A7D9,  # 69a7d9
        "elite":   0xEC8195,  # ec8195
    }

    def __init__(self, tier, user_id, guild_id):
        labels = {"basic": "🌟 Basic", "premium": "💎 Premium", "elite": "🧠 Elite"}
        super().__init__(label=labels[tier], style=discord.ButtonStyle.primary, custom_id=f"select_{tier}")
        self.tier = tier
        self.user_id = user_id
        self.guild_id = guild_id

    async def callback(self, interaction: discord.Interaction):
        veilcoinemoji = str(client.app_emojis["veilcoin"])

        def fmt(n):  # 1000 -> "1,000"
            return f"{n:,}"

        plans = {
            "basic": (
                "Basic Tier 🌟",
                f"💵 • **$2.50** per month\n"
                f"{veilcoinemoji} • **{fmt(250)}** coins per user\n"
                "🔄 • Refills every month"
            ),
            "premium": (
                "Premium Tier 💎",
                f"💵 • **$5** per month\n"
                f"{veilcoinemoji} • **{fmt(1000)}** coins per user\n"
                "🔄 • Refills every month\n"
                "🔍 • Earn coins by unveiling\n"
                "🥇 • Unveiling leaderboard"
            ),
            "elite": (
                "Elite Tier 🧠",
                "💵 • **$10** per month\n"
                f"{veilcoinemoji} • **Unlimited** coins\n"
                "🗃️ • Admin-only logging access\n"
                "🥇 • Unveiling leaderboard\n"
                "💎 • Early access to new features"
            ),
        }

        title, features = plans[self.tier]
        color = self.COLOR_BY_TIER[self.tier]

        embed = discord.Embed(title=title, description=features, color=color)
        await interaction.response.edit_message(
            embed=embed,
            view=ConfirmUpgradeView(self.user_id, self.guild_id, self.tier)
        )

class ConfirmUpgradeView(View):
    def __init__(self, user_id, guild_id, tier):
        super().__init__()
        self.add_item(ConfirmUpgradeButton(user_id, guild_id, tier))
        self.add_item(GoBackButton(user_id, guild_id))

class ConfirmUpgradeButton(Button):
    COLOR_BY_TIER = {
        "free":    0xA0A0A0,
        "basic":   0xF8AF3F,
        "premium": 0x69A7D9,
        "elite":   0xEC8195,
    }
    # nice display names with emojis (optional)
    DISPLAY_BY_TIER = {
        "basic":   "Basic 🌟",
        "premium": "Premium 💎",
        "elite":   "Elite 🧠",
    }

    def __init__(self, user_id, guild_id, tier):
        super().__init__(label="Upgrade Now", style=discord.ButtonStyle.success)
        self.user_id = user_id
        self.guild_id = guild_id
        self.tier = tier.lower()

    async def callback(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.administrator:
            incorrectmoji = str(client.app_emojis["veilincorrect"])
            return await interaction.response.send_message(
                embed=discord.Embed(
                    title=f"{incorrectmoji} Permission Denied",
                    description="Only admins can upgrade the server.",
                    color=0x992d22
                ),
                ephemeral=True
            )

        url = create_checkout_session(self.user_id, self.guild_id, self.tier)
        if not url:
            return await interaction.response.send_message(
                "❌ Failed to create checkout session.", ephemeral=True
            )

        color = self.COLOR_BY_TIER.get(self.tier, 0x00B0F4)
        tier_display = self.DISPLAY_BY_TIER.get(self.tier, self.tier.title())

        embed = discord.Embed(
            title=f"{tier_display} Selected",
            description=(
                f"You've selected the **{tier_display}** tier.\n"
                "Click below to complete your purchase."
            ),
            color=color
        )
        view = View()
        view.add_item(Button(label="Checkout", url=url, style=discord.ButtonStyle.link))
        await interaction.response.edit_message(embed=embed, view=view)

class GoBackButton(Button):
    def __init__(self, user_id, guild_id):
        super().__init__(label="← Go Back", style=discord.ButtonStyle.secondary)
        self.user_id = user_id
        self.guild_id = guild_id

    async def callback(self, interaction: discord.Interaction):
        # put these near the top of your file once, reuse everywhere
        COLOR_BY_TIER = {
            "free":    0xA0A0A0,
            "basic":   0xF8AF3F,  # ← your updated Basic color
            "premium": 0x69A7D9,
            "elite":   0xEC8195,
        }
        fmt = lambda n: f"{n:,}"
        veilcoinemoji = str(client.app_emojis["veilcoin"])

        # current tier
        with get_safe_cursor() as cur:
            cur.execute("SELECT tier FROM veil_subscriptions WHERE guild_id = %s", (self.guild_id,))
            row = cur.fetchone()
            current_tier = (row[0] if row else "free").lower()

        # Elite = info-only, no upgrade view
        if current_tier == "elite":
            embed = discord.Embed(
                title="You're on the Elite Tier 🧠",
                description=(
                    f"{veilcoinemoji} • **Unlimited** coins for all users\n"
                    "🗃️ • Admin-only logging access\n"
                    "🥇 • Unveiling leaderboard\n"
                    "💎 • Early access to new features"
                ),
                color=COLOR_BY_TIER["elite"]
            )
            await interaction.response.edit_message(embed=embed, view=None)
            return

        # Everyone else: show the main upgrade menu with tier-colored embed
        desc_map = {
            "free": (
                "You're currently on the **Free Tier** ✨\n\n"
                f"{veilcoinemoji} • **{fmt(100)}** coins per month\n"
                "🔄 • Refills every month\n\n"
                "Upgrade for more coins, leaderboards, admin logging and early access to new features! 🚀"
            ),
            "basic": (
                "You're on **Basic Tier** 🌟\n\n"
                f"{veilcoinemoji} • **{fmt(250)}** coins per month\n"
                "🔄 • Refills every month\n"
                "🔍 • Earning coins by unveiling\n\n"
                "Upgrade for more coins, leaderboards, admin logging and early access to new features! 🚀"
            ),
            "premium": (
                "You're on **Premium Tier** 💎\n\n"
                f"{veilcoinemoji} • **{fmt(1000)}** coins per month\n"
                "🔄 • Refills every month\n"
                "🔍 • Earning coins by unveiling\n"
                "🥇 • Unveiling leaderboard\n\n"
                "Upgrade for unlimited coins, admin logging, and early access to new features 🚀!"
            ),
        }

        embed = discord.Embed(
            title="Upgrade Your VeilBot Tier",
            description=desc_map[current_tier],
            color=COLOR_BY_TIER.get(current_tier, COLOR_BY_TIER["free"])
        )
        await interaction.response.edit_message(
            embed=embed,
            view=UpgradeMenuView(current_tier, self.user_id, self.guild_id)
        )

class UpgradeButton(View):
    def __init__(self, url):
        super().__init__()
        self.add_item(Button(label="Upgrade Now", url=url, style=discord.ButtonStyle.link))

class InfoView(View):
    def __init__(self, current_tier: str):
        super().__init__(timeout=None)

        # Add Upgrade if not elite
        if current_tier != "elite":
            self.add_item(Button(
                label="Upgrade Tier",
                style=discord.ButtonStyle.primary,
                custom_id="upgrade_menu"  # ✅ uses existing on_interaction logic
            ))

        # Add Cancel if paid Tier
        if current_tier in ["basic", "premium", "elite"]:
            self.add_item(Button(
                label="Cancel Subscription",
                style=discord.ButtonStyle.danger,
                custom_id="cancel_subscription"
            ))

class BuyCoinsButton(discord.ui.Button):
    def __init__(self, coins: int, price_cents: int):
        super().__init__(
            label=f"{_format_coins(coins)} Coins",
            style=discord.ButtonStyle.gray,
            custom_id=f"buy_coins_{coins}",
            emoji=client.app_emojis.get("veilcoin")  # safer: won't KeyError
        )
        self.coins = coins
        self.price_cents = price_cents

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild or interaction.user.bot:
            return await interaction.response.send_message(
                "This store can only be used inside a server.", ephemeral=True
            )

        # create Stripe checkout first (should be well under 3s)
        try:
            session = create_coin_checkout_session(interaction.user.id, interaction.guild.id, self.coins)
        except Exception as e:
            print("❌ Stripe create session failed:", e)
            session = None

        if not session:
            incorrectmoji = str(client.app_emojis.get("veilincorrect", "⚠️"))
            return await interaction.response.send_message(
                embed=discord.Embed(
                    title=f"{incorrectmoji} Store Unavailable",
                    description="Please try again in a moment.",
                    color=0x992d22
                ),
                ephemeral=True
            )

        # save mapping so webhook can edit THIS same message
        save_coin_checkout_mapping(session.id, interaction, self.coins)

        veilcoin = str(client.app_emojis.get("veilcoin", "🪙"))
        coins_str = _format_coins(self.coins)
        price_str = _format_price(self.price_cents)

        embed = discord.Embed(
            title=f"{veilcoin} {coins_str} Veil Coins",
            description=(
                f"You're getting **{coins_str}** coins for **{price_str}**.\n\n"
                "• Adds to your balance right after checkout\n"
                "• This message will update to **Purchase Successful** automatically"
            ),
            color=0xeeac00
        )
        embed.add_field(name="Pack", value=f"`{coins_str} coins`", inline=True)
        embed.add_field(name="Total", value=f"`{price_str}`", inline=True)
        embed.set_footer(text="Tip: run /user any time to see your balance.")

        view = discord.ui.View()
        view.add_item(discord.ui.Button(style=discord.ButtonStyle.url, label="Checkout", url=session.url))

        # IMPORTANT: use send_message (not defer + followup)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

class StoreView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

        # Find best value (lowest $ per 100 coins)
        best_idx = min(
            range(len(COIN_PACKS)),
            key=lambda i: COIN_PACKS[i][1] / (COIN_PACKS[i][0] / 100)
        )

        for i, (coins, cents) in enumerate(COIN_PACKS):
            btn = BuyCoinsButton(coins, cents)

            # Clean, compact label that reads well on mobile
            label = f"{_format_coins(coins)} • {_format_price(cents)}"
            if i == best_idx:
                label += "  💎"

            btn.label = label
            # Use a subtle highlight for the best pack
            btn.style = discord.ButtonStyle.blurple if i == best_idx else discord.ButtonStyle.secondary
            # Force a 2-column layout on mobile: rows 0,1,2…
            btn.row = i // 2

            self.add_item(btn)

class MyStatsButton(Button):
    def __init__(self):
        super().__init__(label="My Stats", style=discord.ButtonStyle.secondary, emoji="👤")

    async def callback(self, interaction: discord.Interaction):
        embed, file = build_user_stats_embed_and_file(interaction.guild, interaction.user)
        await interaction.response.send_message(embed=embed, file=file, ephemeral=True)

class HelpUpgradeButton(Button):
    def __init__(self):
        super().__init__(label="Upgrade Tier", style=discord.ButtonStyle.grey, emoji="🚀")

    async def callback(self, interaction: discord.Interaction):
        # build the embed/view (your helper or inline logic)
        embed, view = build_upgrade_panel(interaction.guild.id, interaction.user.id)

        # ✅ Only include view if it exists (e.g., not Elite)
        if view is not None:
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, ephemeral=True)

class HelpView(View):
    def __init__(self, invoker: discord.Member):
        super().__init__(timeout=180)
        # Buttons: My Stats, Store, Leaderboard (tier-gated on click)
        self.add_item(MyStatsButton())
        self.add_item(StoreButton())
        self.add_item(LeaderboardButton())

        # Admin-only helpers (reuse YOUR Configure and Upgrade flows)
        if invoker.guild_permissions.administrator:
            self.add_item(ConfigureButton())
            self.add_item(HelpUpgradeButton())

class StoreButton(Button):
    def __init__(self):
        super().__init__(label="Open Store", style=discord.ButtonStyle.grey, emoji="🏪")

    async def callback(self, interaction: discord.Interaction):
        embed = build_store_embed()
        view = StoreView()
        file = discord.File("veilstore.png", filename="veilstore.png")
        await interaction.response.send_message(embed=embed, view=view, file=file, ephemeral=True)

class LeaderboardButton(Button):
    def __init__(self):
        super().__init__(label="Leaderboard", style=discord.ButtonStyle.secondary, emoji="🏆")

    async def callback(self, interaction: discord.Interaction):
        guild = interaction.guild
        guild_id = guild.id
        tier = get_subscription_tier(guild_id)

        if tier not in ("premium", "elite"):
            incorrectmoji = str(client.app_emojis["veilincorrect"])
            return await interaction.response.send_message(
                embed=discord.Embed(
                    title=f"{incorrectmoji} Premium Feature",
                    description="The leaderboard is only available for **Premium** and **Elite** tiers.",
                    color=0x992d22
                ),
                ephemeral=True
            )

        channel_ids = tuple(c.id for c in guild.text_channels)
        if not channel_ids:
            return await interaction.response.send_message(
                embed=discord.Embed(
                    title="Veil Leaderboard",
                    description="No text channels found for this server.",
                    color=0xeeac00
                ),
                ephemeral=True
            )

        # Top 10 unveilers in this guild
        with get_safe_cursor() as cur:
            cur.execute("""
                SELECT vg.guesser_id, COUNT(*) AS unveils
                FROM veil_guesses vg
                JOIN veil_messages vm ON vm.message_id = vg.message_id
                WHERE vg.is_correct = TRUE
                  AND vm.channel_id IN %s
                GROUP BY vg.guesser_id
                ORDER BY unveils DESC
                LIMIT 50
            """, (channel_ids,))
            rows = cur.fetchall()

        # Resolve members; filter users no longer in guild
        ranked = []
        for user_id, unveils in rows:
            m = guild.get_member(user_id)
            if m:
                ranked.append((m, unveils))

        ranked = ranked[:10]

        if not ranked:
            return await interaction.response.send_message(
                embed=discord.Embed(
                    title="Veil Leaderboard",
                    description="No users in this server have unveiled any messages yet.",
                    color=0xeeac00
                ),
                ephemeral=True
            )

        # Badges: 1–3 = medals, 4–10 = your custom emojis (fallback -> 🏅)
        badge = {
            1: str(client.app_emojis["1st"]),
            2: str(client.app_emojis["2nd"]),
            3: str(client.app_emojis["3rd"]),
            4: str(client.app_emojis["4th"]),
            5: str(client.app_emojis["5th"]),
            6: str(client.app_emojis["6th"]),
            7: str(client.app_emojis["7th"]),
            8: str(client.app_emojis["8th"]),
            9: str(client.app_emojis["9th"]),
            10: str(client.app_emojis["10th"])
        }


        def fmt(n):
            try: return f"{int(n):,}"
            except: return str(n)

        embed = discord.Embed(
            title="Veil Leaderboard",
            description="**Top 10 Unveilers**",
            color=0xeeac00
        )
        # keep or change this to your logo
        embed.set_thumbnail(url="https://i.imgur.com/E2jxHuj.png")

        for idx, (member, unveils) in enumerate(ranked, start=1):
            name = get_display_name_safe(member).title()
            icon = badge.get(idx, "🏅")
            embed.add_field(
                name=f"{icon} {name}",
                value=f"{fmt(unveils)} unveils",
                inline=False
            )

        await interaction.response.send_message(embed=embed, ephemeral=True)

@client.event
async def on_guild_join(guild):
    ensure_free_subscription(guild.id)

    now = datetime.now(timezone.utc)

    with get_safe_cursor() as cur:
        for member in guild.members:
            if member.bot:
                continue

            # ✅ Ensure entry for user exists
            cur.execute("""
                INSERT INTO veil_users (user_id, guild_id, coins, last_refill)
                VALUES (%s, %s, %s, NULL)
                ON CONFLICT (user_id, guild_id) DO NOTHING
            """, (member.id, guild.id, 0,))  # ✅ last_refill = NULL allows immediate refill

    # ✅ Now refill for all users
    for member in guild.members:
        if not member.bot:
            refill_user_coins(member.id, guild.id)

    # 🟨 Welcome message
    maskemoji = str(client.app_emojis["veilemoji"])  or "🎭"
    embed = discord.Embed(
        title="Welcome to Veil",
        description=f"Thank you for choosing **Veil**, where every message wears a mask. {maskemoji}\n\n"
            "Click the `🛠️Configure` button below to setup the Veil channel",
        color=0xeeac00
    )

    view = SetupView(guild.owner)

    for channel in guild.text_channels:
        if channel.permissions_for(guild.me).send_messages:
            try:
                await channel.send(embed=embed, view=view)
                break
            except Exception as e:
                print(f"❌ Couldn't send message in {channel.name}: {e}")

@client.event
async def on_interaction(interaction: discord.Interaction):
    if interaction.type != discord.InteractionType.component:
        return

    # Safely get custom_id; ignore components without a custom_id (like link buttons)
    data = interaction.data or {}
    cid = (data.get("custom_id") or "").strip()
    if not cid:
        return

    log_channel = interaction.guild.get_channel(1399973286649008158) if interaction.guild else None
    
    if cid == "upgrade_menu":
        guild_id = interaction.guild_id
        user_id = interaction.user.id

        if not interaction.user.guild_permissions.administrator:
            incorrectmoji = str(client.app_emojis["veilincorrect"])
            await interaction.response.send_message(
                embed=discord.Embed(
                    title=f"{incorrectmoji} Admin Only",
                    description="Only admins can manage Veil upgrades.",
                    color=0x992d22
                ),
                ephemeral=True
            )
            return

        # Colors per tier (basic updated to 0xF8AF3F)
        COLOR_BY_TIER = {
            "free":    0xA0A0A0,
            "basic":   0xF8AF3F,  # ← your new basic color
            "premium": 0x69A7D9,
            "elite":   0xEC8195,
        }

        # current tier
        with get_safe_cursor() as cur:
            cur.execute("SELECT tier FROM veil_subscriptions WHERE guild_id = %s", (guild_id,))
            row = cur.fetchone()
            current_tier = (row[0] if row else "free").lower()

        veilcoinemoji = str(client.app_emojis["veilcoin"])
        fmt = lambda n: f"{n:,}"

        # Elite: show info-only embed + bail
        if current_tier == "elite":
            embed = discord.Embed(
                title="You're on the Elite Tier 🧠",
                description=(
                    f"{veilcoinemoji} • **Unlimited** coins for all users\n"
                    "🗃️ • Admin-only logging access\n"
                    "🥇 • Unveiling leaderboard\n"
                    "💎 • Early access to new features"
                ),
                color=COLOR_BY_TIER["elite"]
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            # optional: log usage
            if log_channel:
                await log_channel.send(
                    f"📢 {interaction.user.mention} opened the Upgrade Menu (Elite) in {interaction.channel.mention}."
                )
            return

        # Copy text per tier, with formatted amounts
        desc_map = {
            "free": (
                "You're currently on the **Free Tier** ✨\n\n"
                f"{veilcoinemoji} • **{fmt(100)}** coins per month\n"
                "🔄 • Refills every month\n\n"
                "Upgrade for more coins, leaderboards, admin logging and early access to new features! 🚀"
            ),
            "basic": (
                "You're on **Basic Tier** 🌟\n\n"
                f"{veilcoinemoji} • **{fmt(250)}** coins per month\n"
                "🔄 • Refills every month\n"
                "🔍 • Earning coins by unveiling\n\n"
                "Upgrade for more coins, leaderboards, admin logging and early access to new features! 🚀"
            ),
            "premium": (
                "You're on **Premium Tier** 💎\n\n"
                f"{veilcoinemoji} • **{fmt(1000)}** coins per month\n"
                "🔄 • Refills every month\n"
                "🔍 • Earning coins by unveiling\n"
                "🥇 • Unveiling leaderboard\n\n"
                "Upgrade for unlimited coins, admin logging, and early access to new features 🚀!"
            ),
        }

        embed = discord.Embed(
            title="Upgrade Your VeilBot Tier",
            description=desc_map[current_tier],
            color=COLOR_BY_TIER.get(current_tier, 0xA0A0A0)
        )

        await interaction.response.send_message(
            embed=embed,
            view=UpgradeMenuView(current_tier, user_id, guild_id),
            ephemeral=True
        )

        # 🔹 Log upgrade menu access (optional)
        if log_channel:
            await log_channel.send(
                f"📢 {interaction.user.mention} opened the Upgrade Menu in {interaction.channel.mention} (tier: {current_tier})."
            )

    elif cid == "guess_btn":
        message_id = interaction.message.id

        with get_safe_cursor() as cur:
            cur.execute("SELECT content, author_id FROM veil_messages WHERE message_id = %s", (message_id,))
            result = cur.fetchone()

        if not result:
            incorrectmoji = str(client.app_emojis["veilincorrect"]) 
            return await interaction.response.send_message(
                embed=discord.Embed(
                    title=f"{incorrectmoji} Message Not Found",
                    description="Veil message not found.",
                    color=0x992d22
                ),
                ephemeral=True
            )

        veil_text, author_id = result

        # 🚫 Block guessing your own veil
        if interaction.user.id == author_id:
            incorrectmoji = str(client.app_emojis["veilincorrect"]) 
            return await interaction.response.send_message(
                embed=discord.Embed(
                    title=f"{incorrectmoji} Unveil Error",
                    description="You sent this message, so you cannot unveil it!",
                    color=0x992d22
                ),
                ephemeral=True
            )

        # 🚫 Check if this user has already guessed this veil
        with get_safe_cursor() as cur:
            cur.execute(
                "SELECT 1 FROM veil_guesses WHERE message_id = %s AND guesser_id = %s",
                (message_id, interaction.user.id)
            )
            already_guessed = cur.fetchone() is not None

        if already_guessed:
            incorrectmoji = str(client.app_emojis["veilincorrect"]) 
            return await interaction.response.send_message(
                embed=discord.Embed(
                    title=f"{incorrectmoji} Unveil Error",
                    description="You can only guess **once per veil**.",
                    color=0x992d22
                ),
                ephemeral=True
            )

        # ✅ Proceed with normal guessing
        view = UnveilView(message_id, author_id, interaction)
        maskemoji = str(client.app_emojis["veilemoji"])
        embed = discord.Embed(
            title=f"{maskemoji} Make a Guess",
            description=f"Who do you think sent this veil?\n\n> {veil_text}",
            color=0xeeac00
        )

        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    elif cid == "cancel_subscription":
        guild = interaction.guild
        guild_id = guild.id
        user = interaction.user

        # Check admin permissions
        if not user.guild_permissions.administrator:
            incorrectmoji = str(client.app_emojis["veilincorrect"]) 
            await interaction.response.send_message(
                embed=discord.Embed(
                    title=f"{incorrectmoji} Admin Only",
                    description="Only admins can cancel subscriptions.",
                    color=0x992d22
                ),
                ephemeral=True
            )
            return

        # ✅ Fetch subscription ID from DB
        with get_safe_cursor() as cur:
            cur.execute("SELECT subscription_id FROM veil_subscriptions WHERE guild_id = %s", (guild_id,))
            sub = cur.fetchone()
            subscription_id = sub[0] if sub else None

        # 🔹 Cancel subscription on Stripe if exists
        cautionemoji = str(client.app_emojis["veilcaution"]) 
        stripe_canceled = False
        if subscription_id:
            try:
                # 1️⃣ Retrieve subscription
                sub = stripe.Subscription.retrieve(subscription_id)

                if sub and sub.status in ["active", "trialing"]:
                    # 2️⃣ Only delete if still active/trialing
                    stripe.Subscription.delete(subscription_id)
                    stripe_canceled = True
                else:
                    # 3️⃣ Already canceled, skip Stripe deletion
                    print(f"ℹ️ Subscription {subscription_id} already canceled, skipping delete.")
                    stripe_canceled = False


            except stripe.error.InvalidRequestError as e:
                print(f"⚠️ Stripe cancel failed: {e}")
                await interaction.response.send_message(
                    embed=discord.Embed(
                        title=f"{cautionemoji} Stripe Cancel Failed",
                        description="The subscription could not be canceled on Stripe. Please check logs.",
                        color=0x992d22
                    ),
                    ephemeral=True
                )
                return

        # ✅ Downgrade guild in DB
        with get_safe_cursor() as cur:
            cur.execute("""
                UPDATE veil_subscriptions
                SET tier = 'free',
                    subscribed_at = NOW(),
                    renews_at = NULL,
                    subscription_id = NULL,
                    payment_failed = FALSE
                WHERE guild_id = %s
            """, (guild_id,))

            cur.connection.commit()  # ✅ Commit after the update

        incorrectmoji = str(client.app_emojis["veilincorrect"]) 
        # ✅ Respond to admin
        await interaction.response.send_message(
            embed=discord.Embed(
                title=f"{incorrectmoji} Subscription Canceled",
                description="Your guild has been downgraded to the **Free Tier**.",
                color=0x992d22
            ),
            ephemeral=True
        )

        # 🔹 Log to Veil bot logs
        if log_channel:
            await log_channel.send(
                f"{incorrectmoji} **{guild.name}** (ID: `{guild_id}`) subscription canceled by {user.mention}. "
                f"{'Stripe subscription also canceled ✅' if stripe_canceled else 'Already canceled on Stripe.'}"
            )

@tree.command(name="info", description="📌 Shows Veil account info in this server")
@app_commands.checks.has_permissions(administrator=True)
async def info(interaction: discord.Interaction):
    embed, view = await build_bot_info_embed(interaction.guild)
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

@tree.command(name="leaderboard", description="🏆 Show the top unveilers")
async def leaderboard(interaction: discord.Interaction):
    guild = interaction.guild
    guild_id = guild.id

    # Gate to Premium/Elite
    tier = get_subscription_tier(guild_id)
    if tier not in ("premium", "elite"):
        incorrectmoji = str(client.app_emojis["veilincorrect"])
        return await interaction.response.send_message(
            embed=discord.Embed(
                title=f"{incorrectmoji} Premium Feature",
                description="The leaderboard is only available for **Premium** and **Elite** tiers.",
                color=0x992d22
            ),
            ephemeral=True
        )

    # All text channels in this guild
    channel_ids = tuple(c.id for c in guild.text_channels)
    if not channel_ids:
        return await interaction.response.send_message(
            embed=discord.Embed(
                title="Veil Leaderboard",
                description="No text channels found for this server.",
                color=0xeeac00
            ),
            ephemeral=True
        )

    # Top 10 unveilers
    with get_safe_cursor() as cur:
        cur.execute("""
            SELECT vg.guesser_id, COUNT(*) AS unveils
            FROM veil_guesses vg
            JOIN veil_messages vm ON vm.message_id = vg.message_id
            WHERE vg.is_correct = TRUE
              AND vm.channel_id IN %s
            GROUP BY vg.guesser_id
            ORDER BY unveils DESC
            LIMIT 50
        """, (channel_ids,))
        rows = cur.fetchall()

    # Resolve members that are still in the guild
    ranked = []
    for user_id, unveils in rows:
        m = guild.get_member(user_id)
        if m:
            ranked.append((m, unveils))

            
    ranked = ranked[:10]


    if not ranked:
        return await interaction.response.send_message(
            embed=discord.Embed(
                title="Veil Leaderboard",
                description="No users in this server have unveiled any messages yet.",
                color=0xeeac00
            ),
            ephemeral=True
        )

    # Badges: 1–3 medals, 4–10 your custom participation emojis (fallback 🏅)
    badge = {
        1: str(client.app_emojis["1st"]),
        2: str(client.app_emojis["2nd"]),
        3: str(client.app_emojis["3rd"]),
        4: str(client.app_emojis["4th"]),
        5: str(client.app_emojis["5th"]),
        6: str(client.app_emojis["6th"]),
        7: str(client.app_emojis["7th"]),
        8: str(client.app_emojis["8th"]),
        9: str(client.app_emojis["9th"]),
        10: str(client.app_emojis["10th"])
    }

    def fmt(n):
        try: return f"{int(n):,}"
        except: return str(n)

    embed = discord.Embed(
        title="Veil Leaderboard",
        description="**Top 10 Unveilers**",
        color=0xeeac00
    )
    # Use server icon if available
    embed.set_thumbnail(url="https://i.imgur.com/E2jxHuj.png")

    for idx, (member, unveils) in enumerate(ranked, start=1):
        name = get_display_name_safe(member).title()
        icon = badge.get(idx, "🏅")
        embed.add_field(
            name=f"{icon} {name}",
            value=f"{fmt(unveils)} unveils",
            inline=False
        )

    # Public message (change to ephemeral=True if you prefer)
    await interaction.response.send_message(embed=embed)
    
@tree.command(name="user", description="👤 Check Veil User Stats")
@app_commands.describe(user="The user to check (optional)")
async def user_stats(interaction: discord.Interaction, user: discord.Member | None = None):
    target = user or interaction.user

    # build_user_stats_embed_and_file should return (embed, file)
    embed, file = build_user_stats_embed_and_file(interaction.guild, target)

    if file:
        await interaction.response.send_message(embed=embed, file=file, ephemeral=True)
    else:
        await interaction.response.send_message(embed=embed, ephemeral=True)

@tree.command(name="veil", description="💬 Send a Message Behind a Veil")
@app_commands.describe(message="The message you want to send anonymously.")
async def veil_command(
    interaction: discord.Interaction, 
    message: app_commands.Range[str, 1, 1000]  # allow longer raw input
):
    incorrectmoji = str(client.app_emojis["veilincorrect"]) 
    maskemoji = str(client.app_emojis["veilemoji"]) 

    total_emojis = count_emojis_all(message)
    if total_emojis > EMOJI_LIMIT:
        return await interaction.response.send_message(
            embed=discord.Embed(
                title=f"{incorrectmoji} Too Many Emojis",
                description=f"You can use up to **{EMOJI_LIMIT} emojis** per veil "
                            f"(custom & animated included). You used **{total_emojis}**.",
                color=0x992d22
            ),
            ephemeral=True
        )

    # 🚫 Visual-length limit (grapheme-aware, emojis count as 1)
    visual_count = visual_length(message)
    if visual_count > MAX_VISUAL:
        return await interaction.response.send_message(
            embed=discord.Embed(
                title=f"{incorrectmoji} Message Too Long",
                description=(
                    f"Your veil exceeds **{MAX_VISUAL} visual characters** "
                    f"(emojis count as 1). Current: **{visual_count}**."
                ),
                color=0x992d22
            ),
            ephemeral=True
        )

    # 🚫 Check if veil channel is configured
    channel_id = get_veil_channel(interaction.guild.id)
    channel = interaction.guild.get_channel(channel_id) if channel_id else None
    if not channel:
        return await interaction.response.send_message(
            embed=discord.Embed(
                title=f"{incorrectmoji} Channel Not Linked",
                description="This server hasn’t linked a Veil channel yet.\nUse **/configure** to set one.",
                color=0x992d22
            ),
            ephemeral=True
        )

    # ⏳ Initial ack to avoid timeouts
    await interaction.response.send_message(
        embed=discord.Embed(
            title=f"{maskemoji} Sending Veil...",
            description="Your message is being sent behind a veil.",
            color=0xeeac00
        ),
        ephemeral=True
    )

    # 🖼️ Send the actual image/card
    await send_veil_message(interaction, message, channel)

    # ✅ Success update
    await interaction.edit_original_response(
        embed=discord.Embed(
            title=f"{maskemoji} Veil Sent",
            description="Your message has been sent under a veil.",
            color=0x43b581
        )
    )

@tree.command(name="setup", description="🛠️ Manually Set Up Veil")
@app_commands.checks.has_permissions(administrator=True)
async def setup(interaction: discord.Interaction):
    guild = interaction.guild
    maskemoji = str(client.app_emojis["veilemoji"]) 
    embed = discord.Embed(
        title="Welcome to Veil",
        description=f"Thank you for choosing **Veil**, where every message wears a mask. {maskemoji}\n\n"
            "Click the `🛠️Configure` button below to setup the Veil channel",
        color=0xeeac00
    )
    view = SetupView(interaction.user)

    for channel in guild.text_channels:
        if channel.permissions_for(guild.me).send_messages:
            try:
                await channel.send(embed=embed, view=view)
                successmoji = str(client.app_emojis["veilsuccess"]) 
                await interaction.response.send_message(
                    embed=discord.Embed(
                        title=f"{successmoji} Setup Complete",
                        description=f"Setup message sent in {channel.mention}.",
                        color=0xeeac00
                    ),
                    ephemeral=True
                )
                return
            except Exception as e:
                print(f"❌ Couldn't send message in {channel.name}: {e}")

    incorrectmoji = str(client.app_emojis["veilincorrect"]) 
    await interaction.response.send_message(
        embed=discord.Embed(
            title=f"{incorrectmoji} Channel Not Found",
            description="I couldn't find a channel where I have permission to send messages.",
            color=0x992d22
        ),
        ephemeral=True
    )

@setup.error
async def setup_error(interaction: discord.Interaction, error: AppCommandError):
    if isinstance(error, CheckFailure):
        incorrectmoji = str(client.app_emojis["veilincorrect"]) 
        await interaction.response.send_message(
            embed=discord.Embed(
                title=f"{incorrectmoji} Admin Only",
                description="You must be an **administrator** to use this command.",
                color=0x992d22
            ),
            ephemeral=True
        )

@tree.command(name="configure", description="🔗 Link Veil to a channel.")
@app_commands.checks.has_permissions(administrator=True)
async def configure(interaction: discord.Interaction):
    # look up tier
    tier = get_subscription_tier(interaction.guild.id)
    guild_channels = interaction.guild.text_channels
    maskemoji = str(client.app_emojis["veilemoji"])

    if tier == "elite":
        embed = discord.Embed(
            title=f"{maskemoji} Configure Veil & Admin Logs",
            description=(
                "As an Elite Tier 🧠 server you can configure two things:\n\n"
                "• **Configure Veil** — Choose which channel Veil posts into\n"
                "• **Configure Admin Logs** — Choose which channel to send all Veil logs"
            ),
            color=0xeeac00
        )
        view = EliteConfigureView(guild_channels)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    else:
        # free / premium: just the Veil dropdown
        if not guild_channels:
            incorrectmoji = str(client.app_emojis["veilincorrect"])
            return await interaction.response.send_message(
                embed=discord.Embed(
                    title=f"{incorrectmoji} No Channels Found",
                    description="Couldn't find any text channels in this server.",
                    color=0x992d22
                ),
                ephemeral=True
            )

        embed = discord.Embed(
            title=f"{maskemoji} Configure Veil",
            description="Select a channel from the dropdown below to link it to Veil.",
            color=0xeeac00
        )
        view = ConfigureView(guild_channels)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

@configure.error
async def configure_error(interaction: discord.Interaction, error):
    if isinstance(error, app_commands.errors.MissingPermissions):
        incorrectmoji = str(client.app_emojis["veilincorrect"])
        await interaction.response.send_message(
            embed=discord.Embed(
                title=f"{incorrectmoji} Permission Denied",
                description="You must be an **administrator** to run this command.",
                color=0x992d22
            ),
            ephemeral=True
        )

@tree.command(name="upgrade", description="🚀 Upgrade your server's Veil Tier")
@app_commands.checks.has_permissions(administrator=True)
async def upgrade(interaction: discord.Interaction):
    embed, view = build_upgrade_panel(interaction.guild.id, interaction.user.id)
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

@upgrade.error
async def upgrade_error(interaction: discord.Interaction, error):
    if isinstance(error, app_commands.errors.MissingPermissions):
        incorrectmoji = str(client.app_emojis["veilincorrect"]) 
        await interaction.response.send_message(
            embed=discord.Embed(
                title=f"{incorrectmoji} Permission Denied",
                description="You must be an **administrator** to run this command.",
                color=0x992d22
            ),
            ephemeral=True
        )

@tree.command(name="store", description="🏪 Open the Veil Coin Store.")
@app_commands.checks.has_permissions(send_messages=True)
async def store(interaction: discord.Interaction):
    embed = build_store_embed()
    view = StoreView()
    # attach the thumbnail file so the embed can reference attachment://veilstore.png
    file = discord.File("veilstore.png", filename="veilstore.png")
    await interaction.response.send_message(embed=embed, view=view, file=file, ephemeral=True)

@store.error
async def store_error(interaction: discord.Interaction, error):
    incorrectmoji = str(client.app_emojis["veilincorrect"])
    await interaction.response.send_message(
        embed=discord.Embed(
            title=f"{incorrectmoji} Something went wrong",
            description="Please try again in a moment.",
            color=0x992d22
        ),
        ephemeral=True
    )

@tree.command(name="help", description="❓ Help & Commands for Veil")
async def help_command(interaction: discord.Interaction):
    embed = build_help_embed(interaction.guild)
    await interaction.response.send_message(embed=embed, view=HelpView(interaction.user), ephemeral=True)

@tree.command(name="remove", description="🗑️ Removes a Veil that violates TOS")
@app_commands.describe(number="The Veil #")
@app_commands.checks.has_permissions(administrator=True)
async def remove_veil(interaction: discord.Interaction, number: app_commands.Range[int, 1, None]):
    guild = interaction.guild
    channel = interaction.channel
    incorrectmoji = str(client.app_emojis.get("veilincorrect", "⚠️"))
    maskemoji = str(client.app_emojis.get("veilemoji", "🎭"))

    # Only allow in guild text channels
    if not guild or not isinstance(channel, (discord.TextChannel, discord.Thread)):
        return await interaction.response.send_message(
            embed=discord.Embed(
                title=f"{incorrectmoji} Not Here",
                description="Run this in a server text channel where the veil was posted.",
                color=0x992d22
            ),
            ephemeral=True
        )

    # Look up the message_id for this channel + veil_number
    with get_safe_cursor() as cur:
        cur.execute("""
            SELECT message_id
            FROM veil_messages
            WHERE channel_id = %s AND veil_number = %s
        """, (channel.id, number))
        row = cur.fetchone()

    if not row:
        return await interaction.response.send_message(
            embed=discord.Embed(
                title=f"{incorrectmoji} Veil Not Found",
                description=f"I couldn't find **Veil #{number}** in {channel.mention}.",
                color=0x992d22
            ),
            ephemeral=True
        )

    message_id = int(row[0])

    # Try to fetch the original message
    try:
        msg = await channel.fetch_message(message_id)
    except discord.NotFound:
        return await interaction.response.send_message(
            embed=discord.Embed(
                title=f"{incorrectmoji} Message Gone",
                description="That veil message no longer exists.",
                color=0x992d22
            ),
            ephemeral=True
        )
    except discord.Forbidden:
        return await interaction.response.send_message(
            embed=discord.Embed(
                title=f"{incorrectmoji} Missing Permissions",
                description="I can’t edit messages in this channel.",
                color=0x992d22
            ),
            ephemeral=True
        )

    # Attempt to swap the image+remove the view
    try:
        # Replace attachments with the TOS image
        file = discord.File("veiltos.png", filename="veil.png")
        await msg.edit(attachments=[file], view=None, embed=None)

        # (Optional) mark a flag in DB that this veil was removed by admin
        # with get_safe_cursor() as cur:
        #     cur.execute("ALTER TABLE veil_messages ADD COLUMN IF NOT EXISTS removed_by_admin BOOLEAN DEFAULT FALSE")
        #     cur.execute("UPDATE veil_messages SET removed_by_admin = TRUE WHERE message_id = %s", (message_id,))
        #     cur.connection.commit()

        jump = f"https://discord.com/channels/{guild.id}/{channel.id}/{message_id}"
        await interaction.response.send_message(
            embed=discord.Embed(
                title=f"{maskemoji} Veil #{number} Removed",
                description=f"The veil has been replaced and its buttons were removed.\n[Jump to message]({jump})",
                color=0xeeac00
            ),
            ephemeral=True
        )
    except Exception as e:
        print("❌ remove_veil edit failed:", e)
        await interaction.response.send_message(
            embed=discord.Embed(
                title=f"{incorrectmoji} Edit Failed",
                description="I couldn’t update that message. Check my permissions and try again.",
                color=0x992d22
            ),
            ephemeral=True
        )

@remove_veil.error
async def remove_veil_error(interaction: discord.Interaction, error):
    if isinstance(error, app_commands.errors.MissingPermissions):
        incorrectmoji = str(client.app_emojis.get("veilincorrect", "⚠️"))
        await interaction.response.send_message(
            embed=discord.Embed(
                title=f"{incorrectmoji} Admin Only",
                description="You must be an **administrator** to use this command.",
                color=0x992d22
            ),
            ephemeral=True
        )

@tree.command(name="guilds", description="(Owner) List all guilds the bot is in")
@app_commands.default_permissions(administrator=True)  # hides from non-admins in the picker
async def guilds_cmd(inter: discord.Interaction):
    # Owner gate (safer than admin-gating since this reveals other servers)
    if inter.user.id not in OWNER_IDS:
        incorrectmoji = str(client.app_emojis.get("veilincorrect", "❌"))
        return await inter.response.send_message(
            embed=discord.Embed(
                title=f"{incorrectmoji} Owner Only",
                description="This command is restricted.",
                color=0x992d22
            ),
            ephemeral=True
        )

    # Collect & sort guilds (by member count desc, fallback to 0)
    guilds = sorted(client.guilds, key=lambda g: (g.member_count or 0), reverse=True)

    # Build lines
    def fmt(n):
        try: return f"{int(n):,}"
        except: return str(n)

    lines = [
        f"{idx:>2}. {g.name} — `{g.id}` ({fmt(getattr(g, 'member_count', 0) or 0)} members)"
        for idx, g in enumerate(guilds, start=1)
    ]

    # First chunk fits under Discord 2000-char message limit (keep some headroom)
    out, total = [], 0
    for line in lines:
        if total + len(line) + 1 > 1900:
            break
        out.append(line)
        total += len(line) + 1

    header = f"**Guilds:** {len(guilds)}"
    body = "```" + ("\n".join(out) if out else "No guilds") + "```"
    await inter.response.send_message(f"{header}\n{body}", ephemeral=True)

    # If truncated, also send a full text file as an ephemeral follow-up
    if len(out) < len(lines):
        import io
        full_text = "\n".join(lines)
        buf = io.BytesIO(full_text.encode("utf-8"))
        await inter.followup.send(
            content="(Full list attached as a file)",
            file=discord.File(buf, filename="guilds.txt"),
            ephemeral=True
        )


# ---- admin-only shards command ----
@tree.command(name="shards", description="Show shard status")
@app_commands.guild_only()
@app_commands.default_permissions(administrator=True)  # hides from non-admins in the picker
async def shards_cmd(inter: discord.Interaction):
    # Owner gate (safer than admin-gating since this reveals other servers)
    if inter.user.id not in OWNER_IDS:
        incorrectmoji = str(client.app_emojis.get("veilincorrect", "❌"))
        return await inter.response.send_message(
            embed=discord.Embed(
                title=f"{incorrectmoji} Owner Only",
                description="This command is restricted.",
                color=0x992d22
            ),
            ephemeral=True
        )

    # Shard latencies
    rows = []
    shard_info = getattr(client, "shards", {}) or {}
    # Guilds per shard
    per_shard_counts = Counter(g.shard_id for g in client.guilds)
    total_guilds = len(client.guilds)

    if shard_info:
        for sid, info in sorted(shard_info.items()):
            ms = int((getattr(info, "latency", client.latency) or 0) * 1000)
            dot = "🟢" if ms < 250 else ("🟡" if ms < 600 else "🔴")
            gcount = per_shard_counts.get(sid, 0)
            rows.append(f"{sid:>2}: {dot} {ms} ms • {gcount} guilds")
        shard_count = client.shard_count or len(shard_info)
    else:
        # Unsharded fallback
        ms = int(client.latency * 1000)
        dot = "🟢" if ms < 250 else ("🟡" if ms < 600 else "🔴")
        rows.append(f" 0: {dot} {ms} ms • {total_guilds} guilds")
        shard_count = 1

    header = (
        f"**Shards:** {shard_count}\n"
        f"**Total Guilds:** {total_guilds:,}\n"
        "```"
        + ("\n".join(rows) if rows else "no shard data")
        + "```"
    )

    await inter.response.send_message(header, ephemeral=True)

@client.event
async def on_message(message: discord.Message):
    # Only parse the single relay channel
    if message.channel.id != SUPPORT_CHANNEL_ID:
        return

    # Accept messages from your webhook OR from your own bot user
    is_from_webhook = bool(message.webhook_id)
    is_from_me      = message.author and client.user and message.author.id == client.user.id
    if not (is_from_webhook or is_from_me):
        return

    txt = (message.content or "").strip()
    if not txt:
        return

    # --- A) COIN TOP-UP ------------------------------------------------------
    m_coin = COIN_RE.match(txt)
    if m_coin:
        session_id = m_coin.group(1)
        user_id    = int(m_coin.group(2))
        guild_id   = int(m_coin.group(3))
        coins      = int(m_coin.group(4))

        # look up the stored interaction info (as you already do)
        with get_safe_cursor() as cur:
            cur.execute("""
                SELECT interaction_token, application_id, user_id, guild_id, coins
                FROM coin_checkout_sessions
                WHERE stripe_session_id = %s
            """, (session_id,))
            row = cur.fetchone()

        if not row:
            print(f"[coin] no session {session_id} found; ignoring")
            return

        interaction_token, application_id, u_saved, g_saved, coins_saved = row
        if u_saved != user_id or g_saved != guild_id:
            print(f"[coin] id mismatch for {session_id}; ignoring")
            return
        # ... after verifying row matches ...
        new_balance = get_user_coins(user_id, guild_id) or 0
        veilcoinemoji = str(client.app_emojis.get("veilcoin", "🪙"))
        
        # format numbers
        coins_str = fmt(coins_saved)
        bal_str   = fmt(new_balance)
        
        payload = {
            "embeds": [{
                "title": f"{veilcoinemoji} +{coins_str} Veil Coins Added",
                "description": f"Thanks for your support! Your new balance is **{bal_str}**.",
                "color": 0xeeac00,
                "fields": [
                    {"name": "Amount",  "value": f"{veilcoinemoji} `{coins_str}`", "inline": True},
                    {"name": "Balance", "value": f"`{bal_str}`",                   "inline": True},
                ],
                "footer": {"text": "Tip: use /user any time to see your balance."}
            }],
            "components": []
        }

        url = f"{DISCORD_API_BASE}/webhooks/{int(application_id)}/{interaction_token}/messages/@original"
        try:
            r = requests.patch(url, json=payload, timeout=6)
            print(f"[coin] PATCH @original -> {r.status_code} {r.text[:150]}")
        except Exception as e:
            print(f"[coin] PATCH failed: {e}")

        # Clean up relay post
        try:
            await message.delete()
        except Exception:
            pass
        return

    # --- B) GUILD UPGRADE ----------------------------------------------------
    m_up = UPGRADE_RE.search(txt)
    if m_up:
        guild_id = int(m_up.group(1))
        tier     = m_up.group(2).lower()  # "basic"|"premium"|"elite"

        guild = client.get_guild(guild_id)
        if not guild:
            print(f"[upgrade] guild {guild_id} not in cache")
            return

        with get_safe_cursor() as cur:
            cur.execute("SELECT channel_id FROM veil_channels WHERE guild_id=%s", (guild_id,))
            row = cur.fetchone()
        if not row:
            print(f"[upgrade] no configured veil channel for guild {guild_id}")
            return

        channel_id = row[0]
        channel = guild.get_channel(channel_id) or client.get_channel(channel_id)
        if not channel or not channel.permissions_for(guild.me).send_messages:
            print(f"[upgrade] cannot send in channel {channel_id} (guild {guild_id})")
            return

        tiers_display = {"basic": "Basic 🌟", "premium": "Premium 💎", "elite": "Elite 🧠"}
        if tier not in tiers_display:
            print(f"[upgrade] unknown tier {tier} for guild {guild_id}")
            return

        veilcoinemoji = str(client.app_emojis.get("veilcoin", "🪙"))
        perks = {
            "basic":   [f"{veilcoinemoji} • **250** coins/mo",   "🔄 • Monthly refills", "🔍 • Earn by unveiling"],
            "premium": [f"{veilcoinemoji} • **1,000** coins/mo", "🔄 • Monthly refills", "🔍 • Earn by unveiling", "🥇 • Leaderboard"],
            "elite":   [f"{veilcoinemoji} • Unlimited coins",    "🗃️ • Admin logs",      "🥇 • Leaderboard",       "💎 • Early features"],
        }

        embed = discord.Embed(
            title=f"🎉 {guild.name} Upgraded!",
            description=f"This server has been upgraded to **{tiers_display[tier]}**.\n\nYour members now get:\n" + "\n".join(perks[tier]),
            color=0xeeac00
        )
        if guild.icon:
            embed.set_footer(text="VeilBot • Every message wears a mask", icon_url=guild.icon.url)
        else:
            embed.set_footer(text="VeilBot • Every message wears a mask")

        view = AdminLog() if tier == "elite" else None
        try:
            await channel.send(embed=embed, view=view)
            print(f"✅ upgrade embed sent to {guild.name} ({guild_id}) tier={tier}")
        except Exception as e:
            print(f"❌ failed to send upgrade embed in {guild.name}: {e}")
        return

if __name__ == "__main__":
    init_db()  # ✅ just to ensure veil_subscriptions exists before on_guild_join runs
    client.run(TOKEN)
