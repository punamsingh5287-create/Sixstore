#!/usr/bin/env python3
"""
==============================================================================
 DIGITAL HUB BOT  --  Advanced Python Edition
==============================================================================

 A single-file Telegram digital-goods store: bot + admin REST API + mini-app
 API, backed by PostgreSQL. Drop-in replacement for the TypeScript monorepo,
 with the endpoint contract preserved so an existing React admin panel keeps
 working unchanged.

 WHAT'S DIFFERENT FROM THE TS VERSION
 -----------------------------------
   1. MONEY IS NUMERIC(18,6), NOT FLOAT.
      The TS schema used `doublePrecision` for prices, balances and totals.
      Floats cannot represent 0.1 exactly; balances drift over thousands of
      transactions and reconciliation stops matching. Everything here is
      Decimal end-to-end, quantised at well-defined points.

   2. ON-CHAIN PAYMENT VERIFICATION.
      Screenshot approval is guesswork -- images are trivially forged. This
      version queries TRON / BSC / ETH directly for the transaction, and
      auto-approves only when recipient, token contract, amount and
      confirmation depth all match. Screenshots remain as a manual fallback.

   3. RACE-SAFE FULFILMENT.
      Key handout and wallet debits run inside one transaction using
      SELECT ... FOR UPDATE row locks, so concurrent buyers cannot oversell
      the last key or double-spend a balance.

   4. LEDGER-BASED WALLET.
      Balance is a materialised column, but every change writes an immutable
      wallet_transactions row. `/admin/wallet/audit` re-derives balances from
      the ledger and reports drift.

 DEPLOY
 ------
   sudo apt install -y python3-venv postgresql
   python3 -m venv venv && source venv/bin/activate
   pip install "aiogram>=3.7" "fastapi>=0.110" "uvicorn[standard]" \
               "asyncpg>=0.29" "pydantic>=2.6" "PyJWT>=2.8" \
               "bcrypt>=4.1" "httpx>=0.27" "python-dotenv>=1.0"

   # write .env (see CONFIG below), then:
   python bot.py

 The schema is created automatically on first boot. For production run it
 behind nginx + TLS and set WEBHOOK_URL so Telegram pushes updates instead
 of the bot long-polling.
==============================================================================
"""

from __future__ import annotations

import asyncio
import base64
import contextvars
import csv
import hashlib
import hmac
import html
import io
import json
import logging
import os
import re
import secrets
import string
import sys
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Any, Callable, Sequence

import asyncpg
import bcrypt
import httpx
import jwt
import qrcode
import qrcode.image.svg
import qrcode.image.pure
import uvicorn
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.session.middlewares.base import BaseRequestMiddleware
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    MenuButtonCommands,
    Message,
    Update,
)
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


# =============================================================================
#  CONFIG
# =============================================================================

def _env(key: str, default: str | None = None, *, required: bool = False) -> str:
    val = os.getenv(key, default)
    if required and not val:
        sys.exit(f"FATAL: required environment variable {key} is not set")
    return val or ""


@dataclass(frozen=True)
class Config:
    database_url: str = _env("DATABASE_URL", required=True)
    bot_token: str = _env("TELEGRAM_BOT_TOKEN", required=True)
    session_secret: str = _env("SESSION_SECRET", required=True)
    admin_password: str = _env("ADMIN_PASSWORD", required=True)
    # Comma-separated Telegram user IDs that always have full admin access,
    # independent of the DB admins table. The first one is treated as owner.
    admin_ids: tuple[int, ...] = tuple(
        int(x) for x in _env("ADMIN_TELEGRAM_IDS", "").replace(" ", "").split(",")
        if x.strip().lstrip("-").isdigit()
    )

    host: str = _env("HOST", "0.0.0.0")
    port: int = int(_env("PORT", "8080"))
    webhook_url: str = _env("WEBHOOK_URL", "")
    webhook_secret: str = _env("WEBHOOK_SECRET", secrets.token_urlsafe(32))
    cors_origins: tuple[str, ...] = tuple(
        o.strip() for o in _env("CORS_ORIGINS", "*").split(",") if o.strip()
    )

    jwt_ttl_hours: int = int(_env("JWT_TTL_HOURS", "12"))
    log_level: str = _env("LOG_LEVEL", "INFO")

    # Chain verification. bsc_rpc/eth_rpc accept a comma-separated list of
    # fallback endpoints (see ChainVerifier.verify_evm) -- these env values
    # are only the *default*, overridable per-chain from the admin panel
    # (settings keys bsc_rpc_urls/eth_rpc_urls) without a redeploy.
    tron_api: str = _env("TRON_API", "https://api.trongrid.io")
    tron_api_key: str = _env("TRONGRID_API_KEY", "")
    bsc_rpc: str = _env(
        "BSC_RPC",
        "https://bsc-dataseed.binance.org,https://bsc-rpc.publicnode.com,"
        "https://bsc-dataseed1.defibit.io",
    )
    eth_rpc: str = _env(
        "ETH_RPC",
        "https://ethereum-rpc.publicnode.com,https://rpc.flashbots.net,"
        "https://eth-mainnet.public.blastapi.io",
    )
    etherscan_key: str = _env("ETHERSCAN_API_KEY", "")

    # DeepL auto-translation of admin-authored free text (welcome message,
    # product/category name+description) into every enabled UI language
    # except Hindi (DeepL doesn't support it -- the existing manual _hi
    # columns stay authoritative for that one language). Optional: if unset,
    # translate_dynamic() fails open and every language just sees the
    # original text, same as before this feature existed.
    deepl_api_key: str = _env("DEEPL_API_KEY", "")

    min_confirmations: int = int(_env("MIN_CONFIRMATIONS", "12"))
    payment_tolerance: Decimal = Decimal(_env("PAYMENT_TOLERANCE", "0.02"))
    payment_ttl_minutes: int = int(_env("PAYMENT_TTL_MINUTES", "45"))
    verify_interval_seconds: int = int(_env("VERIFY_INTERVAL_SECONDS", "60"))

    rate_limit_per_minute: int = int(_env("RATE_LIMIT_PER_MINUTE", "30"))
    login_attempts_per_15min: int = int(_env("LOGIN_ATTEMPTS", "8"))


CFG = Config()

logging.basicConfig(
    level=getattr(logging, CFG.log_level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-7s | %(name)-18s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("hub")
logging.getLogger("aiogram.event").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)


# =============================================================================
#  MONEY
# =============================================================================
#  All money is Decimal. Two quantisation levels:
#    Q_FIAT  (2dp) -- INR and anything user-facing in fiat
#    Q_CRYPT (6dp) -- USDT, matching on-chain 6-decimal precision
#  Never let a float touch these values.

Q_FIAT = Decimal("0.01")
Q_CRYPTO = Decimal("0.000001")
ZERO = Decimal("0")


def money(value: Any, q: Decimal = Q_FIAT) -> Decimal:
    """Coerce anything into a safely-quantised Decimal. Never raises."""
    if value is None:
        return ZERO
    try:
        d = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return ZERO
    if d != d or d in (Decimal("Infinity"), Decimal("-Infinity")):
        return ZERO
    return d.quantize(q, rounding=ROUND_HALF_UP)


def crypto(value: Any) -> Decimal:
    return money(value, Q_CRYPTO)


def fmt_money(amount: Decimal, currency: str) -> str:
    a = money(amount, Q_CRYPTO if currency == "USDT" else Q_FIAT)
    if currency == "USDT":
        s = f"{a.normalize():f}"
        return f"{s} USDT"
    return f"₹{a:,.2f}"


def payment_method_label(method: str | None) -> str:
    """Human-readable payment method for receipts -- covers every method
    string this codebase actually produces (wallet, upi, usdt_<chain>,
    cryptomus, oxapay), with a title-cased fallback for anything new."""
    if not method:
        return "—"
    if method == "wallet":
        return "Wallet balance"
    if method == "upi":
        return "UPI"
    if method.startswith("usdt_"):
        return f"USDT ({method.split('_', 1)[1].upper()})"
    if method in ("cryptomus", "oxapay"):
        return method.capitalize()
    return method.replace("_", " ").title()


def receipt_block(order: asyncpg.Record) -> str:
    """Shared 'what/how much/when' footer for order-completion messages --
    both the instant key-delivery path and the manual-activation path build
    on the same order row shape (total, currency, payment_method,
    created_at), so this is the one place that formatting lives."""
    return (
        f"💰 Total: <b>{fmt_money(order['total'], order['currency'])}</b>\n"
        f"💳 Payment: {payment_method_label(order['payment_method'])}\n"
        f"📅 {order['created_at']:%d %b %Y, %H:%M} UTC"
    )


# =============================================================================
#  SCHEMA
# =============================================================================
#  Money columns are NUMERIC(18,6) throughout. Partial unique indexes give us
#  replay protection at the database level rather than in application code --
#  a duplicate txId cannot be inserted even under a race.

SCHEMA = """
CREATE TABLE IF NOT EXISTS admins (
    id              SERIAL PRIMARY KEY,
    username        TEXT NOT NULL UNIQUE,
    password_hash   TEXT NOT NULL,
    role            TEXT NOT NULL DEFAULT 'support',
    active          BOOLEAN NOT NULL DEFAULT TRUE,
    last_login_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS categories (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    name_hi     TEXT NOT NULL DEFAULT '',
    emoji       TEXT NOT NULL DEFAULT '📦',
    sort_order  INTEGER NOT NULL DEFAULT 0,
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    id              SERIAL PRIMARY KEY,
    category_id     INTEGER NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    name_hi         TEXT NOT NULL DEFAULT '',
    description     TEXT NOT NULL DEFAULT '',
    description_hi  TEXT NOT NULL DEFAULT '',
    price           NUMERIC(18,6) NOT NULL CHECK (price >= 0),
    original_price  NUMERIC(18,6),
    delivery_type   TEXT NOT NULL DEFAULT 'auto',
    image_url       TEXT,
    image_file_id   TEXT,
    video_file_id   TEXT,
    active          BOOLEAN NOT NULL DEFAULT TRUE,
    is_deal         BOOLEAN NOT NULL DEFAULT FALSE,
    deliver_as_file BOOLEAN NOT NULL DEFAULT FALSE,
    manual_stock    INTEGER,
    sold_count      INTEGER NOT NULL DEFAULT 0,
    max_per_user    INTEGER,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS products_cat_active_idx ON products(category_id, active);
CREATE INDEX IF NOT EXISTS products_active_deal_idx ON products(active, is_deal);
ALTER TABLE products ADD COLUMN IF NOT EXISTS video_file_id TEXT;
-- Manual-delivery products only: the prompt shown to a buyer after payment,
-- asking them to submit whatever this item needs (an email to invite, a
-- username to add, etc). Empty string -> a generic default prompt is used
-- instead (see MANUAL_INFO_DEFAULT_PROMPT), so this only needs setting when
-- a product wants custom wording.
ALTER TABLE products ADD COLUMN IF NOT EXISTS manual_prompt TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS product_keys (
    id          SERIAL PRIMARY KEY,
    product_id  INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    key_value   TEXT NOT NULL,
    is_used     BOOLEAN NOT NULL DEFAULT FALSE,
    order_id    INTEGER,
    used_at     TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS keys_product_unused_idx
    ON product_keys(product_id) WHERE is_used = FALSE;
CREATE UNIQUE INDEX IF NOT EXISTS keys_product_value_uniq
    ON product_keys(product_id, key_value);

-- Per-product "buy N, get X% (or flat ₹Y) off" tiers -- checked at
-- checkout against the quantity actually being bought, best (highest
-- qualifying min_qty) tier wins. Independent of the admin Bulk Discount
-- tool (which reprices the product's list price directly): this instead
-- discounts *this specific order* based on how many units it contains,
-- same product still shows its normal price to someone buying below the
-- lowest tier.
CREATE TABLE IF NOT EXISTS product_qty_discounts (
    id             SERIAL PRIMARY KEY,
    product_id     INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    min_qty        INTEGER NOT NULL CHECK (min_qty >= 2),
    discount_type  TEXT NOT NULL DEFAULT 'percent',
    value          NUMERIC(18,6) NOT NULL CHECK (value >= 0),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS product_qty_discounts_uniq
    ON product_qty_discounts(product_id, min_qty);
CREATE INDEX IF NOT EXISTS product_qty_discounts_prod_idx
    ON product_qty_discounts(product_id);

CREATE TABLE IF NOT EXISTS store_users (
    id                    SERIAL PRIMARY KEY,
    telegram_id           TEXT NOT NULL UNIQUE,
    username              TEXT,
    first_name            TEXT NOT NULL DEFAULT '',
    language              TEXT NOT NULL DEFAULT 'en',
    currency              TEXT,
    wallet_balance        NUMERIC(18,6) NOT NULL DEFAULT 0
                          CHECK (wallet_balance >= 0),
    total_spent           NUMERIC(18,6) NOT NULL DEFAULT 0,
    tier                  TEXT NOT NULL DEFAULT 'bronze',
    referral_code         TEXT NOT NULL UNIQUE,
    referred_by_id        INTEGER REFERENCES store_users(id) ON DELETE SET NULL,
    referral_bonus_given  BOOLEAN NOT NULL DEFAULT FALSE,
    is_banned             BOOLEAN NOT NULL DEFAULT FALSE,
    ban_reason            TEXT,
    last_seen_at          TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS users_referred_by_idx ON store_users(referred_by_id);
-- Tracks whether the payment-warning message has already been sent+pinned
-- for this user, so /start pins it once per chat instead of re-pinning
-- (and re-notifying) on every single /start.
ALTER TABLE store_users ADD COLUMN IF NOT EXISTS warning_pinned BOOLEAN NOT NULL DEFAULT FALSE;
-- The message_id of that pinned warning, so a later /start can check via
-- get_chat whether it's still actually pinned (Telegram never tells the bot
-- when a user clears their chat or unpins something -- this is the only way
-- to notice and re-pin it) -- see _maybe_send_payment_warning.
ALTER TABLE store_users ADD COLUMN IF NOT EXISTS warning_message_id BIGINT;

CREATE TABLE IF NOT EXISTS coupons (
    id                SERIAL PRIMARY KEY,
    code              TEXT NOT NULL UNIQUE,
    discount_type     TEXT NOT NULL DEFAULT 'percent',
    value             NUMERIC(18,6) NOT NULL CHECK (value >= 0),
    max_uses          INTEGER,
    used_count        INTEGER NOT NULL DEFAULT 0,
    min_order_amount  NUMERIC(18,6) NOT NULL DEFAULT 0,
    per_user_limit    INTEGER,
    expires_at        TIMESTAMPTZ,
    active            BOOLEAN NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    id                 SERIAL PRIMARY KEY,
    user_id            INTEGER NOT NULL REFERENCES store_users(id) ON DELETE CASCADE,
    product_id         INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    quantity           INTEGER NOT NULL DEFAULT 1 CHECK (quantity > 0),
    subtotal           NUMERIC(18,6) NOT NULL,
    discount           NUMERIC(18,6) NOT NULL DEFAULT 0,
    total              NUMERIC(18,6) NOT NULL CHECK (total >= 0),
    currency           TEXT NOT NULL DEFAULT 'INR',
    coupon_id          INTEGER REFERENCES coupons(id) ON DELETE SET NULL,
    coupon_code        TEXT,
    status             TEXT NOT NULL DEFAULT 'pending',
    delivery_type      TEXT NOT NULL DEFAULT 'auto',
    delivered_content  TEXT,
    payment_method     TEXT,
    delivered_at       TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS orders_user_idx ON orders(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS orders_status_idx ON orders(status);
-- Manual-delivery credential collection: what the buyer submitted (email,
-- username, whatever the product's manual_prompt asked for) and when.
-- NULL manual_info means either not manual delivery or not submitted yet.
ALTER TABLE orders ADD COLUMN IF NOT EXISTS manual_info TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS manual_info_at TIMESTAMPTZ;
-- Some manual_prompt asks are naturally a screenshot (e.g. "your game profile
-- ID") rather than typed text -- this is populated instead of, or alongside
-- (as a caption), manual_info.
ALTER TABLE orders ADD COLUMN IF NOT EXISTS manual_info_photo TEXT;
-- The one notification message this whole manual-delivery exchange lives
-- in (see MANUAL DELIVERY — CREDENTIAL COLLECTION below): "submit details"
-- button -> prompt -> received -> activated all edit this same message in
-- place instead of piling up separate ones, so it stays a single tidy
-- bubble in the buyer's chat from payment to activation.
ALTER TABLE orders ADD COLUMN IF NOT EXISTS manual_msg_chat_id BIGINT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS manual_msg_id BIGINT;

CREATE TABLE IF NOT EXISTS payments (
    id                  SERIAL PRIMARY KEY,
    order_id            INTEGER REFERENCES orders(id) ON DELETE SET NULL,
    user_id             INTEGER NOT NULL REFERENCES store_users(id) ON DELETE CASCADE,
    purpose             TEXT NOT NULL DEFAULT 'order',
    method              TEXT NOT NULL,
    amount              NUMERIC(18,6) NOT NULL CHECK (amount > 0),
    currency            TEXT NOT NULL DEFAULT 'INR',
    expected_amount     NUMERIC(18,6),
    expected_address    TEXT,
    chain               TEXT,
    tx_id               TEXT,
    screenshot_file_id  TEXT,
    status              TEXT NOT NULL DEFAULT 'pending',
    verification        TEXT NOT NULL DEFAULT 'manual',
    verify_attempts     INTEGER NOT NULL DEFAULT 0,
    verify_note         TEXT,
    confirmations       INTEGER,
    expires_at          TIMESTAMPTZ,
    reviewed_by         INTEGER REFERENCES admins(id) ON DELETE SET NULL,
    reviewed_at         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS payments_user_idx ON payments(user_id);
CREATE INDEX IF NOT EXISTS payments_status_idx ON payments(status);
CREATE INDEX IF NOT EXISTS payments_pending_verify_idx
    ON payments(status, verification) WHERE status = 'pending';
-- Replay protection: a txId can back at most one live payment.
CREATE UNIQUE INDEX IF NOT EXISTS payments_txid_uniq
    ON payments(lower(tx_id)) WHERE tx_id IS NOT NULL AND status <> 'failed';

-- Cryptomus gateway invoice identifier. Populated in the same INSERT as the
-- payment row (never added later) so a webhook can never arrive before the
-- row is lookup-able, and expires_at is never left NULL in between.
ALTER TABLE payments ADD COLUMN IF NOT EXISTS cryptomus_uuid TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS payments_cryptomus_uuid_uniq
    ON payments(cryptomus_uuid) WHERE cryptomus_uuid IS NOT NULL;

-- NOWPayments payment identifier, same shape as cryptomus_uuid above.
-- NOWPayments has been replaced by OxaPay (see oxapay_track_id below) --
-- this column is kept only so old rows created before the switch still
-- resolve; nothing new is ever written to it.
ALTER TABLE payments ADD COLUMN IF NOT EXISTS nowpayments_payment_id TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS payments_nowpayments_id_uniq
    ON payments(nowpayments_payment_id) WHERE nowpayments_payment_id IS NOT NULL;

-- OxaPay invoice identifier (hosted checkout track_id), same shape as
-- cryptomus_uuid above -- populated in the same INSERT as the payment row,
-- never added later.
ALTER TABLE payments ADD COLUMN IF NOT EXISTS oxapay_track_id TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS payments_oxapay_track_id_uniq
    ON payments(oxapay_track_id) WHERE oxapay_track_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS wallet_transactions (
    id           SERIAL PRIMARY KEY,
    user_id      INTEGER NOT NULL REFERENCES store_users(id) ON DELETE CASCADE,
    type         TEXT NOT NULL,
    amount       NUMERIC(18,6) NOT NULL,
    balance_after NUMERIC(18,6) NOT NULL,
    description  TEXT NOT NULL DEFAULT '',
    ref_type     TEXT,
    ref_id       INTEGER,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS wtx_user_idx ON wallet_transactions(user_id, created_at DESC);
-- One ledger row per (ref_type, ref_id, type): makes credit application idempotent.
CREATE UNIQUE INDEX IF NOT EXISTS wtx_ref_uniq
    ON wallet_transactions(ref_type, ref_id, type)
    WHERE ref_type IS NOT NULL AND ref_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS coupon_uses (
    id         SERIAL PRIMARY KEY,
    coupon_id  INTEGER NOT NULL REFERENCES coupons(id) ON DELETE CASCADE,
    user_id    INTEGER NOT NULL REFERENCES store_users(id) ON DELETE CASCADE,
    order_id   INTEGER REFERENCES orders(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS coupon_uses_idx ON coupon_uses(coupon_id, user_id);

CREATE TABLE IF NOT EXISTS restock_alerts (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES store_users(id) ON DELETE CASCADE,
    product_id  INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    notified    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS restock_uniq
    ON restock_alerts(user_id, product_id) WHERE notified = FALSE;

CREATE TABLE IF NOT EXISTS broadcasts (
    id           SERIAL PRIMARY KEY,
    message      TEXT NOT NULL,
    target       TEXT NOT NULL DEFAULT 'all',
    sent_count   INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL DEFAULT 'pending',
    created_by   INTEGER REFERENCES admins(id) ON DELETE SET NULL,
    product_id   INTEGER REFERENCES products(id) ON DELETE SET NULL,
    image_file_id TEXT,
    media_type   TEXT NOT NULL DEFAULT 'photo',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS product_id INTEGER REFERENCES products(id) ON DELETE SET NULL;
ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS image_file_id TEXT;
ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS media_type TEXT NOT NULL DEFAULT 'photo';

-- Admin-composed message pushed to an audience AND pinned in every
-- recipient's chat (replaces the old auto-pinned-per-user payment
-- warning) -- one row per "send & pin" action, `active` tracks whether it
-- (or any of it) is still pinned so the admin can unpin it later.
CREATE TABLE IF NOT EXISTS pinned_broadcasts (
    id           SERIAL PRIMARY KEY,
    message      TEXT NOT NULL,
    target       TEXT NOT NULL DEFAULT 'all',
    sent_count   INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL DEFAULT 'pending',
    active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_by   INTEGER REFERENCES admins(id) ON DELETE SET NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One row per chat a pinned_broadcasts row actually reached & pinned in --
-- this is what makes "unpin for everyone" possible (Telegram never lets a
-- bot look up "which messages have I pinned across all my chats").
CREATE TABLE IF NOT EXISTS pinned_broadcast_messages (
    id           SERIAL PRIMARY KEY,
    broadcast_id INTEGER NOT NULL REFERENCES pinned_broadcasts(id) ON DELETE CASCADE,
    chat_id      BIGINT NOT NULL,
    message_id   BIGINT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS pinned_broadcast_messages_bid_idx
    ON pinned_broadcast_messages(broadcast_id);

CREATE TABLE IF NOT EXISTS tickets (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES store_users(id) ON DELETE CASCADE,
    subject     TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'open',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS tickets_user_idx ON tickets(user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS tickets_status_idx ON tickets(status, updated_at DESC);

CREATE TABLE IF NOT EXISTS ticket_messages (
    id          SERIAL PRIMARY KEY,
    ticket_id   INTEGER NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
    sender      TEXT NOT NULL, -- 'user' | 'admin'
    admin_id    INTEGER REFERENCES admins(id) ON DELETE SET NULL,
    message     TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ticket_messages_ticket_idx ON ticket_messages(ticket_id, created_at);
-- A ticket message can be a photo instead of (or with a caption as) text --
-- `message` stays NOT NULL (empty string for a caption-less photo) so this
-- is purely additive.
ALTER TABLE ticket_messages ADD COLUMN IF NOT EXISTS photo_file_id TEXT;

CREATE TABLE IF NOT EXISTS audit_logs (
    id          SERIAL PRIMARY KEY,
    admin_id    INTEGER REFERENCES admins(id) ON DELETE SET NULL,
    admin_name  TEXT NOT NULL DEFAULT 'system',
    action      TEXT NOT NULL,
    entity      TEXT,
    entity_id   INTEGER,
    detail      JSONB,
    ip          TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS audit_created_idx ON audit_logs(created_at DESC);

CREATE TABLE IF NOT EXISTS settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Global Emoji Replacement Manager: one row per unicode emoji that should
-- render as a Telegram Premium custom emoji everywhere it appears (message
-- text, captions, button labels/icons). No per-slot or per-button rows — a
-- single dictionary consumed by one central renderer (see render_emoji_text/
-- render_emoji_buttons). If a unicode emoji has no row here, the plain
-- unicode glyph is used as-is.
CREATE TABLE IF NOT EXISTS emoji_map (
    unicode_emoji    TEXT PRIMARY KEY,
    premium_emoji_id TEXT NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Rolling log of messages the bot has sent per chat, so "Clear Chat" can
-- bulk-delete them. Populated centrally by EmojiRenderMiddleware (the one
-- choke point every outgoing send/edit already passes through) rather than
-- at each of the ~50 individual send call sites. Pruned periodically since
-- Telegram itself refuses to delete anything older than 48h anyway.
CREATE TABLE IF NOT EXISTS bot_messages (
    id         BIGSERIAL PRIMARY KEY,
    chat_id    BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    kind       TEXT NOT NULL DEFAULT 'nav',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE bot_messages ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'nav';
CREATE INDEX IF NOT EXISTS bot_messages_chat_idx ON bot_messages(chat_id, created_at);

-- The single canonical "active navigation message" per chat. Every screen
-- render (button tap, typed /command, or the persistent menu button alike)
-- targets this one tracked message: edited via editMessageText/
-- editMessageMedia whenever Telegram allows the transition, and only
-- replaced (old one deleted, new one becomes the tracked pointer) when it
-- genuinely can't be edited. See render_screen().
CREATE TABLE IF NOT EXISTS nav_messages (
    chat_id    BIGINT PRIMARY KEY,
    message_id BIGINT NOT NULL,
    is_photo   BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Content-addressed translation cache: one row per (source text, target
-- language). Reused for every admin free-text field (welcome message,
-- store header/footer, product/category name+description) without needing
-- a dedicated column per language -- an admin edit changes the source text,
-- which changes the hash, which just naturally produces a fresh translation
-- on next render; the old row is simply never looked up again.
CREATE TABLE IF NOT EXISTS text_translations (
    text_hash       TEXT NOT NULL,
    target_lang     TEXT NOT NULL,
    source_text     TEXT NOT NULL,
    translated_text TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (text_hash, target_lang)
);
"""

DEFAULT_SETTINGS: dict[str, str] = {
    "store_name": "Digital Hub",
    "welcome_text": "Welcome to Digital Hub — instant digital delivery.",
    # Sent once per user on their first /start, then pinned (see
    # store_users.warning_pinned) — a short trust/capability notice, not
    # a second welcome message, so it stays short by design.
    "payment_warning_text": (
        "🛍️ <b>Digital Hub</b>\n"
        "We support both <b>manual</b> and <b>instant automatic delivery</b>, "
        "backed by a fully <b>automated payment system</b> for fast, hassle-free checkout.\n\n"
        "🔒 <b>100% Secure</b> — every order and payment is verified.\n"
        "💬 <b>Customer Support</b> — our team is here whenever you need help.\n"
        "✅ Professional, reliable service you can trust."
    ),
    "support_username": "",
    "usdt_rate": "90.00",
    # Extra % added on top of every crypto payment (manual chain address,
    # Cryptomus, OxaPay) -- covers the store's real network/
    # consolidation cost and the risk of a buyer sending to the wrong
    # network/asset or after the payment window expires, both of which are
    # unrecoverable losses checkout used to charge nothing extra for.
    # TRC20 gets its own (higher) rate since its real-world cost to this
    # store runs above the other chains. See crypto_safety_fee_percent().
    "crypto_safety_fee_percent": "0.3",
    "crypto_safety_fee_trc20_percent": "1",
    "referral_bonus_percent": "5",
    "min_topup_inr": "50",
    "min_topup_usdt": "1",
    "usdt_trc20_address": "",
    "usdt_bep20_address": "",
    "usdt_erc20_address": "",
    "upi_id": "",
    "upi_qr_file_id": "",
    "binance_pay_id": "",
    "maintenance_mode": "0",
    "auto_verify_enabled": "1",
    # Mandatory "join our channel" gate — blocks the menu until verified.
    "force_join_enabled": "0",
    "force_join_channel_url": "",
    "force_join_channel_id": "",
    # Payment tuning — editable from the in-bot admin (fall back to env defaults).
    "min_confirmations": _env("MIN_CONFIRMATIONS", "12"),
    # Per-chain overrides -- empty means "use min_confirmations above" (see
    # min_confirmations() helper). Block times differ a lot by chain
    # (TRON/BSC ~3s vs Ethereum ~12s), so these let an admin speed up the
    # fast chains without touching Ethereum's confirmation depth.
    "min_confirmations_trc20": "",
    "min_confirmations_bep20": "",
    "min_confirmations_erc20": "",
    "payment_tolerance": _env("PAYMENT_TOLERANCE", "0.02"),
    "payment_ttl_minutes": _env("PAYMENT_TTL_MINUTES", "45"),
    # How often the background worker re-checks pending payments -- purely
    # how promptly an already-satisfied payment gets *noticed*, no bearing
    # on how many confirmations are required, so lowering it is a free
    # latency win. Lowered from the old 60s default (see verify_interval_seconds()).
    "verify_interval_seconds": "20",
    # On-chain RPC endpoints — comma-separated fallback list, editable from
    # the admin panel so a dead/rate-limited public endpoint can be swapped
    # without a redeploy. Seeded from the env-configured default.
    "bsc_rpc_urls": CFG.bsc_rpc,
    "eth_rpc_urls": CFG.eth_rpc,
    "tron_api_url": CFG.tron_api,
    "tron_api_key": CFG.tron_api_key,
    # UI / branding — all editable from the in-bot UI editor.
    "menu_order": "shop,wallet,profile,support,orders,referral,api_link,language",
    "store_header": "",
    "store_footer": "",
    "store_logo_file_id": "",
    "shop_banner_file_id": "",
    "channel_url": "",
    "terms_url": "",
    "api_link_url": "",
    "rank_bronze_name": "Bronze",
    "rank_silver_name": "Silver",
    "rank_gold_name": "Gold",
    "default_language": "en",
    "default_currency": "USDT",
    "languages_enabled": "en,hi",
    "currencies_enabled": "INR,USDT",
    "tier_silver_at": "5000",
    "tier_gold_at": "25000",
    "tier_silver_discount": "2",
    "tier_gold_discount": "5",
    # Cryptomus gateway -- purely settings-table driven (no env fallback) so
    # an admin can enable/configure it without a redeploy. Credentials are
    # seeded once via direct DB update at setup time, never hardcoded here.
    "cryptomus_enabled": "0",
    "manual_crypto_enabled": "1",
    "cryptomus_merchant_id": "",
    "cryptomus_api_key": "",
    "cryptomus_network_trc20": "TRON",
    "cryptomus_network_bep20": "BSC",
    "cryptomus_network_erc20": "ETH",
    "public_base_url": "https://54-255-42-56.sslip.io",
    "cryptomus_accept_overpayment": "1",
    # OxaPay gateway -- replaces NOWPayments as the hosted-checkout crypto
    # rail. Purely settings-table driven (no env fallback), same convention
    # as Cryptomus above. There's exactly one button in the bot ("Pay with
    # Crypto") -- OxaPay's own payment page is what lets the customer pick
    # a network/currency, not us, so there's no per-currency list to curate
    # here. Confirmed via inbound webhook (HMAC-SHA512 verified) with a live
    # get_status re-check before crediting -- see oxapay_webhook below.
    # Credentials seeded once via direct DB update at setup time, never
    # hardcoded here.
    "oxapay_enabled": "0",
    "oxapay_merchant_api_key": "",
}

# =============================================================================
#  DATABASE
# =============================================================================

async def _migrate_emoji_system(conn: "asyncpg.Connection") -> None:
    """One-time, idempotent fold of the legacy slot-based premium/button
    emoji system (premium_emojis + emoji_assignments + the unused
    button_emojis table) into the single global emoji_map dictionary, then
    drop the old tables. No-op once they're gone."""
    if not await conn.fetchval("SELECT to_regclass('public.premium_emojis') IS NOT NULL"):
        return
    await conn.execute(
        """
        INSERT INTO emoji_map (unicode_emoji, premium_emoji_id)
        SELECT DISTINCT ON (fallback) fallback, custom_emoji_id
          FROM premium_emojis
         WHERE fallback IS NOT NULL AND fallback <> ''
         ORDER BY fallback, id
        ON CONFLICT (unicode_emoji) DO NOTHING
        """
    )
    await conn.execute("DROP TABLE IF EXISTS emoji_assignments")
    await conn.execute("DROP TABLE IF EXISTS button_emojis")
    await conn.execute("DROP TABLE IF EXISTS premium_emojis")
    log.info("migrated legacy premium-emoji slot system into emoji_map")


async def _seed_nav_messages(conn: "asyncpg.Connection") -> None:
    """One-time seed of nav_messages from each chat's most recent tracked
    'nav' message in bot_messages, so existing chats don't pay an extra
    throwaway message on their first render after this table is introduced.
    Guessed as text (is_photo=FALSE); if that guess is wrong for a given
    chat, render_screen's normal edit-failure fallback (delete + send
    fresh) silently self-corrects it on that one render. Runs every
    startup but is a no-op once a chat has a row (ON CONFLICT DO NOTHING)."""
    await conn.execute(
        """
        INSERT INTO nav_messages (chat_id, message_id, is_photo, updated_at)
        SELECT DISTINCT ON (chat_id) chat_id, message_id, FALSE, created_at
          FROM bot_messages
         WHERE kind = 'nav'
         ORDER BY chat_id, created_at DESC
        ON CONFLICT (chat_id) DO NOTHING
        """
    )


class DB:
    """Thin asyncpg wrapper. Decimals stay Decimals; JSONB is auto-coded."""

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        async def init(conn: asyncpg.Connection) -> None:
            await conn.set_type_codec(
                "jsonb",
                encoder=json.dumps,
                decoder=json.loads,
                schema="pg_catalog",
            )

        self.pool = await asyncpg.create_pool(
            self.dsn,
            # Bumped from 2/16 -- at real traffic, every message/callback
            # acquires a connection for a handful of queries, and 16 in
            # flight starts queuing well before this box's actual limits
            # (Postgres here allows 100; this still leaves headroom for the
            # admin panel and manual psql sessions on top).
            min_size=5,
            max_size=40,
            command_timeout=30,
            init=init,
        )
        async with self.pool.acquire() as conn:
            await conn.execute(SCHEMA)
            await _migrate_emoji_system(conn)
            await _seed_nav_messages(conn)
        log.info("database connected, schema ensured")

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()

    async def fetch(self, sql: str, *args) -> list[asyncpg.Record]:
        async with self.pool.acquire() as c:
            return await c.fetch(sql, *args)

    async def fetchrow(self, sql: str, *args) -> asyncpg.Record | None:
        async with self.pool.acquire() as c:
            return await c.fetchrow(sql, *args)

    async def fetchval(self, sql: str, *args) -> Any:
        async with self.pool.acquire() as c:
            return await c.fetchval(sql, *args)

    async def execute(self, sql: str, *args) -> str:
        async with self.pool.acquire() as c:
            return await c.execute(sql, *args)

    @asynccontextmanager
    async def tx(self):
        """Transaction scope. Everything money-related runs inside one."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                yield conn


db = DB(CFG.database_url)


# =============================================================================
#  SETTINGS  (write-through cache)
# =============================================================================

class Settings:
    def __init__(self) -> None:
        self._cache: dict[str, str] = {}
        self._loaded_at = 0.0
        self._ttl = 60.0
        self._lock = asyncio.Lock()

    async def load(self, force: bool = False) -> dict[str, str]:
        if not force and self._cache and (time.monotonic() - self._loaded_at) < self._ttl:
            return self._cache
        async with self._lock:
            rows = await db.fetch("SELECT key, value FROM settings")
            merged = dict(DEFAULT_SETTINGS)
            merged.update({r["key"]: r["value"] for r in rows})
            self._cache = merged
            self._loaded_at = time.monotonic()
            return self._cache

    async def get(self, key: str, default: str = "") -> str:
        return (await self.load()).get(key, default)

    def ui_override(self, lang: str, key: str) -> str | None:
        """Synchronous read of a UI-editor override from the warm cache.
        Returns None if no override is set. Safe to call from sync t()."""
        val = self._cache.get(f"ui:{lang}:{key}")
        if val is None and lang != "en":
            val = self._cache.get(f"ui:en:{key}")
        return val

    async def get_decimal(self, key: str, default: str = "0") -> Decimal:
        return money(await self.get(key, default), Q_CRYPTO)

    async def get_int(self, key: str, default: int = 0) -> int:
        try:
            return int(float(await self.get(key, str(default))))
        except (ValueError, TypeError):
            return default

    async def get_bool(self, key: str, default: bool = False) -> bool:
        return (await self.get(key, "1" if default else "0")).strip() in ("1", "true", "yes", "on")

    async def set(self, key: str, value: str) -> None:
        await db.execute(
            """INSERT INTO settings(key, value, updated_at) VALUES($1,$2,NOW())
               ON CONFLICT (key) DO UPDATE SET value=$2, updated_at=NOW()""",
            key, str(value),
        )
        await self.load(force=True)

    async def seed(self) -> None:
        async with db.tx() as conn:
            for k, v in DEFAULT_SETTINGS.items():
                await conn.execute(
                    "INSERT INTO settings(key,value) VALUES($1,$2) ON CONFLICT (key) DO NOTHING",
                    k, v,
                )
        await self.load(force=True)


settings = Settings()


# =============================================================================
#  CURRENCY CONVERSION
# =============================================================================
#  Prices are stored in INR. USDT display is derived at read time from the
#  usdt_rate setting so a rate change never rewrites historical rows.

async def usdt_rate() -> Decimal:
    rate = await settings.get_decimal("usdt_rate", "90")
    return rate if rate > 0 else Decimal("90")


async def min_confirmations(chain: str | None = None) -> int:
    """Live-editable confirmation depth; falls back to the env default.

    Per-chain override (settings key `min_confirmations_trc20` / `_bep20` /
    `_erc20`) lets an admin tune each chain independently -- block times
    differ a lot (TRON/BSC ~3s vs Ethereum ~12s), so one global count
    either makes the fast chains wait far longer than their own finality
    needs, or would have to be lowered everywhere just to speed ETH up,
    weakening it. Unset per-chain settings fall back to the same global
    value used before this existed, so this is a zero-behavior-change
    default until an admin actually sets one."""
    default = await settings.get_int("min_confirmations", CFG.min_confirmations)
    if not chain:
        return default
    return await settings.get_int(f"min_confirmations_{chain.lower()}", default)


async def payment_tolerance() -> Decimal:
    return await settings.get_decimal("payment_tolerance", str(CFG.payment_tolerance))


async def verify_interval_seconds() -> int:
    """Live-editable poll interval for the chain-verification worker. This
    is a free latency win to lower -- it only controls how promptly an
    already-satisfied payment gets noticed, not how many confirmations are
    required, so it carries no security trade-off (just a bit more RPC/DB
    traffic the tighter it gets)."""
    return await settings.get_int("verify_interval_seconds", CFG.verify_interval_seconds)


async def payment_ttl_minutes() -> int:
    return await settings.get_int("payment_ttl_minutes", CFG.payment_ttl_minutes)


async def crypto_safety_fee_percent(chain: str | None) -> Decimal:
    """Extra % on top of the raw INR->USDT conversion, added at every
    crypto payment creation site (manual chain address, Cryptomus, OxaPay)
    -- see crypto_safety_fee_percent's DEFAULT_SETTINGS entry for why this
    exists. TRC20 (pass "TRC20") uses the higher override; every other
    chain -- including OxaPay, which is chain-agnostic on our side and
    always passes None -- uses the baseline rate. Both are admin-editable
    under Payment Methods."""
    if (chain or "").upper() == "TRC20":
        return await settings.get_decimal("crypto_safety_fee_trc20_percent", "1")
    return await settings.get_decimal("crypto_safety_fee_percent", "0.3")


def apply_safety_fee(usdt_amount: Decimal, fee_percent: Decimal) -> Decimal:
    return crypto(usdt_amount * (1 + fee_percent / 100))


async def to_currency(inr: Decimal, currency: str) -> Decimal:
    if currency != "USDT":
        return money(inr, Q_FIAT)
    return crypto(money(inr, Q_CRYPTO) / await usdt_rate())


async def from_currency(amount: Decimal, currency: str) -> Decimal:
    """Convert a user-facing amount back into canonical INR."""
    if currency != "USDT":
        return money(amount, Q_FIAT)
    return money(money(amount, Q_CRYPTO) * await usdt_rate(), Q_FIAT)


async def display_price(inr: Decimal, currency: str) -> str:
    return fmt_money(await to_currency(inr, currency), currency)


# =============================================================================
#  WALLET LEDGER
# =============================================================================
#  Every balance change goes through here. Two invariants:
#    - the row is locked FOR UPDATE before reading the balance
#    - a ledger row is always written alongside the balance update
#  Debits are refused rather than allowed to go negative; the CHECK constraint
#  on wallet_balance is the backstop if this is ever bypassed.

class InsufficientFunds(Exception):
    pass


async def wallet_apply(
    conn: asyncpg.Connection,
    user_id: int,
    amount: Decimal,          # signed: positive credit, negative debit
    tx_type: str,
    description: str = "",
    ref_type: str | None = None,
    ref_id: int | None = None,
    *,
    idempotent: bool = False,
) -> Decimal:
    """Apply a balance change inside an existing transaction. Returns new balance."""
    amount = crypto(amount)
    if amount == ZERO:
        return crypto(await conn.fetchval(
            "SELECT wallet_balance FROM store_users WHERE id=$1", user_id))

    if idempotent and ref_type and ref_id is not None:
        dup = await conn.fetchval(
            """SELECT 1 FROM wallet_transactions
               WHERE ref_type=$1 AND ref_id=$2 AND type=$3""",
            ref_type, ref_id, tx_type,
        )
        if dup:
            log.info("wallet_apply skipped duplicate %s/%s/%s", ref_type, ref_id, tx_type)
            return crypto(await conn.fetchval(
                "SELECT wallet_balance FROM store_users WHERE id=$1", user_id))

    current = await conn.fetchval(
        "SELECT wallet_balance FROM store_users WHERE id=$1 FOR UPDATE", user_id
    )
    if current is None:
        raise ValueError(f"user {user_id} not found")

    balance = crypto(current)
    new_balance = crypto(balance + amount)
    if new_balance < ZERO:
        raise InsufficientFunds(
            f"balance {balance} cannot absorb {amount}"
        )

    await conn.execute(
        "UPDATE store_users SET wallet_balance=$1 WHERE id=$2", new_balance, user_id
    )
    await conn.execute(
        """INSERT INTO wallet_transactions
             (user_id, type, amount, balance_after, description, ref_type, ref_id)
           VALUES($1,$2,$3,$4,$5,$6,$7)
           ON CONFLICT DO NOTHING""",
        user_id, tx_type, amount, new_balance, description, ref_type, ref_id,
    )
    return new_balance


async def wallet_audit() -> list[dict]:
    """Re-derive every balance from the ledger and report drift."""
    rows = await db.fetch(
        """SELECT u.id, u.telegram_id, u.username, u.wallet_balance AS stored,
                  COALESCE(SUM(w.amount), 0) AS derived
             FROM store_users u
             LEFT JOIN wallet_transactions w ON w.user_id = u.id
            GROUP BY u.id
           HAVING u.wallet_balance <> COALESCE(SUM(w.amount), 0)"""
    )
    return [
        {
            "userId": r["id"],
            "telegramId": r["telegram_id"],
            "username": r["username"],
            "stored": str(crypto(r["stored"])),
            "derived": str(crypto(r["derived"])),
            "drift": str(crypto(r["stored"] - r["derived"])),
        }
        for r in rows
    ]


# =============================================================================
#  TIERS
# =============================================================================
#  Spend-based loyalty discount, recomputed after each delivered order.

async def tier_for_spend(total_spent: Decimal) -> str:
    gold = await settings.get_decimal("tier_gold_at", "25000")
    silver = await settings.get_decimal("tier_silver_at", "5000")
    if total_spent >= gold:
        return "gold"
    if total_spent >= silver:
        return "silver"
    return "bronze"


async def tier_discount_percent(tier: str) -> Decimal:
    if tier == "gold":
        return await settings.get_decimal("tier_gold_discount", "5")
    if tier == "silver":
        return await settings.get_decimal("tier_silver_discount", "2")
    return ZERO


async def refresh_tier(conn: asyncpg.Connection, user_id: int) -> str:
    spent = crypto(await conn.fetchval(
        "SELECT total_spent FROM store_users WHERE id=$1", user_id) or 0)
    tier = await tier_for_spend(spent)
    await conn.execute("UPDATE store_users SET tier=$1 WHERE id=$2", tier, user_id)
    return tier


# =============================================================================
#  USERS
# =============================================================================

def make_referral_code() -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(8))


async def get_or_create_user(
    telegram_id: int | str,
    username: str | None = None,
    first_name: str = "",
    referral_code: str | None = None,
) -> asyncpg.Record:
    tid = str(telegram_id)
    # This runs on every single message/callback in the bot (via guard()), so
    # it's the hottest query path there is. The old version did a SELECT to
    # check for an existing user, then a separate UPDATE, then a second
    # SELECT to return the fresh row — three round trips for what should be
    # one. UPDATE ... RETURNING does the lookup, the touch, and the refetch
    # in a single round trip; CASE keeps the old "never blank out a stored
    # first_name" safety net without needing the row in hand to check it.
    # username gets the identical guard -- a caller that doesn't have a
    # username to report (None or '') must never null out one already on
    # file. This matters more now than it used to: a Telegram chat Message
    # almost always carries from_user.username when set, but a Mini App's
    # initData.user can plausibly omit it under some client/privacy
    # configurations, and that path calls this same function.
    row = await db.fetchrow(
        """UPDATE store_users SET
             username = CASE WHEN $1::text IS NULL OR $1::text = '' THEN username ELSE $1::text END,
             first_name = CASE WHEN $2 = '' THEN first_name ELSE $2 END,
             last_seen_at = NOW()
           WHERE telegram_id=$3
           RETURNING *""",
        username, first_name, tid,
    )
    if row:
        return row

    referrer_id = None
    if referral_code:
        ref = await db.fetchrow(
            "SELECT id FROM store_users WHERE referral_code=$1", referral_code.upper())
        if ref:
            referrer_id = ref["id"]

    for _ in range(6):
        code = make_referral_code()
        try:
            return await db.fetchrow(
                """INSERT INTO store_users
                     (telegram_id, username, first_name, referral_code,
                      referred_by_id, last_seen_at)
                   VALUES($1,$2,$3,$4,$5,NOW())
                   RETURNING *""",
                tid, username, first_name, code, referrer_id,
            )
        except asyncpg.UniqueViolationError:
            continue
    raise RuntimeError("could not allocate a unique referral code")


async def user_currency(user: asyncpg.Record) -> str:
    return user["currency"] or "USDT"

# =============================================================================
#  ON-CHAIN VERIFICATION
# =============================================================================
#  Replaces "admin squints at a screenshot" with an actual chain query.
#
#  A payment is auto-approved only when ALL of these hold:
#     1. the transaction exists and executed successfully
#     2. the token contract is the real USDT contract for that chain
#     3. the recipient is OUR configured address
#     4. the amount is >= expected, within tolerance
#     5. confirmations >= MIN_CONFIRMATIONS
#     6. the txId is not already used (enforced by a DB unique index)
#
#  Anything short of that stays pending for a human. Failing closed matters
#  more here than convenience -- a false positive gives away stock for free.

USDT_CONTRACTS = {
    "TRC20": "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
    "BEP20": "0x55d398326f99059ff775485246999027b3197955",
    "ERC20": "0xdac17f958d2ee523a2206206994597c13d831ec7",
}
USDT_DECIMALS = {"TRC20": 6, "BEP20": 18, "ERC20": 6}

# keccak256("Transfer(address,address,uint256)")
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


@dataclass
class ChainResult:
    ok: bool
    reason: str
    amount: Decimal = ZERO
    to_address: str = ""
    confirmations: int = 0
    raw: dict = field(default_factory=dict)


class ChainVerifier:
    """Queries public chain APIs. Read-only -- never holds a private key."""

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(20.0),
                headers={"User-Agent": "digital-hub-bot/2.0"},
                follow_redirects=True,
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # ---- TRON ---------------------------------------------------------------

    async def verify_tron(self, tx_id: str, expect_to: str, expect_amount: Decimal,
                          not_before: datetime | None = None,
                          enforce_amount: bool = True) -> ChainResult:
        c = await self.client()
        api = (await settings.get("tron_api_url", CFG.tron_api)).strip() or CFG.tron_api
        api_key = (await settings.get("tron_api_key", CFG.tron_api_key)).strip()
        headers = {"TRON-PRO-API-KEY": api_key} if api_key else {}
        try:
            r = await c.post(
                f"{api}/wallet/gettransactionbyid",
                json={"value": tx_id},
                headers=headers,
            )
            tx = r.json() if r.status_code == 200 else {}
        except (httpx.HTTPError, json.JSONDecodeError) as e:
            return ChainResult(False, f"tron rpc error: {e}")

        if not tx or "raw_data" not in tx:
            return ChainResult(False, "transaction not found on TRON")

        ret = (tx.get("ret") or [{}])[0].get("contractRet")
        if ret != "SUCCESS":
            return ChainResult(False, f"transaction failed on chain: {ret}")

        try:
            r2 = await c.post(
                f"{api}/wallet/gettransactioninfobyid",
                json={"value": tx_id},
                headers=headers,
            )
            info = r2.json() if r2.status_code == 200 else {}
        except (httpx.HTTPError, json.JSONDecodeError) as e:
            return ChainResult(False, f"tron info error: {e}")

        logs = info.get("log") or []
        if not logs:
            return ChainResult(False, "no token transfer in transaction")

        contract_hex = self._tron_b58_to_hex(USDT_CONTRACTS["TRC20"])
        for entry in logs:
            addr = "41" + (entry.get("address") or "").lower()
            if addr.lower() != contract_hex.lower():
                continue
            topics = entry.get("topics") or []
            if not topics or topics[0].lower() != TRANSFER_TOPIC[2:].lower():
                continue
            if len(topics) < 3:
                continue
            to_hex = "41" + topics[2][-40:]
            to_b58 = self._tron_hex_to_b58(to_hex)
            try:
                raw_amount = int(entry.get("data") or "0", 16)
            except ValueError:
                continue
            amount = crypto(Decimal(raw_amount) / (10 ** USDT_DECIMALS["TRC20"]))

            if to_b58.lower() != expect_to.lower():
                continue

            block = info.get("blockNumber") or 0
            confs = await self._tron_confirmations(block)
            if enforce_amount and amount + await payment_tolerance() < expect_amount:
                return ChainResult(False, f"amount too low: got {amount}, expected {expect_amount}",
                                   amount, to_b58, confs)
            if confs < await min_confirmations("TRC20"):
                return ChainResult(False, f"only {confs} confirmations",
                                   amount, to_b58, confs)
            if not_before is not None:
                ts_ms = info.get("blockTimeStamp")
                if not ts_ms:
                    # Fail closed: can't rule out a replayed old transaction
                    # without knowing when it was actually mined.
                    return ChainResult(False, "could not confirm transaction time",
                                       amount, to_b58, confs)
                tx_time = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
                if tx_time < not_before - timedelta(minutes=5):
                    return ChainResult(
                        False,
                        f"transaction predates this payment request "
                        f"(mined {tx_time.isoformat()}, requested {not_before.isoformat()}) "
                        f"-- looks like an old/unrelated transaction being replayed",
                        amount, to_b58, confs)
            return ChainResult(True, "verified", amount, to_b58, confs, info)

        return ChainResult(False, "no matching USDT transfer to our address")

    async def _tron_confirmations(self, block: int) -> int:
        if not block:
            return 0
        try:
            c = await self.client()
            api = (await settings.get("tron_api_url", CFG.tron_api)).strip() or CFG.tron_api
            r = await c.post(f"{api}/wallet/getnowblock", json={})
            head = r.json().get("block_header", {}).get("raw_data", {}).get("number", 0)
            return max(0, int(head) - int(block))
        except (httpx.HTTPError, json.JSONDecodeError, ValueError, KeyError):
            return 0

    @staticmethod
    def _b58_alphabet() -> str:
        return "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

    @classmethod
    def _tron_b58_to_hex(cls, addr: str) -> str:
        alpha = cls._b58_alphabet()
        num = 0
        for ch in addr:
            idx = alpha.find(ch)
            if idx < 0:
                return ""
            num = num * 58 + idx
        raw = num.to_bytes(25, "big")
        return raw[:21].hex()

    @classmethod
    def _tron_hex_to_b58(cls, hex_addr: str) -> str:
        try:
            raw = bytes.fromhex(hex_addr)
        except ValueError:
            return ""
        checksum = hashlib.sha256(hashlib.sha256(raw).digest()).digest()[:4]
        full = raw + checksum
        num = int.from_bytes(full, "big")
        alpha = cls._b58_alphabet()
        out = ""
        while num > 0:
            num, rem = divmod(num, 58)
            out = alpha[rem] + out
        for b in full:
            if b == 0:
                out = "1" + out
            else:
                break
        return out

    # ---- EVM (BSC / ETH) ----------------------------------------------------

    async def verify_evm(
        self, chain: str, tx_id: str, expect_to: str, expect_amount: Decimal,
        not_before: datetime | None = None, enforce_amount: bool = True,
    ) -> ChainResult:
        # Admin-configurable (settings table) with the env-configured value as
        # the fallback default, same pattern as usdt_*_address -- lets an
        # admin swap a dead/rate-limited public RPC without a redeploy.
        # Comma-separated so a single bad/slow endpoint doesn't permanently
        # block verification: each call tries them in order.
        setting_key = "bsc_rpc_urls" if chain == "BEP20" else "eth_rpc_urls"
        default_urls = CFG.bsc_rpc if chain == "BEP20" else CFG.eth_rpc
        rpc_urls = [u.strip() for u in (await settings.get(setting_key, default_urls)).split(",") if u.strip()]
        if not rpc_urls:
            return ChainResult(False, f"no RPC endpoint configured for {chain}")

        c = await self.client()

        async def rpc_call(method: str, params: list) -> Any:
            last_err: Exception = RuntimeError("no RPC endpoints configured")
            for url in rpc_urls:
                try:
                    r = await c.post(
                        url,
                        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                    )
                    body = r.json()
                    if "result" in body:
                        return body["result"]
                    last_err = RuntimeError(str(body.get("error") or "RPC returned no result"))
                except (httpx.HTTPError, json.JSONDecodeError) as e:
                    last_err = e
            raise last_err

        try:
            receipt = await rpc_call("eth_getTransactionReceipt", [tx_id])
        except Exception as e:
            return ChainResult(False, f"{chain} rpc error: {e}")

        if not receipt:
            return ChainResult(False, "transaction not found")
        if str(receipt.get("status", "0x0")).lower() not in ("0x1", "1"):
            return ChainResult(False, "transaction reverted on chain")

        contract = USDT_CONTRACTS[chain].lower()
        decimals = USDT_DECIMALS[chain]
        expect_to_norm = expect_to.lower()

        for entry in receipt.get("logs", []):
            if (entry.get("address") or "").lower() != contract:
                continue
            topics = entry.get("topics") or []
            if len(topics) < 3 or topics[0].lower() != TRANSFER_TOPIC:
                continue
            to_addr = "0x" + topics[2][-40:]
            if to_addr.lower() != expect_to_norm:
                continue
            try:
                raw_amount = int(entry.get("data", "0x0"), 16)
            except ValueError:
                continue
            amount = crypto(Decimal(raw_amount) / (10 ** decimals))

            try:
                head_hex = await rpc_call("eth_blockNumber", [])
                head = int(head_hex, 16)
                blk = int(receipt.get("blockNumber", "0x0"), 16)
                confs = max(0, head - blk)
            except (ValueError, TypeError, httpx.HTTPError, RuntimeError):
                confs = 0

            if enforce_amount and amount + await payment_tolerance() < expect_amount:
                return ChainResult(False, f"amount too low: got {amount}, expected {expect_amount}",
                                   amount, to_addr, confs)
            if confs < await min_confirmations(chain):
                return ChainResult(False, f"only {confs} confirmations",
                                   amount, to_addr, confs)
            if not_before is not None:
                try:
                    block_data = await rpc_call("eth_getBlockByNumber", [receipt.get("blockNumber", "0x0"), False])
                    tx_time = datetime.fromtimestamp(int(block_data["timestamp"], 16), tz=timezone.utc)
                except Exception as e:
                    # Fail closed: if we can't determine when this transaction
                    # was actually mined, we can't rule out a replayed old
                    # transaction, so don't approve on this attempt -- retried
                    # automatically on the next poll instead of risking a
                    # false positive on ambiguous data.
                    return ChainResult(False, f"could not confirm transaction time: {e}",
                                       amount, to_addr, confs)
                if tx_time < not_before - timedelta(minutes=5):
                    return ChainResult(
                        False,
                        f"transaction predates this payment request "
                        f"(mined {tx_time.isoformat()}, requested {not_before.isoformat()}) "
                        f"-- looks like an old/unrelated transaction being replayed",
                        amount, to_addr, confs)
            return ChainResult(True, "verified", amount, to_addr, confs, receipt)

        return ChainResult(False, "no matching USDT transfer to our address")

    async def verify(
        self, chain: str, tx_id: str, expect_to: str, expect_amount: Decimal,
        not_before: datetime | None = None, enforce_amount: bool = True,
    ) -> ChainResult:
        """`not_before` (when given) rejects a transaction that was mined
        before the payment request existed -- without this, any historical
        real transaction to our receiving address (an old unrelated order,
        someone else's payment, anything meeting the amount) can be replayed
        by copying its hash, since a transaction that genuinely satisfies
        amount/recipient/contract/confirmations looks identical on-chain
        whether it happened yesterday or five minutes ago. Always pass the
        payment's created_at here for anything that gates auto-approval.

        enforce_amount=False skips the amount-match check entirely --
        recipient/contract/confirmations/replay checks still apply in full,
        only the "did they send at least the expected amount" comparison is
        skipped. Used for wallet top-ups (see _verify_one), which have no
        fixed price to match against and whose displayed "send exactly
        X.XXXXXX" amount always carries a small randomised fractional
        offset (to tell concurrent top-ups apart on-chain) that a sender's
        wallet app rounding, or a manually retyped amount, routinely misses
        by more than payment_tolerance -- credited amount always comes from
        what was actually verified on-chain (result.amount), never from the
        originally-requested figure, so this can't be used to top up for
        less than what was actually sent."""
        tx_id = (tx_id or "").strip()
        if not tx_id:
            return ChainResult(False, "empty transaction id")
        if not expect_to:
            return ChainResult(False, f"no receiving address configured for {chain}")
        if chain == "TRC20":
            return await self.verify_tron(tx_id, expect_to, expect_amount, not_before,
                                          enforce_amount=enforce_amount)
        if chain in ("BEP20", "ERC20"):
            if not tx_id.startswith("0x"):
                tx_id = "0x" + tx_id
            return await self.verify_evm(chain, tx_id, expect_to, expect_amount, not_before,
                                         enforce_amount=enforce_amount)
        return ChainResult(False, f"unsupported chain {chain}")


verifier = ChainVerifier()


async def chain_address(chain: str) -> str:
    key = {
        "TRC20": "usdt_trc20_address",
        "BEP20": "usdt_bep20_address",
        "ERC20": "usdt_erc20_address",
    }.get(chain, "")
    return (await settings.get(key, "")).strip() if key else ""


# =============================================================================
#  CRYPTOMUS GATEWAY
# =============================================================================
#  Hosted invoice + webhook flow, offered alongside (and by default ahead of)
#  the manual on-chain flow above. Cryptomus custodies the crypto and gives
#  us a unique deposit address + webhook per invoice, so unlike ChainVerifier
#  this never queries a public chain directly -- it talks to Cryptomus's own
#  API, which is the authority on whether a given invoice was paid.
#  Credentials are settings-table only (admin panel, no env/code fallback),
#  per the requirement to configure this without a redeploy.

@dataclass
class CryptomusResult:
    ok: bool
    reason: str = ""
    uuid: str = ""
    order_id: str = ""
    status: str = ""
    address: str = ""
    network: str = ""
    currency: str = ""
    payer_currency: str = ""
    amount: Decimal = ZERO          # payer_amount -- what the buyer must send
    is_final: bool = False
    raw: dict = field(default_factory=dict)


CRYPTOMUS_SUCCESS_STATUSES = {"paid", "paid_over"}
CRYPTOMUS_FAILURE_STATUSES = {
    "wrong_amount", "fail", "cancel", "system_fail", "refund_fail", "locked",
}
CRYPTOMUS_PENDING_STATUSES = {
    "process", "check", "confirm_check", "wrong_amount_waiting", "refund_process",
}


class CryptomusClient:
    """Talks to the Cryptomus merchant API. Never holds a private key or
    custodies funds itself -- Cryptomus does that on their own rails and
    tells us the outcome via API response + webhook."""

    BASE = "https://api.cryptomus.com/v1"

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(20.0),
                headers={"User-Agent": "digital-hub-bot/2.0"},
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    @staticmethod
    def _sign(body_str: str, api_key: str) -> str:
        digest = base64.b64encode(body_str.encode()).decode() + api_key
        return hashlib.md5(digest.encode()).hexdigest()

    @staticmethod
    async def _creds() -> tuple[str, str]:
        merchant = (await settings.get("cryptomus_merchant_id", "")).strip()
        api_key = (await settings.get("cryptomus_api_key", "")).strip()
        return merchant, api_key

    async def _post(self, path: str, body: dict) -> tuple[bool, dict, str]:
        """Returns (ok, result, error_reason). API-level failures (bad
        request, invalid state, missing credentials) are reported via the
        return value, never raised -- only a genuine network/transport
        failure raises, so callers needing retry semantics (the webhook
        handler's re-verification call) can let those propagate to the
        caller instead of this silently becoming a false negative."""
        merchant, api_key = await self._creds()
        if not merchant or not api_key:
            return False, {}, "Cryptomus is not configured (missing merchant id or API key)"
        body_str = json.dumps(body, separators=(",", ":"))
        c = await self.client()
        r = await c.post(
            f"{self.BASE}{path}",
            content=body_str.encode(),
            headers={
                "merchant": merchant,
                "sign": self._sign(body_str, api_key),
                "Content-Type": "application/json",
            },
        )
        try:
            payload = r.json()
        except json.JSONDecodeError:
            return False, {}, f"non-JSON response (HTTP {r.status_code})"
        if r.status_code != 200 or "result" not in payload:
            return False, {}, f"Cryptomus API error: {payload.get('message') or payload.get('errors') or payload}"
        return True, payload["result"], ""

    @staticmethod
    def _parse(result: dict) -> "CryptomusResult":
        amount_str = result.get("payer_amount") or result.get("amount") or "0"
        return CryptomusResult(
            ok=True,
            uuid=result.get("uuid", ""),
            order_id=result.get("order_id", ""),
            status=result.get("payment_status") or result.get("status", ""),
            address=result.get("address") or "",
            network=result.get("network") or "",
            currency=result.get("currency") or "",
            payer_currency=result.get("payer_currency") or "",
            amount=crypto(amount_str),
            is_final=bool(result.get("is_final")),
            raw=result,
        )

    async def create_invoice(
        self, *, order_id: str, amount: Decimal, network: str,
        lifetime_seconds: int, callback_url: str = "", additional_data: str = "",
    ) -> CryptomusResult:
        lifetime_seconds = max(300, min(43200, lifetime_seconds))
        body = {
            "amount": str(amount),
            "currency": "USDT",
            "order_id": order_id,
            "to_currency": "USDT",
            "network": network,
            "lifetime": lifetime_seconds,
            "additional_data": additional_data[:255],
        }
        if callback_url:
            body["url_callback"] = callback_url
        ok, result, err = await self._post("/payment", body)
        return self._parse(result) if ok else CryptomusResult(False, err)

    async def get_info(self, uuid: str) -> CryptomusResult:
        """Authoritative status lookup -- the webhook handler treats this,
        not the webhook body, as ground truth. Network/transport errors
        raise (see _post docstring) so a transient failure here surfaces as
        a non-200 to Cryptomus rather than silently dropping a paid webhook."""
        ok, result, err = await self._post("/payment/info", {"uuid": uuid})
        return self._parse(result) if ok else CryptomusResult(False, err)

    async def get_qr(self, uuid: str) -> str | None:
        """Best-effort only -- the payment screen always shows the address
        and amount as text regardless of whether this succeeds."""
        try:
            ok, result, _ = await self._post("/payment/qr", {"merchant_payment_uuid": uuid})
            return result.get("image") if ok else None
        except Exception:
            log.exception("cryptomus QR fetch failed")
            return None

    async def verify_webhook_signature(self, payload: dict) -> bool:
        api_key = (await settings.get("cryptomus_api_key", "")).strip()
        sign = payload.get("sign") if api_key else None
        if not api_key or not sign or not isinstance(sign, str):
            return False
        data = {k: v for k, v in payload.items() if k != "sign"}
        expected = self._sign(self._php_style_json(data), api_key)
        return hmac.compare_digest(expected, sign)

    @staticmethod
    def _php_style_json(data: dict) -> str:
        """PHP's json_encode escapes forward slashes by default; Python's
        json.dumps never does. Cryptomus signs webhook payloads using PHP's
        encoding, so re-serializing with Python's default and comparing
        would fail signature verification on any field containing a slash
        (e.g. a callback URL) even for a genuine webhook -- a documented
        Cryptomus gotcha, not a guess. dict insertion order is preserved by
        both json.loads and json.dumps, so as long as `data` came straight
        from parsing the incoming request body, key order matches what
        Cryptomus originally signed."""
        return json.dumps(data, separators=(",", ":"), ensure_ascii=True).replace("/", "\\/")


cryptomus = CryptomusClient()


async def cryptomus_network_for(chain: str) -> str:
    key = {
        "TRC20": "cryptomus_network_trc20",
        "BEP20": "cryptomus_network_bep20",
        "ERC20": "cryptomus_network_erc20",
    }.get(chain, "")
    default = {"TRC20": "TRON", "BEP20": "BSC", "ERC20": "ETH"}.get(chain, "")
    return (await settings.get(key, default)).strip() if key else ""


async def cryptomus_webhook_url() -> str:
    base = (await settings.get("public_base_url", "")).strip().rstrip("/")
    return f"{base}/api/cryptomus/webhook" if base else ""


# =============================================================================
#  OXAPAY GATEWAY
# =============================================================================
#  Hosted checkout: OxaPay generates a payment_url the customer completes
#  entirely on OxaPay's own page (whatever network/currency they support),
#  so there's no address/QR to render here -- just one "Pay now" link.
#  Confirmed via an inbound webhook (POST /api/oxapay/webhook) whose
#  HMAC-SHA512 signature is verified before it's trusted at all; even then
#  the webhook body is only ever used to look up *which* payment to
#  re-check -- get_status(track_id) is the actual source of truth, same
#  "never trust the claim, always confirm independently" philosophy
#  Cryptomus/ChainVerifier use. The background expiry sweep
#  (payments.expires_at) still times out anything left unpaid, same as
#  every other payment method.

async def oxapay_webhook_url() -> str:
    base = (await settings.get("public_base_url", "")).strip().rstrip("/")
    return f"{base}/api/oxapay/webhook" if base else ""


@dataclass
class OxaPayResult:
    ok: bool
    reason: str = ""
    track_id: str = ""
    order_id: str = ""
    status: str = ""
    payment_url: str = ""
    amount: Decimal = ZERO
    currency: str = ""
    raw: dict = field(default_factory=dict)


# "paid" is the only status OxaPay guarantees means the funds are actually
# settled -- "paying"/"confirming" are still in flight and must never
# trigger delivery. OxaPay doesn't push failure/expiry via webhook; the
# background expiry sweep is what closes those out.
OXAPAY_SUCCESS_STATUSES = {"paid"}
OXAPAY_FAILURE_STATUSES = {"expired", "failed"}


class OxaPayClient:
    """Talks to the OxaPay merchant API with a plain merchant_api_key
    header. Never custodies funds in this bot's flow -- OxaPay hosts the
    actual payment page and is the sole authority on whether an invoice was
    paid."""

    BASE = "https://api.oxapay.com/v1"

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            # Real-world OxaPay latency is well under a second; this is a
            # worst-case cap, not the expected wait. It sits on the live
            # checkout path (button tap -> invoice created), so a wide 20s
            # ceiling here just means a degraded API leaves the user
            # staring at "Processing..." far longer than useful.
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(10.0),
                headers={"User-Agent": "digital-hub-bot/2.0"},
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    @staticmethod
    async def _api_key() -> str:
        return (await settings.get("oxapay_merchant_api_key", "")).strip()

    async def _req(self, method: str, path: str, body: dict | None = None) -> tuple[bool, dict, str]:
        """Returns (ok, result, error_reason), same contract as
        CryptomusClient._post -- API-level failures (bad request, missing
        key, unknown track id) are reported via the return value and never
        raised; only a genuine network/transport failure raises."""
        api_key = await self._api_key()
        if not api_key:
            return False, {}, "OxaPay is not configured (missing merchant API key)"
        c = await self.client()
        r = await c.request(method, f"{self.BASE}{path}", json=body,
                            headers={"merchant_api_key": api_key,
                                     "Content-Type": "application/json"})
        try:
            payload = r.json()
        except json.JSONDecodeError:
            return False, {}, f"non-JSON response (HTTP {r.status_code})"
        if r.status_code >= 400:
            err = payload.get("message") or (payload.get("error") or {}).get("message") or payload
            return False, {}, f"OxaPay API error: {err}"
        return True, payload, ""

    @staticmethod
    def _parse(result: dict) -> "OxaPayResult":
        data = result.get("data") or result
        try:
            amount = crypto(Decimal(str(data.get("amount") or 0)))
        except (InvalidOperation, TypeError):
            amount = ZERO
        return OxaPayResult(
            ok=True,
            track_id=str(data.get("track_id") or ""),
            order_id=str(data.get("order_id") or ""),
            status=str(data.get("status") or "").lower(),
            payment_url=data.get("payment_url") or "",
            amount=amount,
            currency=(data.get("currency") or "").upper(),
            raw=result,
        )

    async def create_invoice(
        self, *, amount: Decimal, order_id: str, callback_url: str,
        lifetime_minutes: int, description: str = "",
    ) -> OxaPayResult:
        # currency="USDT" so the invoice is priced in the exact USDT amount
        # already converted via usdt_rate(), the same "no third-party
        # conversion" approach _create_cryptomus_payment uses.
        body = {
            "amount": str(amount),
            "currency": "USDT",
            "lifetime": max(15, min(2880, lifetime_minutes)),
            "callback_url": callback_url,
            "order_id": order_id,
            "description": description[:255],
        }
        ok, result, err = await self._req("POST", "/payment/invoice", body)
        return self._parse(result) if ok else OxaPayResult(False, err)

    async def get_status(self, track_id: str) -> OxaPayResult:
        """Authoritative status lookup -- called from the webhook handler
        (never trust the callback body alone) and available for a manual
        admin recheck."""
        ok, result, err = await self._req("GET", f"/payment/{track_id}")
        return self._parse(result) if ok else OxaPayResult(False, err)

    async def verify_webhook_signature(self, raw_body: bytes, header_hmac: str) -> bool:
        """OxaPay signs webhook bodies with HMAC-SHA512 over the raw POST
        bytes, using the merchant API key -- unlike Cryptomus, no
        re-serialization gotcha here since this hashes the exact bytes
        OxaPay sent, not a reparsed/re-encoded dict."""
        api_key = await self._api_key()
        if not api_key or not header_hmac:
            return False
        expected = hmac.new(api_key.encode(), raw_body, hashlib.sha512).hexdigest()
        return hmac.compare_digest(expected, header_hmac)


oxapay = OxaPayClient()


# =============================================================================
#  VERIFICATION WORKER
# =============================================================================
#  Polls pending on-chain payments. Approves the ones that check out, expires
#  the ones that ran out of time, and leaves genuinely ambiguous ones for a
#  human. Runs as a background task with a bounded per-payment attempt count
#  so a permanently-bad txId doesn't get retried forever.

MAX_VERIFY_ATTEMPTS_FLOOR = 40


async def max_verify_attempts() -> int:
    """Enough attempts to cover the full payment TTL window at the current
    poll interval. verify_interval_seconds() is meant to be tuned down
    freely for faster detection, but a *fixed* attempt count would silently
    shrink the window a payment stays actively checked in — e.g. 40
    attempts covered ~40min at the old 60s interval, but only ~13min at a
    20s interval, well short of the 45min default payment_ttl_minutes.
    Recomputing this from the actual TTL keeps a payment checked right up
    until it genuinely expires, regardless of how the interval is tuned."""
    ttl_seconds = await payment_ttl_minutes() * 60
    interval = max(1, await verify_interval_seconds())
    return max(MAX_VERIFY_ATTEMPTS_FLOOR, -(-ttl_seconds // interval))  # ceil division


async def verification_worker(bot: Bot) -> None:
    await asyncio.sleep(5)
    log.info("chain verification worker started (interval=%ss)", await verify_interval_seconds())

    while True:
        try:
            if not await settings.get_bool("auto_verify_enabled", True):
                await asyncio.sleep(await verify_interval_seconds())
                continue

            pending = await db.fetch(
                """SELECT * FROM payments
                    WHERE status='pending'
                      AND verification='chain'
                      AND tx_id IS NOT NULL
                      AND verify_attempts < $1
                    ORDER BY created_at ASC
                    LIMIT 25""",
                await max_verify_attempts(),
            )

            for p in pending:
                try:
                    await _verify_one(bot, p)
                except Exception:
                    log.exception("verification failed for payment %s", p["id"])
                await asyncio.sleep(0.4)   # be polite to public RPC endpoints

            expired = await db.fetch(
                """UPDATE payments SET status='expired',
                       verify_note='payment window elapsed'
                    WHERE status='pending'
                      AND expires_at IS NOT NULL
                      AND expires_at < NOW()
                RETURNING id, user_id, order_id""",
            )
            for e in expired:
                await db.execute(
                    "UPDATE orders SET status='cancelled' WHERE id=$1 AND status='pending'",
                    e["order_id"],
                )
                await notify_user(
                    bot, e["user_id"],
                    "⌛ Your payment window expired. Nothing was charged — "
                    "start a new order whenever you're ready.",
                )

        except asyncio.CancelledError:
            log.info("verification worker stopping")
            raise
        except Exception:
            log.exception("verification worker loop error")

        await asyncio.sleep(await verify_interval_seconds())


async def _verify_one(bot: Bot, p: asyncpg.Record) -> None:
    chain = p["chain"] or "TRC20"
    expect_to = p["expected_address"] or await chain_address(chain)
    expect_amount = crypto(p["expected_amount"] or p["amount"])
    # Neither top-ups nor direct order payments require an exact amount
    # match on-chain -- the displayed "send exactly X.XXXXXX" always
    # carries either a small randomised offset (topups, to tell concurrent
    # ones apart on-chain) or a 6-decimal figure a wallet/exchange's 2-4
    # decimal input can't hit exactly. enforce_amount=False always resolves
    # whatever actually arrived; approve_payment below credits that exact
    # amount to the buyer's wallet and, for a direct order purchase, spends
    # from that same balance to deliver it -- covering under/over/exact
    # payment with one path, never losing what was actually sent.
    result = await verifier.verify(chain, p["tx_id"], expect_to, expect_amount, p["created_at"],
                                   enforce_amount=False)

    await db.execute(
        """UPDATE payments
              SET verify_attempts = verify_attempts + 1,
                  verify_note = $2,
                  confirmations = $3
            WHERE id = $1""",
        p["id"], result.reason[:500], result.confirmations,
    )

    if not result.ok:
        log.info("payment %s not yet verified: %s", p["id"], result.reason)
        return

    log.info("payment %s VERIFIED on %s (%s, %s confs)",
             p["id"], chain, result.amount, result.confirmations)

    credit_override = None
    if result.amount > ZERO:
        # Credit exactly what arrived on-chain, converted back to INR --
        # never the originally-requested figure. This is what makes
        # enforce_amount=False safe: a short/rounded payment can't be used
        # to get more credit than it was actually worth.
        credit_override = money(result.amount * await usdt_rate(), Q_FIAT)
    await approve_payment(bot, p["id"], admin_id=None, note="auto-verified on-chain",
                          override_amount=credit_override)


async def notify_user(bot: Bot, user_id: int, text: str, markup=None) -> bool:
    """Send a message by internal user id. Never raises."""
    try:
        tid = await db.fetchval("SELECT telegram_id FROM store_users WHERE id=$1", user_id)
        if not tid:
            return False
        await bot.send_message(int(tid), text, reply_markup=markup)
        return True
    except (TelegramForbiddenError, TelegramBadRequest) as e:
        log.warning("cannot message user %s: %s", user_id, e)
        return False
    except Exception:
        log.exception("notify_user failed for %s", user_id)
        return False


async def notify_admins_text(bot: Bot, text: str) -> None:
    """Push a message to every reachable bot admin: the env allowlist plus
    every active row in `admins` whose username is a Telegram id (the
    existing convention for Telegram-promoted admins -- see the 'promoteok'
    handler, which stores u['telegram_id'] as `username`). Web-only admin
    accounts have a non-numeric username and simply have no chat to message.
    Never raises -- notification failures shouldn't affect payment
    processing."""
    ids = set(CFG.admin_ids)
    try:
        rows = await db.fetch("SELECT username FROM admins WHERE active")
        for r in rows:
            try:
                ids.add(int(r["username"]))
            except (TypeError, ValueError):
                continue
    except Exception:
        log.exception("notify_admins_text: failed to load admin list")
    for tid in ids:
        try:
            await bot.send_message(tid, text)
        except (TelegramForbiddenError, TelegramBadRequest) as e:
            log.warning("cannot message admin %s: %s", tid, e)
        except Exception:
            log.exception("notify_admins_text failed for %s", tid)

# =============================================================================
#  STOCK & PRICING
# =============================================================================

async def stock_of(conn_or_db, product: asyncpg.Record) -> int:
    """Available units. Auto products count unused keys; manual uses a counter."""
    if product["delivery_type"] == "manual":
        return product["manual_stock"] if product["manual_stock"] is not None else 999_999
    q = "SELECT COUNT(*) FROM product_keys WHERE product_id=$1 AND is_used=FALSE"
    return int(await conn_or_db.fetchval(q, product["id"]) or 0)


async def stock_of_many(conn_or_db, products: list[asyncpg.Record]) -> dict[int, int]:
    """Same as stock_of, batched -- one COUNT...GROUP BY for every 'auto'
    (key-based) product in the list instead of a separate round trip per
    product. Used anywhere a whole product *list* renders (shop, deals,
    admin product list): those used `asyncio.gather(*(stock_of(...) ...))`,
    which parallelizes the wait but still costs N real round trips and N
    connection-pool acquisitions -- this is the same information in one
    query. Single-product screens still use stock_of directly; batching
    one product buys nothing."""
    manual_ids = {p["id"] for p in products if p["delivery_type"] == "manual"}
    auto_ids = [p["id"] for p in products if p["id"] not in manual_ids]

    counts: dict[int, int] = {}
    if auto_ids:
        rows = await conn_or_db.fetch(
            """SELECT product_id, COUNT(*) AS cnt FROM product_keys
                WHERE product_id = ANY($1::int[]) AND is_used=FALSE
                GROUP BY product_id""",
            auto_ids,
        )
        counts = {r["product_id"]: int(r["cnt"]) for r in rows}

    return {
        p["id"]: (p["manual_stock"] if p["manual_stock"] is not None else 999_999)
                 if p["id"] in manual_ids else counts.get(p["id"], 0)
        for p in products
    }


@dataclass
class Quote:
    """A fully-resolved price breakdown. All values canonical INR."""
    subtotal: Decimal
    coupon_discount: Decimal
    tier_discount: Decimal
    qty_discount: Decimal
    qty_tier_min: int | None
    total: Decimal
    coupon_id: int | None
    coupon_code: str | None
    tier: str

    @property
    def discount(self) -> Decimal:
        return money(self.coupon_discount + self.tier_discount + self.qty_discount, Q_CRYPTO)


class QuoteError(Exception):
    pass


async def best_qty_discount(conn, product_id: int, quantity: int) -> asyncpg.Record | None:
    """Highest min_qty tier this quantity actually qualifies for, or None.
    Shared by build_quote (applies it) and the customer product screen
    (advertises the tiers before they've picked a quantity)."""
    return await conn.fetchrow(
        """SELECT min_qty, discount_type, value FROM product_qty_discounts
            WHERE product_id=$1 AND min_qty <= $2
            ORDER BY min_qty DESC LIMIT 1""",
        product_id, quantity,
    )


async def build_quote(
    conn,
    user: asyncpg.Record,
    product: asyncpg.Record,
    quantity: int,
    coupon_code: str | None = None,
) -> Quote:
    """Compute a price. Validates coupon eligibility as a side effect."""
    if quantity < 1:
        raise QuoteError("Quantity must be at least 1.")

    unit = money(product["price"], Q_CRYPTO)
    subtotal = money(unit * quantity, Q_CRYPTO)

    tier = user["tier"] or "bronze"
    tier_pct = await tier_discount_percent(tier)
    tier_discount = money(subtotal * tier_pct / 100, Q_CRYPTO)

    # Quantity tier -- independent of (and stacks with) the loyalty tier
    # discount above; both are just "% or flat off this subtotal" in the
    # end, same as the coupon below.
    qty_discount = ZERO
    qty_tier_min = None
    qtier = await best_qty_discount(conn, product["id"], quantity)
    if qtier:
        qty_tier_min = qtier["min_qty"]
        if qtier["discount_type"] == "percent":
            qty_discount = money(subtotal * money(qtier["value"], Q_CRYPTO) / 100, Q_CRYPTO)
        else:
            qty_discount = money(qtier["value"], Q_CRYPTO)
        qty_discount = min(qty_discount, subtotal)

    coupon_discount = ZERO
    coupon_id = None
    resolved_code = None

    if coupon_code:
        code = coupon_code.strip().upper()
        c = await conn.fetchrow(
            "SELECT * FROM coupons WHERE upper(code)=$1 FOR UPDATE", code
        )
        if not c:
            raise QuoteError("That coupon code doesn't exist.")
        if not c["active"]:
            raise QuoteError("That coupon is no longer active.")
        if c["expires_at"] and c["expires_at"] < datetime.now(timezone.utc):
            raise QuoteError("That coupon has expired.")
        if c["max_uses"] is not None and c["used_count"] >= c["max_uses"]:
            raise QuoteError("That coupon has been fully redeemed.")
        if subtotal < money(c["min_order_amount"], Q_CRYPTO):
            need = fmt_money(money(c["min_order_amount"]), "INR")
            raise QuoteError(f"This coupon needs a minimum order of {need}.")
        if c["per_user_limit"] is not None:
            used_by_user = await conn.fetchval(
                "SELECT COUNT(*) FROM coupon_uses WHERE coupon_id=$1 AND user_id=$2",
                c["id"], user["id"],
            )
            if int(used_by_user or 0) >= c["per_user_limit"]:
                raise QuoteError("You've already used this coupon the maximum number of times.")

        if c["discount_type"] == "percent":
            coupon_discount = money(subtotal * money(c["value"], Q_CRYPTO) / 100, Q_CRYPTO)
        else:
            coupon_discount = money(c["value"], Q_CRYPTO)
        coupon_discount = min(coupon_discount, subtotal)
        coupon_id = c["id"]
        resolved_code = c["code"]

    total = money(subtotal - coupon_discount - tier_discount - qty_discount, Q_CRYPTO)
    if total < ZERO:
        total = ZERO

    return Quote(subtotal, coupon_discount, tier_discount, qty_discount, qty_tier_min, total,
                 coupon_id, resolved_code, tier)


# =============================================================================
#  FULFILMENT
# =============================================================================
#  The critical section. Called both by wallet checkout and by payment
#  approval. Everything happens under one transaction with the product row
#  locked, so two buyers racing for the last key cannot both win.

class FulfilError(Exception):
    pass


async def fulfil_order(conn: asyncpg.Connection, order_id: int) -> tuple[str, list[str]]:
    """Allocate keys and mark the order delivered. Returns (status, keys)."""
    order = await conn.fetchrow(
        "SELECT * FROM orders WHERE id=$1 FOR UPDATE", order_id
    )
    if not order:
        raise FulfilError("order not found")
    if order["status"] == "delivered":
        existing = (order["delivered_content"] or "").split("\n")
        return "delivered", [k for k in existing if k]

    product = await conn.fetchrow(
        "SELECT * FROM products WHERE id=$1 FOR UPDATE", order["product_id"]
    )
    if not product:
        raise FulfilError("product no longer exists")

    qty = order["quantity"]

    if product["delivery_type"] == "manual":
        current = product["manual_stock"]
        if current is not None:
            if current < qty:
                raise FulfilError("insufficient stock")
            await conn.execute(
                "UPDATE products SET manual_stock = manual_stock - $1 WHERE id=$2",
                qty, product["id"],
            )
        await conn.execute(
            "UPDATE orders SET status='processing' WHERE id=$1", order_id
        )
        await conn.execute(
            "UPDATE products SET sold_count = sold_count + $1 WHERE id=$2",
            qty, product["id"],
        )
        return "processing", []

    # Auto delivery: lock exactly `qty` unused keys.
    keys = await conn.fetch(
        """SELECT id, key_value FROM product_keys
            WHERE product_id=$1 AND is_used=FALSE
            ORDER BY id
            LIMIT $2
            FOR UPDATE SKIP LOCKED""",
        product["id"], qty,
    )
    if len(keys) < qty:
        raise FulfilError("insufficient stock")

    key_ids = [k["id"] for k in keys]
    key_values = [k["key_value"] for k in keys]

    await conn.execute(
        """UPDATE product_keys
              SET is_used=TRUE, order_id=$1, used_at=NOW()
            WHERE id = ANY($2::int[])""",
        order_id, key_ids,
    )
    await conn.execute(
        """UPDATE orders
              SET status='delivered', delivered_content=$2, delivered_at=NOW()
            WHERE id=$1""",
        order_id, "\n".join(key_values),
    )
    await conn.execute(
        "UPDATE products SET sold_count = sold_count + $1 WHERE id=$2",
        qty, product["id"],
    )
    return "delivered", key_values


async def finalise_order(
    conn: asyncpg.Connection, order: asyncpg.Record
) -> tuple[str, list[str]]:
    """Fulfil, then apply spend totals, tier, coupon usage and referral bonus."""
    status, keys = await fulfil_order(conn, order["id"])

    total = money(order["total"], Q_CRYPTO)
    await conn.execute(
        "UPDATE store_users SET total_spent = total_spent + $1 WHERE id=$2",
        total, order["user_id"],
    )
    await refresh_tier(conn, order["user_id"])

    if order["coupon_id"]:
        await conn.execute(
            "UPDATE coupons SET used_count = used_count + 1 WHERE id=$1",
            order["coupon_id"],
        )
        await conn.execute(
            """INSERT INTO coupon_uses(coupon_id, user_id, order_id)
               VALUES($1,$2,$3)""",
            order["coupon_id"], order["user_id"], order["id"],
        )

    await _apply_referral_bonus(conn, order)
    return status, keys


async def _apply_referral_bonus(conn: asyncpg.Connection, order: asyncpg.Record) -> None:
    """One-time credit to the referrer on their referee's first paid order."""
    user = await conn.fetchrow(
        "SELECT * FROM store_users WHERE id=$1 FOR UPDATE", order["user_id"]
    )
    if not user or not user["referred_by_id"] or user["referral_bonus_given"]:
        return

    pct = await settings.get_decimal("referral_bonus_percent", "5")
    if pct <= 0:
        return

    bonus = money(money(order["total"], Q_CRYPTO) * pct / 100, Q_CRYPTO)
    if bonus <= ZERO:
        return

    await conn.execute(
        "UPDATE store_users SET referral_bonus_given=TRUE WHERE id=$1", user["id"]
    )
    await wallet_apply(
        conn,
        user["referred_by_id"],
        bonus,
        "referral_bonus",
        f"Referral bonus from {user['first_name'] or user['telegram_id']}",
        ref_type="order",
        ref_id=order["id"],
        idempotent=True,
    )
    log.info("referral bonus %s credited to user %s", bonus, user["referred_by_id"])


# =============================================================================
#  PAYMENT APPROVAL
# =============================================================================
#  Single path used by every payment method (chain / Cryptomus / OxaPay)
#  and manual admin approval, so none of them can ever diverge in behaviour.
#  Every payment -- topup or direct order purchase -- lands in the buyer's
#  wallet balance first; a linked order is then fulfilled by spending from
#  that same balance if it covers the price. This makes over/under/exact
#  payment all the same code path: overpay leaves the surplus as balance,
#  underpay leaves the order cancelled but the money credited (never lost,
#  usable on a retry), exact pay nets to the same outcome as before.

async def approve_payment(
    bot: Bot, payment_id: int, admin_id: int | None, note: str = "",
    override_amount: Decimal | None = None,
) -> dict:
    """override_amount: credit this INR figure instead of the
    originally-requested payments.amount -- used when a payment was verified
    with enforce_amount=False (see _verify_one) or against a third-party API
    amount (Cryptomus/OxaPay), so the wallet always gets exactly what
    was actually received, not what was asked for. The payments row's own
    `amount` is updated to match, so admin screens/order history show what
    really happened, not the original ask."""
    delivered_keys: list[str] = []
    order_row = None
    credited_amount = ZERO
    order_outcome = ""
    user_id = None
    purpose = ""

    async with db.tx() as conn:
        p = await conn.fetchrow(
            "SELECT * FROM payments WHERE id=$1 FOR UPDATE", payment_id
        )
        if not p:
            raise FulfilError("payment not found")
        if p["status"] != "pending":
            raise FulfilError(f"payment is already {p['status']}")

        user_id = p["user_id"]
        purpose = p["purpose"]

        await conn.execute(
            """UPDATE payments
                  SET status='approved', reviewed_by=$2, reviewed_at=NOW(),
                      verify_note=COALESCE(NULLIF($3,''), verify_note)
                WHERE id=$1""",
            payment_id, admin_id, note,
        )

        credited_amount = (money(override_amount, Q_FIAT)
                        if override_amount is not None
                        else money(p["amount"], Q_CRYPTO))
        if override_amount is not None:
            await conn.execute(
                "UPDATE payments SET amount=$2 WHERE id=$1", payment_id, credited_amount)

        if p["purpose"] == "topup":
            await wallet_apply(
                conn, p["user_id"], credited_amount, "topup",
                f"Wallet top-up via {p['method']}",
                ref_type="payment", ref_id=payment_id, idempotent=True,
            )
        else:
            if not p["order_id"]:
                raise FulfilError("payment has no linked order")
            order_row = await conn.fetchrow(
                "SELECT * FROM orders WHERE id=$1 FOR UPDATE", p["order_id"]
            )
            if not order_row:
                raise FulfilError("linked order missing")

            new_balance = await wallet_apply(
                conn, p["user_id"], credited_amount, "topup",
                f"Payment #{payment_id} for order #{order_row['id']}",
                ref_type="payment", ref_id=payment_id, idempotent=True,
            )
            order_total = money(order_row["total"], Q_CRYPTO)
            if order_row["status"] == "pending" and new_balance >= order_total:
                await wallet_apply(
                    conn, p["user_id"], -order_total, "purchase",
                    f"Order #{order_row['id']}",
                    ref_type="order", ref_id=order_row["id"], idempotent=True,
                )
                _, delivered_keys = await finalise_order(conn, order_row)
                order_outcome = "delivered"
            elif order_row["status"] == "pending":
                await conn.execute(
                    "UPDATE orders SET status='cancelled' WHERE id=$1", order_row["id"])
                order_outcome = "insufficient"
            else:
                order_outcome = order_row["status"]

    # Notify outside the transaction: Telegram is slow and must never hold locks.
    if purpose == "topup":
        bal = await db.fetchval(
            "SELECT wallet_balance FROM store_users WHERE id=$1", user_id)
        user = await db.fetchrow("SELECT * FROM store_users WHERE id=$1", user_id)
        cur = await user_currency(user)
        await notify_user(
            bot, user_id,
            f"✅ <b>Top-up confirmed</b>\n\n"
            f"Added: {await display_price(credited_amount, cur)}\n"
            f"Balance: <b>{await display_price(money(bal, Q_CRYPTO), cur)}</b>",
        )
    elif order_outcome == "delivered":
        await deliver_to_user(bot, order_row, delivered_keys)
    elif order_outcome == "insufficient":
        bal = await db.fetchval(
            "SELECT wallet_balance FROM store_users WHERE id=$1", user_id)
        user = await db.fetchrow("SELECT * FROM store_users WHERE id=$1", user_id)
        cur = await user_currency(user)
        await notify_user(
            bot, user_id,
            f"⚠️ <b>Order payment didn't cover the price</b>\n\n"
            f"Received: {await display_price(credited_amount, cur)}\n"
            f"Order price: {await display_price(money(order_row['total'], Q_CRYPTO), cur)}\n\n"
            f"Added to your wallet instead: <b>{await display_price(money(bal, Q_CRYPTO), cur)}</b>\n"
            f"Top up the rest, or use it toward another purchase.",
        )
    elif order_outcome:
        # order_row was already resolved (delivered/cancelled) by something
        # else before this payment got approved -- shouldn't happen given
        # payment/order status are always transitioned together, but the
        # money was still credited to the wallet above either way, so this
        # is a "notify a human" case, not a lost-funds one.
        log.warning("payment %s approved but linked order %s was already '%s' -- "
                    "%s credited to wallet %s instead",
                    payment_id, order_row["id"], order_outcome, credited_amount, user_id)

    return {"ok": True, "keys": delivered_keys}


async def reject_payment(
    bot: Bot, payment_id: int, admin_id: int | None, reason: str = ""
) -> dict:
    async with db.tx() as conn:
        p = await conn.fetchrow(
            "SELECT * FROM payments WHERE id=$1 FOR UPDATE", payment_id)
        if not p:
            raise FulfilError("payment not found")
        if p["status"] != "pending":
            raise FulfilError(f"payment is already {p['status']}")
        await conn.execute(
            """UPDATE payments SET status='rejected', reviewed_by=$2,
                   reviewed_at=NOW(), verify_note=$3
                WHERE id=$1""",
            payment_id, admin_id, reason[:500],
        )
        if p["order_id"]:
            await conn.execute(
                "UPDATE orders SET status='cancelled' WHERE id=$1 AND status='pending'",
                p["order_id"],
            )

    msg = "❌ <b>Payment not accepted</b>"
    if reason:
        msg += f"\n\nReason: {reason}"
    msg += "\n\nIf you think this is a mistake, contact support with your transaction ID."
    await notify_user(bot, p["user_id"], msg)
    return {"ok": True}


async def _process_cryptomus_status(
    bot: Bot, payment: asyncpg.Record, info: "CryptomusResult"
) -> str:
    """Given the payment row and a *fresh* CryptomusResult from get_info
    (the source of truth -- never the raw webhook body), drive it through
    approve_payment/reject_payment exactly like every other payment method.
    Shared by the webhook handler and the in-bot 'Check status' button so
    the two paths can never disagree about what counts as paid/failed.
    Returns a short label describing what happened, for the caller to show
    the user ("paid", "failed", "pending")."""
    payment_id = payment["id"]

    if info.status == "paid_over" and not await settings.get_bool("cryptomus_accept_overpayment", True):
        # Admin has opted out of auto-accepting overpayments -- route to
        # manual review instead of silently keeping the difference or
        # guessing what the customer wants done with it.
        log.info("cryptomus payment %s overpaid (%s) -- auto-accept disabled, needs manual review",
                 payment_id, info.amount)
        await notify_admins_text(
            bot, f"💰 Cryptomus payment #{payment_id} was OVERPAID ({info.amount} "
                 f"{info.payer_currency or 'USDT'}) — overpayment auto-accept is off, needs manual review.")
        return "pending"

    if info.status in CRYPTOMUS_SUCCESS_STATUSES:
        # Defense in depth, not redundancy: Cryptomus's own "paid"/
        # "paid_over" status already implies the amount was sufficient, but
        # a real-money decision should never rest on a single third-party
        # field -- same "verify independently" philosophy ChainVerifier
        # applies on-chain. These three are identity/integrity mismatches --
        # the invoice Cryptomus is confirming doesn't actually match the one
        # this payment row was created for, which smells like a mixed-up
        # reference rather than an ordinary short payment. Still routed to
        # manual review; unlike an amount shortfall there's no safe amount
        # to credit here since we can't be sure this confirmation is even
        # about the right deposit.
        mismatch = None
        if info.uuid and payment["cryptomus_uuid"] and info.uuid != payment["cryptomus_uuid"]:
            mismatch = f"invoice uuid mismatch (info={info.uuid}, payment={payment['cryptomus_uuid']})"
        elif info.network and payment["chain"] and info.network.upper() != (payment["chain"] or "").upper():
            mismatch = f"network mismatch (info={info.network}, payment={payment['chain']})"
        elif info.currency and info.currency.upper() != "USDT":
            mismatch = f"unexpected currency (info={info.currency})"

        if mismatch:
            log.error("cryptomus payment %s reported '%s' but failed verification: %s "
                      "-- NOT approving, needs manual review",
                      payment_id, info.status, mismatch)
            await notify_admins_text(
                bot, f"⚠️ Cryptomus payment #{payment_id} reported paid but failed "
                     f"verification ({mismatch}). Not delivered — needs manual review.")
            return "pending"

        # No amount-match gate beyond this -- same reasoning as _verify_one:
        # credit exactly what Cryptomus confirms was received (never the
        # originally-requested figure) and let approve_payment's wallet-
        # balance path decide whether that's enough to deliver the order.
        credit_override = (money(info.amount * await usdt_rate(), Q_FIAT)
                        if info.amount > ZERO else None)
        try:
            await approve_payment(
                bot, payment_id, admin_id=None,
                note=f"auto-verified via Cryptomus ({info.status})",
                override_amount=credit_override)
            await notify_admins_text(
                bot, f"💰 Order paid via Cryptomus — payment #{payment_id} "
                     f"({info.status}, {info.amount} {info.payer_currency or 'USDT'}).")
        except FulfilError:
            cur = await db.fetchval(
                "SELECT status FROM payments WHERE id=$1", payment_id)
            if cur != "approved":
                # Cryptomus says paid, but our own row is neither pending
                # (expected -- another check race) nor approved. Most likely
                # cause: the expiry sweep flipped it to 'expired' before this
                # confirmation landed. Real money, nothing auto-delivered --
                # needs a human, not a silent drop.
                log.error(
                    "cryptomus payment %s reported paid but local status is "
                    "'%s' -- needs manual reconciliation", payment_id, cur)
                await notify_admins_text(
                    bot, f"⚠️ Cryptomus payment #{payment_id} was paid but "
                         f"local status is '{cur}' — needs manual review.")
        return "paid"

    if info.status in CRYPTOMUS_FAILURE_STATUSES:
        try:
            await reject_payment(
                bot, payment_id, admin_id=None,
                reason=f"Cryptomus status: {info.status}")
        except FulfilError:
            pass  # already resolved (approved/rejected/expired) -- fine
        return "failed"

    return "pending"


async def _process_oxapay_status(
    bot: Bot, payment: asyncpg.Record, info: "OxaPayResult"
) -> str:
    """Given the payment row and a *fresh* OxaPayResult from get_status
    (the source of truth -- never the raw webhook body), drive it through
    approve_payment/reject_payment exactly like every other payment method.
    Shared by the webhook handler and any manual admin recheck, so both
    paths can never disagree about what counts as paid/failed. Returns a
    short label describing what happened ('paid', 'failed', 'pending')."""
    payment_id = payment["id"]

    if info.status in OXAPAY_SUCCESS_STATUSES:
        # Defense in depth, not redundancy: OxaPay's own "paid" status
        # already implies enough was paid, but a real-money decision should
        # never rest on a single third-party field -- same "verify
        # independently" philosophy ChainVerifier/Cryptomus apply. This is
        # an identity check only -- which invoice OxaPay is even talking
        # about -- not an amount check: OxaPay's hosted page can settle in
        # any of the networks/currencies it supports, whose native units
        # differ by orders of magnitude from a fixed tolerance, and the
        # wallet-balance path below handles a short amount safely regardless.
        mismatch = None
        if info.track_id and payment["oxapay_track_id"] and info.track_id != payment["oxapay_track_id"]:
            mismatch = f"track id mismatch (info={info.track_id}, payment={payment['oxapay_track_id']})"

        if mismatch:
            log.error("oxapay payment %s reported '%s' but failed verification: %s "
                      "-- NOT approving, needs manual review",
                      payment_id, info.status, mismatch)
            await notify_admins_text(
                bot, f"⚠️ OxaPay payment #{payment_id} reported paid but failed "
                     f"verification ({mismatch}). Not delivered — needs manual review.")
            return "pending"

        # Credit exactly what OxaPay confirms was received (never the
        # originally-requested figure) and let approve_payment's wallet-
        # balance path decide whether that's enough to deliver the order.
        credit_override = (money(info.amount * await usdt_rate(), Q_FIAT)
                        if info.amount > ZERO else None)
        try:
            await approve_payment(
                bot, payment_id, admin_id=None,
                note=f"auto-verified via OxaPay ({info.status})",
                override_amount=credit_override)
            await notify_admins_text(
                bot, f"💰 Order paid via OxaPay — payment #{payment_id} "
                     f"({info.amount} {info.currency or 'USDT'}).")
        except FulfilError:
            cur = await db.fetchval(
                "SELECT status FROM payments WHERE id=$1", payment_id)
            if cur != "approved":
                # OxaPay says paid, but our own row is neither pending
                # (expected -- another check race, already handled by
                # approve_payment's lock) nor approved. Real money, nothing
                # auto-delivered -- needs a human.
                log.error(
                    "oxapay payment %s reported paid but local status is "
                    "'%s' -- needs manual reconciliation", payment_id, cur)
                await notify_admins_text(
                    bot, f"⚠️ OxaPay payment #{payment_id} was paid but "
                         f"local status is '{cur}' — needs manual review.")
            # cur == "approved" means this was just a second, redundant
            # delivery racing the first -- already delivered, nothing more to do.
        return "paid"

    if info.status in OXAPAY_FAILURE_STATUSES:
        try:
            await reject_payment(
                bot, payment_id, admin_id=None,
                reason=f"OxaPay status: {info.status}")
        except FulfilError:
            pass  # already resolved (approved/rejected/expired) -- fine
        return "failed"

    return "pending"


MANUAL_INFO_DEFAULT_PROMPT = (
    "Reply with the email address (or account username) you'd like this "
    "order linked to."
)

# aiogram drops a reply_markup=None field from the outgoing request entirely
# (build_form_data skips falsy values) rather than sending it as "no
# keyboard" -- so editMessageText with reply_markup=None leaves whatever
# inline keyboard was already on the message untouched. An explicitly empty
# InlineKeyboardMarkup is the only way to actually clear one on edit; used
# by every step of the manual-delivery message below so the "Submit
# details" button is removed the moment it's acted on, not left dangling.
_CLEAR_KB = InlineKeyboardMarkup(inline_keyboard=[])


async def deliver_to_user(bot: Bot, order: asyncpg.Record, keys: list[str]) -> None:
    """Send the goods. Files for bulk, plain text otherwise."""
    if order is None:
        return
    product = await db.fetchrow("SELECT * FROM products WHERE id=$1", order["product_id"])
    name = product["name"] if product else "your order"

    if not keys:
        # Manual delivery: don't just say "we'll send it shortly" and leave
        # the admin guessing what to activate -- ask the buyer for whatever
        # the product needs (see MANUAL DELIVERY section below) via a
        # one-tap prompt, so their answer lands in the admin chat instead of
        # a back-and-forth DM. Sent directly (not via notify_user) because
        # the whole exchange -- this notice, the prompt, the "received"
        # confirmation, and eventually "activated" -- all edit *this one*
        # message in place rather than piling up separate messages, so its
        # chat_id/message_id need to be captured and persisted on the order.
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✍️ Submit details", callback_data=f"subinfo:{order['id']}")
        ]])
        tid = await db.fetchval("SELECT telegram_id FROM store_users WHERE id=$1", order["user_id"])
        if tid:
            try:
                sent = await bot.send_message(
                    int(tid),
                    f"✅ <b>Payment confirmed</b>\n\nOrder <code>#{order['id']}</code> — {name}\n\n"
                    f"🛠 One more step to activate it — tap below and send what we need.",
                    reply_markup=kb,
                )
                await db.execute(
                    "UPDATE orders SET manual_msg_chat_id=$1, manual_msg_id=$2 WHERE id=$3",
                    sent.chat.id, sent.message_id, order["id"],
                )
            except (TelegramForbiddenError, TelegramBadRequest) as e:
                log.warning("cannot message user %s: %s", order["user_id"], e)
            except Exception:
                log.exception("manual delivery notice failed for order %s", order["id"])
        return

    body = "\n".join(f"<code>{k}</code>" for k in keys)
    text = (
        f"✅ <b>Order delivered</b>\n\n"
        f"Order <code>#{order['id']}</code> — {name}\n"
        f"Quantity: {order['quantity']}\n"
        f"{receipt_block(order)}\n\n"
        f"{body}\n\n"
        f"<i>Save these now — keep them private.</i>"
    )

    if product and product["deliver_as_file"] or len(text) > 3800:
        payload = "\n".join(keys).encode()
        try:
            tid = await db.fetchval(
                "SELECT telegram_id FROM store_users WHERE id=$1", order["user_id"])
            await bot.send_document(
                int(tid),
                BufferedInputFile(payload, filename=f"order-{order['id']}.txt"),
                caption=(f"✅ Order #{order['id']} — {name} ({order['quantity']} item(s))\n"
                        f"{receipt_block(order)}"),
            )
            return
        except Exception:
            log.exception("file delivery failed, falling back to text")

    await notify_user(bot, order["user_id"], text)


async def activate_manual_delivery(order_id: int) -> asyncpg.Record | None:
    """Marks a manual-delivery order delivered and closes the loop with the
    buyer -- shared by both the in-Telegram admin "Mark activated" action and
    the web panel's Activate button, so the "which message to edit, what to
    say" logic lives in exactly one place. Returns None if the order wasn't
    found or wasn't awaiting this (already activated, or not manual)."""
    row = await db.fetchrow(
        """UPDATE orders SET status='delivered', delivered_at=NOW()
            WHERE id=$1 AND status='processing'
           RETURNING user_id, product_id, manual_msg_chat_id, manual_msg_id,
                     total, currency, payment_method, created_at""",
        order_id)
    if not row:
        return None
    product = await db.fetchrow("SELECT name FROM products WHERE id=$1", row["product_id"])
    activated_text = (
        f"✅ <b>Activated</b>\n\nOrder <code>#{order_id}</code>"
        + (f" — {product['name']}" if product else "") +
        f"\n{receipt_block(row)}\n\n"
        "Your order is live — thanks for your purchase!"
    )
    # Closes the loop on the same message the buyer already saw ("Payment
    # confirmed" -> prompt -> "Received") rather than a completely separate
    # thread -- falls back to a fresh send only if that message is gone or
    # past Telegram's 48h edit window.
    edited = False
    if row["manual_msg_chat_id"] and row["manual_msg_id"]:
        try:
            await bot.edit_message_text(
                chat_id=row["manual_msg_chat_id"], message_id=row["manual_msg_id"],
                text=activated_text, reply_markup=_CLEAR_KB)
            edited = True
        except TelegramBadRequest:
            pass
    if edited:
        # Editing an existing message never pings the buyer -- Telegram
        # doesn't push a notification for edits, only for new messages. This
        # made "Mark activated" look like it silently did nothing even
        # though the edit above succeeded. A short separate message
        # guarantees an actual notification lands, on top of the tidy
        # edited thread above for anyone who scrolls back to it.
        await notify_user(
            bot, row["user_id"],
            f"🔔 Order <code>#{order_id}</code> is now active — see above for details.")
    else:
        await notify_user(bot, row["user_id"], activated_text)
    return row


# =============================================================================
#  RESTOCK ALERTS
# =============================================================================

async def notify_restock(bot: Bot, product_id: int) -> int:
    """Tell everyone waiting that a product is back. Returns notified count."""
    watchers = await db.fetch(
        """SELECT r.id, r.user_id FROM restock_alerts r
            WHERE r.product_id=$1 AND r.notified=FALSE""",
        product_id,
    )
    if not watchers:
        return 0

    product = await db.fetchrow("SELECT * FROM products WHERE id=$1", product_id)
    if not product:
        return 0

    sent = 0
    for w in watchers:
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🛒 Buy now", callback_data=f"prod:{product_id}")
        ]])
        ok = await notify_user(
            bot, w["user_id"],
            f"🔔 <b>Back in stock</b>\n\n{product['name']} is available again.",
            markup=kb,
        )
        if ok:
            sent += 1
        await db.execute("UPDATE restock_alerts SET notified=TRUE WHERE id=$1", w["id"])
        await asyncio.sleep(0.05)
    log.info("restock: notified %s watchers for product %s", sent, product_id)
    return sent

# =============================================================================
#  I18N
# =============================================================================

# Selectable in both the first-run picker (replaces the old "which
# currency" prompt) and the persistent Language settings screen (cb_lang) --
# one shared list so the two never drift apart. "hi" has no STRINGS
# override (falls back to English chrome labels via t()) and DeepL never
# touches it (DEEPL_TARGET_LANG has no "hi" entry, per the deliberate
# "Hindi stays manual" design -- admins fill in *_hi columns directly for
# product content); it's still fully selectable since that's the primary
# audience for this store.
LANGUAGE_OPTIONS = [
    ("hi", "🇮🇳 हिन्दी"), ("en", "🇬🇧 English"), ("es", "🇪🇸 Español"),
    ("fr", "🇫🇷 Français"), ("de", "🇩🇪 Deutsch"), ("pt", "🇵🇹 Português"),
    ("ru", "🇷🇺 Русский"), ("tr", "🇹🇷 Türkçe"), ("ar", "🇸🇦 العربية"),
    ("ja", "🇯🇵 日本語"), ("ko", "🇰🇷 한국어"), ("zh", "🇨🇳 中文"),
]


STRINGS: dict[str, dict[str, str]] = {
    "hi": {},  # chrome labels fall back to English; product content stays manual (localize())
    "en": {
        "menu": "Main menu",
        "shop": "🎉 Shop Deals",
        "deals": "🎁 Deals",
        "wallet": "👛 Wallet",
        "profile": "🙂 Profile",
        "orders": "📌 Orders",
        "referral": "⭐ Refer & Earn",
        "support": "🗨️ Support",
        "api_link": "🔗 API Link",
        "language": "🌐 Language",
        "currency": "💱 Currency",
        "back": "Back",
        "main_menu": "🏠 Main Menu",
        "clear_chat": "🗑 Clear Chat",
        "cancel": "Cancel",
        "buy": "Buy",
        "out_of_stock": "Out of stock",
        "notify_me": "Notify me",
        "in_stock": "In stock",
        "price": "Price",
        "balance": "Balance",
        "shop_title": "Live Digital Drops",
        "pick_product": "Pick a product below:",
        "no_products": "Nothing here yet — check back soon.",
        "banned": "🚫 Your account has been suspended. Contact support if you believe this is an error.",
        "maintenance": "🔧 The store is briefly down for maintenance. Please try again shortly.",
        "select_qty": "Choose your quantity below:",
        "custom_qty": "✏️ Custom Qty",
        "enter_coupon": "Send your coupon code, or tap Skip.",
        "skip": "Skip",
        "pay_wallet": "Pay from wallet",
        "pay_crypto": "Pay with USDT",
        "pay_upi": "Pay with UPI",
        "insufficient": "Not enough balance. Top up or choose another payment method.",
        "topup": "Top up",
        "order_placed": "Order placed.",
        "processing": "⏳ Processing…",
        "fj_prompt": "🔒 <b>Join our channel to continue</b>\n\nPlease join our official channel, then tap Verify to unlock the bot.",
        "fj_join_btn": "📢 Join Channel",
        "fj_verify_btn": "✅ Verify",
        "fj_not_joined": "❌ You haven't joined yet. Please join the channel first, then tap Verify.",
        "fj_verified": "✅ Verified! Welcome.",
    },
    "es": {
        "menu": "Menú principal", "shop": "🎉 Ofertas de la Tienda", "deals": "🎁 Ofertas",
        "wallet": "👛 Billetera", "profile": "🙂 Perfil", "orders": "📌 Pedidos",
        "referral": "⭐ Referir y Ganar", "support": "🗨️ Soporte", "api_link": "🔗 Enlace API",
        "language": "🌐 Idioma", "currency": "💱 Moneda", "back": "Atrás", "cancel": "Cancelar",
        "buy": "Comprar", "out_of_stock": "Agotado", "notify_me": "Notificarme",
        "in_stock": "En stock", "price": "Precio", "balance": "Saldo",
        "shop_title": "Ofertas Digitales en Vivo", "pick_product": "Elige un producto abajo:",
        "no_products": "Aún no hay nada aquí — vuelve pronto.",
        "banned": "🚫 Tu cuenta ha sido suspendida. Contacta con soporte si crees que es un error.",
        "maintenance": "🔧 La tienda está en mantenimiento breve. Inténtalo de nuevo en unos minutos.",
        "select_qty": "Elige tu cantidad abajo:", "custom_qty": "✏️ Cantidad personalizada",
        "enter_coupon": "Envía tu código de cupón, o pulsa Omitir.", "skip": "Omitir",
        "pay_wallet": "Pagar con billetera", "pay_crypto": "Pagar con USDT",
        "pay_upi": "Pagar con UPI",
        "insufficient": "Saldo insuficiente. Recarga o elige otro método de pago.",
        "topup": "Recargar", "order_placed": "Pedido realizado.", "processing": "⏳ Procesando…",
    },
    "fr": {
        "menu": "Menu principal", "shop": "🎉 Offres de la Boutique", "deals": "🎁 Offres",
        "wallet": "👛 Portefeuille", "profile": "🙂 Profil", "orders": "📌 Commandes",
        "referral": "⭐ Parrainer et Gagner", "support": "🗨️ Support", "api_link": "🔗 Lien API",
        "language": "🌐 Langue", "currency": "💱 Devise", "back": "Retour", "cancel": "Annuler",
        "buy": "Acheter", "out_of_stock": "Rupture de stock", "notify_me": "Me notifier",
        "in_stock": "En stock", "price": "Prix", "balance": "Solde",
        "shop_title": "Offres Numériques en Direct",
        "pick_product": "Choisissez un produit ci-dessous :",
        "no_products": "Rien ici pour l'instant — revenez bientôt.",
        "banned": "🚫 Votre compte a été suspendu. Contactez le support si vous pensez qu'il s'agit d'une erreur.",
        "maintenance": "🔧 La boutique est brièvement en maintenance. Réessayez bientôt.",
        "select_qty": "Choisissez votre quantité ci-dessous :",
        "custom_qty": "✏️ Quantité personnalisée",
        "enter_coupon": "Envoyez votre code promo, ou appuyez sur Ignorer.", "skip": "Ignorer",
        "pay_wallet": "Payer avec le portefeuille", "pay_crypto": "Payer avec USDT",
        "pay_upi": "Payer avec UPI",
        "insufficient": "Solde insuffisant. Rechargez ou choisissez un autre moyen de paiement.",
        "topup": "Recharger", "order_placed": "Commande passée.",
        "processing": "⏳ Traitement en cours…",
    },
    "de": {
        "menu": "Hauptmenü", "shop": "🎉 Shop-Angebote", "deals": "🎁 Angebote",
        "wallet": "👛 Wallet", "profile": "🙂 Profil", "orders": "📌 Bestellungen",
        "referral": "⭐ Werben & Verdienen", "support": "🗨️ Support", "api_link": "🔗 API-Link",
        "language": "🌐 Sprache", "currency": "💱 Währung", "back": "Zurück",
        "cancel": "Abbrechen", "buy": "Kaufen", "out_of_stock": "Ausverkauft",
        "notify_me": "Benachrichtigen", "in_stock": "Auf Lager", "price": "Preis",
        "balance": "Guthaben", "shop_title": "Live Digital Drops",
        "pick_product": "Wähle unten ein Produkt:",
        "no_products": "Hier gibt es noch nichts — schau bald wieder vorbei.",
        "banned": "🚫 Dein Konto wurde gesperrt. Kontaktiere den Support, falls du das für einen Fehler hältst.",
        "maintenance": "🔧 Der Shop ist kurz in Wartung. Bitte versuche es gleich noch einmal.",
        "select_qty": "Wähle unten deine Menge:", "custom_qty": "✏️ Individuelle Menge",
        "enter_coupon": "Sende deinen Gutscheincode oder tippe auf Überspringen.",
        "skip": "Überspringen", "pay_wallet": "Mit Wallet bezahlen",
        "pay_crypto": "Mit USDT bezahlen", "pay_upi": "Mit UPI bezahlen",
        "insufficient": "Nicht genug Guthaben. Lade auf oder wähle eine andere Zahlungsmethode.",
        "topup": "Aufladen", "order_placed": "Bestellung aufgegeben.",
        "processing": "⏳ Wird verarbeitet…",
    },
    "pt": {
        "menu": "Menu principal", "shop": "🎉 Ofertas da Loja", "deals": "🎁 Ofertas",
        "wallet": "👛 Carteira", "profile": "🙂 Perfil", "orders": "📌 Pedidos",
        "referral": "⭐ Indique e Ganhe", "support": "🗨️ Suporte", "api_link": "🔗 Link da API",
        "language": "🌐 Idioma", "currency": "💱 Moeda", "back": "Voltar", "cancel": "Cancelar",
        "buy": "Comprar", "out_of_stock": "Fora de estoque", "notify_me": "Notificar-me",
        "in_stock": "Em estoque", "price": "Preço", "balance": "Saldo",
        "shop_title": "Ofertas Digitais ao Vivo", "pick_product": "Escolha um produto abaixo:",
        "no_products": "Ainda não há nada aqui — volte em breve.",
        "banned": "🚫 Sua conta foi suspensa. Entre em contato com o suporte se achar que isso é um erro.",
        "maintenance": "🔧 A loja está em manutenção rápida. Tente novamente em instantes.",
        "select_qty": "Escolha sua quantidade abaixo:", "custom_qty": "✏️ Quantidade personalizada",
        "enter_coupon": "Envie seu código de cupom, ou toque em Pular.", "skip": "Pular",
        "pay_wallet": "Pagar com carteira", "pay_crypto": "Pagar com USDT",
        "pay_upi": "Pagar com UPI",
        "insufficient": "Saldo insuficiente. Recarregue ou escolha outro método de pagamento.",
        "topup": "Recarregar", "order_placed": "Pedido realizado.", "processing": "⏳ Processando…",
    },
    "ru": {
        "menu": "Главное меню", "shop": "🎉 Магазин", "deals": "🎁 Акции",
        "wallet": "👛 Кошелёк", "profile": "🙂 Профиль", "orders": "📌 Заказы",
        "referral": "⭐ Пригласить и заработать", "support": "🗨️ Поддержка",
        "api_link": "🔗 API-ссылка", "language": "🌐 Язык", "currency": "💱 Валюта",
        "back": "Назад", "cancel": "Отмена", "buy": "Купить", "out_of_stock": "Нет в наличии",
        "notify_me": "Уведомить меня", "in_stock": "В наличии", "price": "Цена",
        "balance": "Баланс", "shop_title": "Живые цифровые товары",
        "pick_product": "Выберите товар ниже:", "no_products": "Пока пусто — загляните позже.",
        "banned": "🚫 Ваш аккаунт заблокирован. Обратитесь в поддержку, если считаете это ошибкой.",
        "maintenance": "🔧 Магазин временно на обслуживании. Попробуйте снова через некоторое время.",
        "select_qty": "Выберите количество ниже:", "custom_qty": "✏️ Другое количество",
        "enter_coupon": "Отправьте код купона или нажмите «Пропустить».", "skip": "Пропустить",
        "pay_wallet": "Оплатить с кошелька", "pay_crypto": "Оплатить в USDT",
        "pay_upi": "Оплатить через UPI",
        "insufficient": "Недостаточно средств. Пополните баланс или выберите другой способ оплаты.",
        "topup": "Пополнить", "order_placed": "Заказ оформлен.", "processing": "⏳ Обработка…",
    },
    "tr": {
        "menu": "Ana menü", "shop": "🎉 Mağaza Fırsatları", "deals": "🎁 Fırsatlar",
        "wallet": "👛 Cüzdan", "profile": "🙂 Profil", "orders": "📌 Siparişler",
        "referral": "⭐ Davet Et ve Kazan", "support": "🗨️ Destek",
        "api_link": "🔗 API Bağlantısı", "language": "🌐 Dil", "currency": "💱 Para birimi",
        "back": "Geri", "cancel": "İptal", "buy": "Satın al", "out_of_stock": "Stokta yok",
        "notify_me": "Beni bilgilendir", "in_stock": "Stokta var", "price": "Fiyat",
        "balance": "Bakiye", "shop_title": "Canlı Dijital Ürünler",
        "pick_product": "Aşağıdan bir ürün seçin:",
        "no_products": "Henüz burada bir şey yok — yakında tekrar bakın.",
        "banned": "🚫 Hesabınız askıya alındı. Bunun bir hata olduğunu düşünüyorsanız destek ile iletişime geçin.",
        "maintenance": "🔧 Mağaza kısa bir bakımda. Lütfen birazdan tekrar deneyin.",
        "select_qty": "Aşağıdan miktarınızı seçin:", "custom_qty": "✏️ Özel Miktar",
        "enter_coupon": "Kupon kodunuzu gönderin ya da Atla'ya dokunun.", "skip": "Atla",
        "pay_wallet": "Cüzdandan öde", "pay_crypto": "USDT ile öde", "pay_upi": "UPI ile öde",
        "insufficient": "Bakiye yetersiz. Bakiye yükleyin ya da başka bir ödeme yöntemi seçin.",
        "topup": "Bakiye yükle", "order_placed": "Sipariş oluşturuldu.", "processing": "⏳ İşleniyor…",
    },
    "ar": {
        "menu": "القائمة الرئيسية", "shop": "🎉 عروض المتجر", "deals": "🎁 العروض",
        "wallet": "👛 المحفظة", "profile": "🙂 الملف الشخصي", "orders": "📌 الطلبات",
        "referral": "⭐ ادعُ واربح", "support": "🗨️ الدعم", "api_link": "🔗 رابط API",
        "language": "🌐 اللغة", "currency": "💱 العملة", "back": "رجوع", "cancel": "إلغاء",
        "buy": "شراء", "out_of_stock": "غير متوفر", "notify_me": "أعلمني", "in_stock": "متوفر",
        "price": "السعر", "balance": "الرصيد", "shop_title": "عروض رقمية مباشرة",
        "pick_product": "اختر منتجًا أدناه:",
        "no_products": "لا يوجد شيء هنا بعد — تحقق مرة أخرى قريبًا.",
        "banned": "🚫 تم تعليق حسابك. تواصل مع الدعم إذا كنت تعتقد أن هذا خطأ.",
        "maintenance": "🔧 المتجر متوقف مؤقتًا للصيانة. حاول مرة أخرى بعد قليل.",
        "select_qty": "اختر الكمية أدناه:", "custom_qty": "✏️ كمية مخصصة",
        "enter_coupon": "أرسل كود الخصم، أو اضغط تخطي.", "skip": "تخطي",
        "pay_wallet": "الدفع من المحفظة", "pay_crypto": "الدفع بـ USDT", "pay_upi": "الدفع عبر UPI",
        "insufficient": "الرصيد غير كافٍ. اشحن رصيدك أو اختر طريقة دفع أخرى.",
        "topup": "شحن الرصيد", "order_placed": "تم تقديم الطلب.", "processing": "⏳ جارٍ المعالجة…",
    },
    "ja": {
        "menu": "メインメニュー", "shop": "🎉 ショップセール", "deals": "🎁 セール",
        "wallet": "👛 ウォレット", "profile": "🙂 プロフィール", "orders": "📌 注文履歴",
        "referral": "⭐ 紹介して稼ぐ", "support": "🗨️ サポート", "api_link": "🔗 APIリンク",
        "language": "🌐 言語", "currency": "💱 通貨", "back": "戻る", "cancel": "キャンセル",
        "buy": "購入", "out_of_stock": "在庫切れ", "notify_me": "入荷通知を受け取る",
        "in_stock": "在庫あり", "price": "価格", "balance": "残高",
        "shop_title": "ライブデジタルドロップ", "pick_product": "下から商品を選んでください:",
        "no_products": "まだ商品がありません。またあとで確認してください。",
        "banned": "🚫 アカウントが停止されました。誤りだと思われる場合はサポートまでご連絡ください。",
        "maintenance": "🔧 ただいまメンテナンス中です。しばらくしてから再度お試しください。",
        "select_qty": "下から数量を選んでください:", "custom_qty": "✏️ 数量を指定",
        "enter_coupon": "クーポンコードを送信するか、スキップをタップしてください。", "skip": "スキップ",
        "pay_wallet": "ウォレットで支払う", "pay_crypto": "USDTで支払う", "pay_upi": "UPIで支払う",
        "insufficient": "残高が不足しています。チャージするか、別の支払い方法を選んでください。",
        "topup": "チャージ", "order_placed": "注文が完了しました。", "processing": "⏳ 処理中…",
    },
    "ko": {
        "menu": "메인 메뉴", "shop": "🎉 스토어 딜", "deals": "🎁 딜", "wallet": "👛 지갑",
        "profile": "🙂 프로필", "orders": "📌 주문 내역", "referral": "⭐ 추천하고 적립",
        "support": "🗨️ 고객지원", "api_link": "🔗 API 링크", "language": "🌐 언어",
        "currency": "💱 통화", "back": "뒤로", "cancel": "취소", "buy": "구매",
        "out_of_stock": "품절", "notify_me": "재입고 알림", "in_stock": "재고 있음",
        "price": "가격", "balance": "잔액", "shop_title": "라이브 디지털 드롭",
        "pick_product": "아래에서 상품을 선택하세요:",
        "no_products": "아직 상품이 없습니다 — 곧 다시 확인해주세요.",
        "banned": "🚫 계정이 정지되었습니다. 오류라고 생각되면 고객지원에 문의하세요.",
        "maintenance": "🔧 스토어가 잠시 점검 중입니다. 잠시 후 다시 시도해주세요.",
        "select_qty": "아래에서 수량을 선택하세요:", "custom_qty": "✏️ 수량 직접 입력",
        "enter_coupon": "쿠폰 코드를 보내거나 건너뛰기를 누르세요.", "skip": "건너뛰기",
        "pay_wallet": "지갑으로 결제", "pay_crypto": "USDT로 결제", "pay_upi": "UPI로 결제",
        "insufficient": "잔액이 부족합니다. 충전하거나 다른 결제 방법을 선택하세요.",
        "topup": "충전", "order_placed": "주문이 완료되었습니다.", "processing": "⏳ 처리 중…",
    },
    "zh": {
        "menu": "主菜单", "shop": "🎉 商店优惠", "deals": "🎁 优惠", "wallet": "👛 钱包",
        "profile": "🙂 个人资料", "orders": "📌 订单", "referral": "⭐ 邀请赚钱",
        "support": "🗨️ 客服支持", "api_link": "🔗 API 链接", "language": "🌐 语言",
        "currency": "💱 货币", "back": "返回", "cancel": "取消", "buy": "购买",
        "out_of_stock": "缺货", "notify_me": "到货通知", "in_stock": "有货", "price": "价格",
        "balance": "余额", "shop_title": "实时数字商品", "pick_product": "请选择以下商品：",
        "no_products": "暂时还没有商品，请稍后再来看看。",
        "banned": "🚫 您的账户已被封禁。如果您认为这是误判，请联系客服。",
        "maintenance": "🔧 商店正在短暂维护中，请稍后再试。",
        "select_qty": "请选择以下数量：", "custom_qty": "✏️ 自定义数量",
        "enter_coupon": "发送您的优惠券代码，或点击跳过。", "skip": "跳过",
        "pay_wallet": "使用钱包支付", "pay_crypto": "使用 USDT 支付", "pay_upi": "使用 UPI 支付",
        "insufficient": "余额不足，请充值或选择其他支付方式。", "topup": "充值",
        "order_placed": "订单已提交。", "processing": "⏳ 处理中…",
    },
}


def t(lang: str, key: str) -> str:
    """Translate a UI key. Admin UI-editor overrides (stored as ui:<lang>:<key>
    in settings) win over the built-in STRINGS, so any label can be reworded
    from Telegram without touching code."""
    override = settings.ui_override(lang, key)
    if override is not None:
        return override
    return STRINGS.get(lang, STRINGS["en"]).get(key, STRINGS["en"].get(key, key))


# =============================================================================
#  DYNAMIC TRANSLATION (admin-authored free text)
# =============================================================================
#  t()/STRINGS above covers built-in UI labels, hand-translated once per
#  language. Admin-authored free text (welcome message, store header/footer,
#  product & category name/description) is a different problem: there's only
#  ever one canonical copy (plus, historically, a single bonus Hindi column),
#  so switching a user's language left that text untranslated. This section
#  auto-translates it via DeepL and caches the result, HTML formatting intact
#  (tag_handling=html keeps <b>/<i>/spoiler/etc. exactly as authored).
#
#  Hindi is a deliberate exception: DeepL doesn't support it, so the existing
#  manual name_hi/description_hi columns remain the source of truth there --
#  see localize() below.

# Bot language code -> DeepL target_lang code. Any bot language not listed
# here (currently just "hi") falls back to the untranslated source text.
DEEPL_TARGET_LANG = {
    "en": "EN-US", "es": "ES", "fr": "FR", "de": "DE", "pt": "PT-PT",
    "ru": "RU", "tr": "TR", "ar": "AR", "ja": "JA", "ko": "KO", "zh": "ZH",
}


class Translator:
    """DeepL client. Read-only translation of display text -- never touches
    payments, orders or user data. Fails open (returns None) on any error so
    a translation-provider outage can never break a render; callers fall
    back to the original text."""

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=httpx.Timeout(15.0))
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def translate(self, text: str, target_lang_code: str) -> str | None:
        if not CFG.deepl_api_key:
            return None
        host = "https://api-free.deepl.com" if CFG.deepl_api_key.endswith(":fx") \
            else "https://api.deepl.com"
        try:
            c = await self.client()
            r = await c.post(
                f"{host}/v2/translate",
                headers={"Authorization": f"DeepL-Auth-Key {CFG.deepl_api_key}"},
                data={"text": text, "target_lang": target_lang_code, "tag_handling": "html"},
            )
            if r.status_code != 200:
                log.warning("deepl translate failed: HTTP %s %s", r.status_code, r.text[:200])
                return None
            translations = (r.json() or {}).get("translations") or []
            return translations[0].get("text") if translations else None
        except (httpx.HTTPError, json.JSONDecodeError, KeyError, IndexError):
            log.exception("deepl translate error")
            return None


translator = Translator()

# Short in-memory TTL layer in front of the DB cache -- same shape as
# Settings/_EMOJI_MAP -- so a hot render path (show_menu on every /start)
# doesn't hit text_translations on every request once warm.
_TRANSLATION_MEM_CACHE: dict[tuple[str, str], tuple[str, float]] = {}
_TRANSLATION_MEM_TTL = 300.0


async def translate_dynamic(text: str, target_lang: str) -> str:
    """Translate admin-authored free text into target_lang, cached. Falls
    back to the original text (never blank, never raises) if target_lang
    equals the source language, isn't DeepL-supported, no API key is
    configured, or the API call fails for any reason."""
    text = (text or "").strip()
    if not text:
        return text

    default_lang = await settings.get("default_language", "en")
    if target_lang == default_lang:
        return text

    deepl_code = DEEPL_TARGET_LANG.get(target_lang)
    if not deepl_code or not CFG.deepl_api_key:
        return text

    text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
    cache_key = (text_hash, target_lang)
    now = time.monotonic()
    cached = _TRANSLATION_MEM_CACHE.get(cache_key)
    if cached and (now - cached[1]) < _TRANSLATION_MEM_TTL:
        return cached[0]

    row = await db.fetchrow(
        "SELECT translated_text FROM text_translations WHERE text_hash=$1 AND target_lang=$2",
        text_hash, target_lang,
    )
    if row:
        _TRANSLATION_MEM_CACHE[cache_key] = (row["translated_text"], now)
        return row["translated_text"]

    translated = await translator.translate(text, deepl_code)
    if not translated:
        return text  # fail open -- provider error, quota exhausted, etc.

    try:
        await db.execute(
            """INSERT INTO text_translations(text_hash, target_lang, source_text, translated_text)
               VALUES($1,$2,$3,$4)
               ON CONFLICT (text_hash, target_lang) DO UPDATE SET translated_text=$4""",
            text_hash, target_lang, text, translated,
        )
    except Exception:
        log.exception("failed to cache translation")
    _TRANSLATION_MEM_CACHE[cache_key] = (translated, now)
    return translated


async def localize(base: str, hi_value: str | None, lang: str) -> str:
    """Resolve an admin-authored field (product/category name or
    description) for the given user language. The manual Hindi column wins
    for 'hi' when set -- unchanged existing behavior -- otherwise every
    other enabled language is auto-translated."""
    if lang == "hi" and hi_value:
        return hi_value
    return await translate_dynamic(base or "", lang)


# ---- Global Emoji Replacement Manager -----------------------------------------
#  One dictionary, unicode_emoji -> premium_emoji_id (table: emoji_map). Every
#  outgoing message/caption/button passes through the EmojiRenderMiddleware
#  registered on `bot` below, which is the single place any unicode emoji is
#  ever swapped for its Telegram Premium custom-emoji rendering. There is no
#  per-slot or per-button configuration — authors just write the plain
#  unicode emoji in text/labels as normal, and it's upgraded automatically
#  wherever a mapping exists; unmapped emoji are left as-is.

_EMOJI_MAP: dict[str, str] = {}      # unicode emoji -> premium emoji id
_EMOJI_MAP_AT: float = 0.0
_EMOJI_MAP_TTL = 60.0
_EMOJI_PATTERN: "re.Pattern[str] | None" = None   # compiled alongside _EMOJI_MAP

_VS16 = "️"  # variation selector-16 ("emoji presentation")


def _compile_emoji_pattern(emoji_map: dict[str, str]) -> "re.Pattern[str] | None":
    """One alternation over every mapped emoji's base glyph (VS16 stripped),
    each allowing an optional trailing VS16. Telegram clients inconsistently
    attach VS16 to emoji (admin-captured glyphs from the Telegram app almost
    always carry it; glyphs hand-typed into source often don't), so matching
    must be VS16-agnostic in both directions or a correctly mapped emoji can
    silently fail to match at every call site that spells it differently."""
    if not emoji_map:
        return None
    bases = {key.rstrip(_VS16) or key for key in emoji_map}
    alts = sorted((re.escape(b) + _VS16 + "?" for b in bases), key=len, reverse=True)
    return re.compile("|".join(alts))


def _lookup_emoji_id(matched: str) -> str | None:
    """Resolve a regex match back to its premium id, independent of whether
    the DB key or the matched text happens to carry VS16."""
    emoji_id = _EMOJI_MAP.get(matched)
    if emoji_id is not None:
        return emoji_id
    base = matched.rstrip(_VS16) or matched
    return _EMOJI_MAP.get(base) or _EMOJI_MAP.get(base + _VS16)


async def load_emoji_map(force: bool = False) -> dict[str, str]:
    """Warm cache of unicode_emoji -> premium_emoji_id."""
    global _EMOJI_MAP, _EMOJI_MAP_AT, _EMOJI_PATTERN
    if not force and _EMOJI_MAP and (time.monotonic() - _EMOJI_MAP_AT) < _EMOJI_MAP_TTL:
        return _EMOJI_MAP
    try:
        rows = await db.fetch("SELECT unicode_emoji, premium_emoji_id FROM emoji_map")
        _EMOJI_MAP = {r["unicode_emoji"]: r["premium_emoji_id"] for r in rows}
        _EMOJI_PATTERN = _compile_emoji_pattern(_EMOJI_MAP)
        _EMOJI_MAP_AT = time.monotonic()
    except Exception:
        log.exception("could not load emoji map")
    return _EMOJI_MAP


def invalidate_emoji_map() -> None:
    """Force the next load_emoji_map() call to re-read the DB."""
    global _EMOJI_MAP_AT
    _EMOJI_MAP_AT = 0.0


def render_emoji_text(html: str) -> str:
    """Wrap every mapped unicode emoji found in HTML text/captions with a
    <tg-emoji> entity, in a single left-to-right pass over the original
    string. A single pass (rather than one .replace() per mapped emoji)
    matters: replacing emoji one at a time re-scans text that already
    contains previously-inserted <tg-emoji> tags, so a shorter mapped emoji
    that happens to be a substring of an already-wrapped glyph (e.g. a base
    emoji vs. its VS16 variant) would get matched again inside the tag it
    was just wrapped in and nest broken markup."""
    if not _EMOJI_PATTERN or not html:
        return html

    def _wrap(m: "re.Match[str]") -> str:
        matched = m.group(0)
        emoji_id = _lookup_emoji_id(matched)
        if emoji_id is None:
            return matched
        return f'<tg-emoji emoji-id="{emoji_id}">{matched}</tg-emoji>'

    return _EMOJI_PATTERN.sub(_wrap, html)


def render_emoji_buttons(markup: "InlineKeyboardMarkup") -> "InlineKeyboardMarkup":
    """Give any inline button whose label contains a mapped emoji a premium
    icon (Bot API 9.4+ icon_custom_emoji_id) and strip the plain glyph from
    the label text so it isn't shown twice."""
    if not _EMOJI_PATTERN:
        return markup
    changed = False
    new_rows = []
    for row in markup.inline_keyboard:
        new_row = []
        for btn in row:
            if btn.text and not btn.icon_custom_emoji_id:
                m = _EMOJI_PATTERN.search(btn.text)
                if m:
                    emoji_id = _lookup_emoji_id(m.group(0))
                    if emoji_id is not None:
                        stripped = (btn.text[:m.start()] + btn.text[m.end():]).strip()
                        btn = btn.model_copy(update={
                            "text": stripped or btn.text,
                            "icon_custom_emoji_id": emoji_id,
                        })
                        changed = True
            new_row.append(btn)
        new_rows.append(new_row)
    return markup.model_copy(update={"inline_keyboard": new_rows}) if changed else markup


# Bot API methods that create a brand-new message (as opposed to editing one
# that's already tracked) -- these are the ones worth logging to
# bot_messages for "Clear Chat" to be able to delete later.
_TRACKED_SEND_METHODS = {
    "SendMessage", "SendPhoto", "SendDocument", "SendVideo", "SendAnimation",
    "SendAudio", "SendVoice", "SendSticker", "SendMediaGroup", "CopyMessage",
}

# Set around broadcast sends (see send_broadcast_item) so the middleware can
# tag those rows kind='broadcast' instead of the default 'nav' -- Clear Chat
# only ever deletes 'nav' rows, so announcements a user might still want to
# see later survive a chat clear. Using a contextvar keeps this centralized
# at the one tracking choke point rather than threading a flag through every
# send call site.
_broadcast_send_ctx: "contextvars.ContextVar[bool]" = contextvars.ContextVar(
    "_broadcast_send_ctx", default=False
)


async def _track_bot_message(chat_id: int, message_id: int, kind: str) -> None:
    """Fire-and-forget insert -- never adds latency to the send path, and a
    tracking failure must never take down the actual message delivery."""
    try:
        await db.execute(
            "INSERT INTO bot_messages(chat_id, message_id, kind) VALUES($1,$2,$3)",
            chat_id, message_id, kind,
        )
    except Exception:
        log.exception("bot_messages tracking failed")


def _maybe_track_sent(method, result) -> None:
    if type(method).__name__ not in _TRACKED_SEND_METHODS:
        return
    kind = "broadcast" if _broadcast_send_ctx.get() else "nav"
    msgs = result if isinstance(result, list) else [result]
    for m in msgs:
        if isinstance(m, Message):
            asyncio.create_task(_track_bot_message(m.chat.id, m.message_id, kind))


class EmojiRenderMiddleware(BaseRequestMiddleware):
    """aiogram request middleware: the one central renderer every outgoing
    Bot API call passes through. Applies the global emoji_map to text/
    caption (as <tg-emoji> HTML) and to inline keyboard buttons (as
    icon_custom_emoji_id), across every sendMessage/editMessageText/
    sendPhoto/editMessageCaption/etc. call in the bot, with no per-call-site
    changes needed. Also the single choke point used to log newly-sent
    message ids for the "Clear Chat" feature (see _maybe_track_sent)."""

    async def __call__(self, make_request, bot, method):
        await load_emoji_map()
        if not _EMOJI_MAP:
            result = await make_request(bot, method)
            _maybe_track_sent(method, result)
            return result

        updates: dict = {}
        if hasattr(method, "parse_mode"):
            text = getattr(method, "text", None)
            if isinstance(text, str):
                new_text = render_emoji_text(text)
                if new_text != text:
                    updates["text"] = new_text
                    updates["parse_mode"] = "HTML"
            caption = getattr(method, "caption", None)
            if isinstance(caption, str):
                new_caption = render_emoji_text(caption)
                if new_caption != caption:
                    updates["caption"] = new_caption
                    updates["parse_mode"] = "HTML"

        # editMessageMedia (used to swap a photo in place — e.g. navigating
        # between products that each have their own image) carries its
        # caption inside `media`, not as a top-level field, so the block
        # above never sees it. That's a real, distinct gap: without this,
        # every in-place photo edit silently skips emoji replacement while a
        # fresh photo send (which does have a top-level caption) works fine —
        # exactly the "premium emoji revert after editing" symptom.
        media = getattr(method, "media", None)
        if media is not None and hasattr(media, "caption"):
            media_caption = getattr(media, "caption", None)
            if isinstance(media_caption, str):
                new_media_caption = render_emoji_text(media_caption)
                if new_media_caption != media_caption:
                    updates["media"] = media.model_copy(update={
                        "caption": new_media_caption,
                        "parse_mode": "HTML",
                    })

        reply_markup = getattr(method, "reply_markup", None)
        if isinstance(reply_markup, InlineKeyboardMarkup):
            new_markup = render_emoji_buttons(reply_markup)
            if new_markup is not reply_markup:
                updates["reply_markup"] = new_markup

        if updates:
            method = method.model_copy(update=updates)
        result = await make_request(bot, method)
        _maybe_track_sent(method, result)
        return result


# ---- customer button visibility & order --------------------------------------
#  Stored as settings so the UI editor can reorder/hide menu buttons without
#  a code change. Format: comma-separated keys; missing keys are hidden.

DEFAULT_MENU_ORDER = "shop,wallet,profile,support,orders,referral,api_link,language"

MENU_BUTTON_ROUTES = {
    "shop": "shop:1", "deals": "deals", "wallet": "wallet", "profile": "profile",
    "orders": "orders:1", "referral": "ref", "support": "support",
    "api_link": "apilink", "language": "lang", "currency": "cur",
}

# Keys that always take their own full-width row instead of being paired
# two-per-row — matches the reference layout (a single featured "Shop
# Deals" row up top, a solo "Language" row at the bottom).
FULL_WIDTH_MENU_KEYS = {"shop", "language"}


async def menu_layout() -> list[str]:
    raw = await settings.get("menu_order", DEFAULT_MENU_ORDER)
    keys = [k.strip() for k in raw.split(",") if k.strip() in MENU_BUTTON_ROUTES]
    return keys or DEFAULT_MENU_ORDER.split(",")


UI_EDITABLE_KEYS: dict[str, list[str]] = {
    "Main menu": ["menu", "shop", "deals", "wallet", "profile", "orders",
                  "referral", "support", "api_link", "language", "currency"],
    "Buttons": ["back", "cancel", "buy", "notify_me", "skip", "topup",
                "pay_wallet", "pay_crypto", "pay_upi", "custom_qty"],
    "Messages": ["shop_title", "pick_product", "no_products", "banned", "maintenance",
                 "select_qty", "enter_coupon", "insufficient", "order_placed",
                 "processing", "in_stock", "price", "balance", "out_of_stock"],
}


# =============================================================================
#  BOT SETUP
# =============================================================================

class _KeepAliveSession(AiohttpSession):
    """aiohttp's TCPConnector defaults to a 15s keepalive_timeout -- with
    real usage (a user reads a screen, thinks, taps a button 30-60s later)
    the underlying connection to api.telegram.org has almost always already
    been closed by the time the next Bot API call goes out, so it pays a
    full fresh TCP+TLS handshake. Measured directly against this box: a
    fresh connection to api.telegram.org costs ~480ms (167ms TCP connect +
    163ms TLS handshake + ~155ms first response) versus ~160ms on a reused
    one. That gap was showing up as "button response feels slow" and
    "/start feels slow" -- both are very often the first Bot API call after
    more than 15s of idle. AiohttpSession has no constructor option for
    this (its connector kwargs are a fixed dict set in __init__), so this
    overrides it after the fact, once, at session creation."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._connector_init["keepalive_timeout"] = 75


bot = Bot(
    token=CFG.bot_token,
    session=_KeepAliveSession(),
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
bot.session.middleware.register(EmojiRenderMiddleware())
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)


class Flow(StatesGroup):
    quantity = State()
    coupon = State()
    tx_id = State()
    topup_amount = State()
    support_message = State()
    manual_info = State()


# ---- rate limiting -----------------------------------------------------------
#  Simple sliding window per telegram id. Keeps a single abusive user from
#  hammering the DB; not a substitute for network-level protection.

_rate: dict[int, list[float]] = {}


def rate_ok(tid: int, limit: int | None = None) -> bool:
    limit = limit or CFG.rate_limit_per_minute
    now = time.monotonic()
    window = _rate.setdefault(tid, [])
    window[:] = [t0 for t0 in window if now - t0 < 60]
    if len(window) >= limit:
        return False
    window.append(now)
    return True


#  Membership status doesn't flip from moment to moment, but without a
#  cache this ran a live get_chat_member Telegram API round trip on every
#  single message/callback (via guard() below) whenever force-join is
#  enabled -- real network latency in front of every tap, for a status
#  that's still correct way longer than 5 minutes later. Bumped from 5min:
#  measured directly, a live get_chat_member call costs ~150-500ms
#  (connection-reuse dependent -- see _KeepAliveSession), and at 5 minutes
#  most real taps (a user reads a screen, decides, comes back) landed on a
#  stale cache anyway and paid that cost repeatedly through one session.
#  An hour is still far tighter than this actually needs to be for a
#  channel-membership gate -- nobody join-then-leaves mid-checkout -- while
#  keeping periodic re-verification against long-term unfollows. The
#  explicit "fj:verify" button (cb_force_join_verify) always passes
#  fresh=True so a user who just joined isn't stuck looking at a stale
#  cached "not joined".
_JOIN_VERIFIED_CACHE: dict[int, tuple[bool, float]] = {}
_JOIN_VERIFIED_TTL = 3600.0


async def is_join_verified(telegram_id: int, *, fresh: bool = False) -> bool:
    """True when force-join is off, unconfigured, or the user has joined the
    required channel. Fails open on Telegram API errors (bad chat id, bot not
    a member of the channel, etc.) so a misconfiguration can't lock every
    user out -- only a genuine "not joined" blocks."""
    if not await settings.get_bool("force_join_enabled", False):
        return True
    channel_id = (await settings.get("force_join_channel_id", "")).strip()
    if not channel_id:
        return True

    if not fresh:
        cached = _JOIN_VERIFIED_CACHE.get(telegram_id)
        if cached and (time.monotonic() - cached[1]) < _JOIN_VERIFIED_TTL:
            return cached[0]

    try:
        member = await bot.get_chat_member(chat_id=channel_id, user_id=telegram_id)
        verified = member.status in ("member", "administrator", "creator")
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        log.warning("force-join check failed for %s: %s", telegram_id, e)
        verified = True

    _JOIN_VERIFIED_CACHE[telegram_id] = (verified, time.monotonic())
    return verified


async def force_join_blocked(telegram_id: int, *, fresh: bool = False) -> bool:
    """True when force-join is on, this user isn't admin-exempt, and they
    have not joined the required channel yet."""
    if await _is_admin_telegram_id(telegram_id):
        return False
    return not await is_join_verified(telegram_id, fresh=fresh)


def force_join_kb(lang: str, channel_url: str) -> InlineKeyboardMarkup:
    rows = []
    if channel_url:
        rows.append([InlineKeyboardButton(text=t(lang, "fj_join_btn"), url=channel_url)])
    rows.append([InlineKeyboardButton(text=t(lang, "fj_verify_btn"), callback_data="fj:verify")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def send_force_join(event: Message | CallbackQuery, lang: str) -> None:
    channel_url = (await settings.get("force_join_channel_url", "")).strip()
    markup = force_join_kb(lang, channel_url)
    text = t(lang, "fj_prompt")
    # edit_or_send handles both Message and CallbackQuery uniformly (and
    # keeps this screen in the tracked single-message flow, logo included,
    # same as everywhere else) -- no need to special-case the event type.
    #
    # force_fresh=True: without it, render_screen's identical-content skip
    # (same text+markup as what's already on screen -> no-op, see
    # render_screen's _render_fp_cache check) meant a still-blocked user
    # pressing /start (or tapping any gated button) a second time got no
    # prompt at all -- looking exactly like the gate had silently opened,
    # even though force_join_blocked was still correctly returning True and
    # the menu was never actually reached. Forcing a fresh send makes the
    # "you still need to join" prompt reappear every single time, not once.
    await edit_or_send(event, text, markup, force_fresh=True)


# Whole-table TTL cache instead of a per-id one: the admins table is tiny
# and rarely changes, and _is_admin_telegram_id runs on every single
# message/callback via guard() -- for the overwhelming majority of hits
# (ordinary customers, not admins) this turns a DB round trip into an
# in-memory set lookup. Explicitly invalidated (force=True) right after
# every admin promote/demote/create/delete so access changes take effect
# immediately rather than waiting out the TTL.
_ADMIN_USERNAMES_CACHE: set[str] | None = None
_ADMIN_USERNAMES_AT = 0.0
_ADMIN_USERNAMES_TTL = 120.0
_ADMIN_USERNAMES_LOCK = asyncio.Lock()


async def _load_admin_usernames(force: bool = False) -> set[str]:
    global _ADMIN_USERNAMES_CACHE, _ADMIN_USERNAMES_AT
    if not force and _ADMIN_USERNAMES_CACHE is not None and \
            (time.monotonic() - _ADMIN_USERNAMES_AT) < _ADMIN_USERNAMES_TTL:
        return _ADMIN_USERNAMES_CACHE
    async with _ADMIN_USERNAMES_LOCK:
        if not force and _ADMIN_USERNAMES_CACHE is not None and \
                (time.monotonic() - _ADMIN_USERNAMES_AT) < _ADMIN_USERNAMES_TTL:
            return _ADMIN_USERNAMES_CACHE
        rows = await db.fetch("SELECT username FROM admins")
        _ADMIN_USERNAMES_CACHE = {r["username"] for r in rows}
        _ADMIN_USERNAMES_AT = time.monotonic()
        return _ADMIN_USERNAMES_CACHE


async def _is_admin_telegram_id(telegram_id: int) -> bool:
    if telegram_id in CFG.admin_ids:
        return True
    usernames = await _load_admin_usernames()
    return str(telegram_id) in usernames


async def _ack(cq: CallbackQuery, *args, **kwargs) -> None:
    """cq.answer(...) returns an aiogram Method object (a Pydantic model),
    not a plain coroutine -- asyncio.gather() can't take that directly, it
    needs to hash its arguments to dedupe them and Method objects aren't
    hashable. Wrapping the call in a real async function gives gather an
    actual coroutine object instead. Used everywhere a screen render and
    the tap-acknowledgement are fired concurrently rather than sequentially
    (each is its own ~150-230ms Telegram round trip on this server's
    network path -- see _KeepAliveSession -- so running them in parallel
    roughly halves what a tap waits to fully resolve)."""
    await cq.answer(*args, **kwargs)


async def guard(event: Message | CallbackQuery) -> asyncpg.Record | None:
    """Shared entry check: rate limit, ban, force-join, maintenance. None = stop."""
    tg_user = event.from_user
    # Computed once and reused below -- this used to call
    # _is_admin_telegram_id (a DB query) up to three times per single
    # guard() invocation (here, again inside force_join_blocked, and again
    # for the maintenance check), tripling a query that only ever needs to
    # run once per message.
    is_admin = await _is_admin_telegram_id(tg_user.id)

    # Admins are exempt from the customer-facing rate limit -- same
    # exemption convention as force_join_blocked/maintenance_mode below.
    # Without this, an admin actively building something in the admin panel
    # (every admin action shares the same per-telegram-id sliding window,
    # checked there against a 60/min budget) can burn through the stricter
    # 30/min customer budget purely from their own admin activity, then have
    # a "Buy Now" test tap silently swallowed by a show_alert=False toast
    # that's easy to miss -- looking exactly like "the button doesn't work."
    if not is_admin and not rate_ok(tg_user.id):
        if isinstance(event, CallbackQuery):
            await event.answer("Slow down a moment.", show_alert=False)
        return None

    user = await get_or_create_user(
        tg_user.id, tg_user.username, tg_user.first_name or ""
    )
    lang = user["language"]

    if user["is_banned"]:
        text = t(lang, "banned")
        if isinstance(event, CallbackQuery):
            await event.answer(text, show_alert=True)
        else:
            await event.answer(text)
        return None

    if not is_admin and not await is_join_verified(tg_user.id):
        await send_force_join(event, lang)
        return None

    if await settings.get_bool("maintenance_mode", False):
        if not is_admin:
            text = t(lang, "maintenance")
            if isinstance(event, CallbackQuery):
                await event.answer(text, show_alert=True)
            else:
                await event.answer(text)
            return None

    return user


# ---- keyboards ---------------------------------------------------------------

# Default colours for the home menu, Bot API style values only ('primary'
# blue, 'success' green, 'danger' red — there is no gray/orange style, so
# purely informational buttons just omit the field and get Telegram's own
# default look). Only the featured "Shop Deals" row is coloured; everything
# else stays plain gray. An admin can still override any of these via the
# btn_style_<key> setting, which always wins over this default.
DEFAULT_MENU_STYLES = {
    "shop": "primary",
    "deals": "",
    "wallet": "",
    "profile": "",
    "orders": "",
    "referral": "",
    "support": "",
    "api_link": "",
    "language": "",
    "currency": "",
}


async def main_menu_kb(user: asyncpg.Record) -> InlineKeyboardMarkup:
    """Order and visibility come from the `menu_order` setting so the
    in-bot UI editor can rearrange or hide entries. Labels come from t(),
    which honours UI-editor overrides. Keys in FULL_WIDTH_MENU_KEYS get
    their own row; everything else is paired two-per-row."""
    lang = user["language"]
    keys = await menu_layout()
    # One cache read instead of one await per button style lookup -- the
    # cache is already warm almost always (60s TTL), so this was never real
    # I/O, but a dozen-odd sequential awaits per render still adds up.
    cur = await settings.load()

    api_url = cur.get("api_link_url", "").strip()

    rows: list[list[InlineKeyboardButton]] = []
    pending: list[InlineKeyboardButton] = []

    def flush() -> None:
        if pending:
            rows.append(list(pending))
            pending.clear()

    for k in keys:
        route = MENU_BUTTON_ROUTES.get(k)
        if not route:
            continue
        # Plain unicode emoji lives in the label text like any normal
        # emoji-prefixed button; EmojiRenderMiddleware upgrades it to a
        # premium icon automatically if it's mapped in emoji_map.
        # Padded with spaces for a larger visual footprint -- Telegram
        # inline buttons have no size/style property, so this is the only
        # lever for making them read as bigger tap targets.
        label = f"      {t(lang, k)}      "
        extra = {}
        style = cur.get(f"btn_style_{k}", "") or DEFAULT_MENU_STYLES.get(k, "")
        if style:
            extra["style"] = style

        if k == "api_link" and api_url.startswith("https://"):
            btn = InlineKeyboardButton(text=label, url=api_url, **extra)
        else:
            btn = InlineKeyboardButton(text=label, callback_data=route, **extra)

        if k in FULL_WIDTH_MENU_KEYS:
            flush()
            rows.append([btn])
        else:
            pending.append(btn)
            if len(pending) == 2:
                flush()
    flush()

    # Always present regardless of menu_order customization -- a utility
    # action, not a catalogue entry.
    rows.append([InlineKeyboardButton(
        text=f"      {t(lang, 'clear_chat')}      ", callback_data="clearchat")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def back_kb(lang: str, to: str = "menu") -> InlineKeyboardMarkup:
    """Back button, plus a Main Menu button alongside it whenever the back
    target isn't the menu itself — so a user is never more than one tap from
    home, without losing the ability to step back one screen at a time."""
    row = [InlineKeyboardButton(text=f"      {t(lang, 'back')}      ", callback_data=to)]
    if to != "menu":
        row.append(InlineKeyboardButton(text=f"      {t(lang, 'main_menu')}      ", callback_data="menu"))
    return InlineKeyboardMarkup(inline_keyboard=[row])


_STRIP_TAGS_RE = re.compile(r"<[^>]+>")


def _plain_text_fallback(text: str) -> str:
    """Last-resort render for text Telegram refused to parse as HTML (e.g.
    a typo'd tag that slipped in before validate_telegram_html existed) --
    strips the markup rather than showing nothing at all."""
    return html.unescape(_STRIP_TAGS_RE.sub("", text))


def _html_preview(text: str, n: int) -> str:
    """Truncated plain-text preview of admin-authored HTML content, safe to
    drop straight into a <code>...</code> (or any other) span in an admin
    listing screen. Strips tags *before* truncating -- slicing raw HTML at a
    fixed character count can (and, on this bot, once did -- see
    open_ui_brand) cut a tag like <tg-emoji emoji-id="..."> in half, leaving
    an unterminated attribute that breaks Telegram's HTML parser for the
    entire message, not just this one line. Re-escapes afterwards since
    _plain_text_fallback unescapes entities for readability -- without this
    a literal '&' or stray '<' in the source text would corrupt the
    surrounding tag it gets embedded in."""
    plain = _plain_text_fallback(text or "")
    truncated = (plain[:n] + "…") if len(plain) > n else plain
    return html.escape(truncated) or "—"


# =============================================================================
#  CANONICAL SINGLE-MESSAGE NAVIGATION
# =============================================================================
#  Exactly ONE "active" bot message per chat for all navigation -- button
#  taps, typed /commands, and the persistent menu button alike. Every screen
#  render goes through render_screen(), which looks up the chat's tracked
#  message in nav_messages and edits it (editMessageText / editMessageMedia)
#  whenever Telegram allows the transition. It only deletes the obsolete
#  message and sends a fresh one when Telegram genuinely can't edit across
#  it (a text<->photo type change, or the tracked message is gone) -- and
#  that new message immediately becomes the tracked pointer for every
#  future render in the chat, regardless of what triggers it.
#
#  Deliberately NOT based on inspecting event.message: a callback's message
#  is whatever the user happened to tap, which can be stale (an old button
#  still visible on screen, Clear Chat having just run, ...). Using the
#  chat's tracked pointer as the single source of truth makes rendering
#  self-consistent no matter what triggered it.

_render_fp_cache: dict[int, str] = {}  # chat_id -> content fingerprint, skip-if-unchanged
_RENDER_FP_CAP = 20_000


def _render_fingerprint(*parts: str, markup=None) -> str:
    markup_repr = markup.model_dump_json() if markup is not None else ""
    return hashlib.sha256("\x00".join((*parts, markup_repr)).encode("utf-8")).hexdigest()


def _remember_render_fp(chat_id: int, fingerprint: str) -> None:
    if len(_render_fp_cache) > _RENDER_FP_CAP:
        _render_fp_cache.clear()  # crude but safe bound -- worst case we just re-warm
    _render_fp_cache[chat_id] = fingerprint


def _event_chat_id(event: Message | CallbackQuery) -> int:
    return event.message.chat.id if isinstance(event, CallbackQuery) else event.chat.id


@dataclass
class NavMessage:
    message_id: int
    is_photo: bool


# In-memory mirror of nav_messages, written through on every change. This
# process is the table's only writer, so the cache can never go stale --
# it just saves a DB round trip on every single render (matching the same
# caching approach already used for Settings/emoji_map). Cold after a
# restart; the first render for a given chat falls through to the DB once
# (pre-warmed for existing chats by _seed_nav_messages at startup) and
# every render after that is pure in-memory.
_nav_cache: dict[int, NavMessage] = {}


async def _nav_get(chat_id: int) -> NavMessage | None:
    if chat_id in _nav_cache:
        return _nav_cache[chat_id]
    row = await db.fetchrow(
        "SELECT message_id, is_photo FROM nav_messages WHERE chat_id=$1", chat_id)
    if not row:
        return None
    nav = NavMessage(row["message_id"], row["is_photo"])
    _nav_cache[chat_id] = nav
    return nav


async def _nav_set(chat_id: int, message_id: int, is_photo: bool) -> None:
    _nav_cache[chat_id] = NavMessage(message_id, is_photo)
    await db.execute(
        """INSERT INTO nav_messages(chat_id, message_id, is_photo, updated_at)
           VALUES($1,$2,$3,NOW())
           ON CONFLICT (chat_id) DO UPDATE
               SET message_id=$2, is_photo=$3, updated_at=NOW()""",
        chat_id, message_id, is_photo,
    )


async def _nav_clear(chat_id: int) -> None:
    _nav_cache.pop(chat_id, None)
    _render_fp_cache.pop(chat_id, None)
    await db.execute("DELETE FROM nav_messages WHERE chat_id=$1", chat_id)


async def _nav_delete_old(chat_id: int, message_id: int) -> None:
    try:
        await bot.delete_message(chat_id, message_id)
    except (TelegramBadRequest, TelegramForbiddenError):
        pass  # already gone / too old / bot lacks rights -- not an error state


async def render_screen(
    event: Message | CallbackQuery,
    text: str,
    markup: InlineKeyboardMarkup | None = None,
    photo: str | BufferedInputFile | None = None,
    *,
    force_fresh: bool = False,
    effect_id: str | None = None,
    force_new: bool = False,
) -> None:
    """The one and only way any bot screen should be shown. See module
    docstring above. `photo` is a file_id, an https URL, or a
    BufferedInputFile (e.g. a freshly generated QR image) to upload
    directly; omit for a text-only screen.

    `force_fresh` is only for /start and /menu: an edit-in-place is
    invisible if the tracked message has scrolled out of view (which is
    the common case -- the user has been chatting since), so those two
    entry points always place a new message at the bottom where the user
    is actually looking. Every other caller (Shop, Wallet, Back, ...)
    leaves this False and swaps the same message in place, same as
    always.

    `effect_id` is a Telegram message-effect id (private chats only).
    Effects only ever play on a brand-new message -- there is no edit API
    for them -- so passing this always forces a fresh send (same as
    force_fresh, but keeping the old tracked message cleaned up
    afterwards instead of left behind, since this isn't the /start
    "leave history alone" case)."""
    chat_id = _event_chat_id(event)
    # A BufferedInputFile isn't a stable string -- fingerprint its actual
    # bytes instead so re-renders of the *same* generated image (e.g.
    # re-showing an unchanged QR) are still correctly deduped, while a
    # genuinely different image still busts the cache.
    if isinstance(photo, BufferedInputFile):
        photo_fp = hashlib.sha256(photo.data).hexdigest()
    else:
        photo_fp = photo or ""
    fp = _render_fingerprint(text, photo_fp, markup=markup)
    nav = await _nav_get(chat_id)

    async def send_fresh(delete_old: bool = True) -> None:
        old_message_id = nav.message_id if nav else None
        send_photo, send_text = photo, text
        use_effect = effect_id

        async def _do_send():
            if send_photo:
                return await bot.send_photo(chat_id, send_photo, caption=send_text, reply_markup=markup,
                                            message_effect_id=use_effect)
            return await bot.send_message(chat_id, send_text, reply_markup=markup,
                                          message_effect_id=use_effect)

        try:
            msg = await _do_send()
        except TelegramBadRequest as e:
            msg = None
            if use_effect and "EFFECT_ID_INVALID" in str(e):
                # A bad/expired/unrecognized effect id must never take the
                # whole screen down with it -- drop it and retry once
                # before falling further back to the plain-text path below.
                use_effect = None
                try:
                    msg = await _do_send()
                except TelegramBadRequest as e2:
                    msg = None
                    log.warning("send_fresh: retry without effect also failed: %s", e2)
            else:
                log.warning("send_fresh: send failed (photo=%s effect=%s): %s",
                            bool(send_photo), use_effect, e)
            if msg is None:
                # A stale/invalid photo id, a caption over Telegram's
                # 1024-char cap, or text over its 4096-char cap must never
                # propagate out of here uncaught -- this is the very first
                # send for a chat with no tracked nav message yet (new
                # user, or first tap after a cache-cold restart), so an
                # unhandled exception here skips the caller's cq.answer()
                # entirely and leaves the tapped button looking permanently
                # dead instead of showing anything. Fall back to a
                # guaranteed-sendable plain-text, length-capped retry.
                send_photo = None
                send_text = _plain_text_fallback(send_text)[:_TG_TEXT_LIMIT]
                msg = await bot.send_message(chat_id, send_text, reply_markup=markup)
        await _nav_set(chat_id, msg.message_id, bool(send_photo))
        _remember_render_fp(chat_id, fp)
        if delete_old and old_message_id is not None:
            asyncio.create_task(_nav_delete_old(chat_id, old_message_id))

    if not nav:
        await send_fresh()
        return

    if force_fresh:
        # /start: leave the previous menu message sitting in the chat as-is
        # instead of deleting it -- a fresh message opening on every /start
        # is fine, but the old one visibly vanishing a moment later read as
        # a glitch. Only this explicit "start over" action skips cleanup;
        # ordinary navigation (the same_type branch below) still deletes on
        # a type change so Back/Profile/etc. never leave orphans.
        await send_fresh(delete_old=False)
        return

    if effect_id or force_new:
        # No edit API applies a message effect -- must always be a fresh
        # send for the animation to actually play, unlike force_fresh above
        # this is ordinary navigation so the old message still gets cleaned
        # up (delete_old defaults to True). `force_new` (no effect of its
        # own) exists so a caller can deliberately break away from a
        # still-tracked message that was originally sent *with* an effect
        # -- editing it forever keeps it "the effect message" in Telegram's
        # eyes, replaying the flash on tap even on screens far past the one
        # that earned it (e.g. the quantity step's fire flash otherwise
        # riding along into coupon/summary/payment).
        await send_fresh()
        return

    if _render_fp_cache.get(chat_id) == fp:
        return  # confirmed identical to what's already on screen -- skip the round trip

    same_type = bool(photo) == bool(nav.is_photo)
    try:
        if not same_type:
            await send_fresh()
            return
        if photo:
            await bot.edit_message_media(
                chat_id=chat_id, message_id=nav.message_id,
                media=InputMediaPhoto(media=photo, caption=text), reply_markup=markup)
        else:
            await bot.edit_message_text(
                chat_id=chat_id, message_id=nav.message_id, text=text, reply_markup=markup)
        _remember_render_fp(chat_id, fp)
    except TelegramBadRequest as e:
        msg_l = str(e).lower()
        if "message is not modified" in msg_l:
            _remember_render_fp(chat_id, fp)
            return
        if "can't parse entities" in msg_l:
            # Retry once as plain text via the same edit path rather than
            # giving up -- the user sees *something* instead of a stuck
            # screen, and it still lands on the same tracked message.
            plain = _plain_text_fallback(text)
            try:
                if photo:
                    await bot.edit_message_media(
                        chat_id=chat_id, message_id=nav.message_id,
                        media=InputMediaPhoto(media=photo, caption=plain), reply_markup=markup)
                else:
                    await bot.edit_message_text(
                        chat_id=chat_id, message_id=nav.message_id, text=plain, reply_markup=markup)
                return
            except Exception:
                pass
        # Message gone, too old to edit, or some other real failure --
        # only path left is a fresh send.
        await send_fresh()
    except TelegramForbiddenError:
        pass  # user blocked the bot -- nothing to do


# Telegram's hard cap for a plain text message (sendMessage/editMessageText).
# Used as a last-resort truncation point so an oversized screen still
# renders something instead of raising past send_fresh's fallback.
_TG_TEXT_LIMIT = 4096

# Telegram's real photo-caption limit is 1024 chars of *rendered* text (tag
# markup and entity metadata don't count against it). This is checked
# against the raw pre-render text, which still includes literal tag
# characters like "<b>" -- so it's a conservative overestimate, never an
# underestimate: worst case a screen that would technically still fit falls
# back to text-only rather than risking a caption-too-long failure.
_CAPTION_SAFE_LEN = 1000


async def edit_or_send(
    event: Message | CallbackQuery, text: str, markup=None,
    *, section: str = "", force_fresh: bool = False, effect_id: str | None = None,
    force_new: bool = False,
) -> None:
    """The plain-text nav renderer -- but if a photo is configured, it
    attaches one to *every* screen instead of only the main menu. The reason
    is purely mechanical: Telegram has no edit path that changes a message's
    type (text <-> photo), only delete+send, so a bot that mixes text-only
    and photo-only screens pays that cost every time navigation crosses the
    boundary between them. Making every screen the same type (photo, once
    one exists) means every transition is photo->photo, which
    editMessageMedia handles as a true in-place edit -- no delete, ever.

    `section` picks the photo: a per-section `photo_<section>` setting
    (configured in Admin -> UI Editor -> Section photos) if one is set for
    that section, else the global Store logo, else no photo (plain text, as
    always). Screens with their own specific image (Shop's banner, Profile's
    photo, product images) already call edit_or_send_photo directly with
    that image and are unaffected."""
    photo = ""
    if section:
        photo = (await settings.get(f"photo_{section}", "")).strip()
    if not photo:
        photo = (await settings.get("store_logo_file_id", "")).strip()
    if photo and len(text) <= _CAPTION_SAFE_LEN:
        await render_screen(event, text, markup, photo=photo, force_fresh=force_fresh,
                            effect_id=effect_id, force_new=force_new)
    else:
        await render_screen(event, text, markup, force_fresh=force_fresh,
                            effect_id=effect_id, force_new=force_new)


async def edit_or_send_photo(
    event: Message | CallbackQuery, photo: str | BufferedInputFile, caption: str, markup=None,
    *, force_fresh: bool = False, effect_id: str | None = None, force_new: bool = False,
) -> None:
    await render_screen(event, caption, markup, photo=photo, force_fresh=force_fresh,
                        effect_id=effect_id, force_new=force_new)


# =============================================================================
#  START / MENU
# =============================================================================

async def _pin_chat_message_background(chat_id: int, message_id: int) -> None:
    """Fire-and-forget pin -- see run_pinned_warning_broadcast, the only
    caller. pin_chat_message is a real ~150-230ms Telegram round trip (same
    cost class as everywhere else _KeepAliveSession's latency notes apply),
    and nothing downstream needs the pin to have actually landed before it
    continues, so this is never awaited directly. Must catch everything
    itself: nothing ever awaits this task, so an uncaught exception here
    would only surface as an "exception was never retrieved" log line
    instead of the specific message this gives."""
    try:
        await bot.pin_chat_message(chat_id=chat_id, message_id=message_id,
                                   disable_notification=True)
    except Exception:
        log.exception("pinned broadcast: pin failed for chat %s", chat_id)


# Throttles the get_chat re-check in _maybe_send_payment_warning so it only
# happens once per TTL window; a chat cleared/deleted in between is
# noticed and fixed on the next /start after the cache entry expires,
# not the very next one -- a bounded staleness window traded for not
# paying the round trip on every keystroke.
_WARNING_PIN_CHECK_AT: dict[int, float] = {}
_WARNING_PIN_CHECK_TTL = 3600.0


async def _maybe_send_payment_warning(
    event: Message | CallbackQuery, user: asyncpg.Record
) -> None:
    """Sends the short anti-fraud notice and pins it, so it stays visible in
    the chat without resending (or re-pinning, which fires its own "pinned a
    message" notice) on every later /start -- see store_users.warning_pinned.

    Telegram never tells the bot when a user clears their chat or unpins a
    message, so warning_pinned alone can't detect "it used to be pinned but
    isn't anymore" -- that used to mean a user who cleared their chat lost
    the warning for good. Once warning_pinned is set, every later /start now
    asks get_chat whether the stored warning_message_id is still actually
    pinned, and re-sends+re-pins if not -- throttled by _WARNING_PIN_CHECK_AT
    above so this extra round trip only happens once per TTL window per
    user rather than on every single /start.

    Takes Message | CallbackQuery (not just Message) because a brand new
    user's *first* /start is commonly intercepted by the force-join gate
    before ever reaching this point -- they only actually land past it a
    moment later via the "✅ Verify" button, which is a CallbackQuery. Using
    _event_chat_id + bot.send_message (rather than event.answer) lets both
    call sites share one code path instead of the callback one silently
    never showing the warning at all.

    Every call site awaits this with nothing after it but the menu render
    -- this must never raise. A narrower except here once let an
    unanticipated Telegram error (flood control, a transient network blip,
    anything besides the two exception types originally caught) propagate
    all the way up and abort the whole /start handler, silently skipping
    *both* the warning and the menu that was supposed to render right
    after it. Catching broadly and logging is correct here specifically
    because nothing downstream depends on this succeeding -- see the "must
    always show" requirement in store_users.warning_pinned's design."""
    chat_id = _event_chat_id(event)
    if user["warning_pinned"]:
        now = time.monotonic()
        checked_at = _WARNING_PIN_CHECK_AT.get(user["id"])
        if checked_at is not None and (now - checked_at) < _WARNING_PIN_CHECK_TTL:
            return
        try:
            chat = await bot.get_chat(chat_id)
        except Exception:
            # Can't tell either way -- don't resend on a guess: a wrong
            # guess here means a duplicate pin, not a missing one, so
            # staying quiet is the safer failure mode.
            log.exception("payment warning pin-check failed for chat %s", chat_id)
            return
        _WARNING_PIN_CHECK_AT[user["id"]] = now
        pinned = chat.pinned_message
        wmid = user["warning_message_id"]
        if pinned and (wmid is None or pinned.message_id == wmid):
            if wmid is None:
                # Row predates warning_message_id -- assume the currently
                # pinned message is our warning (the bot never pins anything
                # else in a user's chat) rather than guessing wrong and
                # resending a duplicate right now.
                await db.execute(
                    "UPDATE store_users SET warning_message_id=$2 WHERE id=$1",
                    user["id"], pinned.message_id)
            return
        # Nothing pinned anymore (chat cleared, or manually unpinned), or
        # what's pinned isn't our warning -- fall through and put it back.
    try:
        text = await translate_dynamic(
            await settings.get("payment_warning_text", ""), user["language"])
        if not text:
            return
        sent = await bot.send_message(chat_id, text)
        # Marked done as soon as the message is sent, before the pin
        # attempt -- pinning is a nice-to-have on top of a message that's
        # already visible, not a precondition for "shown".
        await db.execute(
            "UPDATE store_users SET warning_pinned=TRUE, warning_message_id=$2 WHERE id=$1",
            user["id"], sent.message_id)
        try:
            await bot.pin_chat_message(chat_id=chat_id, message_id=sent.message_id,
                                       disable_notification=True)
        except Exception:
            log.exception("payment warning pin failed for chat %s", chat_id)
    except Exception:
        # Send itself failed (rate limit, network blip, whatever) --
        # warning_pinned is still False (or, on the re-pin path, still
        # pointing at the missing message), so this is retried
        # automatically on the user's next /start instead of being lost.
        log.exception("payment warning send failed for user %s", user["id"])


async def _enter_bot(msg: Message, state: FSMContext, ref_code: str | None) -> None:
    await state.clear()
    user = await get_or_create_user(
        msg.from_user.id, msg.from_user.username,
        msg.from_user.first_name or "", ref_code,
    )
    if user["is_banned"]:
        await msg.answer(t(user["language"], "banned"))
        return

    if await force_join_blocked(msg.from_user.id):
        await send_force_join(msg, user["language"])
        return

    await _maybe_send_payment_warning(msg, user)

    # force_fresh: /start and /menu are an explicit "open the menu" action.
    # A plain in-place edit is invisible if the tracked message has scrolled
    # out of view (the normal case after any real conversation), which read
    # as "/start does nothing." A new message at the bottom is always
    # visible; the old one is removed right after, in the background (see
    # send_fresh in render_screen), not deleted up front, so there's no gap
    # where the chat shows nothing.
    await _show_menu_or_language_picker(msg, user, force_fresh=True)


async def _show_menu_or_language_picker(
    event: Message | CallbackQuery, user: asyncpg.Record, *, force_fresh: bool = False
) -> None:
    """Past the ban/force-join gates: ask for a language on first entry,
    otherwise open the main menu straight away. Picking a language here
    (cb_setlang) is what actually completes onboarding — it silently
    defaults currency to INR at the same time (see cb_setlang) rather than
    asking a second question, and opens the menu immediately after."""
    if not user["currency"]:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=label, callback_data=f"setlang:{code}")
             for code, label in LANGUAGE_OPTIONS[i:i + 2]]
            for i in range(0, len(LANGUAGE_OPTIONS), 2)
        ])
        text = "👋 <b>Welcome!</b>\n\nChoose your language:"
        await edit_or_send(event, text, kb, force_fresh=force_fresh)
        return

    await show_menu(event, user, force_fresh=force_fresh)


@router.callback_query(F.data == "fj:verify")
async def cb_force_join_verify(cq: CallbackQuery) -> None:
    tg_user = cq.from_user
    if not rate_ok(tg_user.id):
        await cq.answer("Slow down a moment.")
        return

    user = await get_or_create_user(tg_user.id, tg_user.username, tg_user.first_name or "")
    lang = user["language"]

    if user["is_banned"]:
        await cq.answer(t(lang, "banned"), show_alert=True)
        return

    # fresh=True: this is the user explicitly asking to be re-checked right
    # after joining, so it must never answer from the passive cache guard()
    # relies on elsewhere -- a stale cached "not joined" here would make the
    # Verify button itself look broken.
    if await force_join_blocked(tg_user.id, fresh=True):
        await cq.answer(t(lang, "fj_not_joined"), show_alert=True)
        return

    await cq.answer(t(lang, "fj_verified"))
    # This is the first point a brand-new user actually lands past the
    # gate -- their original /start returned early at force_join_blocked
    # above, in _enter_bot, without ever reaching the warning.
    await _maybe_send_payment_warning(cq, user)
    # force_fresh=True: same reasoning as every plain /start in _enter_bot --
    # an in-place edit of the still-tracked "please join the channel" prompt
    # is invisible if the user has scrolled since, so this always opens a
    # fresh message at the bottom instead.
    await _show_menu_or_language_picker(cq, user, force_fresh=True)


@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext) -> None:
    payload = ""
    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) > 1:
        payload = parts[1].strip()
    ref_code = payload[4:] if payload.lower().startswith("ref_") else None
    await _enter_bot(msg, state, ref_code)


@router.message(Command("menu"))
async def cmd_menu(msg: Message, state: FSMContext) -> None:
    """Reachable from the persistent bottom-left Menu button as well as
    typing /menu directly."""
    await _enter_bot(msg, state, None)


async def rank_name(tier: str) -> str:
    """Admin-editable rank label."""
    return await settings.get(f"rank_{tier or 'bronze'}_name", (tier or "bronze").title())


async def show_menu(
    event: Message | CallbackQuery, user: asyncpg.Record, *, force_fresh: bool = False
) -> None:
    lang = user["language"]
    # Settings reads are cache-warm after the first call, but the three
    # translate_dynamic calls can each be a real DB (or, on a cold cache, a
    # DeepL) round trip -- running them concurrently instead of one after
    # another means /start pays for the slowest of the three, not the sum.
    store, welcome_raw, header_raw, footer_raw = await asyncio.gather(
        settings.get("store_name", "Digital Hub"),
        settings.get("welcome_text", ""),
        settings.get("store_header", ""),
        settings.get("store_footer", ""),
    )
    welcome, header, footer = await asyncio.gather(
        translate_dynamic(welcome_raw, lang),
        translate_dynamic(header_raw, lang),
        translate_dynamic(footer_raw, lang),
    )

    # Plain unicode emoji — EmojiRenderMiddleware upgrades any of these to a
    # premium <tg-emoji> automatically if mapped in emoji_map.
    parts = []
    if header:
        parts.append(header)
    parts.append(f"👑 <b>{store}</b> ✨")
    if welcome:
        parts.append(welcome)
    # Tier/balance live on the Profile screen now — the menu itself is just
    # a short personalised greeting.
    parts.append(f"Hey {user['first_name'] or 'there'} 🔥")
    if footer:
        parts.append(footer)

    text = "\n\n".join(p for p in parts if p)
    markup = await main_menu_kb(user)
    # edit_or_send itself attaches the store logo (if configured) to keep
    # every screen the same message type -- see its docstring.
    await edit_or_send(event, text, markup, force_fresh=force_fresh)


_CLEAR_CHAT_CONCURRENCY = asyncio.Semaphore(5)


async def _delete_one_tracked(chat_id: int, message_id: int) -> None:
    async with _CLEAR_CHAT_CONCURRENCY:
        try:
            await bot.delete_message(chat_id, message_id)
        except (TelegramBadRequest, TelegramForbiddenError):
            pass  # already deleted, too old (>48h), or bot lacks rights -- not an error state
        except Exception:
            log.exception("clear chat: delete failed for message %s in %s", message_id, chat_id)
        await asyncio.sleep(0.05)  # stay polite to Telegram's rate limits


@router.callback_query(F.data == "clearchat")
async def cb_clear_chat(cq: CallbackQuery) -> None:
    user = await guard(cq)
    if not user:
        return
    chat_id = cq.message.chat.id
    # Only 'nav' messages (menu screens the bot itself created) are ever
    # deleted -- broadcast/announcement messages are tracked separately
    # (kind='broadcast') and deliberately left alone so they survive a chat
    # clear. The user's own typed messages were never tracked in the first
    # place (only outgoing bot sends are), so those are untouched too.
    rows = await db.fetch(
        "SELECT message_id FROM bot_messages WHERE chat_id=$1 AND kind='nav'", chat_id)
    await cq.answer("🗑 Clearing…")
    await asyncio.gather(*(_delete_one_tracked(chat_id, r["message_id"]) for r in rows))
    await db.execute("DELETE FROM bot_messages WHERE chat_id=$1 AND kind='nav'", chat_id)
    # The tracked canonical message (if any) was just deleted above along
    # with everything else -- clear the pointer so the fresh menu below
    # starts a brand new one instead of trying to edit a message that's
    # already gone.
    await _nav_clear(chat_id)
    await show_menu(cq.message, user)


async def prune_bot_messages_worker() -> None:
    """Telegram itself refuses to delete messages older than 48h, so keeping
    rows past that point is pure bloat -- sweep them out periodically."""
    while True:
        try:
            await asyncio.sleep(3600)
            deleted = await db.execute(
                "DELETE FROM bot_messages WHERE created_at < NOW() - INTERVAL '48 hours'")
            log.info("bot_messages prune: %s", deleted)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("bot_messages prune failed")


async def memory_cache_sweep_worker() -> None:
    """_rate and _JOIN_VERIFIED_CACHE are plain in-process dicts keyed by
    telegram id, with no eviction on read -- fine at hundreds of users, but
    at real scale (every unique visitor ever leaves a permanent entry) they
    grow without bound for the lifetime of the process, a slow memory leak
    that eventually risks an OOM kill. Sweep out anything stale on a timer
    instead of only trimming the per-user list in place on read."""
    while True:
        try:
            await asyncio.sleep(900)
            now = time.monotonic()
            stale_rate = [tid for tid, hits in _rate.items()
                         if not hits or now - hits[-1] > 60]
            for tid in stale_rate:
                _rate.pop(tid, None)
            stale_join = [tid for tid, (_, at) in _JOIN_VERIFIED_CACHE.items()
                         if now - at > _JOIN_VERIFIED_TTL]
            for tid in stale_join:
                _JOIN_VERIFIED_CACHE.pop(tid, None)
            if stale_rate or stale_join:
                log.info("memory cache sweep: rate=-%s join_verified=-%s (rate=%s join_verified=%s remain)",
                         len(stale_rate), len(stale_join), len(_rate), len(_JOIN_VERIFIED_CACHE))
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("memory cache sweep failed")


@router.callback_query(F.data == "menu")
async def cb_menu(cq: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    user = await guard(cq)
    if not user:
        return
    # gather, not two sequential awaits: each is its own Telegram API round
    # trip (~150-230ms on this server's network path to Telegram), and
    # answering the tap doesn't depend on the menu render finishing first --
    # running them concurrently roughly halves what the user waits to see
    # both take effect (spinner clears + screen updates).
    await asyncio.gather(show_menu(cq, user), _ack(cq))


@router.callback_query(F.data.startswith("setcur:"))
async def cb_setcur(cq: CallbackQuery) -> None:
    cur = cq.data.split(":", 1)[1]
    if cur not in ("INR", "USDT"):
        await cq.answer("Unknown currency")
        return
    await db.execute(
        "UPDATE store_users SET currency=$1 WHERE telegram_id=$2",
        cur, str(cq.from_user.id),
    )
    user = await get_or_create_user(cq.from_user.id, cq.from_user.username,
                                    cq.from_user.first_name or "")
    await show_menu(cq, user)
    await cq.answer(f"Currency set to {cur}")


@router.callback_query(F.data == "cur")
async def cb_cur(cq: CallbackQuery) -> None:
    user = await guard(cq)
    if not user:
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🇮🇳 INR (₹)", callback_data="setcur:INR"),
            InlineKeyboardButton(text="₮ USDT", callback_data="setcur:USDT"),
        ],
        [InlineKeyboardButton(text=t(user["language"], "back"), callback_data="menu")],
    ])
    rate = await usdt_rate()
    await asyncio.gather(
        edit_or_send(cq, f"💱 Choose your display currency.\n\nRate: 1 USDT = ₹{rate}", kb,
                    section="language"),
        _ack(cq),
    )


@router.callback_query(F.data == "lang")
async def cb_lang(cq: CallbackQuery) -> None:
    user = await guard(cq)
    if not user:
        return
    lang_rows = [
        [InlineKeyboardButton(text=label, callback_data=f"setlang:{code}")
         for code, label in LANGUAGE_OPTIONS[i:i + 2]]
        for i in range(0, len(LANGUAGE_OPTIONS), 2)
    ]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        *lang_rows,
        [InlineKeyboardButton(text=t(user["language"], "back"), callback_data="menu")],
    ])
    await asyncio.gather(
        edit_or_send(cq, "🌐 Choose your language.", kb, section="language"),
        _ack(cq),
    )


@router.callback_query(F.data.startswith("setlang:"))
async def cb_setlang(cq: CallbackQuery) -> None:
    lang = cq.data.split(":", 1)[1]
    if lang not in STRINGS:
        await cq.answer("Unsupported language")
        return
    # COALESCE: this is also the completion point for first-run onboarding
    # now (the language picker replaced the old "which currency" prompt),
    # so a brand new user's currency gets a one-time silent default here.
    # A returning user changing their language later must never have an
    # already-chosen currency clobbered back to the default.
    await db.execute(
        "UPDATE store_users SET language=$1, currency=COALESCE(currency, 'USDT') "
        "WHERE telegram_id=$2",
        lang, str(cq.from_user.id),
    )
    user = await get_or_create_user(cq.from_user.id, cq.from_user.username,
                                    cq.from_user.first_name or "")
    # No-op if already pinned -- this is the reliable first-run completion
    # point, so it's worth covering here too even though _enter_bot and
    # cb_force_join_verify already try earlier in the chain. Awaited before
    # the gather below (not folded into it) so the warning always lands
    # before the menu it's supposed to precede.
    await _maybe_send_payment_warning(cq, user)
    await asyncio.gather(show_menu(cq, user), _ack(cq, "✓"))


# =============================================================================
#  CATALOGUE
# =============================================================================

# Telegram hard-caps an inline keyboard at 100 buttons total -- this is a
# safety ceiling, not a page size: every active product renders on one
# screen with no page-flip, and 100 comfortably covers any real catalogue
# this store is likely to carry. If it's ever actually hit, the extras are
# simply not shown rather than the API call failing outright.
MAX_SHOP_BUTTONS = 100


async def _render_shop(event: Message | CallbackQuery, user: asyncpg.Record) -> None:
    """Flat product catalogue, no pagination — every active product is one
    full-width button on a single screen, showing its category icon, name,
    price and stock. One tap from the menu straight to buyable products."""
    lang, cur = user["language"], await user_currency(user)

    products = await db.fetch(
        """SELECT p.*, c.emoji AS cat_emoji FROM products p
             JOIN categories c ON c.id = p.category_id
            WHERE p.active
            ORDER BY p.is_deal DESC, p.sold_count DESC, p.id
            LIMIT $1""",
        MAX_SHOP_BUTTONS,
    )
    if not products:
        await edit_or_send(event, t(lang, "no_products"), back_kb(lang))
        return

    # Stock is one batched query for every product on screen (stock_of_many)
    # instead of a separate round trip per product -- with every active
    # product now on screen at once (no more PAGE_SIZE cap), N separate
    # round trips would only get worse as the catalogue grows. localize()
    # still runs one-per-product but concurrently: on a cold cache it can be
    # a DB round trip or a live DeepL call, and awaiting it one product at a
    # time turned the catalogue into N stacked round trips instead of one
    # batch of concurrent ones.
    stocks_by_id, names = await asyncio.gather(
        stock_of_many(db, products),
        asyncio.gather(*(localize(p["name"], p["name_hi"], lang) for p in products)),
    )
    stocks = [stocks_by_id[p["id"]] for p in products]

    rows = []
    for p, stock, name in zip(products, stocks, names):
        price = await display_price(money(p["price"], Q_CRYPTO), cur)
        if stock > 0:
            stock_label = f"{stock} left" if stock < 999_999 else "∞"
        else:
            stock_label = t(lang, "out_of_stock")
        # A button can carry exactly one premium emoji icon (Telegram's
        # icon_custom_emoji_id), always the leftmost one in the label. If the
        # product's own name already has a mapped emoji, that's the one the
        # merchant actually placed there — don't let the generic category
        # icon steal the only slot and strand it as plain text.
        has_own_emoji = bool(_EMOJI_PATTERN and _EMOJI_PATTERN.search(name))
        # Cap the name so a long admin-entered product name can't push the
        # price/stock past Telegram's per-button character budget and get
        # silently cut off mid-word -- reported happening on iPad, where the
        # button column renders narrower relative to label length.
        short_name = name if len(name) <= 32 else name[:31] + "…"
        if has_own_emoji:
            label = f"{short_name} — {price} · {stock_label}"
        else:
            icon = p["cat_emoji"] or "📦"
            label = f"{icon} {short_name} — {price} · {stock_label}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"prod:{p['id']}")])

    rows.append([
        InlineKeyboardButton(text="🔁 Refresh", callback_data="shop:1"),
        InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu"),
    ])

    text = f"🎉 <b>{t(lang, 'shop_title')}</b> ✨\n\n{t(lang, 'pick_product')}"
    markup = InlineKeyboardMarkup(inline_keyboard=rows)
    banner = (await settings.get("shop_banner_file_id", "")).strip()
    if banner:
        await edit_or_send_photo(event, banner, text, markup)
    else:
        await edit_or_send(event, text, markup)


@router.callback_query(F.data.startswith("shop:"))
async def cb_shop(cq: CallbackQuery) -> None:
    user = await guard(cq)
    if not user:
        return
    await asyncio.gather(_render_shop(cq, user), _ack(cq))


@router.message(Command("shop"))
async def msg_shop(msg: Message, state: FSMContext) -> None:
    user = await guard(msg)
    if not user:
        return
    await state.clear()
    await _render_shop(msg, user)


@router.callback_query(F.data == "deals")
async def cb_deals(cq: CallbackQuery) -> None:
    user = await guard(cq)
    if not user:
        return
    lang, cur = user["language"], await user_currency(user)
    products = await db.fetch(
        "SELECT * FROM products WHERE active AND is_deal ORDER BY sold_count DESC LIMIT 20")
    if not products:
        await asyncio.gather(
            edit_or_send(cq, "No active deals right now.", back_kb(lang)), _ack(cq))
        return

    stocks_by_id, names = await asyncio.gather(
        stock_of_many(db, products),
        asyncio.gather(*(localize(p["name"], p["name_hi"], lang) for p in products)),
    )
    stocks = [stocks_by_id[p["id"]] for p in products]

    rows = []
    for p, stock, name in zip(products, stocks, names):
        price = await display_price(money(p["price"], Q_CRYPTO), cur)
        in_stock = stock > 0
        # Same one-icon-per-button constraint as the shop listing: don't let
        # the generic 🔥 deal marker steal the only premium-icon slot from an
        # emoji the merchant put in the product name itself.
        prefix = "" if (_EMOJI_PATTERN and _EMOJI_PATTERN.search(name)) else "🔥 "
        # Capped for the same reason as the shop listing: a long name plus
        # the "(was $X)" suffix below can otherwise push past Telegram's
        # per-button budget and get silently cut off.
        short_name = name if len(name) <= 26 else name[:25] + "…"
        label = (f"{prefix}{short_name} — {price}" if in_stock
                 else f"{prefix}{short_name} — {t(lang, 'out_of_stock')}")
        if p["original_price"]:
            was = await display_price(money(p["original_price"], Q_CRYPTO), cur)
            label += f" (was {was})"
        rows.append([InlineKeyboardButton(
            text=label, callback_data=f"prod:{p['id']}")])
    rows.append([InlineKeyboardButton(text=t(lang, "back"), callback_data="menu")])
    await asyncio.gather(
        edit_or_send(cq, "🔥 <b>Today's deals</b>", InlineKeyboardMarkup(inline_keyboard=rows)),
        _ack(cq),
    )


@router.callback_query(F.data.startswith("prod:"))
async def cb_product(cq: CallbackQuery, state: FSMContext) -> None:
    user = await guard(cq)
    if not user:
        return
    pid = int(cq.data.split(":")[1])
    p = await db.fetchrow("SELECT * FROM products WHERE id=$1 AND active", pid)
    if not p:
        await cq.answer("That product is no longer available.", show_alert=True)
        return

    lang, cur = user["language"], await user_currency(user)
    stock = await stock_of(db, p)
    name = await localize(p["name"], p["name_hi"], lang)
    desc = await localize(p["description"], p["description_hi"], lang)
    price = await display_price(money(p["price"], Q_CRYPTO), cur)

    text = f"<b>{name}</b>\n\n"
    if desc:
        text += f"{desc}\n\n"
    text += f"💵 {t(lang, 'price')}: <b>{price}</b>\n"
    if p["original_price"] and money(p["original_price"]) > money(p["price"]):
        was = await display_price(money(p["original_price"], Q_CRYPTO), cur)
        text += f"<s>{was}</s>\n"
    text += f"{t(lang, 'in_stock')}: {stock if stock < 999_999 else '∞'}\n"
    if p["sold_count"]:
        text += f"🔥 {p['sold_count']} sold\n"

    qty_tiers = await db.fetch(
        "SELECT min_qty, discount_type, value FROM product_qty_discounts "
        "WHERE product_id=$1 ORDER BY min_qty", pid)
    if qty_tiers:
        parts = [f"{qt['min_qty']}+ → {qt['value']}% off" if qt["discount_type"] == "percent"
                else f"{qt['min_qty']}+ → ₹{qt['value']} off"
                for qt in qty_tiers]
        text += f"🔢 Buy more, save more: {' · '.join(parts)}\n"

    rows = []
    if stock > 0:
        # Full-width Buy Now — its own row, styled as the primary action.
        rows.append([InlineKeyboardButton(
            text=f"🛒 {t(lang, 'buy')} — {price}", callback_data=f"buy:{pid}",
            style="success")])
    else:
        rows.append([InlineKeyboardButton(
            text=t(lang, "notify_me"), callback_data=f"watch:{pid}")])
    rows.append([InlineKeyboardButton(
        text=t(lang, "back"), callback_data="shop:1")])

    await state.update_data(product_id=pid)
    markup = InlineKeyboardMarkup(inline_keyboard=rows)
    image = p["image_file_id"] or p["image_url"]
    render = (edit_or_send_photo(cq, image, text, markup) if image
             else edit_or_send(cq, text, markup))
    await asyncio.gather(render, _ack(cq))


@router.callback_query(F.data.startswith("watch:"))
async def cb_watch(cq: CallbackQuery) -> None:
    user = await guard(cq)
    if not user:
        return
    pid = int(cq.data.split(":")[1])
    await db.execute(
        """INSERT INTO restock_alerts(user_id, product_id) VALUES($1,$2)
           ON CONFLICT DO NOTHING""",
        user["id"], pid,
    )
    await cq.answer("🔔 We'll message you when it's back in stock.", show_alert=True)

# =============================================================================
#  MANUAL DELIVERY — CREDENTIAL COLLECTION
# =============================================================================
#  For delivery_type='manual' products, deliver_to_user (see FULFILMENT
#  section) sends the buyer a "Submit details" button instead of the goods
#  straight away. Tapping it opens a one-question popup (an FSM text
#  prompt -- Telegram has no native input dialog) asking for whatever the
#  product needs (email, username, ...); the answer is stored on the order
#  and pushed to every admin. The admin activates the account by hand
#  outside Telegram, then taps "✅ Mark activated" in 🛠 Manual Deliveries
#  (admin panel) to close the loop and notify the buyer.

@router.callback_query(F.data.startswith("subinfo:"))
async def cb_subinfo(cq: CallbackQuery, state: FSMContext) -> None:
    user = await guard(cq)
    if not user:
        return
    order_id = int(cq.data.split(":")[1])
    order = await db.fetchrow(
        "SELECT * FROM orders WHERE id=$1 AND user_id=$2", order_id, user["id"])
    if not order:
        await cq.answer("Order not found.", show_alert=True)
        return
    if order["status"] not in ("processing", "paid"):
        await cq.answer("This order is no longer waiting on this step.", show_alert=True)
        return
    if order["manual_info"] or order["manual_info_photo"]:
        await cq.answer("Already submitted — sit tight, we're on it.", show_alert=True)
        return

    product = await db.fetchrow("SELECT manual_prompt FROM products WHERE id=$1", order["product_id"])
    prompt = (product["manual_prompt"].strip() if product and product["manual_prompt"] else "") \
        or MANUAL_INFO_DEFAULT_PROMPT

    await state.set_state(Flow.manual_info)
    await state.update_data(order_id=order_id)
    # Edit the exact message this button lives on -- not the global
    # single-nav-message system (edit_or_send/render_screen). This notice
    # arrived out of band (the buyer could be anywhere else in the bot when
    # payment clears), so routing it through the shared nav message would
    # hijack whatever screen they're actually looking at there, while this
    # message's own button sat untouched and still tappable. Editing
    # cq.message directly keeps the whole exchange in this one bubble and
    # removes the button as part of the same edit.
    # Fired as a task rather than awaited inline so it starts concurrently
    # with the edit below instead of after it -- two Telegram round trips
    # in parallel instead of back to back. Still awaited at the end, so a
    # genuine failure here still surfaces same as before; only the edit's
    # own TelegramBadRequest is treated as harmless.
    ack = asyncio.create_task(_ack(cq))
    try:
        await cq.message.edit_text(
            f"✍️ <b>Order #{order_id}</b>\n\n{prompt}\n\n"
            f"Send it now as a plain text message, or as a photo (with an optional caption).",
            reply_markup=_CLEAR_KB)
    except TelegramBadRequest:
        pass  # "message is not modified" or similarly harmless
    await ack


async def _handle_manual_info_submission(
    msg: Message, state: FSMContext, info: str, photo_file_id: str
) -> None:
    """Shared by the text and photo handlers below -- everything past "what
    did the buyer send" (validate, persist, close the loop, notify admins)
    is identical either way."""
    user = await guard(msg)
    if not user:
        return
    data = await state.get_data()
    order_id = data.get("order_id")
    if not info and not photo_file_id:
        await msg.answer("Please send some text or a photo.")
        return
    if len(info) > 300:
        await msg.answer("That's too long — keep it under 300 characters.")
        return

    order = await db.fetchrow(
        "SELECT o.*, p.name AS product_name FROM orders o JOIN products p ON p.id=o.product_id "
        "WHERE o.id=$1 AND o.user_id=$2", order_id, user["id"])
    if not order or order["status"] not in ("processing", "paid"):
        await state.clear()
        await msg.answer("That order is no longer waiting on this step.")
        return
    if order["manual_info"] or order["manual_info_photo"]:
        await state.clear()
        await msg.answer("Already submitted — sit tight, we're on it.")
        return

    await db.execute(
        "UPDATE orders SET manual_info=$1, manual_info_photo=$2, manual_info_at=NOW() WHERE id=$3",
        info or None, photo_file_id or None, order_id,
    )
    await state.clear()

    # Close the loop on the same message that started this (the "Payment
    # confirmed / Submit details" notice) rather than sending a new one --
    # see cb_subinfo for why edit_or_send's shared nav message is the wrong
    # tool here. Falls back to a fresh send only if that message is gone or
    # too old to edit (Telegram caps edits at 48h), so the buyer is never
    # left without confirmation.
    confirm_text = (
        f"✅ <b>Received</b>\n\nOrder <code>#{order_id}</code> — {order['product_name']}\n\n"
        f"We'll activate it and confirm here shortly."
    )
    edited = False
    if order["manual_msg_chat_id"] and order["manual_msg_id"]:
        try:
            await bot.edit_message_text(
                chat_id=order["manual_msg_chat_id"], message_id=order["manual_msg_id"],
                text=confirm_text, reply_markup=_CLEAR_KB)
            edited = True
        except TelegramBadRequest:
            pass
    if not edited:
        await msg.answer(confirm_text)

    # info is raw user-typed text, not admin-authored HTML -- escape before
    # embedding it in this HTML-parsed admin message (it's about to sit
    # inside a <code> span, so an unescaped '<', '>' or '&' would either
    # break the parse or let a buyer inject markup into an admin's chat).
    who = html.escape(msg.from_user.first_name or msg.from_user.username or str(msg.from_user.id))
    submitted_line = f"Submitted: <code>{html.escape(info[:300])}</code>\n" if info else ""
    photo_line = "📷 Photo attached\n" if photo_file_id else ""
    await notify_admins_text(
        bot,
        f"🛠 <b>Manual delivery — info received</b>\n\n"
        f"Order <code>#{order_id}</code> — {html.escape(order['product_name'])}\n"
        f"From: {who} (<code>{msg.from_user.id}</code>)\n"
        f"{submitted_line}{photo_line}\n"
        f"Open 🛠 Manual Deliveries in /admin to activate.",
    )


@router.message(Flow.manual_info, F.text)
async def msg_manual_info_text(msg: Message, state: FSMContext) -> None:
    await _handle_manual_info_submission(msg, state, (msg.text or "").strip(), "")


@router.message(Flow.manual_info, F.photo)
async def msg_manual_info_photo(msg: Message, state: FSMContext) -> None:
    await _handle_manual_info_submission(
        msg, state, (msg.caption or "").strip(), msg.photo[-1].file_id)


@router.message(Flow.manual_info)
async def msg_manual_info_other(msg: Message) -> None:
    # Catches anything that's neither text nor photo (voice, sticker, video,
    # ...) -- without this, splitting the old single handler into F.text/
    # F.photo variants would leave those message types silently unrouted
    # instead of getting the same "try again" nudge as before.
    await msg.answer("Please send it as plain text, or a photo.")


# =============================================================================
#  CHECKOUT
# =============================================================================

# Message effect for the quantity screen only -- render_screen's send_fresh
# forces a fresh send whenever effect_id is set (no edit API carries an
# effect), and cb_buy is the only handler that passes one, so it never
# carries forward into coupon/summary/payment.
#
# Only 6 effect ids work without the bot OWNER having Telegram Premium:
# 👍 5107584321108051014  👎 5104858069142078462  ❤ 5159385139981059251
# 🔥 5104841245755180586  🎉 5046509860389126442  💩 5046589136895476101
# Every other id (incl. an earlier lightning-bolt pick) is Premium-gated and
# fails outright with "PREMIUM_ACCOUNT_REQUIRED" (confirmed live in prod logs)
# -- send_fresh's generic exception path then drops photo+effect *both*
# together and falls back to bare plain text, which is why the whole screen
# looked broken rather than just missing the animation.
_BUY_EFFECT_ID = "5104841245755180586"  # 🔥 fire (one of the 6 free effects)


@router.callback_query(F.data.startswith("buy:"))
async def cb_buy(cq: CallbackQuery, state: FSMContext) -> None:
    user = await guard(cq)
    if not user:
        return
    pid = int(cq.data.split(":")[1])
    p = await db.fetchrow("SELECT * FROM products WHERE id=$1 AND active", pid)
    if not p:
        await cq.answer("Unavailable.", show_alert=True)
        return

    stock = await stock_of(db, p)
    if stock < 1:
        await cq.answer("Out of stock.", show_alert=True)
        return

    await state.update_data(product_id=pid)
    lang = user["language"]
    options = [n for n in (1, 2, 3, 5, 10, 15, 20, 25) if n <= stock]
    rows = [options[i:i + 4] for i in range(0, len(options), 4)]
    rows = [[InlineKeyboardButton(text=str(n), callback_data=f"qty:{pid}:{n}")
             for n in row] for row in rows]
    rows.append([InlineKeyboardButton(text=t(lang, "custom_qty"), callback_data=f"qtyask:{pid}")])
    rows.append([InlineKeyboardButton(text=t(lang, "back"), callback_data=f"prod:{pid}")])

    await asyncio.gather(
        edit_or_send(cq, f"🔢 <b>{t(lang, 'select_qty')}</b>\n\nAvailable: {stock}",
                    InlineKeyboardMarkup(inline_keyboard=rows), section="quantity",
                    effect_id=_BUY_EFFECT_ID),
        _ack(cq),
    )


@router.callback_query(F.data.startswith("qtyask:"))
async def cb_qtyask(cq: CallbackQuery, state: FSMContext) -> None:
    user = await guard(cq)
    if not user:
        return
    pid = int(cq.data.split(":")[1])
    await state.update_data(product_id=pid)
    await state.set_state(Flow.quantity)
    await asyncio.gather(
        edit_or_send(cq, "🔢 Send the quantity you want as a number.",
                    back_kb(user["language"], f"prod:{pid}"), section="quantity"),
        _ack(cq),
    )


@router.message(Flow.quantity)
async def msg_quantity(msg: Message, state: FSMContext) -> None:
    user = await guard(msg)
    if not user:
        return
    data = await state.get_data()
    pid = data.get("product_id")
    try:
        qty = int((msg.text or "").strip())
    except ValueError:
        await msg.answer("Please send a plain number, like 3.")
        return
    if qty < 1 or qty > 500:
        await msg.answer("Quantity must be between 1 and 500.")
        return
    await state.clear()
    await start_coupon_step(msg, user, pid, qty, state)


@router.callback_query(F.data.startswith("qty:"))
async def cb_qty(cq: CallbackQuery, state: FSMContext) -> None:
    user = await guard(cq)
    if not user:
        return
    _, pid, qty = cq.data.split(":")
    await asyncio.gather(start_coupon_step(cq, user, int(pid), int(qty), state), _ack(cq))


async def start_coupon_step(
    event, user: asyncpg.Record, pid: int, qty: int, state: FSMContext
) -> None:
    await state.update_data(product_id=pid, quantity=qty)
    await state.set_state(Flow.coupon)
    lang = user["language"]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "skip"), callback_data="coupon_skip")],
        [InlineKeyboardButton(text=t(lang, "cancel"), callback_data=f"prod:{pid}")],
    ])
    # force_new: this is the exit point from the quantity step, whose message
    # may still be carrying the qty-screen's fire effect (cb_buy) -- editing
    # it in place would drag that effect along into coupon/summary/payment
    # forever, so break away to a clean new message here instead.
    await edit_or_send(event, f"🎟 <b>{t(lang, 'enter_coupon')}</b>", kb, section="coupon", force_new=True)


@router.message(Flow.coupon)
async def msg_coupon(msg: Message, state: FSMContext) -> None:
    user = await guard(msg)
    if not user:
        return
    data = await state.get_data()
    await state.clear()
    await show_summary(msg, user, data["product_id"], data["quantity"],
                       (msg.text or "").strip(), state)


@router.callback_query(F.data == "coupon_skip")
async def cb_coupon_skip(cq: CallbackQuery, state: FSMContext) -> None:
    user = await guard(cq)
    if not user:
        return
    data = await state.get_data()
    await state.clear()
    await asyncio.gather(
        show_summary(cq, user, data["product_id"], data["quantity"], None, state), _ack(cq))


async def show_summary(
    event, user: asyncpg.Record, pid: int, qty: int,
    coupon_code: str | None, state: FSMContext
) -> None:
    lang, cur = user["language"], await user_currency(user)
    p = await db.fetchrow("SELECT * FROM products WHERE id=$1 AND active", pid)
    if not p:
        await edit_or_send(event, "That product is no longer available.", back_kb(lang))
        return

    async with db.tx() as conn:
        try:
            quote = await build_quote(conn, user, p, qty, coupon_code)
        except QuoteError as e:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔁 Try another code",
                                      callback_data=f"qty:{pid}:{qty}")],
                [InlineKeyboardButton(text=t(lang, "skip"), callback_data="coupon_skip")],
            ])
            await state.update_data(product_id=pid, quantity=qty)
            await edit_or_send(event, f"⚠️ {e}", kb, section="coupon")
            return

    await state.update_data(
        product_id=pid, quantity=qty,
        coupon_code=quote.coupon_code, coupon_id=quote.coupon_id,
    )

    name = await localize(p["name"], p["name_hi"], lang)
    lines = [
        "🧾 <b>Order summary</b>\n",
        f"Item: <b>{name}</b>",
        f"Quantity: <b>{qty}</b>",
        f"Subtotal: {await display_price(quote.subtotal, cur)}",
    ]
    if quote.qty_discount > ZERO:
        lines.append(f"🔢 Qty discount ({quote.qty_tier_min}+): "
                     f"−{await display_price(quote.qty_discount, cur)}")
    if quote.coupon_discount > ZERO:
        lines.append(f"Coupon {quote.coupon_code}: −{await display_price(quote.coupon_discount, cur)}")
    if quote.tier_discount > ZERO:
        lines.append(f"{quote.tier.title()} discount: −{await display_price(quote.tier_discount, cur)}")
    lines.append(f"\n<b>Total: {await display_price(quote.total, cur)}</b>")

    # Every payment rail this bot actually implements -- wallet, manual/
    # Cryptomus crypto, UPI, OxaPay -- each already has a fully working
    # handler (pay:wallet / pay:crypto / pay:upi / pay:oxapay). The
    # manual/Cryptomus "Pay with USDT" chain-picker (pay:crypto) is
    # deliberately not offered here -- OxaPay's own hosted page already
    # covers USDT among the networks it offers, and having both was
    # redundant.
    rows = []
    rows.append([InlineKeyboardButton(
        text=f"👛 {t(lang, 'pay_wallet')}", callback_data="pay:wallet")])
    if (await settings.get("upi_id", "")).strip():
        rows.append([InlineKeyboardButton(
            text=f"💳 {t(lang, 'pay_upi')}", callback_data="pay:upi")])
    if await settings.get_bool("oxapay_enabled", False):
        rows.append([InlineKeyboardButton(
            text="🟢 Pay with Crypto", callback_data="pay:oxapay")])
    rows.append([InlineKeyboardButton(text=t(lang, "cancel"), callback_data=f"prod:{pid}")])

    await edit_or_send(event, "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows),
                       section="payment")


# ---- wallet checkout ---------------------------------------------------------
#  The whole thing is one transaction: lock user, re-quote, re-check stock,
#  debit, create order, allocate keys. If any step fails nothing is applied.

@router.callback_query(F.data == "pay:wallet")
async def cb_pay_wallet(cq: CallbackQuery, state: FSMContext) -> None:
    user = await guard(cq)
    if not user:
        return
    data = await state.get_data()
    pid, qty = data.get("product_id"), data.get("quantity")
    if not pid:
        await cq.answer("Session expired — start again.", show_alert=True)
        return

    lang, cur = user["language"], await user_currency(user)
    await cq.answer(t(lang, "processing"))

    order_id = None
    keys: list[str] = []
    try:
        async with db.tx() as conn:
            fresh = await conn.fetchrow(
                "SELECT * FROM store_users WHERE id=$1 FOR UPDATE", user["id"])
            p = await conn.fetchrow(
                "SELECT * FROM products WHERE id=$1 AND active FOR UPDATE", pid)
            if not p:
                raise FulfilError("Product is no longer available.")

            stock = await stock_of(conn, p)
            if stock < qty:
                raise FulfilError(f"Only {stock} left in stock.")

            quote = await build_quote(conn, fresh, p, qty, data.get("coupon_code"))

            if money(fresh["wallet_balance"], Q_CRYPTO) < quote.total:
                raise InsufficientFunds("balance too low")

            order = await conn.fetchrow(
                """INSERT INTO orders
                     (user_id, product_id, quantity, subtotal, discount, total,
                      currency, coupon_id, coupon_code, status, delivery_type,
                      payment_method)
                   VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'paid',$10,'wallet')
                   RETURNING *""",
                fresh["id"], pid, qty, quote.subtotal, quote.discount, quote.total,
                cur, quote.coupon_id, quote.coupon_code, p["delivery_type"],
            )
            order_id = order["id"]

            await wallet_apply(
                conn, fresh["id"], -quote.total, "purchase",
                f"Order #{order_id}", ref_type="order", ref_id=order_id,
            )
            _, keys = await finalise_order(conn, order)

    except InsufficientFunds:
        await edit_or_send(cq, f"⚠️ {t(lang, 'insufficient')}", await wallet_kb(user), section="payment")
        return
    except (FulfilError, QuoteError) as e:
        await edit_or_send(cq, f"⚠️ {e}", back_kb(lang), section="payment")
        return
    except Exception:
        log.exception("wallet checkout failed")
        await edit_or_send(cq, "⚠️ Something went wrong. Nothing was charged.", back_kb(lang),
                           section="payment")
        return

    await state.clear()
    order = await db.fetchrow("SELECT * FROM orders WHERE id=$1", order_id)
    await deliver_to_user(bot, order, keys)
    await show_menu(cq, await db.fetchrow(
        "SELECT * FROM store_users WHERE id=$1", user["id"]))


# ---- crypto checkout ---------------------------------------------------------

# Premium custom emoji for the USDT glyph, one per network. These are
# distinct from the generic emoji_map (unicode_emoji -> one premium id):
# that table can only give a single glyph one global replacement, but here
# a different premium icon is needed depending which chain is being shown,
# so it's wired directly at the specific payment-method display sites below
# rather than through the generic map.
#
# IMPORTANT: Telegram's <tg-emoji emoji-id="..."> HTML entity requires its
# enclosed text to be the *exact* standard-emoji fallback that custom emoji
# represents -- get it wrong and Telegram rejects the entire message with
# "ENTITY_TEXT_INVALID" (silently breaking the whole screen, not just the
# icon). The correct fallback per id was fetched from Telegram itself via
# getCustomEmojiStickers rather than assumed:
#   5388839713320742959 -> 🪙 (coin)
#   5391239186994967770 -> 💎 (gem)
#   5280848571753584087 -> 🤑 (money-mouth face)
USDT_CHAIN_EMOJI = {
    "ERC20": ("5388839713320742959", "🪙"),
    "TRC20": ("5391239186994967770", "💎"),
    "BEP20": ("5280848571753584087", "🤑"),
}


def chain_glyph_html(chain: str) -> str:
    """Inline <tg-emoji> HTML for a chain's payment glyph in message text.
    Falls back to a plain "₮" if no premium id is set for that chain."""
    entry = USDT_CHAIN_EMOJI.get(chain)
    if not entry:
        return "₮"
    eid, glyph = entry
    return f'<tg-emoji emoji-id="{eid}">{glyph}</tg-emoji>'


def _usdt_chain_button(chain: str, callback_prefix: str, suffix: str = "") -> InlineKeyboardButton:
    """Chain-selection button with the network's premium USDT icon. The
    glyph lives only in icon_custom_emoji_id, not the text, so it isn't
    shown twice (same one-icon-per-button convention EmojiRenderMiddleware
    uses elsewhere). Shared by the manual (chain:) and instant Cryptomus
    (cmpay:) chain-pickers, which differ only in callback prefix and the
    "(instant)" suffix."""
    entry = USDT_CHAIN_EMOJI.get(chain)
    eid = entry[0] if entry else None
    extra = {"icon_custom_emoji_id": eid} if eid else {}
    text = f"USDT — {chain}{suffix}" if eid else f"₮ USDT — {chain}{suffix}"
    return InlineKeyboardButton(text=text, callback_data=f"{callback_prefix}:{chain}", **extra)


def usdt_chain_button(chain: str) -> InlineKeyboardButton:
    return _usdt_chain_button(chain, "chain")


class OrderCreationError(Exception):
    """User-facing reason a pending order/topup couldn't be created."""


async def _create_pending_order_or_topup(
    conn: asyncpg.Connection, user: asyncpg.Record,
    payment_method: str, order_currency: str, *,
    topup_inr: Any = None, product_id: int | None = None,
    quantity: int | None = None, coupon_code: str | None = None,
) -> tuple[int | None, Decimal]:
    """Resolve either a wallet-topup amount or a freshly inserted 'pending'
    order, validating stock + coupon as a side effect. Returns
    (order_id_or_None, total_amount).

    Explicit keyword args rather than an FSM-state dict -- this is called
    from both aiogram callback handlers (which have a `FSMContext` data
    dict to pull from) and, since the Mini App has no FSM, plain HTTP
    request bodies. Callers own translating their own input shape into
    these params; this function only knows about the resulting values.

    Must be called inside the same `async with db.tx() as conn:` block the
    caller uses for the payment-row INSERT that follows immediately after --
    that's what makes order-creation and payment-creation a single atomic
    unit (previously two separate statements/transactions in each of
    cb_chain/cb_pay_upi, so a crash in between could leave a permanently
    'pending' order with no payment row and nothing to ever expire it).

    Raises OrderCreationError with a caller-displayable message on any
    validation failure -- callers keep their own exact wording via
    `cq.answer(str(e), show_alert=True)` (bot) or an HTTP 400/409 (Mini App)
    so existing user-facing text for each payment method is unchanged."""
    if topup_inr is not None:
        return None, money(topup_inr, Q_CRYPTO)

    if not product_id:
        raise OrderCreationError("Session expired — start again.")
    p = await conn.fetchrow("SELECT * FROM products WHERE id=$1 AND active", product_id)
    if not p:
        raise OrderCreationError("Unavailable.")
    stock = await stock_of(conn, p)
    if stock < quantity:
        raise OrderCreationError(f"Only {stock} left." if stock else "Out of stock.")
    quote = await build_quote(conn, user, p, quantity, coupon_code)
    order = await conn.fetchrow(
        """INSERT INTO orders
             (user_id, product_id, quantity, subtotal, discount, total,
              currency, coupon_id, coupon_code, status, delivery_type,
              payment_method)
           VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending',$10,$11)
           RETURNING *""",
        user["id"], product_id, quantity, quote.subtotal, quote.discount, quote.total,
        order_currency, quote.coupon_id, quote.coupon_code,
        p["delivery_type"], payment_method,
    )
    return order["id"], quote.total


async def _create_chain_payment(
    user: asyncpg.Record, chain: str, payment_method: str, order_currency: str, *,
    topup_inr: Any = None, product_id: int | None = None,
    quantity: int | None = None, coupon_code: str | None = None,
) -> asyncpg.Record:
    """Direct on-chain USDT payment: a configured wallet address + a
    user-submitted transaction hash, verified via ChainVerifier -- the
    exact flow cb_chain uses in the bot chat, extracted here so the Mini
    App's order endpoint can use it too without depending on Cryptomus
    (which needs a merchant ID that isn't configured yet). Raises
    OrderCreationError on any validation failure, same contract as
    _create_cryptomus_payment."""
    address = await chain_address(chain)
    if not address:
        raise OrderCreationError(f"{chain} isn't available right now.")

    async with db.tx() as conn:
        order_id, inr_total = await _create_pending_order_or_topup(
            conn, user, payment_method, order_currency,
            topup_inr=topup_inr, product_id=product_id,
            quantity=quantity, coupon_code=coupon_code)

        usdt_amount = crypto(inr_total / await usdt_rate())
        usdt_amount = apply_safety_fee(usdt_amount, await crypto_safety_fee_percent(chain))
        # Unique cents make two concurrent payments distinguishable on-chain.
        usdt_amount = crypto(usdt_amount + Decimal(secrets.randbelow(900) + 100) / Decimal(1_000_000))

        ttl = await payment_ttl_minutes()
        expires = datetime.now(timezone.utc) + timedelta(minutes=ttl)

        payment = await conn.fetchrow(
            """INSERT INTO payments
                 (order_id, user_id, purpose, method, amount, currency,
                  expected_amount, expected_address, chain, status, verification, expires_at)
               VALUES($1,$2,$3,$4,$5,'USDT',$6,$7,$8,'pending','chain',$9)
               RETURNING *""",
            order_id, user["id"], "topup" if topup_inr is not None else "order",
            payment_method, inr_total, usdt_amount, address, chain, expires,
        )
    return payment


def chain_payment_qr(address: str) -> str | None:
    """SVG QR code for a direct wallet address, as a data URI -- <img
    src=...> contract for web contexts (e.g. the admin panel). Never send
    this to Telegram's sendPhoto: Telegram only accepts raster formats and
    rejects SVG with IMAGE_PROCESS_FAILED -- use payment_qr_png below for
    anything sent as a bot message. Best-effort: the payment screen always
    shows the address as text regardless of whether this succeeds."""
    try:
        img = qrcode.make(address, image_factory=qrcode.image.svg.SvgPathImage)
        buf = io.BytesIO()
        img.save(buf)
        b64 = base64.b64encode(buf.getvalue()).decode()
        return f"data:image/svg+xml;base64,{b64}"
    except Exception:
        log.exception("chain payment QR generation failed")
        return None


def payment_qr_png(data: str) -> bytes | None:
    """Raster PNG QR code, generated with the pure-Python PyPNG backend (no
    Pillow needed) -- the only format Telegram's sendPhoto actually
    accepts. Best-effort: callers must treat None as "skip the photo",
    since the address/amount text is always shown regardless."""
    try:
        img = qrcode.make(data, image_factory=qrcode.image.pure.PyPNGImage)
        buf = io.BytesIO()
        img.save(buf)
        return buf.getvalue()
    except Exception:
        log.exception("PNG QR generation failed")
        return None


class CryptomusInvoiceError(Exception):
    """User-facing reason a Cryptomus invoice couldn't be created."""


async def _create_cryptomus_payment(
    user: asyncpg.Record, chain: str, network: str,
    payment_method: str, order_currency: str, *,
    topup_inr: Any = None, product_id: int | None = None,
    quantity: int | None = None, coupon_code: str | None = None,
) -> asyncpg.Record:
    """Create the order (or resolve a topup amount) and a live Cryptomus
    invoice together, ending in a single payment-row INSERT with
    cryptomus_uuid/expected_address/expires_at all populated at once --
    shared by the bot's in-chat instant-pay flow and the Mini App's order
    endpoint, so the two surfaces can never diverge on this financially
    sensitive sequencing (see the no-orphan-window rationale on
    _create_pending_order_or_topup itself, which this builds on).

    Raises OrderCreationError (stock/coupon/session problems, propagated
    from _create_pending_order_or_topup) or CryptomusInvoiceError
    (Cryptomus unreachable or rejected the request) -- both carry a
    caller-displayable message; callers render it however fits their
    surface (an alert in the bot, an HTTP error body in the API)."""
    is_topup = topup_inr is not None

    async with db.tx() as conn:
        order_id, inr_total = await _create_pending_order_or_topup(
            conn, user, payment_method, order_currency,
            topup_inr=topup_inr, product_id=product_id,
            quantity=quantity, coupon_code=coupon_code)

    usdt_amount = crypto(inr_total / await usdt_rate())
    usdt_amount = apply_safety_fee(usdt_amount, await crypto_safety_fee_percent(chain))
    ttl = await payment_ttl_minutes()

    try:
        invoice = await cryptomus.create_invoice(
            # Not our payment id (doesn't exist yet -- see below); just
            # needs to be unique per attempt for Cryptomus's own
            # bookkeeping. Our actual lookup key for the webhook is
            # cryptomus_uuid, set once we have it.
            order_id=f"{'topup' if is_topup else 'order'}-{order_id or 'na'}-{secrets.token_hex(6)}",
            amount=usdt_amount, network=network, lifetime_seconds=ttl * 60,
            callback_url=await cryptomus_webhook_url(),
            additional_data=f"user:{user['id']}",
        )
    except Exception:
        # Network/transport failure talking to Cryptomus -- create_invoice
        # only converts *API-level* failures (bad request, etc) into
        # invoice.ok=False; a genuine connection/timeout error raises. Both
        # must land in the same "couldn't create the invoice" handling
        # below so the just-created order never ends up dangling with no
        # payment row and no way for the user to pay it.
        log.exception("cryptomus invoice creation raised for user %s", user["id"])
        invoice = CryptomusResult(False, "network error")

    if not invoice.ok or not invoice.uuid or not invoice.address:
        log.error("cryptomus invoice creation failed for user %s: %s",
                  user["id"], invoice.reason)
        if order_id:
            await db.execute(
                "UPDATE orders SET status='cancelled' WHERE id=$1 AND status='pending'",
                order_id)
        raise CryptomusInvoiceError(
            "Couldn't create the crypto invoice right now. Try again, or use manual payment.")

    expected_amount = invoice.amount if invoice.amount > ZERO else usdt_amount
    expires = datetime.now(timezone.utc) + timedelta(minutes=ttl)

    # Single INSERT with cryptomus_uuid/expected_address/expires_at all
    # populated together -- never a window where the row exists but is
    # invisible to the webhook lookup (by cryptomus_uuid) or immortal in the
    # expiry sweep (which requires expires_at IS NOT NULL).
    return await db.fetchrow(
        """INSERT INTO payments
             (order_id, user_id, purpose, method, amount, currency,
              expected_amount, expected_address, chain, cryptomus_uuid,
              status, verification, expires_at)
           VALUES($1,$2,$3,$4,$5,'USDT',$6,$7,$8,$9,'pending','cryptomus',$10)
           RETURNING *""",
        order_id, user["id"], "topup" if is_topup else "order",
        payment_method, inr_total, expected_amount,
        invoice.address, chain, invoice.uuid, expires,
    )


class OxaPayInvoiceError(Exception):
    """User-facing reason an OxaPay invoice couldn't be created."""


async def _create_oxapay_payment(
    user: asyncpg.Record, order_currency: str, *,
    topup_inr: Any = None, product_id: int | None = None,
    quantity: int | None = None, coupon_code: str | None = None,
) -> asyncpg.Record:
    """Create the order (or resolve a topup amount) and a live OxaPay
    invoice together, ending in a single payment-row INSERT with
    oxapay_track_id/expected_address(=payment_url)/expires_at all
    populated at once -- same no-orphan-window shape as
    _create_cryptomus_payment. Raises OrderCreationError (stock/coupon/
    session problems) or OxaPayInvoiceError (OxaPay unreachable or
    rejected the request), both carrying a caller-displayable message."""
    is_topup = topup_inr is not None

    async with db.tx() as conn:
        order_id, inr_total = await _create_pending_order_or_topup(
            conn, user, "oxapay", order_currency,
            topup_inr=topup_inr, product_id=product_id,
            quantity=quantity, coupon_code=coupon_code)

    usdt_amount = crypto(inr_total / await usdt_rate())
    # chain=None: OxaPay is chain-agnostic on our side (the customer picks
    # the network on OxaPay's own page), so this always uses the baseline
    # safety-fee rate, never the TRC20 override.
    usdt_amount = apply_safety_fee(usdt_amount, await crypto_safety_fee_percent(None))
    ttl = await payment_ttl_minutes()

    try:
        invoice = await oxapay.create_invoice(
            amount=usdt_amount,
            order_id=f"{'topup' if is_topup else 'order'}-{order_id or 'na'}-{secrets.token_hex(6)}",
            callback_url=await oxapay_webhook_url(),
            lifetime_minutes=ttl,
            description=f"user:{user['id']}",
        )
    except Exception:
        # Network/transport failure talking to OxaPay -- create_invoice
        # only converts *API-level* failures (bad request, etc) into
        # invoice.ok=False; a genuine connection/timeout error raises. Both
        # must land in the same "couldn't create the invoice" handling
        # below so the just-created order never ends up dangling with no
        # payment row and no way for the user to pay it.
        log.exception("oxapay invoice creation raised for user %s", user["id"])
        invoice = OxaPayResult(False, "network error")

    if not invoice.ok or not invoice.track_id or not invoice.payment_url:
        log.error("oxapay invoice creation failed for user %s: %s",
                  user["id"], invoice.reason)
        if order_id:
            await db.execute(
                "UPDATE orders SET status='cancelled' WHERE id=$1 AND status='pending'",
                order_id)
        raise OxaPayInvoiceError(
            "Couldn't create the crypto invoice right now. Try again, or use another payment method.")

    expires = datetime.now(timezone.utc) + timedelta(minutes=ttl)

    # Single INSERT with oxapay_track_id/expected_address/expires_at all
    # populated together -- never a window where the row exists but is
    # invisible to the webhook lookup (by oxapay_track_id) or immortal in
    # the expiry sweep (which requires expires_at IS NOT NULL).
    # expected_address holds the OxaPay hosted-checkout payment_url here
    # (never an on-chain address -- OxaPay resolves that on its own page).
    return await db.fetchrow(
        """INSERT INTO payments
             (order_id, user_id, purpose, method, amount, currency,
              expected_amount, expected_address, chain, oxapay_track_id,
              status, verification, expires_at)
           VALUES($1,$2,$3,'oxapay',$4,'USDT',$5,$6,'USDT',$7,'pending','oxapay',$8)
           RETURNING *""",
        order_id, user["id"], "topup" if is_topup else "order",
        inr_total, usdt_amount, invoice.payment_url, invoice.track_id, expires,
    )


def cryptomus_chain_button(chain: str) -> InlineKeyboardButton:
    """Same premium-icon convention as usdt_chain_button, pointed at the
    instant Cryptomus invoice flow instead of the manual tx-hash one."""
    return _usdt_chain_button(chain, "cmpay", " (instant)")


@router.callback_query(F.data == "pay:crypto")
async def cb_pay_crypto(cq: CallbackQuery, state: FSMContext) -> None:
    user = await guard(cq)
    if not user:
        return
    lang = user["language"]
    cryptomus_on = await settings.get_bool("cryptomus_enabled", False)
    manual_on = await settings.get_bool("manual_crypto_enabled", True)

    rows = []
    if cryptomus_on:
        for chain in ("TRC20", "BEP20", "ERC20"):
            rows.append([cryptomus_chain_button(chain)])
    if manual_on:
        for chain in ("TRC20", "BEP20", "ERC20"):
            if await chain_address(chain):
                btn = usdt_chain_button(chain)
                if cryptomus_on:
                    # Distinguish the manual fallback from the instant
                    # option above -- model_copy preserves icon_custom_emoji_id,
                    # unlike rebuilding the button from scratch.
                    btn = btn.model_copy(update={"text": f"{btn.text} (manual)"})
                rows.append([btn])
    if not rows:
        await asyncio.gather(
            edit_or_send(cq, "Crypto payment isn't configured yet.", back_kb(lang), section="payment"),
            _ack(cq),
        )
        return
    rows.append([InlineKeyboardButton(text=t(lang, "back"), callback_data="menu")])
    await asyncio.gather(
        edit_or_send(cq, "₮ <b>Choose a network</b>\n\nSend only USDT on the network you pick.",
                    InlineKeyboardMarkup(inline_keyboard=rows), section="payment"),
        _ack(cq),
    )


@router.callback_query(F.data.startswith("chain:"))
async def cb_chain(cq: CallbackQuery, state: FSMContext) -> None:
    user = await guard(cq)
    if not user:
        return
    chain = cq.data.split(":")[1]
    data = await state.get_data()
    lang = user["language"]

    if not await chain_address(chain):
        await cq.answer("That network isn't available.", show_alert=True)
        return

    fee_pct = await crypto_safety_fee_percent(chain)

    try:
        payment = await _create_chain_payment(
            user, chain, f"usdt_{chain.lower()}", await user_currency(user),
            topup_inr=data.get("topup_inr"), product_id=data.get("product_id"),
            quantity=data.get("quantity"), coupon_code=data.get("coupon_code"))
    except OrderCreationError as e:
        await cq.answer(str(e), show_alert=True)
        return

    usdt_amount = crypto(payment["expected_amount"])
    address = payment["expected_address"]
    _ttl = await payment_ttl_minutes()

    await state.update_data(payment_id=payment["id"])
    await state.set_state(Flow.tx_id)

    text = (
        f"{chain_glyph_html(chain)} <b>USDT payment — {chain}</b>\n\n"
        f"Send <b>exactly</b>:\n"
        f"<code>{usdt_amount.normalize():f}</code> USDT\n"
        f"<i>(includes {fee_pct.normalize():f}% safety fee)</i>\n\n"
        f"To this address:\n<code>{address}</code>\n\n"
        f"⚠️ <b>{chain} network only.</b> Other networks lose the funds.\n"
        f"⏱ Expires in {_ttl} minutes.\n\n"
        f"After sending, paste your <b>transaction hash</b> here. "
        f"We verify it on-chain and deliver automatically."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "cancel"), callback_data=f"cancelpay:{payment['id']}")],
    ])
    await asyncio.gather(edit_or_send(cq, text, kb, section="payment"), _ack(cq))


@router.callback_query(F.data.startswith("cmpay:"))
async def cb_cryptomus_chain(cq: CallbackQuery, state: FSMContext) -> None:
    """Instant Cryptomus invoice flow: unique deposit address + webhook
    auto-confirmation, no manual transaction hash. Sits alongside cb_chain
    (the manual flow) -- both are reachable from cb_pay_crypto depending on
    which are admin-enabled."""
    user = await guard(cq)
    if not user:
        return
    if not await settings.get_bool("cryptomus_enabled", False):
        await cq.answer("This payment method isn't available.", show_alert=True)
        return
    chain = cq.data.split(":")[1]
    data = await state.get_data()
    lang = user["language"]

    network = await cryptomus_network_for(chain)
    if not network:
        await cq.answer("That network isn't available.", show_alert=True)
        return

    try:
        payment = await _create_cryptomus_payment(
            user, chain, network, f"cryptomus_{chain.lower()}", await user_currency(user),
            topup_inr=data.get("topup_inr"), product_id=data.get("product_id"),
            quantity=data.get("quantity"), coupon_code=data.get("coupon_code"))
    except OrderCreationError as e:
        await cq.answer(str(e), show_alert=True)
        return
    except CryptomusInvoiceError as e:
        await cq.answer(str(e), show_alert=True)
        return

    expected_amount = crypto(payment["expected_amount"])
    address = payment["expected_address"]
    invoice_uuid = payment["cryptomus_uuid"]
    _ttl = await payment_ttl_minutes()
    fee_pct = await crypto_safety_fee_percent(chain)

    await state.update_data(payment_id=payment["id"])
    # Deliberately no Flow.tx_id state -- Cryptomus users are never asked
    # for a transaction hash; confirmation is fully automatic.

    text = (
        f"{chain_glyph_html(chain)} <b>USDT payment — {chain} (instant)</b>\n\n"
        f"Send <b>exactly</b>:\n"
        f"<code>{expected_amount.normalize():f}</code> USDT\n"
        f"<i>(includes {fee_pct.normalize():f}% safety fee)</i>\n\n"
        f"To this address:\n<code>{address}</code>\n\n"
        f"⚠️ <b>{chain} network only.</b> Other networks lose the funds.\n"
        f"🧾 Invoice: <code>{invoice_uuid}</code>\n"
        f"⏱ Expires in {_ttl} minutes.\n\n"
        f"This is confirmed automatically — no transaction hash needed."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Check status", callback_data=f"cmcheck:{payment['id']}")],
        [InlineKeyboardButton(text=t(lang, "cancel"), callback_data=f"cancelpay:{payment['id']}")],
    ])

    # Render the primary screen first -- amount/address/buttons are the
    # important, time-sensitive part, and invoice creation already cost one
    # round trip to Cryptomus. The QR is a nice-to-have image; fetching it
    # (a second round trip) before showing anything made every Cryptomus
    # payment wait on two sequential external calls instead of one. The
    # render and the tap-ack are themselves gathered for the same reason.
    await asyncio.gather(edit_or_send(cq, text, kb, section="payment"), _ack(cq))

    qr_b64 = await cryptomus.get_qr(invoice_uuid)
    if qr_b64 and qr_b64.startswith("data:image") and "," in qr_b64:
        try:
            _, b64_data = qr_b64.split(",", 1)
            photo = BufferedInputFile(base64.b64decode(b64_data), filename="cryptomus-qr.png")
            # Sent as its own message rather than folded into edit_or_send's
            # tracked nav slot -- the amount/address/buttons above are what
            # Check-status/Cancel keep live; making the QR the nav message
            # would force every later screen in this chat into photo type
            # (see edit_or_send_photo's docstring on that cost) for a QR
            # that's only relevant for the next few minutes.
            await cq.message.answer_photo(photo, caption="Scan to pay")
        except Exception:
            log.exception("failed to send cryptomus QR image for payment %s", payment["id"])


@router.callback_query(F.data.startswith("cmcheck:"))
async def cb_cryptomus_check(cq: CallbackQuery, state: FSMContext) -> None:
    """Manual 'check now' for a Cryptomus invoice, for users who don't want
    to wait for the webhook. Uses the exact same authoritative get_info +
    _process_cryptomus_status path the webhook itself uses -- never a
    separate, weaker verification."""
    user = await guard(cq)
    if not user:
        return
    pid = int(cq.data.split(":")[1])
    p = await db.fetchrow(
        "SELECT * FROM payments WHERE id=$1 AND user_id=$2 AND verification='cryptomus'",
        pid, user["id"])
    if not p:
        await cq.answer("Payment not found.", show_alert=True)
        return
    if p["status"] != "pending":
        await cq.answer(f"This payment is already {p['status']}.", show_alert=True)
        return
    if not p["cryptomus_uuid"]:
        await cq.answer("Still setting up — try again in a moment.", show_alert=True)
        return

    try:
        info = await cryptomus.get_info(p["cryptomus_uuid"])
    except Exception:
        log.exception("cryptomus get_info failed for payment %s", pid)
        await cq.answer("Couldn't reach Cryptomus right now — try again shortly.", show_alert=True)
        return
    if not info.ok:
        await cq.answer("Couldn't check status right now — try again shortly.", show_alert=True)
        return

    outcome = await _process_cryptomus_status(bot, p, info)
    if outcome == "paid":
        await cq.answer("Payment confirmed! ✅")
    elif outcome == "failed":
        await cq.answer("This payment didn't go through.", show_alert=True)
    else:
        await cq.answer("Still waiting for confirmation — check again shortly.")


@router.callback_query(F.data == "pay:oxapay")
async def cb_pay_oxapay(cq: CallbackQuery, state: FSMContext) -> None:
    """OxaPay hosted checkout: one button, one invoice, one link -- the
    customer picks their own network/currency entirely on OxaPay's own
    page, so there's no chain-picker, address, or QR to render here.
    Confirmation is webhook-driven (see /api/oxapay/webhook + a live
    get_status re-check), same "never trust the claim alone" pattern
    Cryptomus uses."""
    user = await guard(cq)
    if not user:
        return
    if not await settings.get_bool("oxapay_enabled", False):
        await cq.answer("This payment method isn't available.", show_alert=True)
        return

    data = await state.get_data()
    lang = user["language"]
    # Telegram only accepts one answerCallbackQuery per tap -- answering
    # "processing" here up front, then trying to answer *again* with
    # show_alert=True on a failure below, silently fails on the second call
    # (the query id is already spent) and aiogram just logs it. The user
    # saw the fleeting "Processing…" toast and then nothing: no error, no
    # updated screen -- exactly the "stuck on processing" symptom. Errors
    # below now edit the message instead, same pattern cb_pay_wallet uses.
    await cq.answer(t(lang, "processing"))

    try:
        payment = await _create_oxapay_payment(
            user, await user_currency(user),
            topup_inr=data.get("topup_inr"), product_id=data.get("product_id"),
            quantity=data.get("quantity"), coupon_code=data.get("coupon_code"))
    except OrderCreationError as e:
        await edit_or_send(cq, f"⚠️ {e}", back_kb(lang), section="payment")
        return
    except OxaPayInvoiceError as e:
        await edit_or_send(cq, f"⚠️ {e}", back_kb(lang), section="payment")
        return

    payment_url = payment["expected_address"]
    _ttl = await payment_ttl_minutes()

    await state.update_data(payment_id=payment["id"])

    text = (
        f"🟢 <b>Crypto payment</b>\n\n"
        f"Tap <b>Pay now</b> to open the secure checkout page and pay "
        f"with whichever crypto/network is offered.\n\n"
        f"⏱ Expires in {_ttl} minutes.\n\n"
        f"This is checked automatically — no need to tap anything here once you've paid."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Pay now", url=payment_url)],
        [InlineKeyboardButton(text=t(lang, "cancel"), callback_data=f"cancelpay:{payment['id']}")],
    ])
    await edit_or_send(cq, text, kb, section="payment")


@router.callback_query(F.data.startswith("cancelpay:"))
async def cb_cancelpay(cq: CallbackQuery, state: FSMContext) -> None:
    user = await guard(cq)
    if not user:
        return
    pid = int(cq.data.split(":")[1])
    async with db.tx() as conn:
        p = await conn.fetchrow(
            "SELECT * FROM payments WHERE id=$1 AND user_id=$2 FOR UPDATE",
            pid, user["id"])
        if p and p["status"] == "pending":
            await conn.execute(
                "UPDATE payments SET status='cancelled' WHERE id=$1", pid)
            if p["order_id"]:
                await conn.execute(
                    "UPDATE orders SET status='cancelled' WHERE id=$1 AND status='pending'",
                    p["order_id"])
    await state.clear()
    await asyncio.gather(
        _ack(cq, "Cancelled — nothing was charged."),
        show_menu(cq, user),
    )


@router.message(Flow.tx_id, F.text)
async def msg_txid(msg: Message, state: FSMContext) -> None:
    user = await guard(msg)
    if not user:
        return
    data = await state.get_data()
    payment_id = data.get("payment_id")
    tx = (msg.text or "").strip()

    if len(tx) < 16 or len(tx) > 128 or " " in tx:
        await msg.answer("That doesn't look like a transaction hash. Paste the full hash.")
        return

    p = await db.fetchrow(
        "SELECT * FROM payments WHERE id=$1 AND user_id=$2", payment_id, user["id"])
    if not p or p["status"] != "pending":
        await state.clear()
        await msg.answer("That payment is no longer open.")
        return

    try:
        await db.execute(
            "UPDATE payments SET tx_id=$1, verification='chain' WHERE id=$2", tx, payment_id)
    except asyncpg.UniqueViolationError:
        await msg.answer(
            "⚠️ That transaction hash has already been submitted. "
            "Each transaction can only be used once."
        )
        return

    await state.clear()
    await edit_or_send(
        msg,
        "🔍 <b>Checking the blockchain…</b>\n\n"
        f"We need {await min_confirmations(p['chain'])} confirmations. This usually takes "
        f"a few minutes — you'll get your order automatically as soon as it clears.\n\n"
        "You can close Telegram; we'll message you.",
        back_kb(user["language"]),
        section="payment",
    )

    # Try immediately so fast chains feel instant.
    asyncio.create_task(_verify_soon(payment_id))


async def _verify_soon(payment_id: int) -> None:
    await asyncio.sleep(3)
    try:
        p = await db.fetchrow("SELECT * FROM payments WHERE id=$1", payment_id)
        if p and p["status"] == "pending":
            await _verify_one(bot, p)
    except Exception:
        log.exception("eager verification failed for %s", payment_id)


@router.message(Flow.tx_id, F.photo)
async def msg_screenshot(msg: Message, state: FSMContext) -> None:
    """Screenshot fallback -- always manual review, never auto-approved."""
    user = await guard(msg)
    if not user:
        return
    data = await state.get_data()
    payment_id = data.get("payment_id")
    p = await db.fetchrow(
        "SELECT * FROM payments WHERE id=$1 AND user_id=$2", payment_id, user["id"])
    if not p or p["status"] != "pending":
        await state.clear()
        await msg.answer("That payment is no longer open.")
        return

    file_id = msg.photo[-1].file_id
    await db.execute(
        """UPDATE payments SET screenshot_file_id=$1, verification='manual',
               verify_note='screenshot submitted' WHERE id=$2""",
        file_id, payment_id,
    )
    await state.clear()
    await edit_or_send(
        msg,
        "📸 Screenshot received.\n\n"
        "Screenshots need manual review, so this is slower than submitting a "
        "transaction hash. We'll message you once it's checked.",
        back_kb(user["language"]),
        section="payment",
    )


# ---- UPI ---------------------------------------------------------------------

@router.callback_query(F.data == "pay:upi")
async def cb_pay_upi(cq: CallbackQuery, state: FSMContext) -> None:
    user = await guard(cq)
    if not user:
        return
    data = await state.get_data()
    upi = (await settings.get("upi_id", "")).strip()
    if not upi:
        await cq.answer("UPI isn't configured.", show_alert=True)
        return

    try:
        async with db.tx() as conn:
            order_id, total = await _create_pending_order_or_topup(
                conn, user, "upi", "INR",
                topup_inr=data.get("topup_inr"), product_id=data.get("product_id"),
                quantity=data.get("quantity"), coupon_code=data.get("coupon_code"))

            _ttl = await payment_ttl_minutes()
            expires = datetime.now(timezone.utc) + timedelta(minutes=_ttl)
            payment = await conn.fetchrow(
                """INSERT INTO payments
                     (order_id, user_id, purpose, method, amount, currency, status,
                      verification, expires_at)
                   VALUES($1,$2,'order','upi',$3,'INR','pending','manual',$4)
                   RETURNING *""",
                order_id, user["id"], total, expires,
            )
    except OrderCreationError as e:
        await cq.answer(str(e), show_alert=True)
        return

    await state.update_data(payment_id=payment["id"])
    await state.set_state(Flow.tx_id)

    text = (
        f"🇮🇳 <b>UPI payment</b>\n\n"
        f"Amount: <b>₹{total:,.2f}</b>\n"
        f"UPI ID: <code>{upi}</code>\n\n"
        f"After paying, send the <b>UPI reference number</b> or a screenshot.\n"
        f"⏱ Expires in {_ttl} minutes."
    )
    qr = (await settings.get("upi_qr_file_id", "")).strip()
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=t(user["language"], "cancel"),
                             callback_data=f"cancelpay:{payment['id']}")
    ]])
    render = (edit_or_send_photo(cq, qr, text, kb) if qr
             else edit_or_send(cq, text, kb, section="payment"))
    await asyncio.gather(render, _ack(cq))


# =============================================================================
#  WALLET
# =============================================================================

async def wallet_kb(user: asyncpg.Record) -> InlineKeyboardMarkup:
    lang = user["language"]
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "topup"), callback_data="topup")],
        [InlineKeyboardButton(text="📜 History", callback_data="wtx:1")],
        [InlineKeyboardButton(text=t(lang, "back"), callback_data="menu")],
    ])


async def _render_wallet(event: Message | CallbackQuery, user: asyncpg.Record) -> None:
    cur = await user_currency(user)
    bal = await display_price(money(user["wallet_balance"], Q_CRYPTO), cur)
    spent = await display_price(money(user["total_spent"], Q_CRYPTO), cur)
    text = (
        f"💰 <b>Wallet</b>\n\n"
        f"Balance: <b>{bal}</b>\n"
        f"Lifetime spend: {spent}\n"
        f"Tier: <b>{(user['tier'] or 'bronze').title()}</b>"
    )
    disc = await tier_discount_percent(user["tier"] or "bronze")
    if disc > ZERO:
        text += f" — {disc}% off every order"

    await edit_or_send(event, text, await wallet_kb(user), section="wallet")


@router.callback_query(F.data == "wallet")
async def cb_wallet(cq: CallbackQuery) -> None:
    user = await guard(cq)
    if not user:
        return
    await asyncio.gather(_render_wallet(cq, user), _ack(cq))


@router.message(Command("wallet"))
async def msg_wallet(msg: Message, state: FSMContext) -> None:
    user = await guard(msg)
    if not user:
        return
    await state.clear()
    await _render_wallet(msg, user)


@router.callback_query(F.data == "topup")
async def cb_topup(cq: CallbackQuery, state: FSMContext) -> None:
    user = await guard(cq)
    if not user:
        return
    await state.set_state(Flow.topup_amount)
    cur = await user_currency(user)
    min_amt = await settings.get_decimal(
        "min_topup_usdt" if cur == "USDT" else "min_topup_inr", "50")
    await asyncio.gather(
        edit_or_send(
            cq,
            f"➕ <b>Top up</b>\n\nHow much would you like to add?\n"
            f"Minimum: {fmt_money(min_amt, cur)}",
            back_kb(user["language"], "wallet"),
            section="wallet",
        ),
        _ack(cq),
    )


@router.message(Flow.topup_amount)
async def msg_topup(msg: Message, state: FSMContext) -> None:
    user = await guard(msg)
    if not user:
        return
    cur = await user_currency(user)
    raw = (msg.text or "").strip().replace(",", "").replace("₹", "")
    try:
        amount = money(Decimal(raw), Q_CRYPTO)
    except (InvalidOperation, ValueError):
        await msg.answer("Please send a number, like 500.")
        return

    min_amt = await settings.get_decimal(
        "min_topup_usdt" if cur == "USDT" else "min_topup_inr", "50")
    if amount < min_amt:
        await msg.answer(f"Minimum top-up is {fmt_money(min_amt, cur)}.")
        return
    if amount > Decimal("1000000"):
        await msg.answer("That's above the maximum. Please contact support for large top-ups.")
        return

    inr = await from_currency(amount, cur)
    await state.update_data(topup_inr=str(inr))
    await state.set_state(None)

    rows = []
    if await settings.get_bool("oxapay_enabled", False):
        rows.append([InlineKeyboardButton(
            text="🟢 Pay with Crypto", callback_data="pay:oxapay")])
    rows.append([InlineKeyboardButton(
        text=t(user["language"], "cancel"), callback_data="wallet")])

    await edit_or_send(
        msg,
        f"➕ Topping up <b>{fmt_money(amount, cur)}</b>\n\nChoose a payment network:",
        InlineKeyboardMarkup(inline_keyboard=rows),
        section="payment",
    )


@router.callback_query(F.data.startswith("wtx:"))
async def cb_wtx(cq: CallbackQuery) -> None:
    user = await guard(cq)
    if not user:
        return
    page = max(1, int(cq.data.split(":")[1]))
    cur = await user_currency(user)
    rows = await db.fetch(
        """SELECT * FROM wallet_transactions WHERE user_id=$1
            ORDER BY created_at DESC LIMIT 10 OFFSET $2""",
        user["id"], (page - 1) * 10,
    )
    if not rows:
        await asyncio.gather(
            edit_or_send(cq, "No transactions yet.", await wallet_kb(user), section="wallet"),
            _ack(cq),
        )
        return

    lines = ["📜 <b>Wallet history</b>\n"]
    for r in rows:
        amt = money(r["amount"], Q_CRYPTO)
        sign = "+" if amt > ZERO else ""
        when = r["created_at"].strftime("%d %b %H:%M")
        lines.append(
            f"{when} · {sign}{await display_price(abs(amt), cur)} · {r['description'] or r['type']}"
        )

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"wtx:{page-1}"))
    if len(rows) == 10:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"wtx:{page+1}"))
    kb_rows = [nav] if nav else []
    kb_rows.append([InlineKeyboardButton(
        text=t(user["language"], "back"), callback_data="wallet")])

    await asyncio.gather(
        edit_or_send(cq, "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=kb_rows),
                    section="wallet"),
        _ack(cq),
    )


# =============================================================================
#  ORDERS / REFERRAL / SUPPORT
# =============================================================================

ORDER_STATUS_ICONS = {"delivered": "✅", "pending": "⏳", "processing": "🔄",
                       "cancelled": "❌", "paid": "💳"}


async def _render_orders(event: Message | CallbackQuery, user: asyncpg.Record, page: int = 1) -> None:
    cur = await user_currency(user)
    rows = await db.fetch(
        """SELECT o.*, p.name FROM orders o
             JOIN products p ON p.id = o.product_id
            WHERE o.user_id=$1
            ORDER BY o.created_at DESC LIMIT 5 OFFSET $2""",
        user["id"], (page - 1) * 5,
    )
    if not rows:
        await edit_or_send(event, "📦 No orders yet.", back_kb(user["language"]), section="orders")
        return

    lines = ["📦 <b>Your orders</b>\n"]
    kb_rows = []
    for o in rows:
        icon = ORDER_STATUS_ICONS.get(o["status"], "•")
        when = o["created_at"].strftime("%d %b")
        lines.append(
            f"{icon} <code>#{o['id']}</code> {o['name']} ×{o['quantity']} — "
            f"{await display_price(money(o['total'], Q_CRYPTO), cur)} · {when}"
        )
        if o["status"] == "delivered" and o["delivered_content"]:
            kb_rows.append([InlineKeyboardButton(
                text=f"🔑 View #{o['id']}", callback_data=f"vieworder:{o['id']}")])

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"orders:{page-1}"))
    if len(rows) == 5:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"orders:{page+1}"))
    if nav:
        kb_rows.append(nav)
    kb_rows.append([InlineKeyboardButton(
        text=t(user["language"], "back"), callback_data="menu")])

    await edit_or_send(event, "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=kb_rows),
                       section="orders")


@router.callback_query(F.data.startswith("orders:"))
async def cb_orders(cq: CallbackQuery) -> None:
    user = await guard(cq)
    if not user:
        return
    page = max(1, int(cq.data.split(":")[1]))
    await asyncio.gather(_render_orders(cq, user, page), _ack(cq))


@router.message(Command("orders"))
async def msg_orders(msg: Message, state: FSMContext) -> None:
    user = await guard(msg)
    if not user:
        return
    await state.clear()
    await _render_orders(msg, user, 1)


@router.callback_query(F.data.startswith("vieworder:"))
async def cb_vieworder(cq: CallbackQuery) -> None:
    user = await guard(cq)
    if not user:
        return
    oid = int(cq.data.split(":")[1])
    o = await db.fetchrow(
        """SELECT o.*, p.name FROM orders o JOIN products p ON p.id=o.product_id
            WHERE o.id=$1 AND o.user_id=$2""",
        oid, user["id"],
    )
    if not o or not o["delivered_content"]:
        await cq.answer("Nothing to show.", show_alert=True)
        return
    keys = [k for k in o["delivered_content"].split("\n") if k]
    body = "\n".join(f"<code>{k}</code>" for k in keys)
    await asyncio.gather(
        edit_or_send(
            cq,
            f"🔑 <b>Order #{oid}</b> — {o['name']}\n\n{body}",
            back_kb(user["language"], "orders:1"),
        ),
        _ack(cq),
    )


@router.callback_query(F.data == "ref")
async def cb_ref(cq: CallbackQuery) -> None:
    user = await guard(cq)
    if not user:
        return
    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start=ref_{user['referral_code']}"
    pct = await settings.get_decimal("referral_bonus_percent", "5")
    count = int(await db.fetchval(
        "SELECT COUNT(*) FROM store_users WHERE referred_by_id=$1", user["id"]) or 0)
    earned = money(await db.fetchval(
        """SELECT COALESCE(SUM(amount),0) FROM wallet_transactions
            WHERE user_id=$1 AND type='referral_bonus'""", user["id"]) or 0, Q_CRYPTO)
    cur = await user_currency(user)

    text = (
        f"🎁 <b>Refer & earn</b>\n\n"
        f"Share your link and earn <b>{pct}%</b> of your friend's first order.\n\n"
        f"🔗 <code>{link}</code>\n\n"
        f"👥 Referred: <b>{count}</b>\n"
        f"💰 Earned: <b>{await display_price(earned, cur)}</b>"
    )
    await asyncio.gather(
        edit_or_send(cq, text, back_kb(user["language"]), section="referral"), _ack(cq))


TICKET_STATUS_ICON = {"open": "🟢 Open", "closed": "⚫ Closed"}
TICKET_PAGE_SIZE = 5


def _ticket_message_line(m: asyncpg.Record, *, viewer: str) -> str:
    """One "<b>Label:</b> body" line for a ticket_messages row, shared by
    the customer-facing thread view and the in-Telegram admin one -- only
    the label wording differs by perspective. Admin replies are validated
    HTML at write time (see TicketReplyIn / _ticket_reply) so they render
    as intended; user messages are raw free text and must be escaped or a
    literal "<" would break HTML parsing."""
    if viewer == "user":
        label = "🧑 You" if m["sender"] == "user" else "🛠 Support"
    else:
        label = "🧑 User" if m["sender"] == "user" else "🛠 You"
    body = html.escape(m["message"]) if m["sender"] == "user" else m["message"]
    if m["photo_file_id"]:
        body += ("\n" if body else "") + "📷 <i>Photo attached</i>"
    return f"<b>{label}:</b>\n{body}\n"


def support_kb(lang: str, has_tickets: bool) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="🎫 Open a Ticket", callback_data="ticket:new")]]
    if has_tickets:
        rows.append([InlineKeyboardButton(text="📋 My Tickets", callback_data="ticket:list:1")])
    rows.append([InlineKeyboardButton(text=t(lang, "back"), callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _render_support(event: Message | CallbackQuery, user: asyncpg.Record) -> None:
    handle = (await settings.get("support_username", "")).strip().lstrip("@")
    if handle:
        text = (f"💬 <b>Support</b>\n\nMessage @{handle} and include your order number, "
                f"or open a ticket below and we'll reply right here in the bot.")
    else:
        text = "💬 <b>Support</b>\n\nOpen a ticket below and we'll reply right here in the bot."
    has_tickets = bool(await db.fetchval(
        "SELECT 1 FROM tickets WHERE user_id=$1 LIMIT 1", user["id"]))
    await edit_or_send(event, text, support_kb(user["language"], has_tickets), section="support")


@router.callback_query(F.data == "support")
async def cb_support(cq: CallbackQuery) -> None:
    user = await guard(cq)
    if not user:
        return
    await asyncio.gather(_render_support(cq, user), _ack(cq))


@router.message(Command("support"))
async def msg_support(msg: Message, state: FSMContext) -> None:
    user = await guard(msg)
    if not user:
        return
    await state.clear()
    await _render_support(msg, user)


@router.callback_query(F.data == "ticket:new")
async def cb_ticket_new(cq: CallbackQuery, state: FSMContext) -> None:
    user = await guard(cq)
    if not user:
        return
    await state.set_state(Flow.support_message)
    await state.update_data(ticket_id=None)
    await asyncio.gather(
        edit_or_send(
            cq,
            "🎫 <b>New ticket</b>\n\nDescribe your issue in one message (up to 1000 "
            "characters), or send a photo with an optional caption. We'll reply here in the bot.",
            back_kb(user["language"], "support"),
            section="support",
        ),
        _ack(cq),
    )


def _ticket_notify_body(text: str, photo_file_id: str) -> str:
    """Shared by both the new-ticket and reply paths (user and admin side
    notifications) so the "text, photo, or both" formatting lives in one
    place. `text` must already be HTML-escaped/validated by the caller --
    this only decides whether to append the photo marker."""
    body = text or ""
    if photo_file_id:
        body += ("\n" if body else "") + "📷 <i>Photo attached</i>"
    return body


async def _handle_ticket_submission(
    msg: Message, state: FSMContext, text: str, photo_file_id: str
) -> None:
    """Shared by the text and photo message handlers below -- everything
    past "what did the user send" (create-or-reply, persist, notify) is
    identical either way."""
    user = await guard(msg)
    if not user:
        return
    if not text and not photo_file_id:
        await msg.answer("Please send some text or a photo.")
        return
    if len(text) > 1000:
        await msg.answer("That's too long — keep it under 1000 characters.")
        return

    data = await state.get_data()
    ticket_id = data.get("ticket_id")
    await state.clear()
    who = html.escape(msg.from_user.first_name or msg.from_user.username or str(msg.from_user.id))
    notify_body = _ticket_notify_body(html.escape(text) if text else "", photo_file_id)

    if ticket_id:
        ticket = await db.fetchrow(
            "SELECT * FROM tickets WHERE id=$1 AND user_id=$2", ticket_id, user["id"])
        if not ticket:
            await msg.answer("That ticket no longer exists.")
            return
        if ticket["status"] != "open":
            await msg.answer("That ticket is closed — open a new one if you still need help.")
            return
        await db.execute(
            "INSERT INTO ticket_messages(ticket_id, sender, message, photo_file_id) VALUES($1,'user',$2,$3)",
            ticket_id, text, photo_file_id or None)
        await db.execute("UPDATE tickets SET updated_at=NOW() WHERE id=$1", ticket_id)
        await msg.answer(f"✅ Reply added to ticket #{ticket_id}.")
        asyncio.create_task(notify_admins_text(
            bot, f"💬 New reply on ticket #{ticket_id} from {who}:\n\n{notify_body}"))
    else:
        ticket = await db.fetchrow(
            "INSERT INTO tickets(user_id, subject) VALUES($1,$2) RETURNING *",
            user["id"], text[:80] if text else "📷 Photo")
        await db.execute(
            "INSERT INTO ticket_messages(ticket_id, sender, message, photo_file_id) VALUES($1,'user',$2,$3)",
            ticket["id"], text, photo_file_id or None)
        await msg.answer(
            f"✅ Ticket #{ticket['id']} created. We'll reply right here — check "
            f"📋 My Tickets under Support for updates.")
        asyncio.create_task(notify_admins_text(
            bot, f"🎫 New support ticket #{ticket['id']} from {who}:\n\n{notify_body}"))


@router.message(Flow.support_message, F.text)
async def msg_ticket_text(msg: Message, state: FSMContext) -> None:
    await _handle_ticket_submission(msg, state, (msg.text or "").strip(), "")


@router.message(Flow.support_message, F.photo)
async def msg_ticket_photo(msg: Message, state: FSMContext) -> None:
    await _handle_ticket_submission(
        msg, state, (msg.caption or "").strip(), msg.photo[-1].file_id)


@router.message(Flow.support_message)
async def msg_ticket_other(msg: Message) -> None:
    # Catches anything that's neither text nor photo -- see the identical
    # comment on msg_manual_info_other for why this exists.
    await msg.answer("Please send text or a photo.")


async def _render_ticket_list(
    event: Message | CallbackQuery, user: asyncpg.Record, page: int = 1
) -> None:
    rows = await db.fetch(
        """SELECT id, subject, status, updated_at FROM tickets
             WHERE user_id=$1 ORDER BY updated_at DESC LIMIT $2 OFFSET $3""",
        user["id"], TICKET_PAGE_SIZE, (page - 1) * TICKET_PAGE_SIZE)
    if not rows:
        await edit_or_send(
            event, "📋 No tickets yet.", back_kb(user["language"], "support"), section="support")
        return

    lines = ["📋 <b>Your tickets</b>"]
    kb_rows = []
    for r in rows:
        icon = TICKET_STATUS_ICON.get(r["status"], r["status"])
        when = r["updated_at"].strftime("%d %b")
        kb_rows.append([InlineKeyboardButton(
            text=f"#{r['id']} {icon} — {r['subject'][:30]} ({when})",
            callback_data=f"ticket:view:{r['id']}")])

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"ticket:list:{page-1}"))
    if len(rows) == TICKET_PAGE_SIZE:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"ticket:list:{page+1}"))
    if nav:
        kb_rows.append(nav)
    kb_rows.append([InlineKeyboardButton(text=t(user["language"], "back"), callback_data="support")])

    await edit_or_send(
        event, "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=kb_rows), section="support")


@router.callback_query(F.data.startswith("ticket:list:"))
async def cb_ticket_list(cq: CallbackQuery) -> None:
    user = await guard(cq)
    if not user:
        return
    page = max(1, int(cq.data.split(":")[2]))
    await asyncio.gather(_render_ticket_list(cq, user, page), _ack(cq))


async def _render_ticket_view(
    event: Message | CallbackQuery, user: asyncpg.Record, ticket_id: int
) -> None:
    ticket = await db.fetchrow(
        "SELECT * FROM tickets WHERE id=$1 AND user_id=$2", ticket_id, user["id"])
    if not ticket:
        await edit_or_send(
            event, "Ticket not found.", back_kb(user["language"], "ticket:list:1"), section="support")
        return
    msgs = await db.fetch(
        "SELECT * FROM ticket_messages WHERE ticket_id=$1 ORDER BY created_at LIMIT 30", ticket_id)

    icon = TICKET_STATUS_ICON.get(ticket["status"], ticket["status"])
    lines = [f"🎫 <b>Ticket #{ticket_id}</b> — {icon}\n"]
    photo_rows = []
    for m in msgs:
        lines.append(_ticket_message_line(m, viewer="user"))
        if m["photo_file_id"]:
            who = "You" if m["sender"] == "user" else "Support"
            photo_rows.append([InlineKeyboardButton(
                text=f"📷 View photo ({who})", callback_data=f"ticket:photo:{m['id']}")])

    kb_rows = list(photo_rows)
    if ticket["status"] == "open":
        kb_rows.append([InlineKeyboardButton(text="✍️ Reply", callback_data=f"ticket:reply:{ticket_id}")])
    kb_rows.append([InlineKeyboardButton(text=t(user["language"], "back"), callback_data="ticket:list:1")])

    await edit_or_send(
        event, "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=kb_rows), section="support")


@router.callback_query(F.data.startswith("ticket:view:"))
async def cb_ticket_view(cq: CallbackQuery) -> None:
    user = await guard(cq)
    if not user:
        return
    ticket_id = int(cq.data.split(":")[2])
    await asyncio.gather(_render_ticket_view(cq, user, ticket_id), _ack(cq))


@router.callback_query(F.data.startswith("ticket:photo:"))
async def cb_ticket_photo(cq: CallbackQuery) -> None:
    user = await guard(cq)
    if not user:
        return
    mid = int(cq.data.split(":")[2])
    # Joined through tickets to enforce ownership -- a message id alone
    # doesn't prove this user's ticket, and photos may include another
    # buyer's screenshot.
    row = await db.fetchrow(
        """SELECT m.photo_file_id FROM ticket_messages m JOIN tickets t ON t.id=m.ticket_id
             WHERE m.id=$1 AND t.user_id=$2""", mid, user["id"])
    if not row or not row["photo_file_id"]:
        await cq.answer("Photo not found.", show_alert=True)
        return
    try:
        await bot.send_photo(_event_chat_id(cq), row["photo_file_id"])
    except Exception:
        await cq.answer("Couldn't load the image.", show_alert=True)
        return
    await _ack(cq)


@router.callback_query(F.data.startswith("ticket:reply:"))
async def cb_ticket_reply(cq: CallbackQuery, state: FSMContext) -> None:
    user = await guard(cq)
    if not user:
        return
    ticket_id = int(cq.data.split(":")[2])
    ticket = await db.fetchrow(
        "SELECT * FROM tickets WHERE id=$1 AND user_id=$2", ticket_id, user["id"])
    if not ticket or ticket["status"] != "open":
        await cq.answer("This ticket can't be replied to.", show_alert=True)
        return
    await state.set_state(Flow.support_message)
    await state.update_data(ticket_id=ticket_id)
    await asyncio.gather(
        edit_or_send(
            cq, f"✍️ <b>Reply to ticket #{ticket_id}</b>\n\nSend your reply as text or a photo.",
            back_kb(user["language"], f"ticket:view:{ticket_id}"), section="support",
        ),
        _ack(cq),
    )


async def _render_profile(event: Message | CallbackQuery, user: asyncpg.Record) -> None:
    """Tier, balance, order count and referral code — split out of the
    main menu into its own screen so the menu itself can stay a short
    greeting."""
    lang = user["language"]
    cur = await user_currency(user)
    tier = await rank_name(user["tier"] or "bronze")
    bal = await display_price(money(user["wallet_balance"], Q_CRYPTO), cur)
    spent = await display_price(money(user["total_spent"], Q_CRYPTO), cur)
    orders_count = int(await db.fetchval(
        "SELECT COUNT(*) FROM orders WHERE user_id=$1", user["id"]) or 0)
    joined = user["created_at"].strftime("%d %b %Y")

    text = (
        f"🙂 <b>Your profile</b>\n\n"
        f"👤 {user['first_name'] or 'there'}  •  {tier}\n"
        f"💰 {t(lang, 'balance')}: <b>{bal}</b>\n"
        f"🧾 Total spent: <b>{spent}</b>\n"
        f"📦 Orders: <b>{orders_count}</b>\n"
        f"🔗 Referral code: <code>{user['referral_code']}</code>\n"
        f"📅 Member since: {joined}"
    )

    markup = back_kb(lang)

    photo_id = None
    try:
        photos = await bot.get_user_profile_photos(event.from_user.id, limit=1)
        if photos.total_count:
            photo_id = photos.photos[0][-1].file_id
    except Exception:
        pass  # private profile photo, or a transient API hiccup — text-only is a fine fallback

    # Same single tracked message as every other screen: render_screen turns
    # it into a photo in place when it's already a photo, or does a clean
    # send-new-then-delete-old swap when switching type (text<->photo) --
    # never a second, orphaned message left behind after Back.
    if photo_id:
        await edit_or_send_photo(event, photo_id, text, markup)
    else:
        await edit_or_send(event, text, markup)


@router.callback_query(F.data == "profile")
async def cb_profile(cq: CallbackQuery) -> None:
    user = await guard(cq)
    if not user:
        return
    await asyncio.gather(_render_profile(cq, user), _ack(cq))


@router.message(Command("profile"))
async def msg_profile(msg: Message, state: FSMContext) -> None:
    user = await guard(msg)
    if not user:
        return
    await state.clear()
    await _render_profile(msg, user)


@router.callback_query(F.data == "apilink")
async def cb_apilink(cq: CallbackQuery) -> None:
    """Reached only when api_link_url isn't a valid https link — main_menu_kb
    renders a real url button instead of this callback once one is set."""
    user = await guard(cq)
    if not user:
        return
    text = (
        "🔗 <b>API Link</b>\n\n"
        "No API link has been configured yet. "
        "Set one from the admin panel under UI Editor → API link URL."
    )
    await asyncio.gather(edit_or_send(cq, text, back_kb(user["language"])), _ack(cq))


@router.callback_query(F.data == "noop")
async def cb_noop(cq: CallbackQuery) -> None:
    await _ack(cq)


# =============================================================================
#  ADMIN PANEL  —  in-Telegram administration
# =============================================================================
#  A complete admin application inside Telegram. No website required. Access is
#  gated to DB `admins` (matched by telegram_id) plus the ADMIN_TELEGRAM_IDS
#  env allowlist. Every destructive action passes through a confirm step.
#
#  Design notes (aiogram best practices):
#    - Callback payloads use a CallbackData factory (AdminCB) so there is no
#      stringly-typed parsing and no risk of colliding prefixes.
#    - A single AdminStates group drives every text/media capture flow; the
#      pending action is stored in FSM data, so one handler serves many edits.
#    - All money movement reuses the existing transaction-safe helpers
#      (wallet_apply, approve_payment, finalise_order, ...). The admin layer
#      never touches balances directly.
# =============================================================================

from aiogram.filters.callback_data import CallbackData


class AdminCB(CallbackData, prefix="adm"):
    # section: which panel screen (dash, products, cats, pay, users, ...)
    # action: verb within the screen (open, edit, del, add, toggle, save, ...)
    # arg: primary id / key (stringly typed, parsed per-handler)
    # page: pagination cursor
    section: str
    action: str = "open"
    arg: str = ""
    page: int = 0


class AdminStates(StatesGroup):
    # Generic capture: FSM data carries {"flow": "...", ...ctx} and the handler
    # dispatches on flow. Keeps us from declaring dozens of near-identical states.
    capturing = State()


# ---- access control ----------------------------------------------------------

async def is_admin(telegram_id: int) -> asyncpg.Record | dict | None:
    """Return an admin-like record if this Telegram user may use /admin.
    Env allowlist wins first (always owner), then the DB admins table matched
    by username == telegram_id (the bot stores TG id there for TG-native admins)."""
    if telegram_id in CFG.admin_ids:
        role = "owner" if (not CFG.admin_ids or telegram_id == CFG.admin_ids[0]) else "manager"
        return {"id": -telegram_id, "username": str(telegram_id), "role": role,
                "telegram": True}
    row = await db.fetchrow(
        "SELECT * FROM admins WHERE username=$1 AND active", str(telegram_id))
    if row:
        return row
    return None


def admin_rank(admin) -> int:
    return ROLE_RANK.get(admin["role"] if isinstance(admin, dict) else admin["role"], 0)


async def admin_guard(event: Message | CallbackQuery):
    """Gate + rate-limit for admin surfaces. Returns the admin record or None."""
    tid = event.from_user.id
    admin = await is_admin(tid)
    if not admin:
        if isinstance(event, CallbackQuery):
            await event.answer("Not authorised.", show_alert=True)
        # For messages we stay silent: never reveal the admin surface exists.
        return None
    if not rate_ok(tid, 60):
        if isinstance(event, CallbackQuery):
            await event.answer("Slow down a moment.")
        return None
    return admin


async def admin_audit(admin, action: str, entity: str = "",
                      entity_id: int | None = None, detail: dict | None = None) -> None:
    """Audit log entry from a Telegram admin action."""
    aid = _admin_db_id(admin)
    await db.execute(
        """INSERT INTO audit_logs(admin_id, admin_name, action, entity, entity_id, detail, ip)
           VALUES($1,$2,$3,$4,$5,$6,'telegram')""",
        aid, str(admin["username"]), action, entity or None, entity_id, detail or {},
    )


# ---- shared rendering helpers ------------------------------------------------

def kb_rows(*rows: list[InlineKeyboardButton]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[r for r in rows if r])


def abtn(text: str, section: str, action: str = "open", arg: str = "", page: int = 0):
    return InlineKeyboardButton(
        text=text,
        callback_data=AdminCB(section=section, action=action, arg=str(arg), page=page).pack(),
    )


def admin_back_row(section: str = "home", action: str = "open", arg: str = ""):
    return [abtn("🔙 Back", section, action, arg)]


async def admin_render(event: Message | CallbackQuery, text: str,
                       markup: InlineKeyboardMarkup | None = None) -> None:
    """Edit in place for callbacks, send fresh for messages/commands.

    Mirrors render_screen's resilience: a screen that Telegram refuses to
    parse as HTML (e.g. admin-authored text with a stray '<') falls back to
    a plain-text render of the same content instead of raising -- an
    unhandled exception here used to permanently lock the admin out of
    whole sections (see open_ui_brand's 2026-07-24 incident: one bad value
    in a listing screen took down every future visit to that screen,
    including the "send fresh" fallback that was retrying with the exact
    same broken text)."""
    async def _do(t: str) -> None:
        if isinstance(event, CallbackQuery):
            msg = event.message
            if msg.photo or msg.document or msg.video:
                await msg.delete()
                await msg.answer(t, reply_markup=markup)
            else:
                await msg.edit_text(t, reply_markup=markup, disable_web_page_preview=True)
        else:
            await event.answer(t, reply_markup=markup, disable_web_page_preview=True)

    try:
        await _do(text)
        return
    except TelegramBadRequest as e:
        msg_l = str(e).lower()
        if "message is not modified" in msg_l:
            return
        if "can't parse entities" in msg_l:
            try:
                await _do(_plain_text_fallback(text))
                return
            except TelegramBadRequest:
                pass  # fall through to the "send fresh" attempt below

    target = event.message if isinstance(event, CallbackQuery) else event
    try:
        await target.answer(text, reply_markup=markup, disable_web_page_preview=True)
    except TelegramBadRequest:
        try:
            await target.answer(_plain_text_fallback(text), reply_markup=markup,
                               disable_web_page_preview=True)
        except TelegramBadRequest:
            # Still failing even as plain text almost always means the
            # underlying content is over Telegram's 4096-char cap --
            # _plain_text_fallback only strips tags, it doesn't shorten.
            # Truncate as a last resort so the screen (and whatever button
            # led here) shows *something* instead of going permanently
            # unresponsive -- see the 2026-07-25 "message is too long"
            # incident where this admin_render call gave up outright and
            # the section stayed dead on every subsequent tap.
            try:
                truncated = _plain_text_fallback(text)[:_TG_TEXT_LIMIT - 100] + "\n\n… (truncated)"
                await target.answer(truncated, reply_markup=markup,
                                   disable_web_page_preview=True)
            except TelegramBadRequest:
                log.exception("admin_render: giving up, screen would not render even as plain text")


# =============================================================================
#  ADMIN — ENTRY & HOME
# =============================================================================

@router.message(Command("admin"))
async def cmd_admin(msg: Message, state: FSMContext) -> None:
    admin = await admin_guard(msg)
    if not admin:
        # Silent for non-admins: behave like an unknown command.
        return
    await state.clear()
    await admin_home(msg, admin)


async def admin_home(event, admin) -> None:
    store = await settings.get("store_name", "Digital Hub")
    role = admin["role"] if isinstance(admin, dict) else admin["role"]

    # Live counters for the header so the home screen is useful at a glance.
    pending_pay = int(await db.fetchval(
        "SELECT COUNT(*) FROM payments WHERE status='pending'") or 0)
    pending_ord = int(await db.fetchval(
        "SELECT COUNT(*) FROM orders WHERE status IN ('paid','processing')") or 0)
    pending_manual = int(await db.fetchval(
        """SELECT COUNT(*) FROM orders o JOIN products p ON p.id=o.product_id
            WHERE o.status='processing' AND p.delivery_type='manual'""") or 0)
    open_tickets_count = int(await db.fetchval(
        "SELECT COUNT(*) FROM tickets WHERE status='open'") or 0)

    alerts = []
    if pending_pay:
        alerts.append(f"💳 <b>{pending_pay}</b> payment(s) awaiting review")
    if pending_ord:
        alerts.append(f"📦 <b>{pending_ord}</b> order(s) to deliver")
    if pending_manual:
        alerts.append(f"🛠 <b>{pending_manual}</b> manual delivery(ies) to activate")
    if open_tickets_count:
        alerts.append(f"🎫 <b>{open_tickets_count}</b> open ticket(s)")
    alert_text = ("\n" + "\n".join(alerts) + "\n") if alerts else ""

    text = (
        f"⚙️ <b>{store} — Admin</b>\n"
        f"Signed in as <code>{admin['username']}</code> · {role}\n"
        f"{alert_text}\n"
        f"Choose a section:"
    )

    # Compact 2-3 per row, matching the requested layout.
    pay_label = f"💳 Payments{f' ({pending_pay})' if pending_pay else ''}"
    mdeliv_label = f"🛠 Manual Deliveries{f' ({pending_manual})' if pending_manual else ''}"
    tickets_label = f"🎫 Tickets{f' ({open_tickets_count})' if open_tickets_count else ''}"
    markup = kb_rows(
        [abtn("📊 Dashboard", "dash"), abtn("🛍 Products", "products")],
        [abtn("📂 Categories", "cats"), abtn("📦 Stock / Keys", "stock")],
        [abtn(pay_label, "payments"), abtn("💰 Prices", "prices")],
        [abtn(mdeliv_label, "mdeliv"), abtn("🎁 Deals", "deals")],
        [abtn("👥 Users", "users"), abtn("💳 Payment Methods", "paymethods")],
        [abtn("🎟 Coupons", "coupons"), abtn("📢 Broadcast", "broadcast")],
        [abtn(tickets_label, "tickets"), abtn("📌 Pin Notice", "pinned")],
        [abtn("🌍 Languages", "langs"), abtn("💱 Currency", "currency")],
        [abtn("⭐ Premium Emojis", "emojis"), abtn("🎨 UI Editor", "ui")],
        [abtn("⚙️ Bot Settings", "settings")],
        [abtn("🔄 Refresh", "home")],
    )
    await admin_render(event, text, markup)


@router.callback_query(AdminCB.filter(F.section == "home"))
async def cb_admin_home(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    await state.clear()
    await admin_home(cq, admin)
    await _ack(cq)


# ---- generic cancel of any in-progress capture -------------------------------

@router.callback_query(AdminCB.filter((F.section == "cancel")))
async def cb_admin_cancel(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    await state.clear()
    cb = AdminCB.unpack(cq.data)
    # arg carries where to return to, defaulting home.
    dest = cb.arg or "home"
    await cq.answer("Cancelled")
    if dest == "home":
        await admin_home(cq, admin)
    else:
        # Re-dispatch to the section's open handler via a synthetic route.
        await _admin_dispatch_open(cq, admin, dest)


async def _admin_dispatch_open(cq, admin, section: str) -> None:
    """Best-effort re-open of a section after cancel. Sections register their
    own open handlers; this falls back home if unknown."""
    handler = _ADMIN_OPENERS.get(section)
    if handler:
        await handler(cq, admin)
    else:
        await admin_home(cq, admin)


# Registry of section openers, populated by later layers. Lets cancel/back
# return to a section without duplicating routing logic.
_ADMIN_OPENERS: dict[str, Callable] = {}

# =============================================================================
#  ADMIN — SHARED CAPTURE FLOW
# =============================================================================
#  Many edits are "prompt the admin, capture their next message, apply it".
#  Rather than a state per field, we store a `flow` descriptor in FSM data and
#  a single message handler dispatches on it. Each flow entry names an applier
#  registered in _CAPTURE_APPLIERS.

_CAPTURE_APPLIERS: dict[str, Callable] = {}


def capture_applier(name: str):
    def deco(fn: Callable):
        _CAPTURE_APPLIERS[name] = fn
        return fn
    return deco


async def begin_capture(cq: CallbackQuery | Message, state: FSMContext, flow: str,
                        prompt: str, *, back: str = "home", back_arg: str = "",
                        accept: str = "text", **ctx) -> None:
    """Start a capture flow. `accept` ∈ text|photo|any. ctx is carried through.
    `cq` may also be a plain Message, so an applier can chain a second
    capture step (e.g. emoji unicode -> then its premium ID)."""
    await state.set_state(AdminStates.capturing)
    await state.update_data(flow=flow, accept=accept, back=back, back_arg=back_arg, **ctx)
    cancel = kb_rows([abtn("✖️ Cancel", "cancel", "open", back)])
    await admin_render(cq, prompt, cancel)
    if isinstance(cq, CallbackQuery):
        await _ack(cq)


@router.message(AdminStates.capturing)
async def admin_capture(msg: Message, state: FSMContext) -> None:
    admin = await admin_guard(msg)
    if not admin:
        await state.clear()
        return
    data = await state.get_data()
    flow = data.get("flow")
    applier = _CAPTURE_APPLIERS.get(flow)
    if not applier:
        await state.clear()
        await msg.answer("That action expired. Send /admin to start again.")
        return

    accept = data.get("accept", "text")
    if accept == "text" and not (msg.text and msg.text.strip()):
        await msg.answer("Please send text, or tap Cancel.")
        return
    if accept == "photo" and not msg.photo:
        await msg.answer("Please send a photo, or tap Cancel.")
        return
    if accept == "media" and not (msg.photo or msg.video):
        await msg.answer("Please send a photo or a video, or tap Cancel.")
        return

    try:
        await applier(msg, state, admin, data)
    except (FulfilError, InsufficientFunds, QuoteError, ValueError) as e:
        await msg.answer(f"⚠️ {e}")
        return
    except Exception:
        log.exception("admin capture flow %s failed", flow)
        await msg.answer("⚠️ Something went wrong. Nothing was changed.")
        return


# =============================================================================
#  ADMIN — DASHBOARD  (§9 analytics)
# =============================================================================

async def open_dashboard(event, admin) -> None:
    row = await db.fetchrow(
        """SELECT
             (SELECT COUNT(*) FROM store_users) AS users,
             (SELECT COUNT(*) FROM store_users WHERE created_at > NOW() - INTERVAL '24 hours') AS new_users,
             (SELECT COUNT(*) FROM orders WHERE created_at > NOW() - INTERVAL '24 hours') AS orders_today,
             (SELECT COALESCE(SUM(total),0) FROM orders
               WHERE status IN ('delivered','processing')) AS revenue,
             (SELECT COALESCE(SUM(total),0) FROM orders
               WHERE status IN ('delivered','processing')
                 AND created_at > NOW() - INTERVAL '24 hours') AS revenue_today,
             (SELECT COUNT(*) FROM payments WHERE status='pending') AS pending_pay,
             (SELECT COUNT(*) FROM payments WHERE status='approved') AS ok_pay,
             (SELECT COUNT(*) FROM product_keys WHERE NOT is_used) AS stock,
             (SELECT COALESCE(SUM(wallet_balance),0) FROM store_users) AS liability"""
    )
    top = await db.fetch(
        """SELECT name, sold_count FROM products
            WHERE sold_count > 0 ORDER BY sold_count DESC LIMIT 5""")

    top_lines = "\n".join(
        f"   {i+1}. {_html_preview(p['name'], 28)} — {p['sold_count']} sold"
        for i, p in enumerate(top)) or "   (no sales yet)"

    text = (
        f"📊 <b>Dashboard</b>\n\n"
        f"👥 Users: <b>{row['users']}</b>  (+{row['new_users']} today)\n"
        f"📦 Orders today: <b>{row['orders_today']}</b>\n"
        f"💰 Revenue: <b>{fmt_money(money(row['revenue']), 'INR')}</b>\n"
        f"   └ today: {fmt_money(money(row['revenue_today']), 'INR')}\n\n"
        f"💳 Payments: <b>{row['pending_pay']}</b> pending · {row['ok_pay']} approved\n"
        f"🔑 Keys in stock: <b>{row['stock']}</b>\n"
        f"🏦 Wallet liability: {fmt_money(money(row['liability']), 'INR')}\n\n"
        f"🔥 <b>Top sellers</b>\n{top_lines}"
    )
    markup = kb_rows(
        [abtn("💳 Review payments", "payments"), abtn("👥 Users", "users")],
        [abtn("🔄 Refresh", "dash")],
        admin_back_row("home"),
    )
    await admin_render(event, text, markup)


@router.callback_query(AdminCB.filter(F.section == "dash"))
async def cb_dashboard(cq: CallbackQuery) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    await open_dashboard(cq, admin)
    await _ack(cq)


_ADMIN_OPENERS["dash"] = lambda e, a, *_: open_dashboard(e, a)


# =============================================================================
#  ADMIN — CATEGORIES  (§5)
# =============================================================================

CATS_PER_PAGE = 8


async def open_categories(event, admin, page: int = 0) -> None:
    total = int(await db.fetchval("SELECT COUNT(*) FROM categories") or 0)
    cats = await db.fetch(
        """SELECT c.*, (SELECT COUNT(*) FROM products p WHERE p.category_id=c.id) AS n
             FROM categories c ORDER BY c.sort_order, c.id
            LIMIT $1 OFFSET $2""",
        CATS_PER_PAGE, page * CATS_PER_PAGE)

    rows = []
    for c in cats:
        vis = "" if c["active"] else "🚫 "
        rows.append([abtn(f"{vis}{c['emoji']} {c['name']} ({c['n']})",
                          "cats", "view", str(c["id"]))])

    nav = []
    if page > 0:
        nav.append(abtn("◀️", "cats", "open", "", page - 1))
    if (page + 1) * CATS_PER_PAGE < total:
        nav.append(abtn("▶️", "cats", "open", "", page + 1))

    markup = kb_rows(
        *rows,
        nav or None,
        [abtn("➕ Add category", "cats", "add")],
        admin_back_row("home"),
    )
    await admin_render(event, f"📂 <b>Categories</b> ({total})\n\nTap one to manage it.", markup)


async def view_category(event, admin, cid: int) -> None:
    c = await db.fetchrow("SELECT * FROM categories WHERE id=$1", cid)
    if not c:
        await admin_render(event, "Category not found.", kb_rows(admin_back_row("cats")))
        return
    n = int(await db.fetchval("SELECT COUNT(*) FROM products WHERE category_id=$1", cid) or 0)
    text = (
        f"📂 <b>{c['emoji']} {c['name']}</b>\n\n"
        f"Hindi name: {c['name_hi'] or '—'}\n"
        f"Products: {n}\n"
        f"Sort order: {c['sort_order']}\n"
        f"Status: {'✅ visible' if c['active'] else '🚫 hidden'}"
    )
    markup = kb_rows(
        [abtn("✏️ Name", "cats", "ename", str(cid)),
         abtn("😀 Emoji", "cats", "eemoji", str(cid))],
        [abtn("🇮🇳 Hindi name", "cats", "enamehi", str(cid)),
         abtn("↕️ Sort order", "cats", "esort", str(cid))],
        [abtn("🚫 Hide" if c["active"] else "✅ Show", "cats", "toggle", str(cid))],
        [abtn("🗑 Delete", "cats", "del", str(cid))],
        admin_back_row("cats"),
    )
    await admin_render(event, text, markup)


@router.callback_query(AdminCB.filter(F.section == "cats"))
async def cb_categories(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    act, cid = cb.action, cb.arg

    if act == "open":
        await open_categories(cq, admin, cb.page)
    elif act == "view":
        await view_category(cq, admin, int(cid))
    elif act == "add":
        await begin_capture(cq, state, "cat_add",
                            "➕ <b>New category</b>\n\nSend the category name.",
                            back="cats")
        return
    elif act == "ename":
        await begin_capture(cq, state, "cat_name",
                            "Send the new category name.", back="cats", cid=cid)
        return
    elif act == "enamehi":
        await begin_capture(cq, state, "cat_namehi",
                            "Send the Hindi name (or a dash to clear).", back="cats", cid=cid)
        return
    elif act == "eemoji":
        await begin_capture(cq, state, "cat_emoji",
                            "Send one emoji for this category.", back="cats", cid=cid)
        return
    elif act == "esort":
        await begin_capture(cq, state, "cat_sort",
                            "Send a sort number (lower shows first).", back="cats", cid=cid)
        return
    elif act == "toggle":
        await db.execute("UPDATE categories SET active = NOT active WHERE id=$1", int(cid))
        await settings.load(force=True)
        await admin_audit(admin, "cat_toggle", "category", int(cid))
        await view_category(cq, admin, int(cid))
    elif act == "del":
        n = int(await db.fetchval(
            "SELECT COUNT(*) FROM products WHERE category_id=$1", int(cid)) or 0)
        if n:
            await cq.answer(f"Has {n} product(s) — move or delete them first.", show_alert=True)
            return
        await admin_render(cq,
            "🗑 Delete this category? This cannot be undone.",
            kb_rows([abtn("✅ Yes, delete", "cats", "delok", cid),
                     abtn("✖️ No", "cats", "view", cid)]))
    elif act == "delok":
        await db.execute("DELETE FROM categories WHERE id=$1", int(cid))
        await admin_audit(admin, "cat_delete", "category", int(cid))
        await cq.answer("Deleted")
        await open_categories(cq, admin)
    await _ack(cq)


@capture_applier("cat_add")
async def _cat_add(msg, state, admin, data):
    name = msg.text.strip()[:120]
    row = await db.fetchrow(
        "INSERT INTO categories(name, emoji) VALUES($1,'📦') RETURNING id", name)
    await settings.load(force=True)
    await admin_audit(admin, "cat_create", "category", row["id"], {"name": name})
    await state.clear()
    await msg.answer(f"✅ Category “{name}” created.")
    await view_category(msg, admin, row["id"])


@capture_applier("cat_name")
async def _cat_name(msg, state, admin, data):
    await db.execute("UPDATE categories SET name=$1 WHERE id=$2",
                     msg.text.strip()[:120], int(data["cid"]))
    await settings.load(force=True)
    await state.clear()
    await msg.answer("✅ Name updated.")
    await view_category(msg, admin, int(data["cid"]))


@capture_applier("cat_namehi")
async def _cat_namehi(msg, state, admin, data):
    val = "" if msg.text.strip() == "-" else msg.text.strip()[:120]
    await db.execute("UPDATE categories SET name_hi=$1 WHERE id=$2", val, int(data["cid"]))
    await settings.load(force=True)
    await state.clear()
    await msg.answer("✅ Hindi name updated.")
    await view_category(msg, admin, int(data["cid"]))


@capture_applier("cat_emoji")
async def _cat_emoji(msg, state, admin, data):
    emoji = msg.text.strip()[:8]
    await db.execute("UPDATE categories SET emoji=$1 WHERE id=$2", emoji, int(data["cid"]))
    await settings.load(force=True)
    await state.clear()
    await msg.answer("✅ Emoji updated.")
    await view_category(msg, admin, int(data["cid"]))


@capture_applier("cat_sort")
async def _cat_sort(msg, state, admin, data):
    try:
        sort = int(msg.text.strip())
    except ValueError:
        await msg.answer("Send a whole number.")
        return
    await db.execute("UPDATE categories SET sort_order=$1 WHERE id=$2", sort, int(data["cid"]))
    await settings.load(force=True)
    await state.clear()
    await msg.answer("✅ Sort order updated.")
    await view_category(msg, admin, int(data["cid"]))


_ADMIN_OPENERS["cats"] = lambda e, a, *_: open_categories(e, a)


# =============================================================================
#  ADMIN — PRODUCTS  (§5)
# =============================================================================

PRODS_PER_PAGE = 6


async def open_products(event, admin, page: int = 0, category_id: int | None = None) -> None:
    offset = page * PRODS_PER_PAGE
    if category_id:
        total = int(await db.fetchval(
            "SELECT COUNT(*) FROM products WHERE category_id=$1", category_id) or 0)
        prods = await db.fetch(
            """SELECT p.*, c.name AS cat_name FROM products p
                  JOIN categories c ON c.id=p.category_id
                 WHERE p.category_id=$1
                 ORDER BY p.id DESC LIMIT $2 OFFSET $3""",
            category_id, PRODS_PER_PAGE, offset)
    else:
        total = int(await db.fetchval("SELECT COUNT(*) FROM products") or 0)
        prods = await db.fetch(
            """SELECT p.*, c.name AS cat_name FROM products p
                  JOIN categories c ON c.id=p.category_id
                 ORDER BY p.id DESC LIMIT $1 OFFSET $2""",
            PRODS_PER_PAGE, offset)

    stocks_by_id = await stock_of_many(db, prods)
    stocks = [stocks_by_id[p["id"]] for p in prods]

    rows = []
    for p, stock in zip(prods, stocks):
        flag = "" if p["active"] else "🚫 "
        deal = "🔥" if p["is_deal"] else ""
        rows.append([abtn(
            f"{flag}{deal}{p['name'][:24]} · {fmt_money(money(p['price']),'INR')} · {stock}📦",
            "products", "view", str(p["id"]))])

    nav = []
    if page > 0:
        nav.append(abtn("◀️", "products", "open", "", page - 1))
    if (page + 1) * PRODS_PER_PAGE < total:
        nav.append(abtn("▶️", "products", "open", "", page + 1))

    markup = kb_rows(
        *rows,
        nav or None,
        [abtn("➕ Add product", "products", "add")],
        [abtn("🔥 Bulk discount", "bulkdisc", "open")],
        admin_back_row("home"),
    )
    await admin_render(event,
        f"🛍 <b>Products</b> ({total})\n\nname · price · stock", markup)


async def view_product(event, admin, pid: int) -> None:
    p = await db.fetchrow(
        """SELECT p.*, c.name AS cat_name FROM products p
             JOIN categories c ON c.id=p.category_id WHERE p.id=$1""", pid)
    if not p:
        await admin_render(event, "Product not found.", kb_rows(admin_back_row("products")))
        return
    stock = await stock_of(db, p)
    orig = f"\nWas: {fmt_money(money(p['original_price']),'INR')}" if p["original_price"] else ""
    qty_tiers = await db.fetch(
        "SELECT min_qty, discount_type, value FROM product_qty_discounts "
        "WHERE product_id=$1 ORDER BY min_qty", pid)
    qty_line = ""
    if qty_tiers:
        parts = [f"{qt['min_qty']}+: {qt['value']}{'%' if qt['discount_type']=='percent' else '₹'}"
                for qt in qty_tiers]
        qty_line = f"Qty discounts: {', '.join(parts)}\n"
    is_manual = p["delivery_type"] == "manual"
    manual_line = ""
    if is_manual:
        prompt_shown = _html_preview(p["manual_prompt"], 60) if p["manual_prompt"] else "(default prompt)"
        manual_line = f"Buyer prompt: <code>{prompt_shown}</code>\n"
    text = (
        f"🛍 <b>{p['name']}</b>\n\n"
        f"Category: {p['cat_name']}\n"
        f"Price: <b>{fmt_money(money(p['price']),'INR')}</b>{orig}\n"
        f"Delivery: {p['delivery_type']}\n"
        f"Stock: <b>{stock}</b>{' (manual)' if is_manual else ' keys'}\n"
        f"{manual_line}"
        f"Sold: {p['sold_count']}\n"
        f"Deal: {'🔥 yes' if p['is_deal'] else 'no'} · "
        f"File delivery: {'yes' if p['deliver_as_file'] else 'no'}\n"
        f"Status: {'✅ visible' if p['active'] else '🚫 hidden'}\n"
        f"{qty_line}\n"
        f"{_html_preview(p['description'], 200)}"
    )
    markup = kb_rows(
        [abtn("✏️ Name", "products", "ename", str(pid)),
         abtn("💵 Price", "products", "eprice", str(pid))],
        [abtn("📝 Description", "products", "edesc", str(pid)),
         abtn("📂 Category", "products", "ecat", str(pid))],
        [abtn("🔑 Manage keys", "stock", "prod", str(pid)),
         abtn("🖼 Image/Video", "products", "eimg", str(pid))],
        [abtn("🔥 Toggle deal", "products", "deal", str(pid)),
         abtn("📄 Toggle file", "products", "file", str(pid))],
        [abtn("🔀 Delivery: Manual" if not is_manual else "🔀 Delivery: Auto",
              "products", "toggledeliv", str(pid))],
        [abtn("✉️ Buyer info prompt", "products", "einfo", str(pid))] if is_manual else None,
        [abtn("🔢 Qty discounts", "qtydisc", "open", str(pid))],
        [abtn("🚫 Hide" if p["active"] else "✅ Show", "products", "toggle", str(pid))],
        [abtn("🗑 Delete", "products", "del", str(pid))],
        admin_back_row("products"),
    )
    await admin_render(event, text, markup)


@router.callback_query(AdminCB.filter(F.section == "products"))
async def cb_products(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    act, pid = cb.action, cb.arg

    if act == "open":
        await open_products(cq, admin, cb.page)
    elif act == "view":
        await view_product(cq, admin, int(pid))
    elif act == "add":
        cats = await db.fetch("SELECT id, emoji, name FROM categories ORDER BY sort_order, id")
        if not cats:
            await cq.answer("Create a category first.", show_alert=True)
            return
        rows = [[abtn(f"{c['emoji']} {c['name']}", "products", "addto", str(c["id"]))]
                for c in cats]
        await admin_render(cq, "➕ <b>New product</b>\n\nPick a category:",
                           kb_rows(*rows, admin_back_row("products")))
    elif act == "addto":
        await begin_capture(cq, state, "prod_add",
                            "Send the product name.", back="products", cid=pid)
        return
    elif act == "ename":
        await begin_capture(cq, state, "prod_name",
                            "Send the new product name.", back="products", pid=pid)
        return
    elif act == "eprice":
        await begin_capture(cq, state, "prod_price",
                            "Send the new price in INR (e.g. 299 or 299.50).",
                            back="products", pid=pid)
        return
    elif act == "edesc":
        await begin_capture(cq, state, "prod_desc",
                            "Send the product description.", back="products", pid=pid)
        return
    elif act == "eimg":
        await begin_capture(cq, state, "prod_img",
                            "Send a photo or a video to use as the product media.\n\n"
                            "A video replaces the photo entirely (and vice versa) -- "
                            "a product shows one or the other, never both.",
                            back="products", pid=pid, accept="media")
        return
    elif act == "ecat":
        cats = await db.fetch("SELECT id, emoji, name FROM categories ORDER BY sort_order, id")
        rows = [[abtn(f"{c['emoji']} {c['name']}", "products", "setcat", f"{pid}:{c['id']}")]
                for c in cats]
        await admin_render(cq, "Move to which category?",
                           kb_rows(*rows, admin_back_row("products", "view", pid)))
    elif act == "setcat":
        p_id, c_id = pid.split(":")
        await db.execute("UPDATE products SET category_id=$1 WHERE id=$2", int(c_id), int(p_id))
        await admin_audit(admin, "prod_move", "product", int(p_id))
        await view_product(cq, admin, int(p_id))
    elif act == "toggle":
        await db.execute("UPDATE products SET active = NOT active WHERE id=$1", int(pid))
        await admin_audit(admin, "prod_toggle", "product", int(pid))
        await view_product(cq, admin, int(pid))
    elif act == "deal":
        await db.execute("UPDATE products SET is_deal = NOT is_deal WHERE id=$1", int(pid))
        await view_product(cq, admin, int(pid))
    elif act == "file":
        await db.execute("UPDATE products SET deliver_as_file = NOT deliver_as_file WHERE id=$1", int(pid))
        await view_product(cq, admin, int(pid))
    elif act == "toggledeliv":
        await db.execute(
            """UPDATE products
                  SET delivery_type = CASE WHEN delivery_type='manual' THEN 'auto' ELSE 'manual' END
                WHERE id=$1""", int(pid))
        await admin_audit(admin, "prod_delivery_type", "product", int(pid))
        await view_product(cq, admin, int(pid))
    elif act == "einfo":
        await begin_capture(cq, state, "prod_info",
            "✉️ Send the message the buyer sees after paying, asking for what "
            "this product needs (e.g. “Send the Gmail address to add to the "
            "Family Plan”). Max 300 characters. Send a dash (-) to reset to "
            "the default prompt.",
            back="products", pid=pid)
        return
    elif act == "del":
        await admin_render(cq,
            "🗑 Delete this product? All its keys are deleted too. This cannot be undone.",
            kb_rows([abtn("✅ Yes, delete", "products", "delok", pid),
                     abtn("✖️ No", "products", "view", pid)]))
    elif act == "delok":
        await db.execute("DELETE FROM products WHERE id=$1", int(pid))
        await admin_audit(admin, "prod_delete", "product", int(pid))
        await cq.answer("Deleted")
        await open_products(cq, admin)
    await _ack(cq)


@capture_applier("prod_add")
async def _prod_add(msg, state, admin, data):
    name = msg.text.strip()[:200]
    row = await db.fetchrow(
        """INSERT INTO products(category_id, name, price, delivery_type)
           VALUES($1,$2,0,'auto') RETURNING id""", int(data["cid"]), name)
    await admin_audit(admin, "prod_create", "product", row["id"], {"name": name})
    await state.clear()
    await msg.answer(f"✅ Product “{name}” created. Set its price and add keys next.")
    await view_product(msg, admin, row["id"])


@capture_applier("prod_name")
async def _prod_name(msg, state, admin, data):
    await db.execute("UPDATE products SET name=$1 WHERE id=$2",
                     msg.text.strip()[:200], int(data["pid"]))
    await state.clear()
    await msg.answer("✅ Name updated.")
    await view_product(msg, admin, int(data["pid"]))


@capture_applier("prod_price")
async def _prod_price(msg, state, admin, data):
    try:
        price = money(Decimal(msg.text.strip().replace("₹", "").replace(",", "")), Q_CRYPTO)
        if price < 0:
            raise ValueError
    except (InvalidOperation, ValueError):
        await msg.answer("Send a valid price, e.g. 299 or 299.50.")
        return
    await db.execute("UPDATE products SET price=$1 WHERE id=$2", price, int(data["pid"]))
    await admin_audit(admin, "prod_price", "product", int(data["pid"]), {"price": str(price)})
    await state.clear()
    await msg.answer(f"✅ Price set to {fmt_money(price,'INR')}.")
    await view_product(msg, admin, int(data["pid"]))


@capture_applier("prod_desc")
async def _prod_desc(msg, state, admin, data):
    # html_text carries over bold/italic/underline/spoiler/code/blockquote as
    # real HTML tags (and safely escapes stray <, >, & along the way) instead
    # of the plain msg.text, which silently drops all of that formatting.
    desc = msg.html_text.strip()[:2000]
    err = validate_telegram_html(desc)
    if err:
        await msg.answer(f"⚠️ {err}\n\nSend the description again.")
        return
    await db.execute("UPDATE products SET description=$1 WHERE id=$2",
                     desc, int(data["pid"]))
    await state.clear()
    await msg.answer("✅ Description updated.")
    await view_product(msg, admin, int(data["pid"]))


@capture_applier("prod_info")
async def _prod_info(msg, state, admin, data):
    raw = msg.text.strip()
    prompt = "" if raw == "-" else raw[:300]
    await db.execute("UPDATE products SET manual_prompt=$1 WHERE id=$2",
                     prompt, int(data["pid"]))
    await admin_audit(admin, "prod_manual_prompt", "product", int(data["pid"]))
    await state.clear()
    await msg.answer("✅ Reset to the default prompt." if not prompt else "✅ Buyer prompt updated.")
    await view_product(msg, admin, int(data["pid"]))


@capture_applier("prod_img")
async def _prod_img(msg, state, admin, data):
    if msg.video:
        # image_file_id keeps working as the bot chat's own product screen
        # already expects it (edit_or_send_photo) -- Telegram auto-generates
        # a thumbnail for every uploaded video, so reusing it there means the
        # chat view needs zero changes to keep showing *something* sensible.
        # video_file_id is the new bit the Mini App's product page plays in
        # full, which the chat view doesn't touch at all.
        thumb = msg.video.thumbnail.file_id if msg.video.thumbnail else None
        await db.execute(
            "UPDATE products SET video_file_id=$1, image_file_id=$2 WHERE id=$3",
            msg.video.file_id, thumb, int(data["pid"]))
        answer = "✅ Video updated."
    else:
        file_id = msg.photo[-1].file_id
        await db.execute(
            "UPDATE products SET image_file_id=$1, video_file_id=NULL WHERE id=$2",
            file_id, int(data["pid"]))
        answer = "✅ Image updated."
    await state.clear()
    await msg.answer(answer)
    await view_product(msg, admin, int(data["pid"]))


_ADMIN_OPENERS["products"] = lambda e, a, *_: open_products(e, a)


# =============================================================================
#  ADMIN — BULK DISCOUNT  (from the Products list, not the broadcast flow)
# =============================================================================
#  Same apply_bulk_discount_tx/_discounted_price engine "Discount Sale
#  broadcast" (dsale, further down) uses -- this is the plain version for
#  when an admin just wants to reprice a category or a specific set of
#  products without composing and sending an announcement. Also the only
#  in-bot path to *remove* a bulk discount (restore original_price) --
#  dsale has no such button since a sale broadcast is one-directional by
#  design.

async def open_bulkdisc(event, admin) -> None:
    await admin_render(event,
        "🔥 <b>Bulk discount</b>\n\n"
        "Reprice a whole category or a specific list of products, right "
        "now, with no announcement sent. (Want to announce it too? Use "
        "\"Discount Sale broadcast\" instead.)",
        kb_rows(
            [abtn("📁 Apply — a whole category", "bulkdisc", "applycat")],
            [abtn("🔢 Apply — product ID list", "bulkdisc", "applyids")],
            [abtn("♻️ Remove — a whole category", "bulkdisc", "removecat")],
            [abtn("♻️ Remove — product ID list", "bulkdisc", "removeids")],
            admin_back_row("products")))


@router.callback_query(AdminCB.filter(F.section == "bulkdisc"))
async def cb_bulkdisc(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    act, arg = cb.action, cb.arg

    if act == "open":
        await open_bulkdisc(cq, admin)
    elif act in ("applycat", "removecat"):
        cats = await db.fetch("SELECT id, emoji, name FROM categories ORDER BY sort_order, id")
        if not cats:
            await cq.answer("No categories yet.", show_alert=True)
            return
        next_action = "setcatA" if act == "applycat" else "setcatR"
        rows = [[abtn(f"{c['emoji']} {c['name']}", "bulkdisc", next_action, str(c["id"]))]
                for c in cats]
        await admin_render(cq, "📁 Pick a category:", kb_rows(*rows, admin_back_row("bulkdisc")))
    elif act == "applyids":
        await begin_capture(cq, state, "bulkdisc_ids",
            "🔢 Send product IDs separated by commas.\n"
            "Example: <code>12,15,19</code>",
            back="bulkdisc", mode="apply")
        return
    elif act == "removeids":
        await begin_capture(cq, state, "bulkdisc_ids",
            "🔢 Send product IDs separated by commas.\n"
            "Example: <code>12,15,19</code>",
            back="bulkdisc", mode="remove")
        return
    elif act == "setcatA":
        cat = await db.fetchrow("SELECT name FROM categories WHERE id=$1", int(arg))
        if not cat:
            await cq.answer("Unavailable.", show_alert=True)
            return
        await begin_capture(cq, state, "bulkdisc_value",
            f"📁 <b>{cat['name']}</b>\n\n"
            "Send the discount: a percent like <code>20%</code>, or a flat "
            "INR amount like <code>100</code>.",
            back="bulkdisc", scope="category", target_ids=[int(arg)])
        return
    elif act == "setcatR":
        cat = await db.fetchrow("SELECT name FROM categories WHERE id=$1", int(arg))
        if not cat:
            await cq.answer("Unavailable.", show_alert=True)
            return
        await state.update_data(scope="category", target_ids=[int(arg)], mode="remove")
        await _bulkdisc_show_confirm(cq, state)
        return
    elif act == "confirm":
        await _bulkdisc_apply(cq, state, admin)
        return
    await _ack(cq)


@capture_applier("bulkdisc_ids")
async def _bulkdisc_ids(msg, state, admin, data):
    ids = [int(x) for x in re.findall(r"\d+", msg.text or "")]
    if not ids:
        await msg.answer("No valid numeric IDs found. Try again.")
        return
    if data["mode"] == "remove":
        await state.update_data(scope="products", target_ids=ids, mode="remove")
        await _bulkdisc_show_confirm(msg, state)
        return
    await begin_capture(msg, state, "bulkdisc_value",
        f"🔢 <b>{len(ids)} product ID(s)</b>\n\n"
        "Send the discount: a percent like <code>20%</code>, or a flat "
        "INR amount like <code>100</code>.",
        back="bulkdisc", scope="products", target_ids=ids)


@capture_applier("bulkdisc_value")
async def _bulkdisc_value(msg, state, admin, data):
    raw = (msg.text or "").strip()
    is_percent = raw.endswith("%")
    num = raw[:-1].strip() if is_percent else raw
    try:
        value = money(Decimal(num.replace(",", "")), Q_CRYPTO)
        if value <= 0:
            raise ValueError
        if is_percent and value >= 100:
            raise ValueError
    except (InvalidOperation, ValueError):
        await msg.answer(
            "Send a positive percent like <code>20%</code> (under 100), "
            "or a flat INR amount like <code>100</code>.")
        return

    await state.update_data(
        mode="apply", discount_type="percent" if is_percent else "fixed", value=str(value))
    await _bulkdisc_show_confirm(msg, state)


async def _bulkdisc_show_confirm(event, state: FSMContext) -> None:
    data = await state.get_data()
    scope, target_ids, mode = data["scope"], data["target_ids"], data["mode"]

    if mode == "apply":
        preview = await _dsale_preview(scope, target_ids, data["discount_type"], Decimal(data["value"]))
    else:
        # Remove has no discount math to preview -- just show which
        # products currently have a bulk price active (original_price set)
        # within scope, so the admin knows what's actually about to change.
        if scope == "category":
            rows = await db.fetch(
                "SELECT id, name, price, original_price FROM products "
                "WHERE category_id = ANY($1::int[]) AND original_price IS NOT NULL",
                target_ids)
        else:
            rows = await db.fetch(
                "SELECT id, name, price, original_price FROM products "
                "WHERE id = ANY($1::int[]) AND original_price IS NOT NULL",
                target_ids)
        preview = [{"id": r["id"], "name": r["name"],
                   "old_price": money(r["price"], Q_CRYPTO),
                   "new_price": money(r["original_price"], Q_CRYPTO)} for r in rows]

    if not preview:
        await state.clear()
        text = "No matching products found. Nothing to change."
        kb = kb_rows(admin_back_row("bulkdisc"))
        if isinstance(event, CallbackQuery):
            await admin_render(event, text, kb)
            await event.answer()
        else:
            await event.answer(text, reply_markup=kb)
        return

    lines = [
        f"{p['name'][:22]}: {fmt_money(p['old_price'],'INR')} → "
        f"<b>{fmt_money(p['new_price'],'INR')}</b>"
        for p in preview[:15]
    ]
    if len(preview) > 15:
        lines.append(f"…and {len(preview) - 15} more")

    verb = "discounted" if mode == "apply" else "restored"
    header = "🔥 Apply discount" if mode == "apply" else "♻️ Remove discount"
    text = (
        f"{header}\n\n<b>{len(preview)} product(s) will be {verb}.</b>\n\n"
        + "\n".join(lines) +
        "\n\nThis applies immediately and affects live prices."
    )
    kb = kb_rows(
        [abtn("✅ Confirm", "bulkdisc", "confirm")],
        [abtn("✖️ Cancel", "cancel", "open", "products")])
    if isinstance(event, CallbackQuery):
        await admin_render(event, text, kb)
        await event.answer()
    else:
        await event.answer(text, reply_markup=kb)


async def _bulkdisc_apply(cq: CallbackQuery, state: FSMContext, admin) -> None:
    data = await state.get_data()
    scope, target_ids, mode = data["scope"], data["target_ids"], data["mode"]

    async with db.tx() as conn:
        if mode == "apply":
            updated = await apply_bulk_discount_tx(
                conn, scope, target_ids, data["discount_type"], Decimal(data["value"]), "apply")
        else:
            updated = await apply_bulk_discount_tx(conn, scope, target_ids, "", ZERO, "remove")

    await state.clear()
    if not updated:
        await admin_render(cq, "No matching products found — nothing was changed.",
                           kb_rows(admin_back_row("products")))
        await _ack(cq)
        return

    await admin_audit(admin, f"bulk_discount_{mode}", "product", None,
                      {"scope": scope, "targetIds": target_ids, "updated": len(updated)})
    verb = "discounted" if mode == "apply" else "restored to original price"
    await admin_render(cq,
        f"✅ <b>{len(updated)} product(s) {verb}.</b>",
        kb_rows(admin_back_row("products")))
    await _ack(cq)


_ADMIN_OPENERS["bulkdisc"] = lambda e, a, *_: open_bulkdisc(e, a)


# =============================================================================
#  ADMIN — QUANTITY DISCOUNTS  (per product, from the product's own screen)
# =============================================================================
#  "Buy N of *this* product, get X% (or flat Rs Y) off *this order*" -- built
#  on build_quote's qty_discount, separate from both the Bulk Discount tool
#  (reprices the product itself) and Discount Sale broadcast (announces a
#  sale). Scoped to one product at a time, so its screens carry the product
#  id in every callback_data rather than relying on the generic
#  cancel/_ADMIN_OPENERS reopen path (which only ever calls a section's
#  opener with no arguments) -- see the cancel buttons below.

async def open_qtydisc(event, admin, pid: int) -> None:
    p = await db.fetchrow("SELECT id, name FROM products WHERE id=$1", pid)
    if not p:
        await admin_render(event, "Product not found.", kb_rows(admin_back_row("products")))
        return
    tiers = await db.fetch(
        "SELECT id, min_qty, discount_type, value FROM product_qty_discounts "
        "WHERE product_id=$1 ORDER BY min_qty", pid)

    lines = [f"🔢 <b>Qty discounts — {p['name']}</b>\n",
             "Buy this many or more of this product, get this much off the order."]
    rows = []
    if tiers:
        lines.append("")
        for tr in tiers:
            disc = f"{tr['value']}%" if tr["discount_type"] == "percent" else f"₹{tr['value']}"
            lines.append(f"• {tr['min_qty']}+ → {disc} off")
            rows.append([abtn(f"🗑 Remove {tr['min_qty']}+", "qtydisc", "del", str(tr["id"]))])
    else:
        lines.append("\nNo tiers yet.")

    rows.append([abtn("➕ Add tier", "qtydisc", "add", str(pid))])
    rows.append([abtn("⬅️ Back to product", "products", "view", str(pid))])
    await admin_render(event, "\n".join(lines), kb_rows(*rows))


@router.callback_query(AdminCB.filter(F.section == "qtydisc"))
async def cb_qtydisc(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    act, arg = cb.action, cb.arg

    if act == "open":
        await open_qtydisc(cq, admin, int(arg))
    elif act == "add":
        pid = int(arg)
        await state.set_state(AdminStates.capturing)
        await state.update_data(flow="qtydisc_minqty", accept="text", pid=pid)
        await admin_render(cq,
            "🔢 Send the minimum quantity for this tier (e.g. <code>5</code>). "
            "Must be 2 or more.",
            kb_rows([abtn("✖️ Cancel", "qtydisc", "open", str(pid))]))
        await _ack(cq)
        return
    elif act == "del":
        row = await db.fetchrow(
            "DELETE FROM product_qty_discounts WHERE id=$1 RETURNING product_id", int(arg))
        if row:
            await admin_audit(admin, "qty_discount_remove", "product", row["product_id"], {})
            await open_qtydisc(cq, admin, row["product_id"])
        else:
            await cq.answer("Already removed.", show_alert=True)
            return
    await _ack(cq)


@capture_applier("qtydisc_minqty")
async def _qtydisc_minqty(msg, state, admin, data):
    pid = data["pid"]
    raw = (msg.text or "").strip()
    if not raw.isdigit() or int(raw) < 2:
        await msg.answer("Send a whole number, 2 or more.")
        return
    min_qty = int(raw)
    await state.set_state(AdminStates.capturing)
    await state.update_data(flow="qtydisc_value", accept="text", pid=pid, min_qty=min_qty)
    await msg.answer(
        f"🔢 <b>{min_qty}+ units</b>\n\n"
        "Send the discount: a percent like <code>10%</code>, or a flat "
        "INR amount off the order like <code>100</code>.",
        reply_markup=kb_rows([abtn("✖️ Cancel", "qtydisc", "open", str(pid))]))


@capture_applier("qtydisc_value")
async def _qtydisc_value(msg, state, admin, data):
    pid, min_qty = data["pid"], data["min_qty"]
    raw = (msg.text or "").strip()
    is_percent = raw.endswith("%")
    num = raw[:-1].strip() if is_percent else raw
    try:
        value = money(Decimal(num.replace(",", "")), Q_CRYPTO)
        if value <= 0:
            raise ValueError
        if is_percent and value >= 100:
            raise ValueError
    except (InvalidOperation, ValueError):
        await msg.answer(
            "Send a positive percent like <code>10%</code> (under 100), "
            "or a flat INR amount like <code>100</code>.")
        return

    await db.execute(
        """INSERT INTO product_qty_discounts(product_id, min_qty, discount_type, value)
           VALUES($1,$2,$3,$4)
           ON CONFLICT (product_id, min_qty)
           DO UPDATE SET discount_type=EXCLUDED.discount_type, value=EXCLUDED.value""",
        pid, min_qty, "percent" if is_percent else "fixed", value,
    )
    await admin_audit(admin, "qty_discount_set", "product", pid,
                      {"minQty": min_qty, "discountType": "percent" if is_percent else "fixed",
                       "value": str(value)})
    await state.clear()
    await msg.answer(f"✅ {min_qty}+ tier saved.")
    await open_qtydisc(msg, admin, pid)


_ADMIN_OPENERS["qtydisc"] = lambda e, a, *_: open_products(e, a)


# =============================================================================
#  ADMIN — STOCK / KEYS  (§5)
# =============================================================================

async def open_stock(event, admin, page: int = 0) -> None:
    """Overview of all auto products and their key counts, low stock first."""
    prods = await db.fetch(
        """SELECT p.id, p.name, p.delivery_type, p.manual_stock,
                  (SELECT COUNT(*) FROM product_keys k
                    WHERE k.product_id=p.id AND NOT k.is_used) AS unused,
                  (SELECT COUNT(*) FROM product_keys k
                    WHERE k.product_id=p.id AND k.is_used) AS used
             FROM products p WHERE p.active
            ORDER BY unused ASC, p.id DESC LIMIT 20""")
    if not prods:
        await admin_render(event, "No active products yet.", kb_rows(admin_back_row("home")))
        return

    lines = []
    rows = []
    for p in prods:
        if p["delivery_type"] == "manual":
            s = f"{p['manual_stock'] if p['manual_stock'] is not None else '∞'} (manual)"
        else:
            s = f"{p['unused']} keys"
        warn = "⚠️ " if (p["delivery_type"] == "auto" and p["unused"] < 5) else ""
        lines.append(f"{warn}{p['name'][:26]} — {s}")
        rows.append([abtn(f"🔑 {p['name'][:24]}", "stock", "prod", str(p["id"]))])

    text = "📦 <b>Stock overview</b>\n\n" + "\n".join(lines)
    await admin_render(event, text, kb_rows(*rows, admin_back_row("home")))


async def view_stock(event, admin, pid: int) -> None:
    p = await db.fetchrow("SELECT * FROM products WHERE id=$1", pid)
    if not p:
        await admin_render(event, "Product not found.", kb_rows(admin_back_row("stock")))
        return
    unused = int(await db.fetchval(
        "SELECT COUNT(*) FROM product_keys WHERE product_id=$1 AND NOT is_used", pid) or 0)
    used = int(await db.fetchval(
        "SELECT COUNT(*) FROM product_keys WHERE product_id=$1 AND is_used", pid) or 0)

    if p["delivery_type"] == "manual":
        text = (
            f"📦 <b>{p['name']}</b> — manual delivery\n\n"
            f"Manual stock: <b>{p['manual_stock'] if p['manual_stock'] is not None else '∞'}</b>\n\n"
            f"Manual products don't use keys. Set a stock count or leave unlimited."
        )
        markup = kb_rows(
            [abtn("✏️ Set stock count", "stock", "setmanual", str(pid))],
            [abtn("♾ Unlimited", "stock", "unlimited", str(pid))],
            admin_back_row("stock"),
        )
    else:
        text = (
            f"🔑 <b>{p['name']}</b> — key delivery\n\n"
            f"Available keys: <b>{unused}</b>\n"
            f"Used keys: {used}\n\n"
            f"Add or remove keys by pasting them (one per line) or uploading "
            f"a .txt file — either works. Duplicates are skipped on add. If "
            f"stock was 0, everyone waiting on a restock alert is notified "
            f"automatically."
        )
        markup = kb_rows(
            [abtn("➕ Add keys", "stock", "addkeys", str(pid))],
            [abtn("➖ Remove keys", "stock", "remkeys", str(pid))] if unused else None,
            [abtn("👁 View unused", "stock", "list", str(pid))] if unused else None,
            admin_back_row("stock"),
        )
    await admin_render(event, text, markup)


@router.callback_query(AdminCB.filter(F.section == "stock"))
async def cb_stock(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    act, pid = cb.action, cb.arg

    if act == "open":
        await open_stock(cq, admin, cb.page)
    elif act == "prod":
        await view_stock(cq, admin, int(pid))
    elif act == "addkeys":
        await begin_capture(cq, state, "keys_add",
            "🔑 Send keys, one per line — or upload a .txt file with one "
            "key per line (up to a few thousand either way).",
            back="stock", accept="any", pid=pid)
        return
    elif act == "remkeys":
        await begin_capture(cq, state, "keys_remove",
            "🗑 Send the keys to remove, one per line — or upload a .txt "
            "file. Only keys still unused are removed; anything already "
            "delivered is left alone.",
            back="stock", accept="any", pid=pid)
        return
    elif act == "setmanual":
        await begin_capture(cq, state, "stock_manual",
            "Send the stock count as a whole number.", back="stock", pid=pid)
        return
    elif act == "unlimited":
        await db.execute("UPDATE products SET manual_stock=NULL WHERE id=$1", int(pid))
        await cq.answer("Set to unlimited")
        await view_stock(cq, admin, int(pid))
    elif act == "list":
        keys = await db.fetch(
            """SELECT key_value FROM product_keys
                WHERE product_id=$1 AND NOT is_used ORDER BY id LIMIT 30""", int(pid))
        body = "\n".join(f"<code>{k['key_value']}</code>" for k in keys) or "(none)"
        more = "\n\n<i>Showing first 30.</i>" if len(keys) == 30 else ""
        await admin_render(cq, f"🔑 <b>Unused keys</b>\n\n{body}{more}",
                           kb_rows(admin_back_row("stock", "prod", pid)))
    await _ack(cq)


_MAX_KEY_FILE_BYTES = 3_000_000  # ~3MB -- generous for a keys-per-line .txt


async def _extract_keys_from_message(msg: Message) -> list[str] | None:
    """One-key-per-line content from either a pasted text message or an
    uploaded .txt file, whichever the admin sent -- so the same Add/Remove
    keys flows work with either. Returns None (having already told the
    admin why) if neither is usable. File download mirrors the existing
    file proxy's get_file + httpx pattern."""
    if msg.document:
        if msg.document.file_size and msg.document.file_size > _MAX_KEY_FILE_BYTES:
            await msg.answer(
                f"That file is too large ({msg.document.file_size // 1000} KB, "
                f"max {_MAX_KEY_FILE_BYTES // 1_000_000}MB). Split it up and send again.")
            return None
        try:
            f = await bot.get_file(msg.document.file_id)
            url = f"https://api.telegram.org/file/bot{CFG.bot_token}/{f.file_path}"
            async with httpx.AsyncClient(timeout=30) as c:
                r = await c.get(url)
                r.raise_for_status()
            text = r.content.decode("utf-8", errors="ignore")
        except Exception:
            log.exception("failed to download key file")
            await msg.answer("Couldn't download that file. Try again, or paste the keys as text.")
            return None
    elif msg.text:
        text = msg.text
    else:
        await msg.answer("Send a .txt file, or paste the keys as text (one per line).")
        return None

    lines = [k.strip() for k in text.splitlines() if k.strip()]
    if not lines:
        await msg.answer("No keys found in that file/message.")
        return None
    return lines


@capture_applier("keys_add")
async def _keys_add(msg, state, admin, data):
    pid = int(data["pid"])
    raw = await _extract_keys_from_message(msg)
    if raw is None:
        return
    before = int(await db.fetchval(
        "SELECT COUNT(*) FROM product_keys WHERE product_id=$1 AND NOT is_used", pid) or 0)
    async with db.tx() as conn:
        rows = await conn.fetch(
            """INSERT INTO product_keys(product_id, key_value)
                 SELECT $1, k FROM UNNEST($2::text[]) AS k
               ON CONFLICT DO NOTHING
               RETURNING key_value""",
            pid, raw[:5000])
    inserted = len(rows)
    await admin_audit(admin, "keys_import", "product", pid,
                      {"submitted": len(raw), "inserted": inserted})
    await state.clear()
    dupes = len(raw) - inserted
    capped = " (only the first 5000 were processed)" if len(raw) > 5000 else ""
    await msg.answer(
        f"✅ Detected <b>{len(raw)}</b> key(s){capped}. "
        f"Added <b>{inserted}</b>."
        + (f" {dupes} duplicate(s) skipped." if dupes else ""))
    # Wake restock watchers if this product just went from 0 to stocked.
    if before == 0 and inserted > 0:
        n = await notify_restock(bot, pid)
        if n:
            await msg.answer(f"🔔 Notified {n} user(s) waiting for restock.")
    await view_stock(msg, admin, pid)


@capture_applier("keys_remove")
async def _keys_remove(msg, state, admin, data):
    pid = int(data["pid"])
    raw = await _extract_keys_from_message(msg)
    if raw is None:
        return
    unique = list(dict.fromkeys(raw[:5000]))  # de-dupe, keep order for the report

    # Only ever removes UNUSED keys -- a key already delivered to a paying
    # customer is never deleted, matching the existing single-key DELETE
    # endpoint's safety invariant (it 409s on a used key rather than
    # silently deleting delivery evidence).
    removed_rows = await db.fetch(
        """DELETE FROM product_keys
            WHERE product_id=$1 AND key_value = ANY($2::text[]) AND NOT is_used
        RETURNING key_value""",
        pid, unique)
    removed = len(removed_rows)
    skipped = len(unique) - removed

    await admin_audit(admin, "keys_remove", "product", pid,
                      {"submitted": len(unique), "removed": removed})
    await state.clear()
    await msg.answer(
        f"🗑 Detected <b>{len(unique)}</b> key(s). Removed <b>{removed}</b>."
        + (f" {skipped} skipped (already delivered or not found)." if skipped else ""))
    await view_stock(msg, admin, pid)


@capture_applier("stock_manual")
async def _stock_manual(msg, state, admin, data):
    try:
        n = int(msg.text.strip())
        if n < 0:
            raise ValueError
    except ValueError:
        await msg.answer("Send a whole number (0 or more).")
        return
    pid = int(data["pid"])
    before = int(await db.fetchval("SELECT manual_stock FROM products WHERE id=$1", pid) or 0)
    await db.execute("UPDATE products SET manual_stock=$1 WHERE id=$2", n, pid)
    await state.clear()
    await msg.answer(f"✅ Manual stock set to {n}.")
    if before == 0 and n > 0:
        await notify_restock(bot, pid)
    await view_stock(msg, admin, pid)


_ADMIN_OPENERS["stock"] = lambda e, a, *_: open_stock(e, a)

# =============================================================================
#  ADMIN — PAYMENTS REVIEW  (§6 + manual verification)
# =============================================================================
#  Approve / reject the same way the web panel and auto-verifier do: through
#  approve_payment / reject_payment, which handle delivery, wallet credit,
#  referral bonus and user notification in one transaction. The admin layer
#  only decides; it never moves money itself.

PAY_PER_PAGE = 6


def _admin_db_id(admin) -> int | None:
    """Resolve an admin to its admins.id, or None for env/telegram admins
    that have no row of their own -- the id shape approve_payment/
    reject_payment/broadcasts.created_by/audit_logs.admin_id all want."""
    return None if (isinstance(admin, dict) and admin.get("telegram")) else admin["id"]


async def open_payments(event, admin, page: int = 0, status: str = "pending") -> None:
    total = int(await db.fetchval(
        "SELECT COUNT(*) FROM payments WHERE status=$1", status) or 0)
    pays = await db.fetch(
        """SELECT p.*, u.first_name, u.username, u.telegram_id
             FROM payments p JOIN store_users u ON u.id=p.user_id
            WHERE p.status=$1 ORDER BY p.created_at ASC
            LIMIT $2 OFFSET $3""",
        status, PAY_PER_PAGE, page * PAY_PER_PAGE)

    rows = []
    for p in pays:
        who = p["first_name"] or p["username"] or p["telegram_id"]
        tag = "₮" if p["method"].startswith("usdt") else "🇮🇳" if p["method"] == "upi" else "💰"
        purpose = "⬆️" if p["purpose"] == "topup" else "🛒"
        rows.append([abtn(
            f"{tag}{purpose} #{p['id']} · {fmt_money(money(p['amount']),'INR')} · {who[:14]}",
            "payments", "view", str(p["id"]))])

    # status filter chips
    chips = [abtn(
        ("• " if status == s else "") + s.title(),
        "payments", "filter", s)
        for s in ("pending", "approved", "rejected", "expired")]

    nav = []
    if page > 0:
        nav.append(abtn("◀️", "payments", "filter", status, page - 1))
    if (page + 1) * PAY_PER_PAGE < total:
        nav.append(abtn("▶️", "payments", "filter", status, page + 1))

    header = (
        f"💳 <b>Payments — {status}</b> ({total})\n\n"
        + ("Tap a payment to review it." if pays else "Nothing here.")
    )
    await admin_render(event, header, kb_rows(
        *rows, nav or None, chips[:2], chips[2:], admin_back_row("home")))


async def view_payment(event, admin, pid: int) -> None:
    p = await db.fetchrow(
        """SELECT p.*, u.first_name, u.username, u.telegram_id, u.id AS uid
             FROM payments p JOIN store_users u ON u.id=p.user_id
            WHERE p.id=$1""", pid)
    if not p:
        await admin_render(event, "Payment not found.", kb_rows(admin_back_row("payments")))
        return

    who = p["first_name"] or p["username"] or p["telegram_id"]
    lines = [
        f"💳 <b>Payment #{p['id']}</b>  ·  {p['status']}",
        "",
        f"User: {who} (<code>{p['telegram_id']}</code>)",
        f"Purpose: {'wallet top-up' if p['purpose']=='topup' else 'order'}"
        + (f" #{p['order_id']}" if p["order_id"] else ""),
        f"Method: {p['method']}",
        f"Amount: <b>{fmt_money(money(p['amount']),'INR')}</b>",
    ]
    if p["expected_amount"]:
        lines.append(f"Expected: {crypto(p['expected_amount']).normalize()} USDT"
                     + (f" on {p['chain']}" if p["chain"] else ""))
    if p["expected_address"]:
        lines.append(f"To: <code>{p['expected_address']}</code>")
    if p["tx_id"]:
        lines.append(f"Tx: <code>{p['tx_id']}</code>")
    if p["verify_note"]:
        conf = f" ({p['confirmations']} confs)" if p["confirmations"] is not None else ""
        lines.append(f"Note: {p['verify_note']}{conf}")
    lines.append(f"Submitted: {p['created_at'].strftime('%d %b %H:%M')}")

    # A 'pending' payment with a tx_id can never have verify_note=='verified'
    # -- a successful check calls approve_payment in the same breath, which
    # moves it out of 'pending'. So still being here means either nobody has
    # checked yet, or the last check failed. Either way, this specific tx_id
    # has NOT been confirmed on-chain -- exactly the gap that let fabricated
    # tx hashes get rubber-stamped by a direct tap of "Approve" without ever
    # hitting "Re-check on-chain" first.
    needs_verification = p["status"] == "pending" and p["tx_id"] and p["verify_note"] != "verified"
    if needs_verification:
        lines.insert(2, "⚠️ <b>Not yet confirmed on-chain.</b> Re-check before approving.")

    rows = []
    if p["status"] == "pending":
        if needs_verification:
            rows.append([abtn("🔗 Re-check on-chain", "payments", "recheck", str(pid))])
        if p["screenshot_file_id"]:
            rows.append([abtn("📷 View screenshot", "payments", "shot", str(pid))])
        if needs_verification:
            # No direct "Approve" here -- re-check must be attempted first;
            # a failed re-check still offers "Approve anyway" (with its own
            # explicit warning) as the deliberate override path.
            rows.append([abtn("❌ Reject", "payments", "reject", str(pid))])
        else:
            rows.append([abtn("✅ Approve", "payments", "approve", str(pid)),
                         abtn("❌ Reject", "payments", "reject", str(pid))])
    rows.append(admin_back_row("payments"))
    await admin_render(event, "\n".join(lines), kb_rows(*rows))


@router.callback_query(AdminCB.filter(F.section == "payments"))
async def cb_payments(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    act, arg = cb.action, cb.arg

    if act == "open":
        await open_payments(cq, admin, cb.page)
    elif act == "filter":
        await open_payments(cq, admin, cb.page, arg or "pending")
    elif act == "view":
        await view_payment(cq, admin, int(arg))
    elif act == "shot":
        p = await db.fetchrow("SELECT screenshot_file_id, user_id FROM payments WHERE id=$1", int(arg))
        if p and p["screenshot_file_id"]:
            try:
                await cq.message.answer_photo(
                    p["screenshot_file_id"],
                    caption=f"Screenshot for payment #{arg}\n\n"
                            "⚠️ Screenshots are easy to fake — prefer on-chain re-check.")
            except Exception:
                await cq.answer("Couldn't load the image.", show_alert=True)
        await _ack(cq)
        return
    elif act == "recheck":
        await cq.answer("Checking the chain…")
        p = await db.fetchrow("SELECT * FROM payments WHERE id=$1", int(arg))
        if not p or p["status"] != "pending" or not p["tx_id"]:
            await cq.answer("Nothing to re-check.", show_alert=True)
            return
        chain = p["chain"] or "TRC20"
        result = await verifier.verify(
            chain, p["tx_id"], p["expected_address"] or await chain_address(chain),
            crypto(p["expected_amount"] or p["amount"]), p["created_at"])
        if result.ok:
            await approve_payment(bot, int(arg), _admin_db_id(admin),
                                  "auto-verified via admin re-check")
            await admin_audit(admin, "payment_recheck_ok", "payment", int(arg))
            await admin_render(cq,
                f"✅ Verified on {chain}: {result.amount} USDT, "
                f"{result.confirmations} confirmations. Delivered.",
                kb_rows(admin_back_row("payments")))
        else:
            await db.execute("UPDATE payments SET verify_note=$2, confirmations=$3 WHERE id=$1",
                             int(arg), result.reason[:500], result.confirmations)
            await admin_render(cq,
                f"⚠️ Not verified: {result.reason}\n"
                f"Confirmations: {result.confirmations}",
                kb_rows([abtn("🔗 Try again", "payments", "recheck", arg)],
                        [abtn("✅ Approve anyway", "payments", "approve", arg)],
                        admin_back_row("payments")))
        return
    elif act == "approve":
        await admin_render(cq,
            f"✅ Approve payment #{arg}?\n\n"
            "This delivers the order (or credits the wallet) and notifies the user.",
            kb_rows([abtn("✅ Confirm approve", "payments", "approveok", arg),
                     abtn("✖️ Cancel", "payments", "view", arg)]))
    elif act == "approveok":
        try:
            r = await approve_payment(bot, int(arg), _admin_db_id(admin), "approved from Telegram")
            await admin_audit(admin, "payment_approve", "payment", int(arg))
            keys = r.get("keys", [])
            await admin_render(cq,
                f"✅ Payment #{arg} approved."
                + (f" {len(keys)} key(s) delivered." if keys else " User notified."),
                kb_rows(admin_back_row("payments")))
        except FulfilError as e:
            await admin_render(cq, f"⚠️ {e}", kb_rows(admin_back_row("payments")))
    elif act == "reject":
        await begin_capture(cq, state, "pay_reject",
            f"❌ Rejecting payment #{arg}.\n\nSend a short reason (the user sees it), "
            "or send a dash for no reason.", back="payments", pid=arg)
        return
    await _ack(cq)


@capture_applier("pay_reject")
async def _pay_reject(msg, state, admin, data):
    reason = "" if msg.text.strip() == "-" else msg.text.strip()[:300]
    pid = int(data["pid"])
    await state.clear()
    try:
        await reject_payment(bot, pid, _admin_db_id(admin), reason)
        await admin_audit(admin, "payment_reject", "payment", pid, {"reason": reason})
        await msg.answer(f"❌ Payment #{pid} rejected. User notified.")
    except FulfilError as e:
        await msg.answer(f"⚠️ {e}")
    await open_payments(msg, admin)


_ADMIN_OPENERS["payments"] = lambda e, a, *_: open_payments(e, a)


# =============================================================================
#  ADMIN — MANUAL DELIVERIES
# =============================================================================
#  Orders for delivery_type='manual' products sit in status='processing'
#  until an admin activates the account by hand (outside Telegram) and
#  confirms here. Mirrors the Payments review screen one level down: list ->
#  view -> one-tap action. See the MANUAL DELIVERY — CREDENTIAL COLLECTION
#  section (buyer side) for where manual_info gets filled in.

async def open_manual_deliveries(event, admin, page: int = 0) -> None:
    PAGE = 8
    offset = page * PAGE
    total = int(await db.fetchval(
        """SELECT COUNT(*) FROM orders o JOIN products p ON p.id=o.product_id
            WHERE o.status='processing' AND p.delivery_type='manual'""") or 0)
    rows_data = await db.fetch(
        """SELECT o.id, o.manual_info, o.manual_info_photo, o.created_at, p.name AS product_name,
                  u.first_name, u.username, u.telegram_id
             FROM orders o
             JOIN products p ON p.id=o.product_id
             JOIN store_users u ON u.id=o.user_id
            WHERE o.status='processing' AND p.delivery_type='manual'
            ORDER BY (o.manual_info IS NOT NULL OR o.manual_info_photo IS NOT NULL) DESC,
                     o.created_at ASC
            LIMIT $1 OFFSET $2""", PAGE, offset)

    if not rows_data:
        await admin_render(event, "🛠 <b>Manual Deliveries</b>\n\nNothing waiting — all caught up.",
                           kb_rows(admin_back_row("home")))
        return

    rows = []
    for o in rows_data:
        who = o["first_name"] or o["username"] or o["telegram_id"]
        flag = "📨" if (o["manual_info"] or o["manual_info_photo"]) else "⏳"
        rows.append([abtn(f"{flag} #{o['id']} {o['product_name'][:20]} — {who}",
                          "mdeliv", "view", str(o["id"]))])

    nav = []
    if page > 0:
        nav.append(abtn("◀️", "mdeliv", "open", "", page - 1))
    if (page + 1) * PAGE < total:
        nav.append(abtn("▶️", "mdeliv", "open", "", page + 1))

    text = (f"🛠 <b>Manual Deliveries</b> ({total})\n\n"
            f"📨 = info submitted, ready to activate · ⏳ = waiting on the buyer")
    await admin_render(event, text, kb_rows(*rows, nav or None, admin_back_row("home")))


async def view_manual_delivery(event, admin, oid: int) -> None:
    o = await db.fetchrow(
        """SELECT o.*, p.name AS product_name, u.first_name, u.username, u.telegram_id
             FROM orders o
             JOIN products p ON p.id=o.product_id
             JOIN store_users u ON u.id=o.user_id
            WHERE o.id=$1""", oid)
    if not o:
        await admin_render(event, "Order not found.", kb_rows(admin_back_row("mdeliv")))
        return

    who = o["first_name"] or o["username"] or o["telegram_id"]
    lines = [
        f"🛠 <b>Order #{o['id']}</b> — {html.escape(o['product_name'])}",
        "",
        f"Buyer: {html.escape(str(who))} (<code>{o['telegram_id']}</code>)",
        f"Quantity: {o['quantity']} · Total: {fmt_money(money(o['total']),'INR')}",
        f"Placed: {o['created_at'].strftime('%d %b %H:%M')}",
        "",
    ]
    if o["manual_info"] or o["manual_info_photo"]:
        if o["manual_info"]:
            lines.append(f"📨 <b>Submitted:</b>\n<code>{html.escape(o['manual_info'])}</code>")
        if o["manual_info_photo"]:
            lines.append("📷 A photo was attached — see below.")
    else:
        lines.append("⏳ Buyer hasn't submitted details yet.")

    rows = []
    if o["manual_info_photo"]:
        rows.append([abtn("📷 View photo", "mdeliv", "photo", str(oid))])
    if o["status"] == "processing":
        rows.append([abtn("✅ Mark activated", "mdeliv", "actok", str(oid))])
    rows.append(admin_back_row("mdeliv"))
    await admin_render(event, "\n".join(lines), kb_rows(*rows))


@router.callback_query(AdminCB.filter(F.section == "mdeliv"))
async def cb_manual_deliveries(cq: CallbackQuery) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    act, arg = cb.action, cb.arg

    if act == "open":
        await open_manual_deliveries(cq, admin, cb.page)
    elif act == "view":
        await view_manual_delivery(cq, admin, int(arg))
    elif act == "photo":
        row = await db.fetchrow(
            "SELECT manual_info_photo FROM orders WHERE id=$1", int(arg))
        if not row or not row["manual_info_photo"]:
            await cq.answer("Photo not found.", show_alert=True)
            return
        try:
            await bot.send_photo(cq.message.chat.id, row["manual_info_photo"])
        except Exception:
            await cq.answer("Couldn't load the image.", show_alert=True)
            return
        await _ack(cq)
        return
    elif act == "actok":
        oid = int(arg)
        row = await activate_manual_delivery(oid)
        if not row:
            await cq.answer("Already handled.", show_alert=True)
            await view_manual_delivery(cq, admin, oid)
            return
        await admin_audit(admin, "manual_deliver_confirm", "order", oid)
        await cq.answer("Marked activated. Buyer notified.")
        await open_manual_deliveries(cq, admin)
        return
    await _ack(cq)


_ADMIN_OPENERS["mdeliv"] = lambda e, a, *_: open_manual_deliveries(e, a)


# =============================================================================
#  ADMIN — PAYMENT METHODS  (§6)
# =============================================================================
#  Editable wallet addresses per chain, UPI, Binance Pay, and the on-chain
#  tuning knobs (min confirmations, tolerance, timeout). Each is a settings key.

PAYMETHOD_FIELDS = [
    ("usdt_trc20_address", "USDT · TRC20 address", "wallet"),
    ("usdt_bep20_address", "USDT · BEP20 address", "wallet"),
    ("usdt_erc20_address", "USDT · ERC20 address", "wallet"),
    ("upi_id", "UPI ID", "text"),
    ("upi_qr_file_id", "UPI QR image", "photo"),
    ("binance_pay_id", "Binance Pay ID", "text"),
    ("min_confirmations", "Min confirmations (default, all chains)", "int"),
    ("min_confirmations_trc20", "Min confirmations — TRC20 override", "int"),
    ("min_confirmations_bep20", "Min confirmations — BEP20 override", "int"),
    ("min_confirmations_erc20", "Min confirmations — ERC20 override", "int"),
    ("payment_tolerance", "Amount tolerance (USDT)", "decimal"),
    ("payment_ttl_minutes", "Payment timeout (minutes)", "int"),
    ("min_topup_inr", "Min top-up (INR)", "decimal"),
    ("min_topup_usdt", "Min top-up (USDT)", "decimal"),
    ("crypto_safety_fee_percent", "Safety fee % (default, all chains)", "decimal"),
    ("crypto_safety_fee_trc20_percent", "Safety fee % — TRC20 override", "decimal"),
]


async def open_paymethods(event, admin) -> None:
    cur = await settings.load(force=True)
    lines = ["💳 <b>Payment Methods</b>\n"]
    rows = []
    for key, label, kind in PAYMETHOD_FIELDS:
        val = cur.get(key, "")
        if kind == "wallet" or key == "upi_id":
            shown = (val[:10] + "…" + val[-6:]) if len(val) > 20 else (val or "— not set")
        elif kind == "photo":
            shown = "✅ set" if val else "— not set"
        else:
            shown = val or "—"
        lines.append(f"• {label}: <code>{shown}</code>")
        rows.append([abtn(f"✏️ {label}", "paymethods", "edit", key)])

    await admin_render(event, "\n".join(lines), kb_rows(*rows, admin_back_row("home")))


@router.callback_query(AdminCB.filter(F.section == "paymethods"))
async def cb_paymethods(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    if cb.action == "open":
        await open_paymethods(cq, admin)
    elif cb.action == "edit":
        key = cb.arg
        meta = next((m for m in PAYMETHOD_FIELDS if m[0] == key), None)
        if not meta:
            await cq.answer("Unknown field.", show_alert=True)
            return
        _, label, kind = meta
        if kind == "photo":
            await begin_capture(cq, state, "pm_photo",
                f"Send a photo for “{label}”.", back="paymethods", key=key, accept="photo")
        else:
            hint = {
                "wallet": "Paste the wallet address. Double-check the network!",
                "int": "Send a whole number.",
                "decimal": "Send a number (e.g. 0.02 or 50).",
                "text": "Send the new value.",
            }.get(kind, "Send the new value.")
            await begin_capture(cq, state, "pm_text",
                f"✏️ <b>{label}</b>\n\n{hint}", back="paymethods", key=key, kind=kind)
        return
    await _ack(cq)


@capture_applier("pm_text")
async def _pm_text(msg, state, admin, data):
    key, kind = data["key"], data.get("kind", "text")
    raw = msg.text.strip()
    # Validate numeric kinds so a typo can't poison the verifier.
    if kind == "int":
        if not raw.lstrip("-").isdigit():
            await msg.answer("Send a whole number.")
            return
        raw = str(int(raw))
    elif kind == "decimal":
        try:
            raw = str(money(Decimal(raw.replace("₹", "").replace(",", "")), Q_CRYPTO).normalize())
        except (InvalidOperation, ValueError):
            await msg.answer("Send a valid number.")
            return
    await settings.set(key, raw)
    await admin_audit(admin, "paymethod_edit", "settings", None, {"key": key})
    await state.clear()
    await msg.answer("✅ Saved.")
    await open_paymethods(msg, admin)


@capture_applier("pm_photo")
async def _pm_photo(msg, state, admin, data):
    file_id = msg.photo[-1].file_id
    await settings.set(data["key"], file_id)
    await state.clear()
    await msg.answer("✅ Image saved.")
    await open_paymethods(msg, admin)


_ADMIN_OPENERS["paymethods"] = lambda e, a, *_: open_paymethods(e, a)


# =============================================================================
#  ADMIN — PRICES  (§6 exchange rate + quick price tools)
# =============================================================================

async def open_prices(event, admin) -> None:
    rate = await usdt_rate()
    ref_pct = await settings.get_decimal("referral_bonus_percent", "5")
    prod_count = int(await db.fetchval("SELECT COUNT(*) FROM products") or 0)
    text = (
        f"💰 <b>Prices & Rates</b>\n\n"
        f"USDT rate: <b>1 USDT = ₹{rate.normalize()}</b>\n"
        f"Referral bonus: <b>{ref_pct.normalize()}%</b>\n"
        f"Products: {prod_count}\n\n"
        f"Product prices are set individually under 🛍 Products. "
        f"The USDT rate converts every INR price to USDT at display and checkout."
    )
    markup = kb_rows(
        [abtn("💱 Set USDT rate", "prices", "rate")],
        [abtn("🎁 Set referral %", "prices", "refpct")],
        [abtn("🛍 Edit product prices", "products")],
        admin_back_row("home"),
    )
    await admin_render(event, text, markup)


@router.callback_query(AdminCB.filter(F.section == "prices"))
async def cb_prices(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    if cb.action == "open":
        await open_prices(cq, admin)
    elif cb.action == "rate":
        await begin_capture(cq, state, "price_rate",
            "💱 Send the new USDT→INR rate (e.g. 90 or 90.50).", back="prices")
    elif cb.action == "refpct":
        await begin_capture(cq, state, "price_refpct",
            "🎁 Send the referral bonus percent (e.g. 5).", back="prices")
    await _ack(cq)


@capture_applier("price_rate")
async def _price_rate(msg, state, admin, data):
    try:
        rate = money(Decimal(msg.text.strip().replace(",", "")), Q_CRYPTO)
        if rate <= 0:
            raise ValueError
    except (InvalidOperation, ValueError):
        await msg.answer("Send a positive number.")
        return
    await settings.set("usdt_rate", str(rate.normalize()))
    await admin_audit(admin, "rate_edit", "settings", None, {"usdt_rate": str(rate)})
    await state.clear()
    await msg.answer(f"✅ USDT rate set to ₹{rate.normalize()}.")
    await open_prices(msg, admin)


@capture_applier("price_refpct")
async def _price_refpct(msg, state, admin, data):
    try:
        pct = money(Decimal(msg.text.strip()), Q_CRYPTO)
        if pct < 0 or pct > 100:
            raise ValueError
    except (InvalidOperation, ValueError):
        await msg.answer("Send a percent between 0 and 100.")
        return
    await settings.set("referral_bonus_percent", str(pct.normalize()))
    await state.clear()
    await msg.answer(f"✅ Referral bonus set to {pct.normalize()}%.")
    await open_prices(msg, admin)


_ADMIN_OPENERS["prices"] = lambda e, a, *_: open_prices(e, a)

# =============================================================================
#  ADMIN — USERS  (§8)
# =============================================================================
#  Search, ban/unban, adjust balance, change rank, grant coupons and referral
#  bonuses, inspect and cancel orders, and promote a user to bot admin.
#  Every balance change goes through wallet_apply so the ledger stays truthful.

USERS_PER_PAGE = 6


async def open_users(event, admin, page: int = 0, term: str = "") -> None:
    if term:
        like = f"%{term}%"
        total = int(await db.fetchval(
            """SELECT COUNT(*) FROM store_users
                WHERE username ILIKE $1 OR first_name ILIKE $1 OR telegram_id ILIKE $1""",
            like) or 0)
        users = await db.fetch(
            """SELECT * FROM store_users
                WHERE username ILIKE $1 OR first_name ILIKE $1 OR telegram_id ILIKE $1
                ORDER BY created_at DESC LIMIT $2 OFFSET $3""",
            like, USERS_PER_PAGE, page * USERS_PER_PAGE)
    else:
        total = int(await db.fetchval("SELECT COUNT(*) FROM store_users") or 0)
        users = await db.fetch(
            "SELECT * FROM store_users ORDER BY created_at DESC LIMIT $1 OFFSET $2",
            USERS_PER_PAGE, page * USERS_PER_PAGE)

    rows = []
    for u in users:
        who = u["first_name"] or u["username"] or u["telegram_id"]
        ban = "🚫" if u["is_banned"] else ""
        tier = {"gold": "🥇", "silver": "🥈"}.get(u["tier"], "")
        rows.append([abtn(
            f"{ban}{tier}{who[:16]} · {fmt_money(money(u['wallet_balance']),'INR')}",
            "users", "view", str(u["id"]))])

    nav = []
    arg = f"q:{term}" if term else ""
    if page > 0:
        nav.append(abtn("◀️", "users", "page", arg, page - 1))
    if (page + 1) * USERS_PER_PAGE < total:
        nav.append(abtn("▶️", "users", "page", arg, page + 1))

    head = f"👥 <b>Users</b> ({total})"
    if term:
        head += f"\nSearch: <code>{term}</code>"
    markup = kb_rows(
        *rows, nav or None,
        [abtn("🔍 Search", "users", "search"),
         abtn("🧹 Clear" if term else "🔄 Refresh", "users", "open")],
        admin_back_row("home"),
    )
    await admin_render(event, head, markup)


async def view_user(event, admin, uid: int) -> None:
    u = await db.fetchrow("SELECT * FROM store_users WHERE id=$1", uid)
    if not u:
        await admin_render(event, "User not found.", kb_rows(admin_back_row("users")))
        return
    orders = int(await db.fetchval(
        "SELECT COUNT(*) FROM orders WHERE user_id=$1", uid) or 0)
    refs = int(await db.fetchval(
        "SELECT COUNT(*) FROM store_users WHERE referred_by_id=$1", uid) or 0)
    is_bot_admin = await db.fetchval(
        "SELECT 1 FROM admins WHERE username=$1 AND active", u["telegram_id"])

    who = u["first_name"] or "—"
    text = (
        f"👤 <b>{who}</b>"
        + (f" (@{u['username']})" if u["username"] else "") + "\n\n"
        f"ID: <code>{u['telegram_id']}</code>\n"
        f"Balance: <b>{fmt_money(money(u['wallet_balance']),'INR')}</b>\n"
        f"Lifetime spend: {fmt_money(money(u['total_spent']),'INR')}\n"
        f"Rank: <b>{(u['tier'] or 'bronze').title()}</b>\n"
        f"Orders: {orders} · Referrals: {refs}\n"
        f"Language: {u['language']} · Currency: {u['currency'] or 'INR'}\n"
        f"Referral code: <code>{u['referral_code']}</code>\n"
        f"Joined: {u['created_at'].strftime('%d %b %Y')}\n"
        f"Status: {'🚫 BANNED' if u['is_banned'] else '✅ active'}"
        + (f"\nReason: {u['ban_reason']}" if u["is_banned"] and u["ban_reason"] else "")
        + ("\n⚙️ <b>Bot admin</b>" if is_bot_admin else "")
    )
    markup = kb_rows(
        [abtn("💰 Adjust balance", "users", "bal", str(uid)),
         abtn("🏅 Change rank", "users", "rank", str(uid))],
        [abtn("🎟 Give coupon", "users", "coupon", str(uid)),
         abtn("🎁 Referral bonus", "users", "refbonus", str(uid))],
        [abtn("📦 View orders", "users", "orders", str(uid)),
         abtn("💬 Message", "users", "msg", str(uid))],
        [abtn("✅ Unban" if u["is_banned"] else "🚫 Ban", "users",
              "unban" if u["is_banned"] else "ban", str(uid))],
        [abtn("⬇️ Remove admin" if is_bot_admin else "⬆️ Make admin",
              "users", "demote" if is_bot_admin else "promote", str(uid))]
        if admin_rank(admin) >= ROLE_RANK["owner"] else None,
        admin_back_row("users"),
    )
    await admin_render(event, text, markup)


async def view_user_orders(event, admin, uid: int) -> None:
    orders = await db.fetch(
        """SELECT o.*, p.name FROM orders o JOIN products p ON p.id=o.product_id
            WHERE o.user_id=$1 ORDER BY o.created_at DESC LIMIT 10""", uid)
    if not orders:
        await admin_render(event, "No orders for this user.",
                           kb_rows(admin_back_row("users", "view", str(uid))))
        return
    lines = ["📦 <b>Recent orders</b>\n"]
    rows = []
    for o in orders:
        icon = ORDER_STATUS_ICONS.get(o["status"], "•")
        lines.append(
            f"{icon} #{o['id']} {o['name'][:20]} ×{o['quantity']} — "
            f"{fmt_money(money(o['total']),'INR')} · {o['created_at'].strftime('%d %b')}")
        if o["status"] in ("pending", "paid", "processing"):
            rows.append([abtn(f"❌ Cancel #{o['id']}", "users", "cancelord",
                              f"{uid}:{o['id']}")])
    rows.append(admin_back_row("users", "view", str(uid)))
    await admin_render(event, "\n".join(lines), kb_rows(*rows))


@router.callback_query(AdminCB.filter(F.section == "users"))
async def cb_users(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    act, arg = cb.action, cb.arg

    if act == "open":
        await open_users(cq, admin)
    elif act == "page":
        term = arg[2:] if arg.startswith("q:") else ""
        await open_users(cq, admin, cb.page, term)
    elif act == "search":
        await begin_capture(cq, state, "user_search",
            "🔍 Send a name, @username, or Telegram ID to search for.", back="users")
        return
    elif act == "view":
        await view_user(cq, admin, int(arg))
    elif act == "orders":
        await view_user_orders(cq, admin, int(arg))
    elif act == "cancelord":
        uid, oid = arg.split(":")
        await db.execute(
            "UPDATE orders SET status='cancelled' WHERE id=$1 AND status <> 'delivered'",
            int(oid))
        await admin_audit(admin, "order_cancel", "order", int(oid))
        await cq.answer("Order cancelled")
        await view_user_orders(cq, admin, int(uid))
    elif act == "bal":
        await begin_capture(cq, state, "user_bal",
            "💰 Send the amount to add, or a negative number to deduct.\n"
            "Example: <code>500</code> or <code>-250</code>", back="users", uid=arg)
        return
    elif act == "rank":
        rows = [[abtn(f"{ic} {name.title()}", "users", "setrank", f"{arg}:{name}")]
                for name, ic in (("bronze", "🥉"), ("silver", "🥈"), ("gold", "🥇"))]
        await admin_render(cq, "🏅 Choose a rank for this user:",
                           kb_rows(*rows, admin_back_row("users", "view", arg)))
    elif act == "setrank":
        uid, tier = arg.split(":")
        await db.execute("UPDATE store_users SET tier=$1 WHERE id=$2", tier, int(uid))
        await admin_audit(admin, "user_rank", "user", int(uid), {"tier": tier})
        await notify_user(bot, int(uid), f"🏅 Your rank is now <b>{tier.title()}</b>.")
        await view_user(cq, admin, int(uid))
    elif act == "coupon":
        coupons = await db.fetch(
            "SELECT id, code, discount_type, value FROM coupons WHERE active ORDER BY id DESC LIMIT 10")
        if not coupons:
            await cq.answer("No active coupons. Create one first.", show_alert=True)
            return
        rows = [[abtn(
            f"{c['code']} ({c['value']}{'%' if c['discount_type']=='percent' else '₹'})",
            "users", "givecoupon", f"{arg}:{c['id']}")] for c in coupons]
        await admin_render(cq, "🎟 Send which coupon to this user?",
                           kb_rows(*rows, admin_back_row("users", "view", arg)))
    elif act == "givecoupon":
        uid, cid = arg.split(":")
        c = await db.fetchrow("SELECT * FROM coupons WHERE id=$1", int(cid))
        if c:
            disc = (f"{money(c['value']).normalize()}%" if c["discount_type"] == "percent"
                    else fmt_money(money(c["value"]), "INR"))
            await notify_user(bot, int(uid),
                f"🎟 <b>A coupon for you!</b>\n\n"
                f"Code: <code>{c['code']}</code>\n"
                f"Discount: {disc}\n\n"
                f"Enter it at checkout.")
            await admin_audit(admin, "coupon_grant", "user", int(uid), {"code": c["code"]})
            await cq.answer("Coupon sent to the user")
        await view_user(cq, admin, int(uid))
    elif act == "refbonus":
        await begin_capture(cq, state, "user_refbonus",
            "🎁 Send the bonus amount in INR to credit this user.", back="users", uid=arg)
        return
    elif act == "msg":
        await begin_capture(cq, state, "user_msg",
            "💬 Send the message to deliver to this user.", back="users", uid=arg)
        return
    elif act == "ban":
        await begin_capture(cq, state, "user_ban",
            "🚫 Send a ban reason (the user sees it), or a dash for none.",
            back="users", uid=arg)
        return
    elif act == "unban":
        await db.execute(
            "UPDATE store_users SET is_banned=FALSE, ban_reason=NULL WHERE id=$1", int(arg))
        await admin_audit(admin, "user_unban", "user", int(arg))
        await notify_user(bot, int(arg), "✅ Your account has been reinstated.")
        await view_user(cq, admin, int(arg))
    elif act == "promote":
        if admin_rank(admin) < ROLE_RANK["owner"]:
            await cq.answer("Owners only.", show_alert=True)
            return
        u = await db.fetchrow("SELECT telegram_id, first_name FROM store_users WHERE id=$1", int(arg))
        await admin_render(cq,
            f"⬆️ Make <b>{u['first_name'] or u['telegram_id']}</b> a bot admin?\n\n"
            "They'll get full access to /admin as a manager.",
            kb_rows([abtn("✅ Confirm", "users", "promoteok", arg),
                     abtn("✖️ Cancel", "users", "view", arg)]))
    elif act == "promoteok":
        u = await db.fetchrow("SELECT telegram_id FROM store_users WHERE id=$1", int(arg))
        # Password is unusable: Telegram admins authenticate by their TG id.
        unusable = bcrypt.hashpw(secrets.token_bytes(32), bcrypt.gensalt()).decode()
        await db.execute(
            """INSERT INTO admins(username, password_hash, role, active)
               VALUES($1,$2,'manager',TRUE)
               ON CONFLICT (username) DO UPDATE SET active=TRUE, role='manager'""",
            u["telegram_id"], unusable)
        await _load_admin_usernames(force=True)
        await admin_audit(admin, "admin_promote", "user", int(arg))
        await notify_user(bot, int(arg),
            "⚙️ You've been given admin access. Send /admin to open the panel.")
        await cq.answer("Promoted")
        await view_user(cq, admin, int(arg))
    elif act == "demote":
        if admin_rank(admin) < ROLE_RANK["owner"]:
            await cq.answer("Owners only.", show_alert=True)
            return
        u = await db.fetchrow("SELECT telegram_id FROM store_users WHERE id=$1", int(arg))
        await db.execute("UPDATE admins SET active=FALSE WHERE username=$1", u["telegram_id"])
        await _load_admin_usernames(force=True)
        await admin_audit(admin, "admin_demote", "user", int(arg))
        await cq.answer("Admin access removed")
        await view_user(cq, admin, int(arg))
    await _ack(cq)


@capture_applier("user_search")
async def _user_search(msg, state, admin, data):
    term = msg.text.strip().lstrip("@")[:64]
    await state.clear()
    await open_users(msg, admin, 0, term)


@capture_applier("user_bal")
async def _user_bal(msg, state, admin, data):
    uid = int(data["uid"])
    raw = msg.text.strip().replace("₹", "").replace(",", "")
    try:
        amount = money(Decimal(raw), Q_CRYPTO)
    except (InvalidOperation, ValueError):
        await msg.answer("Send a number, e.g. 500 or -250.")
        return
    if amount == ZERO:
        await msg.answer("Amount can't be zero.")
        return
    try:
        async with db.tx() as conn:
            new_bal = await wallet_apply(
                conn, uid, amount, "admin_adjust",
                f"Admin adjustment by {admin['username']}")
    except InsufficientFunds:
        await msg.answer("⚠️ That would put the balance below zero.")
        return
    await admin_audit(admin, "balance_adjust", "user", uid, {"amount": str(amount)})
    await state.clear()
    sign = "+" if amount > ZERO else ""
    await notify_user(bot, uid,
        f"💰 Your wallet was adjusted by an admin.\n"
        f"{sign}{fmt_money(abs(amount),'INR') if amount < ZERO else fmt_money(amount,'INR')}\n"
        f"New balance: <b>{fmt_money(new_bal,'INR')}</b>")
    await msg.answer(f"✅ Adjusted. New balance: {fmt_money(new_bal,'INR')}")
    await view_user(msg, admin, uid)


@capture_applier("user_refbonus")
async def _user_refbonus(msg, state, admin, data):
    uid = int(data["uid"])
    try:
        amount = money(Decimal(msg.text.strip().replace("₹", "")), Q_CRYPTO)
        if amount <= ZERO:
            raise ValueError
    except (InvalidOperation, ValueError):
        await msg.answer("Send a positive amount.")
        return
    async with db.tx() as conn:
        new_bal = await wallet_apply(conn, uid, amount, "referral_bonus",
                                     "Referral bonus (admin granted)")
    await admin_audit(admin, "referral_grant", "user", uid, {"amount": str(amount)})
    await state.clear()
    await notify_user(bot, uid,
        f"🎁 <b>Referral bonus!</b>\n\n"
        f"{fmt_money(amount,'INR')} added to your wallet.\n"
        f"Balance: <b>{fmt_money(new_bal,'INR')}</b>")
    await msg.answer(f"✅ Bonus credited. New balance: {fmt_money(new_bal,'INR')}")
    await view_user(msg, admin, uid)


@capture_applier("user_msg")
async def _user_msg(msg, state, admin, data):
    uid = int(data["uid"])
    ok = await notify_user(bot, uid, msg.text.strip()[:3500])
    await state.clear()
    await msg.answer("✅ Message sent." if ok else
                     "⚠️ Couldn't deliver — the user may have blocked the bot.")
    await view_user(msg, admin, uid)


@capture_applier("user_ban")
async def _user_ban(msg, state, admin, data):
    uid = int(data["uid"])
    reason = "" if msg.text.strip() == "-" else msg.text.strip()[:300]
    await db.execute(
        "UPDATE store_users SET is_banned=TRUE, ban_reason=$1 WHERE id=$2",
        reason or None, uid)
    await admin_audit(admin, "user_ban", "user", uid, {"reason": reason})
    await state.clear()
    await notify_user(bot, uid,
        "🚫 Your account has been suspended."
        + (f"\n\nReason: {reason}" if reason else ""))
    await msg.answer("🚫 User banned.")
    await view_user(msg, admin, uid)


_ADMIN_OPENERS["users"] = lambda e, a, *_: open_users(e, a)


# =============================================================================
#  ADMIN — BROADCAST  (§7)
# =============================================================================
#  Text, photo, video, document and forwarded messages. Audience can be
#  everyone, buyers, non-buyers, a rank, a language, or a list of IDs.
#  Composition is two-step: pick audience, then send the content.

BROADCAST_AUDIENCES = [
    ("all", "🌍 Everyone"),
    ("buyers", "🛒 Has ordered"),
    ("no_orders", "👀 Never ordered"),
    ("tier~gold", "🥇 Gold rank"),
    ("tier~silver", "🥈 Silver rank"),
    ("lang~en", "🇬🇧 English users"),
    ("lang~hi", "🇮🇳 Hindi users"),
]


async def open_broadcast(event, admin) -> None:
    recent = await db.fetch(
        "SELECT * FROM broadcasts ORDER BY created_at DESC LIMIT 5")
    lines = ["📢 <b>Broadcast</b>\n", "Pick an audience, then send your content.\n"]
    if recent:
        lines.append("<b>Recent</b>")
        for b in recent:
            icon = "✅" if b["status"] == "done" else "🔄"
            lines.append(
                f"{icon} {b['target']} — {b['sent_count']} sent"
                + (f", {b['failed_count']} failed" if b["failed_count"] else ""))

    rows = [
        [abtn("🛒 Product broadcast", "pbcast", "open")],
        [abtn("🔥 Discount Sale broadcast", "dsale", "open")],
    ]
    rows += [[abtn(label, "broadcast", "aud", key)]
             for key, label in BROADCAST_AUDIENCES]
    rows.append([abtn("🎯 Specific user IDs", "broadcast", "audids")])
    await admin_render(event, "\n".join(lines), kb_rows(*rows, admin_back_row("home")))


@router.callback_query(AdminCB.filter(F.section == "broadcast"))
async def cb_broadcast(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    act, arg = cb.action, cb.arg

    if act == "open":
        await open_broadcast(cq, admin)
    elif act == "aud":
        count = await _audience_count(arg)
        await begin_capture(cq, state, "bc_content",
            f"📢 <b>Audience: {arg}</b> — about <b>{count}</b> user(s).\n\n"
            "Now send what to broadcast:\n"
            "• plain text\n• a photo (caption optional)\n• a video\n• a document\n\n"
            "Or forward a message to relay it.",
            back="broadcast", accept="any", target=arg)
        return
    elif act == "audids":
        await begin_capture(cq, state, "bc_ids",
            "🎯 Send Telegram IDs separated by commas.\n"
            "Example: <code>111111,222222</code>", back="broadcast")
        return
    elif act == "confirm":
        data = await state.get_data()
        await _launch_broadcast(cq, state, admin, data)
        return
    await _ack(cq)


async def _audience_count(target: str) -> int:
    q, params = broadcast_audience_sql(target)
    rows = await db.fetch(q, *params)
    return len(rows)


@capture_applier("bc_ids")
async def _bc_ids(msg, state, admin, data):
    ids = [x.strip() for x in msg.text.split(",") if x.strip().isdigit()]
    if not ids:
        await msg.answer("No valid numeric IDs found. Try again.")
        return
    target = "ids~" + ",".join(ids)
    count = await _audience_count(target)
    await state.update_data(flow="bc_content", accept="any", target=target,
                            back="broadcast")
    await msg.answer(
        f"🎯 <b>{count}</b> of {len(ids)} ID(s) matched known users.\n\n"
        "Now send the content to broadcast (text, photo, video or document).")


@capture_applier("bc_content")
async def _bc_content(msg, state, admin, data):
    """Capture the broadcast payload, then show a confirm screen."""
    # html_text falls back to caption+caption_entities when text is empty, so
    # this covers both a plain broadcast and a photo/video with a caption —
    # and keeps whatever formatting the admin actually used.
    media_type, file_id, text = "", "", msg.html_text.strip()
    if msg.photo:
        media_type, file_id = "photo", msg.photo[-1].file_id
    elif msg.video:
        media_type, file_id = "video", msg.video.file_id
    elif msg.document:
        media_type, file_id = "document", msg.document.file_id
    elif msg.animation:
        media_type, file_id = "animation", msg.animation.file_id
    elif not text:
        await msg.answer("Send text, a photo, a video or a document.")
        return

    if text:
        err = validate_telegram_html(text)
        if err:
            await msg.answer(f"⚠️ {err}\n\nSend the content again.")
            return

    target = data["target"]
    count = await _audience_count(target)
    await state.update_data(media_type=media_type, file_id=file_id, text=text,
                            target=target)

    preview = _html_preview(text, 150) if text else "(no caption)"
    kind = media_type or "text"
    await msg.answer(
        f"📢 <b>Ready to send</b>\n\n"
        f"Audience: <code>{target}</code> (~{count} users)\n"
        f"Type: {kind}\n"
        f"Content: {preview}\n\n"
        f"This cannot be undone.",
        reply_markup=kb_rows(
            [abtn("🚀 Send now", "broadcast", "confirm")],
            [abtn("✖️ Cancel", "cancel", "open", "broadcast")]))


async def _launch_broadcast(cq, state, admin, data) -> None:
    target = data.get("target", "all")
    text = data.get("text", "")
    media_type = data.get("media_type", "")
    file_id = data.get("file_id", "")

    aid = _admin_db_id(admin)
    row = await db.fetchrow(
        """INSERT INTO broadcasts(message, target, created_by, status)
           VALUES($1,$2,$3,'running') RETURNING *""",
        (text or f"[{media_type}]")[:4000], target, aid)

    await admin_audit(admin, "broadcast_start", "broadcast", row["id"],
                      {"target": target, "type": media_type or "text"})
    asyncio.create_task(run_broadcast(row["id"], text, target, media_type, file_id))
    await state.clear()
    await admin_render(cq,
        "🚀 <b>Broadcast started.</b>\n\n"
        "It sends in the background at a safe pace. "
        "Come back to 📢 Broadcast to see the delivered count.",
        kb_rows(admin_back_row("home")))
    await _ack(cq)


_ADMIN_OPENERS["broadcast"] = lambda e, a, *_: open_broadcast(e, a)

# =============================================================================
#  ADMIN — PRODUCT BROADCAST
# =============================================================================
#  Promote one product: pick it, write a message (or attach your own photo/
#  video -- no attachment falls back to the product's own listing image),
#  pick an audience, send. Mirrors the web admin's Product broadcast panel
#  (create_product_broadcast) exactly, and reuses the same run_broadcast
#  sender with a "🛒 Buy Now" button wired to that product.

async def open_pbcast(event, admin) -> None:
    prods = await db.fetch(
        "SELECT id, name FROM products WHERE active ORDER BY sold_count DESC, id DESC LIMIT 20")
    if not prods:
        await admin_render(event, "No active products to promote.",
                           kb_rows(admin_back_row("broadcast")))
        return
    rows = [[abtn(p["name"][:26], "pbcast", "pick", str(p["id"]))] for p in prods]
    await admin_render(event, "🛒 <b>Product broadcast</b>\n\nPick a product to promote:",
                       kb_rows(*rows, admin_back_row("broadcast")))


@router.callback_query(AdminCB.filter(F.section == "pbcast"))
async def cb_pbcast(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    act, arg = cb.action, cb.arg

    if act == "open":
        await open_pbcast(cq, admin)
    elif act == "pick":
        pid = int(arg)
        product = await db.fetchrow("SELECT name FROM products WHERE id=$1 AND active", pid)
        if not product:
            await cq.answer("Unavailable.", show_alert=True)
            return
        await begin_capture(cq, state, "pbcast_content",
            f"🛒 <b>{product['name']}</b>\n\n"
            "Send the broadcast message (HTML allowed, max 1024 characters -- "
            "Telegram's photo/video caption limit). Attach your own photo or "
            "video to use instead of the product's own image, or send text only.",
            back="pbcast", accept="any", product_id=pid)
        return
    elif act == "aud":
        await _pbcast_show_confirm(cq, state, arg)
        return
    elif act == "audids":
        await begin_capture(cq, state, "pbcast_ids",
            "🎯 Send Telegram IDs separated by commas.\n"
            "Example: <code>111111,222222</code>", back="pbcast")
        return
    elif act == "confirm":
        data = await state.get_data()
        await _launch_pbcast(cq, state, admin, data)
        return
    await _ack(cq)


@capture_applier("pbcast_content")
async def _pbcast_content(msg, state, admin, data):
    media_type, file_id, text = "", "", msg.html_text.strip()
    if msg.photo:
        media_type, file_id = "photo", msg.photo[-1].file_id
    elif msg.video:
        media_type, file_id = "video", msg.video.file_id
    elif not text:
        await msg.answer("Send a message, a photo, or a video.")
        return

    if text:
        err = validate_telegram_html(text)
        if err:
            await msg.answer(f"⚠️ {err}\n\nSend the content again.")
            return
    if len(text) > 1024:
        await msg.answer(
            f"That's {len(text)} characters -- product broadcasts are capped "
            "at 1024 (Telegram's caption limit for photo/video). Shorten it "
            "and resend.")
        return

    pid = int(data["product_id"])
    product = await db.fetchrow("SELECT * FROM products WHERE id=$1", pid)
    if not product:
        await state.clear()
        await msg.answer("That product no longer exists.")
        return

    if not media_type:
        # Same fallback as the web admin: a bare message defaults to the
        # product's own listing image; if it has none, send text-only.
        media_type = "photo"
        file_id = product["image_file_id"] or product["image_url"] or ""
        if not file_id:
            media_type = ""

    await state.update_data(message=text, media_type=media_type, file_id=file_id)
    rows = [[abtn(label, "pbcast", "aud", key)] for key, label in BROADCAST_AUDIENCES]
    rows.append([abtn("🎯 Specific user IDs", "pbcast", "audids")])
    await msg.answer(f"🛒 <b>{product['name']}</b> — pick an audience:",
                     reply_markup=kb_rows(*rows))


@capture_applier("pbcast_ids")
async def _pbcast_ids(msg, state, admin, data):
    ids = [x.strip() for x in msg.text.split(",") if x.strip().isdigit()]
    if not ids:
        await msg.answer("No valid numeric IDs found. Try again.")
        return
    await _pbcast_show_confirm(msg, state, "ids~" + ",".join(ids))


async def _pbcast_show_confirm(event, state: FSMContext, target: str) -> None:
    data = await state.get_data()
    pid = int(data["product_id"])
    product = await db.fetchrow("SELECT name FROM products WHERE id=$1", pid)
    if not product:
        await state.clear()
        body = "That product no longer exists."
        if isinstance(event, CallbackQuery):
            await admin_render(event, body, kb_rows(admin_back_row("broadcast")))
            await event.answer()
        else:
            await event.answer(body)
        return

    count = await _audience_count(target)
    await state.update_data(target=target)
    text = (
        f"🛒 <b>Ready to send</b>\n\n"
        f"Product: {product['name']}\n"
        f"Audience: <code>{target}</code> (~{count} users)\n"
        f"Content: {_html_preview(data.get('message', ''), 150) or '(no caption)'}\n\n"
        f"This cannot be undone."
    )
    kb = kb_rows(
        [abtn("🚀 Send now", "pbcast", "confirm")],
        [abtn("✖️ Cancel", "cancel", "open", "broadcast")])
    if isinstance(event, CallbackQuery):
        await admin_render(event, text, kb)
        await event.answer()
    else:
        await event.answer(text, reply_markup=kb)


async def _launch_pbcast(cq, state, admin, data) -> None:
    pid = int(data["product_id"])
    target = data.get("target", "all")
    text = data.get("message", "")
    media_type = data.get("media_type", "")
    file_id = data.get("file_id", "")

    aid = _admin_db_id(admin)
    row = await db.fetchrow(
        """INSERT INTO broadcasts(message, target, created_by, status, product_id,
                                   image_file_id, media_type)
           VALUES($1,$2,$3,'running',$4,$5,$6) RETURNING *""",
        (text or "[product]")[:4000], target, aid, pid, file_id or None, media_type or "photo",
    )
    await admin_audit(admin, "product_broadcast_start", "broadcast", row["id"],
                      {"target": target, "productId": pid, "mediaType": media_type})
    asyncio.create_task(run_broadcast(
        row["id"], text, target, media_type, file_id, buy_product_id=pid))
    await state.clear()
    await admin_render(cq,
        "🚀 <b>Product broadcast started.</b>\n\n"
        "It sends in the background at a safe pace.",
        kb_rows(admin_back_row("home")))
    await _ack(cq)


_ADMIN_OPENERS["pbcast"] = lambda e, a, *_: open_pbcast(e, a)

# =============================================================================
#  ADMIN — DISCOUNT SALE BROADCAST
# =============================================================================
#  Apply a percent/fixed discount to a whole category or a single product,
#  then announce it -- both in one confirm, using apply_bulk_discount_tx (the
#  same pricing logic the web Mass Discount panel uses) and run_broadcast
#  (the same sender every broadcast in this bot uses). For picking *which*
#  products, this in-bot flow only offers "a whole category" or "one
#  product" -- the web admin's multi-select checkbox picker isn't practical
#  in a chat UI, so anything more granular still belongs there.

async def open_dsale(event, admin) -> None:
    await admin_render(event,
        "🔥 <b>Discount Sale broadcast</b>\n\n"
        "Apply a discount and announce it in one step. Choose what it "
        "applies to:",
        kb_rows(
            [abtn("📁 A whole category", "dsale", "scope", "category")],
            [abtn("📦 One product", "dsale", "scope", "products")],
            admin_back_row("broadcast")))


@router.callback_query(AdminCB.filter(F.section == "dsale"))
async def cb_dsale(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    act, arg = cb.action, cb.arg

    if act == "open":
        await open_dsale(cq, admin)
    elif act == "scope":
        if arg == "category":
            cats = await db.fetch("SELECT id, emoji, name FROM categories ORDER BY sort_order, id")
            if not cats:
                await cq.answer("No categories yet.", show_alert=True)
                return
            rows = [[abtn(f"{c['emoji']} {c['name']}", "dsale", "setcat", str(c["id"]))]
                    for c in cats]
            await admin_render(cq, "📁 Pick a category:", kb_rows(*rows, admin_back_row("dsale")))
        else:
            prods = await db.fetch(
                "SELECT id, name FROM products WHERE active ORDER BY sold_count DESC, id DESC LIMIT 20")
            if not prods:
                await cq.answer("No active products.", show_alert=True)
                return
            rows = [[abtn(p["name"][:26], "dsale", "setprod", str(p["id"]))] for p in prods]
            await admin_render(cq, "📦 Pick a product:", kb_rows(*rows, admin_back_row("dsale")))
    elif act == "setcat":
        cat = await db.fetchrow("SELECT name FROM categories WHERE id=$1", int(arg))
        if not cat:
            await cq.answer("Unavailable.", show_alert=True)
            return
        await begin_capture(cq, state, "dsale_value",
            f"📁 <b>{cat['name']}</b>\n\n"
            "Send the discount: a percent like <code>20%</code>, or a flat "
            "INR amount like <code>100</code>.",
            back="dsale", scope="category", target_ids=[int(arg)])
        return
    elif act == "setprod":
        product = await db.fetchrow("SELECT name FROM products WHERE id=$1 AND active", int(arg))
        if not product:
            await cq.answer("Unavailable.", show_alert=True)
            return
        await begin_capture(cq, state, "dsale_value",
            f"📦 <b>{product['name']}</b>\n\n"
            "Send the discount: a percent like <code>20%</code>, or a flat "
            "INR amount like <code>100</code>.",
            back="dsale", scope="products", target_ids=[int(arg)])
        return
    elif act == "aud":
        await _dsale_show_confirm(cq, state, arg)
        return
    elif act == "audids":
        await begin_capture(cq, state, "dsale_ids",
            "🎯 Send Telegram IDs separated by commas.\n"
            "Example: <code>111111,222222</code>", back="dsale")
        return
    elif act == "confirm":
        data = await state.get_data()
        await _launch_dsale(cq, state, admin, data)
        return
    await _ack(cq)


async def _dsale_preview(scope: str, target_ids: list[int],
                         discount_type: str, value: Decimal) -> list[dict]:
    """Read-only preview using the same math apply_bulk_discount_tx will use
    for real at confirm time -- no lock, no write; just for the admin to see
    before committing. A product edited between preview and confirm just
    gets recomputed fresh at apply time, same as any other race on an admin
    tool (not a fraud-relevant path)."""
    if scope == "category":
        rows = await db.fetch(
            "SELECT id, name, price, original_price FROM products WHERE category_id = ANY($1::int[])",
            target_ids)
    else:
        rows = await db.fetch(
            "SELECT id, name, price, original_price FROM products WHERE id = ANY($1::int[])",
            target_ids)
    out = []
    for r in rows:
        base = money(r["original_price"], Q_CRYPTO) if r["original_price"] is not None \
            else money(r["price"], Q_CRYPTO)
        out.append({"id": r["id"], "name": r["name"], "old_price": base,
                    "new_price": _discounted_price(base, discount_type, value)})
    return out


@capture_applier("dsale_value")
async def _dsale_value(msg, state, admin, data):
    raw = (msg.text or "").strip()
    is_percent = raw.endswith("%")
    num = raw[:-1].strip() if is_percent else raw
    try:
        value = money(Decimal(num.replace(",", "")), Q_CRYPTO)
        if value <= 0:
            raise ValueError
        if is_percent and value >= 100:
            raise ValueError
    except (InvalidOperation, ValueError):
        await msg.answer(
            "Send a positive percent like <code>20%</code> (under 100), "
            "or a flat INR amount like <code>100</code>.")
        return

    discount_type = "percent" if is_percent else "fixed"
    preview = await _dsale_preview(data["scope"], data["target_ids"], discount_type, value)
    if not preview:
        await state.clear()
        await msg.answer("No matching products found. Nothing to discount.")
        return

    lines = [
        f"{p['name'][:22]}: {fmt_money(p['old_price'],'INR')} → "
        f"<b>{fmt_money(p['new_price'],'INR')}</b>"
        for p in preview[:10]
    ]
    if len(preview) > 10:
        lines.append(f"…and {len(preview) - 10} more")

    await state.update_data(flow="dsale_message", discount_type=discount_type, value=str(value))
    await msg.answer(
        f"🔥 <b>{len(preview)} product(s) will be discounted</b>\n\n"
        + "\n".join(lines) +
        "\n\nNow send the announcement message (HTML allowed), or send "
        "<code>skip</code> to auto-generate one."
    )


@capture_applier("dsale_message")
async def _dsale_message(msg, state, admin, data):
    text = (msg.html_text or "").strip()
    if text.lower() == "skip":
        preview = await _dsale_preview(data["scope"], data["target_ids"],
                                       data["discount_type"], Decimal(data["value"]))
        if data["discount_type"] == "percent":
            headline = f"🔥 {data['value']}% OFF"
        else:
            headline = f"🔥 ₹{data['value']} OFF"
        sample = preview[0] if preview else None
        text = f"{headline}\n\nLimited time only — grab it before the price goes back up!"
        if sample and len(preview) == 1:
            text = (f"{headline} — {sample['name']}\n\n"
                    f"Now just {fmt_money(sample['new_price'],'INR')} "
                    f"(was {fmt_money(sample['old_price'],'INR')})!")
    elif not text:
        await msg.answer("Send a message, or send 'skip' to auto-generate one.")
        return
    else:
        err = validate_telegram_html(text)
        if err:
            await msg.answer(f"⚠️ {err}\n\nSend the message again.")
            return

    await state.update_data(message=text[:4000])
    rows = [[abtn(label, "dsale", "aud", key)] for key, label in BROADCAST_AUDIENCES]
    rows.append([abtn("🎯 Specific user IDs", "dsale", "audids")])
    await msg.answer("Pick an audience:", reply_markup=kb_rows(*rows))


@capture_applier("dsale_ids")
async def _dsale_ids(msg, state, admin, data):
    ids = [x.strip() for x in msg.text.split(",") if x.strip().isdigit()]
    if not ids:
        await msg.answer("No valid numeric IDs found. Try again.")
        return
    await _dsale_show_confirm(msg, state, "ids~" + ",".join(ids))


async def _dsale_show_confirm(event, state: FSMContext, target: str) -> None:
    data = await state.get_data()
    count = await _audience_count(target)
    await state.update_data(target=target)
    scope_label = "category" if data["scope"] == "category" else "product"
    disc = (f"{data['value']}%" if data["discount_type"] == "percent" else f"₹{data['value']}")
    text = (
        f"🔥 <b>Ready to send</b>\n\n"
        f"Scope: {scope_label}\n"
        f"Discount: {disc}\n"
        f"Audience: <code>{target}</code> (~{count} users)\n"
        f"Message: {_html_preview(data.get('message', ''), 150)}\n\n"
        f"This applies the discount immediately and cannot be undone."
    )
    kb = kb_rows(
        [abtn("🚀 Apply & Send", "dsale", "confirm")],
        [abtn("✖️ Cancel", "cancel", "open", "broadcast")])
    if isinstance(event, CallbackQuery):
        await admin_render(event, text, kb)
        await event.answer()
    else:
        await event.answer(text, reply_markup=kb)


async def _launch_dsale(cq, state, admin, data) -> None:
    scope = data["scope"]
    target_ids = data["target_ids"]
    discount_type = data["discount_type"]
    value = Decimal(data["value"])
    target = data.get("target", "all")
    text = data.get("message", "")

    async with db.tx() as conn:
        updated = await apply_bulk_discount_tx(conn, scope, target_ids, discount_type, value, "apply")
    if not updated:
        await state.clear()
        await admin_render(cq, "No matching products found — nothing was changed or sent.",
                           kb_rows(admin_back_row("broadcast")))
        await _ack(cq)
        return

    await admin_audit(admin, "discount_sale_broadcast", "product", None,
                      {"scope": scope, "targetIds": target_ids, "discountType": discount_type,
                       "value": str(value), "updated": len(updated), "target": target})

    aid = _admin_db_id(admin)
    buy_product_id = updated[0]["id"] if scope == "products" else None
    row = await db.fetchrow(
        """INSERT INTO broadcasts(message, target, created_by, status, product_id)
           VALUES($1,$2,$3,'running',$4) RETURNING *""",
        text[:4000], target, aid, buy_product_id)
    asyncio.create_task(run_broadcast(
        row["id"], text, target, buy_product_id=buy_product_id,
        shop_button=(scope != "products")))

    await state.clear()
    await admin_render(cq,
        f"🚀 <b>Discount applied to {len(updated)} product(s) and broadcast started.</b>\n\n"
        "It sends in the background at a safe pace.",
        kb_rows(admin_back_row("home")))
    await _ack(cq)


_ADMIN_OPENERS["dsale"] = lambda e, a, *_: open_dsale(e, a)

# =============================================================================
#  ADMIN — SUPPORT TICKETS
# =============================================================================
#  In-Telegram mirror of the web panel's Tickets section (see admin.html
#  Tickets()) -- list by status, view a thread, reply or close/reopen,
#  without needing the browser.

async def open_tickets(event, admin, status: str = "open") -> None:
    rows = await db.fetch(
        """SELECT t.*, u.telegram_id, u.username, u.first_name FROM tickets t
             JOIN store_users u ON u.id=t.user_id
            WHERE $1='all' OR t.status=$1
            ORDER BY t.updated_at DESC LIMIT 15""",
        status)

    lines = [f"🎫 <b>Tickets</b> — {status}\n"]
    kb = []
    if not rows:
        lines.append("Nothing here.")
    for r in rows:
        icon = TICKET_STATUS_ICON.get(r["status"], r["status"])
        who = r["first_name"] or r["username"] or r["telegram_id"]
        kb.append([abtn(
            f"#{r['id']} {icon} {_html_preview(who, 14)} — {_html_preview(r['subject'], 24)}",
            "tickets", "view", r["id"])])

    tabs = [abtn(("• " if status == s else "") + s, "tickets", "list", s)
            for s in ("open", "closed", "all")]
    await admin_render(event, "\n".join(lines), kb_rows(tabs, *kb, admin_back_row("home")))


@router.callback_query(AdminCB.filter(F.section == "tickets"))
async def cb_tickets(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    act, arg = cb.action, cb.arg

    if act in ("open", "list"):
        await open_tickets(cq, admin, arg or "open")
    elif act == "view":
        await open_ticket_view(cq, admin, int(arg))
        return
    elif act == "reply":
        tid = int(arg)
        ticket = await db.fetchrow("SELECT * FROM tickets WHERE id=$1", tid)
        if not ticket or ticket["status"] != "open":
            await cq.answer("This ticket can't be replied to.", show_alert=True)
            return
        await begin_capture(cq, state, "ticket_reply",
            f"✍️ <b>Reply to ticket #{tid}</b>\n\nSend your reply as text (HTML allowed) or a photo.",
            back="tickets", accept="any", ticket_id=tid)
        return
    elif act == "photo":
        row = await db.fetchrow(
            "SELECT photo_file_id FROM ticket_messages WHERE id=$1", int(arg))
        if not row or not row["photo_file_id"]:
            await cq.answer("Photo not found.", show_alert=True)
            return
        try:
            await bot.send_photo(cq.message.chat.id, row["photo_file_id"])
        except Exception:
            await cq.answer("Couldn't load the image.", show_alert=True)
            return
        await _ack(cq)
        return
    elif act in ("close", "reopen"):
        tid = int(arg)
        new_status = "closed" if act == "close" else "open"
        row = await db.fetchrow(
            "UPDATE tickets SET status=$1, updated_at=NOW() WHERE id=$2 RETURNING *",
            new_status, tid)
        if row:
            await admin_audit(admin, f"ticket_{act}", "ticket", tid)
            await open_ticket_view(cq, admin, tid)
            await _ack(cq)
            return
    await _ack(cq)


async def open_ticket_view(event, admin, ticket_id: int) -> None:
    ticket = await db.fetchrow(
        """SELECT t.*, u.telegram_id, u.username, u.first_name FROM tickets t
             JOIN store_users u ON u.id=t.user_id WHERE t.id=$1""", ticket_id)
    if not ticket:
        await admin_render(event, "Ticket not found.", kb_rows(admin_back_row("tickets")))
        return
    msgs = await db.fetch(
        "SELECT * FROM ticket_messages WHERE ticket_id=$1 ORDER BY created_at LIMIT 20",
        ticket_id)

    who = ticket["first_name"] or ticket["username"] or ticket["telegram_id"]
    icon = TICKET_STATUS_ICON.get(ticket["status"], ticket["status"])
    lines = [f"🎫 <b>Ticket #{ticket_id}</b> — {icon}",
             f"From: {_html_preview(who, 30)} (<code>{ticket['telegram_id']}</code>)\n"]
    photo_rows = []
    for m in msgs:
        lines.append(_ticket_message_line(m, viewer="admin"))
        if m["photo_file_id"]:
            label = "User" if m["sender"] == "user" else "You"
            photo_rows.append([abtn(f"📷 View photo ({label})", "tickets", "photo", m["id"])])

    rows = list(photo_rows)
    if ticket["status"] == "open":
        rows.append([abtn("✍️ Reply", "tickets", "reply", ticket_id),
                     abtn("✅ Close", "tickets", "close", ticket_id)])
    else:
        rows.append([abtn("↩️ Reopen", "tickets", "reopen", ticket_id)])
    await admin_render(event, "\n".join(lines), kb_rows(*rows, admin_back_row("tickets")))


@capture_applier("ticket_reply")
async def _ticket_reply(msg, state, admin, data):
    tid = data["ticket_id"]
    photo_file_id = msg.photo[-1].file_id if msg.photo else ""
    # html_text falls back to caption+caption_entities when text is empty --
    # covers a plain reply and a photo with an HTML-formatted caption alike.
    text = (msg.html_text or "").strip()
    if not text and not photo_file_id:
        await msg.answer("Send text or a photo for your reply.")
        return
    if text:
        err = validate_telegram_html(text)
        if err:
            await msg.answer(f"⚠️ {err}\n\nSend the reply again.")
            return

    ticket = await db.fetchrow(
        """SELECT t.*, u.telegram_id FROM tickets t JOIN store_users u ON u.id=t.user_id
            WHERE t.id=$1""", tid)
    if not ticket or ticket["status"] != "open":
        await state.clear()
        await msg.answer("That ticket is no longer open.")
        return
    await db.execute(
        """INSERT INTO ticket_messages(ticket_id, sender, admin_id, message, photo_file_id)
           VALUES($1,'admin',$2,$3,$4)""",
        tid, _admin_db_id(admin), text, photo_file_id or None)
    await db.execute("UPDATE tickets SET updated_at=NOW() WHERE id=$1", tid)

    notify_text = f"💬 <b>Support replied to ticket #{tid}</b>" + (f"\n\n{text}" if text else "")
    await notify_user(bot, ticket["user_id"], notify_text)
    if photo_file_id:
        try:
            await bot.send_photo(int(ticket["telegram_id"]), photo_file_id)
        except (TelegramForbiddenError, TelegramBadRequest) as e:
            log.warning("ticket reply photo forward failed for %s: %s", ticket["telegram_id"], e)

    await admin_audit(admin, "ticket_reply", "ticket", tid)
    await state.clear()
    await open_ticket_view(msg, admin, tid)


_ADMIN_OPENERS["tickets"] = lambda e, a, *_: open_tickets(e, a)

# =============================================================================
#  ADMIN — PIN NOTICE
# =============================================================================
#  In-Telegram mirror of the web panel's Pin Notice section: compose a
#  message, send + pin it to an audience, and unpin any past one for
#  everyone it reached. See run_pinned_warning_broadcast / run_unpin_broadcast.

async def open_pinned(event, admin) -> None:
    recent = await db.fetch(
        "SELECT * FROM pinned_broadcasts ORDER BY created_at DESC LIMIT 5")
    lines = ["📌 <b>Pin Notice</b>\n", "Pick an audience, then send the notice to pin.\n"]
    kb = []
    if recent:
        lines.append("<b>Recent</b>")
        for b in recent:
            icon = "✅" if b["status"] == "done" else "🔄"
            lines.append(
                f"{icon} {b['target']} — {b['sent_count']} sent"
                + (f", {b['failed_count']} failed" if b["failed_count"] else ""))
            if b["active"]:
                kb.append([abtn(f"🗑 Unpin #{b['id']} ({_html_preview(b['message'], 20)})",
                                "pinned", "unpin", b["id"])])

    rows = [[abtn(label, "pinned", "aud", key)] for key, label in BROADCAST_AUDIENCES]
    rows.append([abtn("🎯 Specific user IDs", "pinned", "audids")])
    await admin_render(event, "\n".join(lines), kb_rows(*rows, *kb, admin_back_row("home")))


@router.callback_query(AdminCB.filter(F.section == "pinned"))
async def cb_pinned(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    act, arg = cb.action, cb.arg

    if act == "open":
        await open_pinned(cq, admin)
    elif act == "aud":
        count = await _audience_count(arg)
        await begin_capture(cq, state, "pin_content",
            f"📌 <b>Audience: {arg}</b> — about <b>{count}</b> user(s).\n\n"
            "Send the notice text to pin (HTML allowed).",
            back="pinned", target=arg)
        return
    elif act == "audids":
        await begin_capture(cq, state, "pin_ids",
            "🎯 Send Telegram IDs separated by commas.\n"
            "Example: <code>111111,222222</code>", back="pinned")
        return
    elif act == "confirm":
        data = await state.get_data()
        await _launch_pinned(cq, state, admin, data)
        return
    elif act == "unpin":
        bid = int(arg)
        row = await db.fetchrow("SELECT * FROM pinned_broadcasts WHERE id=$1", bid)
        if row:
            await admin_audit(admin, "pinned_warning_unpin", "pinned_broadcast", bid)
            asyncio.create_task(run_unpin_broadcast(bid))
            await cq.answer("Unpinning started")
        await open_pinned(cq, admin)
        return
    await _ack(cq)


@capture_applier("pin_ids")
async def _pin_ids(msg, state, admin, data):
    ids = [x.strip() for x in msg.text.split(",") if x.strip().isdigit()]
    if not ids:
        await msg.answer("No valid numeric IDs found. Try again.")
        return
    target = "ids~" + ",".join(ids)
    count = await _audience_count(target)
    await state.update_data(flow="pin_content", target=target, back="pinned")
    await msg.answer(
        f"🎯 <b>{count}</b> of {len(ids)} ID(s) matched known users.\n\n"
        "Now send the notice text to pin.")


@capture_applier("pin_content")
async def _pin_content(msg, state, admin, data):
    text = msg.html_text.strip()
    if not text:
        await msg.answer("Send text for the notice.")
        return
    err = validate_telegram_html(text)
    if err:
        await msg.answer(f"⚠️ {err}\n\nSend the content again.")
        return

    target = data["target"]
    count = await _audience_count(target)
    await state.update_data(text=text, target=target)

    await msg.answer(
        f"📌 <b>Ready to send & pin</b>\n\n"
        f"Audience: <code>{target}</code> (~{count} users)\n"
        f"Content: {_html_preview(text, 150)}\n\n"
        f"This cannot be undone.",
        reply_markup=kb_rows(
            [abtn("📌 Send & Pin now", "pinned", "confirm")],
            [abtn("✖️ Cancel", "cancel", "open", "pinned")]))


async def _launch_pinned(cq, state, admin, data) -> None:
    target = data.get("target", "all")
    text = data.get("text", "")

    aid = _admin_db_id(admin)
    row = await db.fetchrow(
        """INSERT INTO pinned_broadcasts(message, target, created_by, status)
           VALUES($1,$2,$3,'running') RETURNING *""",
        text[:4000], target, aid)

    await admin_audit(admin, "pinned_warning_start", "pinned_broadcast", row["id"],
                      {"target": target})
    asyncio.create_task(run_pinned_warning_broadcast(row["id"], text, target))
    await state.clear()
    await admin_render(cq,
        "📌 <b>Pinning started.</b>\n\n"
        "It sends and pins in the background at a safe pace. "
        "Come back to 📌 Pin Notice to see progress or unpin later.",
        kb_rows(admin_back_row("home")))
    await _ack(cq)


_ADMIN_OPENERS["pinned"] = lambda e, a, *_: open_pinned(e, a)

# =============================================================================
#  ADMIN — UI EDITOR  (§2)
# =============================================================================
#  Every user-visible string, image and link is editable here. Text labels are
#  stored as ui:<lang>:<key> overrides consumed by t(); branding and links are
#  plain settings keys. Nothing the customer sees is hardcoded.

UI_BRANDING_FIELDS = [
    ("store_name", "Store name", "text"),
    ("welcome_text", "Welcome message", "text"),
    ("payment_warning_text", "Payment warning (auto-pinned on /start)", "text"),
    ("store_header", "Header (above title)", "text"),
    ("store_footer", "Footer (below menu)", "text"),
    ("store_logo_file_id", "Store logo", "photo"),
    ("shop_banner_file_id", "Shop banner", "photo"),
    ("support_username", "Support @username", "text"),
    ("channel_url", "Channel link", "text"),
    ("terms_url", "Terms link", "text"),
    ("api_link_url", "API link URL", "text"),
    ("rank_bronze_name", "Rank name — bronze", "text"),
    ("rank_silver_name", "Rank name — silver", "text"),
    ("rank_gold_name", "Rank name — gold", "text"),
]

# Per-section photos: each key here is a `photo_<key>` setting. edit_or_send
# looks these up by `section` and falls back to the global Store logo when a
# section has no photo of its own set -- see edit_or_send's docstring for
# why every screen wants *some* photo (keeps every screen the same message
# type so navigation between them is always an in-place edit, never a
# delete+resend).
SECTION_PHOTO_FIELDS = [
    ("quantity", "Quantity screen"),
    ("wallet", "Wallet screen"),
    ("payment", "Payment / checkout screens"),
    ("coupon", "Coupon entry screen"),
    ("orders", "Orders screen"),
    ("support", "Support screen"),
    ("referral", "Refer & Earn screen"),
    ("language", "Language / currency screens"),
]


async def open_ui(event, admin) -> None:
    text = (
        "🎨 <b>UI Editor</b>\n\n"
        "Change any text, image or link the customer sees.\n"
        "Button labels are per-language — short labels render as smaller buttons."
    )
    markup = kb_rows(
        [abtn("🏷 Branding & text", "ui", "brand"),
         abtn("🔤 Button labels", "ui", "labels")],
        [abtn("↕️ Menu order", "ui", "order"),
         abtn("👁 Show / hide buttons", "ui", "vis")],
        [abtn("🖼 Section photos", "ui", "photos"),
         abtn("♻️ Reset a label", "ui", "reset")],
        admin_back_row("home"),
    )
    await admin_render(event, text, markup)


async def open_ui_section_photos(event, admin) -> None:
    cur = await settings.load(force=True)
    lines = [
        "🖼 <b>Section photos</b>\n",
        "Give any section its own photo. Unset sections fall back to the "
        "Store logo (Branding & text); if neither is set, that section "
        "stays plain text.\n",
    ]
    rows = []
    for key, label in SECTION_PHOTO_FIELDS:
        shown = "✅ set" if cur.get(f"photo_{key}", "") else "— using store logo"
        lines.append(f"• {label}: <code>{shown}</code>")
        rows.append([abtn(f"🖼 {label}", "ui", "pedit", key)])
    await admin_render(event, "\n".join(lines), kb_rows(*rows, admin_back_row("ui")))


async def open_ui_brand(event, admin) -> None:
    cur = await settings.load(force=True)
    rows = []
    lines = ["🏷 <b>Branding & text</b>\n"]
    for key, label, kind in UI_BRANDING_FIELDS:
        val = cur.get(key, "")
        if kind == "photo":
            shown = "✅ set" if val else "—"
        else:
            shown = _html_preview(val, 28)
        lines.append(f"• {label}: <code>{shown}</code>")
        rows.append([abtn(f"✏️ {label}", "ui", "bedit", key)])
    await admin_render(event, "\n".join(lines), kb_rows(*rows, admin_back_row("ui")))


UI_LABEL_LANGS = [
    ("en", "🇬🇧 English"), ("es", "🇪🇸 Español"), ("fr", "🇫🇷 Français"),
    ("de", "🇩🇪 Deutsch"), ("pt", "🇵🇹 Português"), ("ru", "🇷🇺 Русский"),
    ("tr", "🇹🇷 Türkçe"), ("ar", "🇸🇦 العربية"), ("ja", "🇯🇵 日本語"),
    ("ko", "🇰🇷 한국어"), ("zh", "🇨🇳 中文"),
]


async def open_ui_labels(event, admin, group: str = "", lang: str = "en") -> None:
    if not group:
        rows = [[abtn(f"📁 {g}", "ui", "lgroup", f"{lang}~{g[:12]}")]
                for g in UI_EDITABLE_KEYS]
        lang_btns = [abtn(f"• {label[2:]}" if lang == code else label, "ui", "labels", code)
                     for code, label in UI_LABEL_LANGS]
        rows.extend([lang_btns[i:i + 2] for i in range(0, len(lang_btns), 2)])
        await admin_render(event,
            f"🔤 <b>Button labels — {lang}</b>\n\nPick a group to edit.",
            kb_rows(*rows, admin_back_row("ui")))
        return

    keys = next((v for k, v in UI_EDITABLE_KEYS.items() if k[:12] == group), [])
    rows = []
    lines = [f"🔤 <b>{group} — {lang}</b>\n"]
    for k in keys:
        current = t(lang, k)
        overridden = settings.ui_override(lang, k) is not None
        mark = "✏️" if overridden else "  "
        lines.append(f"{mark} <code>{k}</code>: {current}")
        rows.append([abtn(f"{k} → {current[:14]}", "ui", "ledit", f"{lang}~{k}")])
    await admin_render(event, "\n".join(lines),
                       kb_rows(*rows, admin_back_row("ui", "labels", lang)))


@router.callback_query(AdminCB.filter(F.section == "ui"))
async def cb_ui(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    act, arg = cb.action, cb.arg

    if act == "open":
        await open_ui(cq, admin)
    elif act == "brand":
        await open_ui_brand(cq, admin)
    elif act == "bedit":
        meta = next((m for m in UI_BRANDING_FIELDS if m[0] == arg), None)
        if not meta:
            await cq.answer("Unknown field.", show_alert=True)
            return
        _, label, kind = meta
        if kind == "photo":
            await begin_capture(cq, state, "ui_photo",
                f"Send a photo for “{label}”.", back="ui", key=arg, accept="photo")
        else:
            await begin_capture(cq, state, "ui_brand",
                f"✏️ <b>{label}</b>\n\nSend the new value, or a dash to clear.\n"
                f"You can use premium emoji here — they render in message text.",
                back="ui", key=arg)
        return
    elif act == "photos":
        await open_ui_section_photos(cq, admin)
    elif act == "pedit":
        meta = next((m for m in SECTION_PHOTO_FIELDS if m[0] == arg), None)
        if not meta:
            await cq.answer("Unknown section.", show_alert=True)
            return
        _, label = meta
        await begin_capture(cq, state, "ui_photo",
            f"Send a photo for “{label}”.", back="ui", key=f"photo_{arg}", accept="photo")
        return
    elif act == "labels":
        await open_ui_labels(cq, admin, "", arg or "en")
    elif act == "lgroup":
        lang, group = arg.split("~", 1)
        await open_ui_labels(cq, admin, group, lang)
    elif act == "ledit":
        lang, key = arg.split("~", 1)
        await begin_capture(cq, state, "ui_label",
            f"✏️ <b>{key}</b> ({lang})\n\nCurrent: <code>{t(lang, key)}</code>\n\n"
            f"Send the new label. Keep it short for compact buttons.",
            back="ui", key=key, lang=lang)
        return
    elif act == "order":
        current = ",".join(await menu_layout())
        await begin_capture(cq, state, "ui_order",
            f"↕️ <b>Menu order</b>\n\nCurrent:\n<code>{current}</code>\n\n"
            f"Send a comma-separated list. Omit any key to hide that button.\n"
            f"Available: <code>{DEFAULT_MENU_ORDER}</code>", back="ui")
        return
    elif act == "vis":
        keys = await menu_layout()
        rows = []
        for k in MENU_BUTTON_ROUTES:
            on = k in keys
            rows.append([abtn(f"{'✅' if on else '🚫'} {k}", "ui", "tvis", k)])
        await admin_render(cq,
            "👁 <b>Show / hide menu buttons</b>\n\nTap to toggle.",
            kb_rows(*rows, admin_back_row("ui")))
    elif act == "tvis":
        keys = await menu_layout()
        if arg in keys:
            keys = [k for k in keys if k != arg]
        else:
            keys.append(arg)
        await settings.set("menu_order", ",".join(keys))
        await admin_audit(admin, "ui_visibility", "settings", None, {"key": arg})
        # Re-render the toggle screen
        keys2 = await menu_layout()
        rows = [[abtn(f"{'✅' if k in keys2 else '🚫'} {k}", "ui", "tvis", k)]
                for k in MENU_BUTTON_ROUTES]
        await admin_render(cq, "👁 <b>Show / hide menu buttons</b>\n\nTap to toggle.",
                           kb_rows(*rows, admin_back_row("ui")))
    elif act == "reset":
        overrides = [k for k in (await settings.load(force=True)) if k.startswith("ui:")]
        if not overrides:
            await cq.answer("No custom labels to reset.", show_alert=True)
            return
        rows = [[abtn(f"♻️ {o[3:]}", "ui", "resetok", o[3:])] for o in overrides[:20]]
        await admin_render(cq,
            f"♻️ <b>Reset a label</b>\n\n{len(overrides)} custom label(s). "
            f"Resetting restores the built-in text.",
            kb_rows(*rows, admin_back_row("ui")))
    elif act == "resetok":
        await db.execute("DELETE FROM settings WHERE key=$1", f"ui:{arg}")
        await settings.load(force=True)
        await admin_audit(admin, "ui_reset", "settings", None, {"key": arg})
        await cq.answer("Reset to default")
        await open_ui(cq, admin)
    await _ack(cq)


@capture_applier("ui_brand")
async def _ui_brand(msg, state, admin, data):
    # Same fix as product descriptions: keep the admin's actual formatting
    # (bold/italic/underline/spoiler/code/blockquote) as HTML instead of
    # flattening it to plain text.
    val = "" if msg.text.strip() == "-" else msg.html_text.strip()[:3000]
    err = validate_telegram_html(val)
    if err:
        await msg.answer(f"⚠️ {err}\n\nSend the value again.")
        return
    await settings.set(data["key"], val)
    await admin_audit(admin, "ui_brand_edit", "settings", None, {"key": data["key"]})
    await state.clear()
    await msg.answer("✅ Saved.")
    await open_ui_brand(msg, admin)


@capture_applier("ui_photo")
async def _ui_photo(msg, state, admin, data):
    key = data["key"]
    await settings.set(key, msg.photo[-1].file_id)
    await admin_audit(admin, "ui_photo_edit", "settings", None, {"key": key})
    await state.clear()
    await msg.answer("✅ Image saved.")
    if key.startswith("photo_"):
        await open_ui_section_photos(msg, admin)
    else:
        await open_ui_brand(msg, admin)


@capture_applier("ui_label")
async def _ui_label(msg, state, admin, data):
    lang, key = data["lang"], data["key"]
    await settings.set(f"ui:{lang}:{key}", msg.text.strip()[:80])
    await admin_audit(admin, "ui_label_edit", "settings", None, {"key": key, "lang": lang})
    await state.clear()
    await msg.answer(f"✅ Label updated: {t(lang, key)}")
    await open_ui_labels(msg, admin, "", lang)


@capture_applier("ui_order")
async def _ui_order(msg, state, admin, data):
    keys = [k.strip() for k in msg.text.split(",") if k.strip() in MENU_BUTTON_ROUTES]
    if not keys:
        await msg.answer(f"No valid keys found. Use: {DEFAULT_MENU_ORDER}")
        return
    await settings.set("menu_order", ",".join(keys))
    await admin_audit(admin, "ui_order", "settings", None, {"order": keys})
    await state.clear()
    await msg.answer(f"✅ Menu order set: {', '.join(keys)}")
    await open_ui(msg, admin)


_ADMIN_OPENERS["ui"] = lambda e, a, *_: open_ui(e, a)


# =============================================================================
#  ADMIN — EMOJI MAPPINGS  (Global Emoji Replacement Manager)
# =============================================================================
#  One flat dictionary: unicode emoji -> premium emoji id (table: emoji_map).
#  No slots, no per-button assignment — mapping an emoji here makes every
#  occurrence of it, anywhere in the bot (messages, captions, buttons),
#  render as its Telegram Premium custom emoji automatically, via
#  EmojiRenderMiddleware. See also the matching "Emoji Mappings" page in the
#  web admin panel (admin.html) — both write to the same emoji_map table.


async def open_emojis(event, admin) -> None:
    rows = await db.fetch("SELECT * FROM emoji_map ORDER BY updated_at DESC LIMIT 30")

    lines = [
        "🙂 <b>Emoji Mappings</b>\n",
        f"Mapped: <b>{len(rows)}</b>\n",
        "ℹ️ Every unicode emoji mapped here renders as its Telegram Premium "
        "custom emoji everywhere — messages, captions, and buttons — "
        "automatically. Unmapped emoji are shown as plain unicode. This "
        "needs the bot itself on a Premium/Business account (or a Fragment "
        "username); otherwise Telegram drops the custom emoji and the plain "
        "glyph is shown instead.\n",
    ]
    btn_rows = [[abtn(f"{r['unicode_emoji']}  →  premium", "emojis", "view", r["unicode_emoji"])]
                for r in rows]
    await admin_render(event, "\n".join(lines), kb_rows(
        *btn_rows,
        [abtn("➕ Add mapping", "emojis", "add")],
        admin_back_row("home")))


async def view_mapping(event, admin, emoji: str) -> None:
    r = await db.fetchrow("SELECT * FROM emoji_map WHERE unicode_emoji=$1", emoji)
    if not r:
        await admin_render(event, "Mapping not found.", kb_rows(admin_back_row("emojis")))
        return
    preview = f'<tg-emoji emoji-id="{r["premium_emoji_id"]}">{r["unicode_emoji"]}</tg-emoji>'
    text = (
        f"🙂 <b>{r['unicode_emoji']}</b>\n\n"
        f"Preview: {preview}\n"
        f"Premium ID: <code>{r['premium_emoji_id']}</code>\n\n"
        f"<i>If the preview shows the plain glyph, the bot isn't on a Premium "
        f"account or the ID is invalid.</i>"
    )
    markup = kb_rows(
        [abtn("✏️ Edit ID", "emojis", "eid", emoji)],
        [abtn("🗑 Delete", "emojis", "del", emoji)],
        admin_back_row("emojis"),
    )
    await admin_render(event, text, markup)


@router.callback_query(AdminCB.filter(F.section == "emojis"))
async def cb_emojis(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    act, arg = cb.action, cb.arg

    if act == "open":
        await open_emojis(cq, admin)
    elif act == "view":
        await view_mapping(cq, admin, arg)
    elif act == "add":
        await begin_capture(cq, state, "emoji_map_add",
            "🙂 <b>Add emoji mapping</b>\n\n"
            "Send the premium emoji directly — the plain unicode emoji it "
            "stands for and its ID are both captured in one step.\n\n"
            "Or send just the plain unicode emoji you want to map (e.g. ❤️), "
            "then send its premium emoji ID next.",
            back="emojis", accept="any")
        return
    elif act == "eid":
        await begin_capture(cq, state, "emoji_map_set_id",
            f"Send the new premium emoji ID for {arg}, or send a premium emoji.",
            back="emojis", emoji=arg, accept="any")
        return
    elif act == "del":
        await db.execute("DELETE FROM emoji_map WHERE unicode_emoji=$1", arg)
        invalidate_emoji_map()
        await admin_audit(admin, "emoji_map_delete", "emoji_map", None, {"emoji": arg})
        await cq.answer("Deleted")
        await open_emojis(cq, admin)
    await _ack(cq)


def _extract_custom_emoji(msg: Message) -> tuple[str, str] | None:
    """Pull (custom_emoji_id, unicode_glyph) from a message's entities.

    Telegram entity offset/length are UTF-16 code units, not Python code
    points. Almost every custom/premium emoji is outside the BMP (one code
    point but a UTF-16 surrogate pair, i.e. length 2), so indexing msg.text
    directly with the raw offset/length only happens to work when the emoji
    is alone at the very start of the message; anywhere else (extra text
    before it, or more than one emoji in the message) it slices the wrong
    characters and silently stores a corrupted glyph as the map key, which
    then never matches the real emoji anywhere it's actually used. Round-trip
    through UTF-16 to slice by code unit correctly."""
    for ent in (msg.entities or []):
        if ent.type == "custom_emoji" and ent.custom_emoji_id:
            text = msg.text or ""
            utf16 = text.encode("utf-16-le")
            start, end = ent.offset * 2, (ent.offset + ent.length) * 2
            glyph = utf16[start:end].decode("utf-16-le", "ignore")
            if glyph:
                return ent.custom_emoji_id, glyph
    return None


async def _upsert_emoji_map(emoji: str, custom_id: str, admin) -> None:
    await db.execute(
        """INSERT INTO emoji_map(unicode_emoji, premium_emoji_id) VALUES($1,$2)
           ON CONFLICT (unicode_emoji) DO UPDATE
             SET premium_emoji_id=$2, updated_at=now()""",
        emoji, custom_id)
    invalidate_emoji_map()
    await admin_audit(admin, "emoji_map_upsert", "emoji_map", None, {"emoji": emoji})


@capture_applier("emoji_map_add")
async def _emoji_map_add(msg, state, admin, data):
    found = _extract_custom_emoji(msg)
    if found:
        custom_id, glyph = found
        await _upsert_emoji_map(glyph, custom_id, admin)
        await state.clear()
        await msg.answer("✅ Mapping added.")
        await view_mapping(msg, admin, glyph)
        return

    raw = (msg.text or "").strip()
    if not raw:
        await msg.answer("Send a premium emoji, or a plain unicode emoji to map.")
        return
    await begin_capture(msg, state, "emoji_map_set_id",
        f"Now send the premium emoji ID for {raw}, or send a premium emoji.",
        back="emojis", emoji=raw, accept="any")


@capture_applier("emoji_map_set_id")
async def _emoji_map_set_id(msg, state, admin, data):
    found = _extract_custom_emoji(msg)
    custom_id = found[0] if found else (msg.text or "").strip()
    if not custom_id.isdigit():
        await msg.answer("Send a premium emoji or a numeric ID.")
        return
    emoji = data["emoji"]
    await _upsert_emoji_map(emoji, custom_id, admin)
    await state.clear()
    await msg.answer("✅ Mapping saved.")
    await view_mapping(msg, admin, emoji)


_ADMIN_OPENERS["emojis"] = lambda e, a, *_: open_emojis(e, a)


# =============================================================================
#  PRICING — BULK DISCOUNT
# =============================================================================
#  Shared by the web admin's Mass Discount panel and the in-bot Discount Sale
#  broadcast flow, so the two surfaces can never compute prices differently.

def _discounted_price(base: Decimal, discount_type: str, value: Decimal) -> Decimal:
    """Pure price math, no I/O -- shared by apply_bulk_discount_tx (which
    writes it) and the in-bot Discount Sale flow's preview screen (which
    needs the same numbers before anything is written)."""
    if discount_type == "percent":
        new_price = money(base * (Decimal(100) - value) / 100, Q_CRYPTO)
    else:
        new_price = money(base - value, Q_CRYPTO)
    return new_price if new_price > ZERO else ZERO


async def apply_bulk_discount_tx(
    conn: asyncpg.Connection, scope: str, target_ids: list[int],
    discount_type: str, value: Decimal, action: str = "apply",
) -> list[dict]:
    """Apply (or remove) a percent/fixed discount across a set of products or
    an entire category, inside an existing transaction. `original_price` is
    always the pristine pre-discount price: re-applying a discount recomputes
    from it rather than compounding on an already-discounted price, and
    `remove` restores it exactly. Returns the list of affected products as
    {id, name, old_price, new_price} for callers that want to show a
    before/after (the API endpoint just counts len(), the in-bot flow uses
    the detail for its confirm screen and broadcast text)."""
    if scope == "category":
        rows = await conn.fetch(
            """SELECT id, name, price, original_price FROM products
                WHERE category_id = ANY($1::int[]) FOR UPDATE""",
            target_ids)
    else:
        rows = await conn.fetch(
            """SELECT id, name, price, original_price FROM products
                WHERE id = ANY($1::int[]) FOR UPDATE""",
            target_ids)
    if not rows:
        return []

    updated = []
    if action == "remove":
        for r in rows:
            if r["original_price"] is None:
                continue
            new_price = money(r["original_price"], Q_CRYPTO)
            await conn.execute(
                "UPDATE products SET price=$2, original_price=NULL, is_deal=FALSE WHERE id=$1",
                r["id"], new_price,
            )
            updated.append({"id": r["id"], "name": r["name"],
                            "old_price": r["price"], "new_price": new_price})
    else:
        value = money(value, Q_CRYPTO)
        for r in rows:
            base = money(r["original_price"], Q_CRYPTO) if r["original_price"] is not None \
                else money(r["price"], Q_CRYPTO)
            new_price = _discounted_price(base, discount_type, value)
            await conn.execute(
                "UPDATE products SET price=$2, original_price=$3, is_deal=TRUE WHERE id=$1",
                r["id"], new_price, base,
            )
            updated.append({"id": r["id"], "name": r["name"],
                            "old_price": base, "new_price": new_price})
    return updated


# =============================================================================
#  ADMIN — DEALS
# =============================================================================

async def open_deals(event, admin) -> None:
    deals = await db.fetch(
        "SELECT * FROM products WHERE is_deal ORDER BY id DESC LIMIT 15")
    lines = ["🎁 <b>Deals</b>\n"]
    rows = []
    if deals:
        for p in deals:
            was = (f" (was {fmt_money(money(p['original_price']),'INR')})"
                   if p["original_price"] else "")
            lines.append(f"🔥 {p['name'][:24]} — {fmt_money(money(p['price']),'INR')}{was}")
            rows.append([abtn(f"⚙️ {p['name'][:20]}", "deals", "view", str(p["id"]))])
    else:
        lines.append("No active deals.")
    rows.append([abtn("➕ Mark a product as deal", "deals", "pick")])
    await admin_render(event, "\n".join(lines), kb_rows(*rows, admin_back_row("home")))


@router.callback_query(AdminCB.filter(F.section == "deals"))
async def cb_deals_admin(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    act, arg = cb.action, cb.arg

    if act == "open":
        await open_deals(cq, admin)
    elif act == "pick":
        prods = await db.fetch(
            "SELECT id, name FROM products WHERE active AND NOT is_deal ORDER BY id DESC LIMIT 15")
        if not prods:
            await cq.answer("No eligible products.", show_alert=True)
            return
        rows = [[abtn(p["name"][:26], "deals", "mark", str(p["id"]))] for p in prods]
        await admin_render(cq, "Pick a product to feature as a deal:",
                           kb_rows(*rows, admin_back_row("deals")))
    elif act == "mark":
        await db.execute("UPDATE products SET is_deal=TRUE WHERE id=$1", int(arg))
        await admin_audit(admin, "deal_add", "product", int(arg))
        await cq.answer("Marked as deal")
        await view_deal(cq, admin, int(arg))
    elif act == "view":
        await view_deal(cq, admin, int(arg))
    elif act == "unmark":
        await db.execute("UPDATE products SET is_deal=FALSE WHERE id=$1", int(arg))
        await cq.answer("Removed from deals")
        await open_deals(cq, admin)
    elif act == "orig":
        await begin_capture(cq, state, "deal_orig",
            "Send the original (pre-discount) price in INR, shown struck through.",
            back="deals", pid=arg)
        return
    await _ack(cq)


async def view_deal(event, admin, pid: int) -> None:
    p = await db.fetchrow("SELECT * FROM products WHERE id=$1", pid)
    if not p:
        await admin_render(event, "Product not found.", kb_rows(admin_back_row("deals")))
        return
    text = (
        f"🔥 <b>{p['name']}</b>\n\n"
        f"Price now: <b>{fmt_money(money(p['price']),'INR')}</b>\n"
        f"Original: {fmt_money(money(p['original_price']),'INR') if p['original_price'] else '— not set'}\n\n"
        f"The original price shows struck through in the shop."
    )
    await admin_render(event, text, kb_rows(
        [abtn("💵 Set original price", "deals", "orig", str(pid))],
        [abtn("💰 Change price", "products", "eprice", str(pid))],
        [abtn("❌ Remove from deals", "deals", "unmark", str(pid))],
        admin_back_row("deals")))


@capture_applier("deal_orig")
async def _deal_orig(msg, state, admin, data):
    try:
        val = money(Decimal(msg.text.strip().replace("₹", "").replace(",", "")), Q_CRYPTO)
        if val <= 0:
            raise ValueError
    except (InvalidOperation, ValueError):
        await msg.answer("Send a positive number.")
        return
    await db.execute("UPDATE products SET original_price=$1 WHERE id=$2",
                     val, int(data["pid"]))
    await state.clear()
    await msg.answer(f"✅ Original price set to {fmt_money(val,'INR')}.")
    await view_deal(msg, admin, int(data["pid"]))


_ADMIN_OPENERS["deals"] = lambda e, a, *_: open_deals(e, a)


# =============================================================================
#  ADMIN — COUPONS
# =============================================================================

async def open_coupons(event, admin) -> None:
    coupons = await db.fetch("SELECT * FROM coupons ORDER BY id DESC LIMIT 15")
    lines = ["🎟 <b>Coupons</b>\n"]
    rows = []
    for c in coupons:
        disc = (f"{money(c['value']).normalize()}%" if c["discount_type"] == "percent"
                else fmt_money(money(c["value"]), "INR"))
        state_ic = "✅" if c["active"] else "🚫"
        used = f"{c['used_count']}" + (f"/{c['max_uses']}" if c["max_uses"] else "")
        lines.append(f"{state_ic} <code>{c['code']}</code> — {disc} · used {used}")
        rows.append([abtn(f"{c['code'][:14]} ({disc})", "coupons", "view", str(c["id"]))])
    if not coupons:
        lines.append("None yet.")
    rows.append([abtn("➕ New coupon", "coupons", "add")])
    await admin_render(event, "\n".join(lines), kb_rows(*rows, admin_back_row("home")))


async def view_coupon(event, admin, cid: int) -> None:
    c = await db.fetchrow("SELECT * FROM coupons WHERE id=$1", cid)
    if not c:
        await admin_render(event, "Coupon not found.", kb_rows(admin_back_row("coupons")))
        return
    disc = (f"{money(c['value']).normalize()}%" if c["discount_type"] == "percent"
            else fmt_money(money(c["value"]), "INR"))
    text = (
        f"🎟 <b>{c['code']}</b>\n\n"
        f"Discount: {disc}\n"
        f"Used: {c['used_count']}" + (f" / {c['max_uses']}" if c["max_uses"] else " (unlimited)") + "\n"
        f"Min order: {fmt_money(money(c['min_order_amount']),'INR')}\n"
        f"Per-user limit: {c['per_user_limit'] or '—'}\n"
        f"Status: {'✅ active' if c['active'] else '🚫 disabled'}"
    )
    await admin_render(event, text, kb_rows(
        [abtn("💵 Value", "coupons", "eval", str(cid)),
         abtn("🔢 Max uses", "coupons", "emax", str(cid))],
        [abtn("📉 Min order", "coupons", "emin", str(cid)),
         abtn("👤 Per-user limit", "coupons", "eper", str(cid))],
        [abtn("🚫 Disable" if c["active"] else "✅ Enable", "coupons", "toggle", str(cid))],
        [abtn("🗑 Delete", "coupons", "del", str(cid))],
        admin_back_row("coupons")))


@router.callback_query(AdminCB.filter(F.section == "coupons"))
async def cb_coupons(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    act, arg = cb.action, cb.arg

    if act == "open":
        await open_coupons(cq, admin)
    elif act == "view":
        await view_coupon(cq, admin, int(arg))
    elif act == "add":
        await begin_capture(cq, state, "coupon_add",
            "🎟 Send the coupon code (letters/numbers, e.g. <code>WELCOME10</code>).",
            back="coupons")
        return
    elif act in ("eval", "emax", "emin", "eper"):
        prompts = {
            "eval": ("coupon_val", "Send the discount value (a number)."),
            "emax": ("coupon_max", "Send max total uses, or a dash for unlimited."),
            "emin": ("coupon_min", "Send the minimum order amount in INR."),
            "eper": ("coupon_per", "Send the per-user limit, or a dash for none."),
        }
        flow, prompt = prompts[act]
        await begin_capture(cq, state, flow, prompt, back="coupons", cid=arg)
        return
    elif act == "toggle":
        await db.execute("UPDATE coupons SET active = NOT active WHERE id=$1", int(arg))
        await view_coupon(cq, admin, int(arg))
    elif act == "del":
        await db.execute("DELETE FROM coupons WHERE id=$1", int(arg))
        await admin_audit(admin, "coupon_delete", "coupon", int(arg))
        await cq.answer("Deleted")
        await open_coupons(cq, admin)
    await _ack(cq)


@capture_applier("coupon_add")
async def _coupon_add(msg, state, admin, data):
    code = msg.text.strip().upper()[:40]
    if not code.replace("_", "").replace("-", "").isalnum():
        await msg.answer("Use letters, numbers, dashes or underscores only.")
        return
    try:
        row = await db.fetchrow(
            """INSERT INTO coupons(code, discount_type, value) VALUES($1,'percent',10)
               RETURNING id""", code)
    except asyncpg.UniqueViolationError:
        await msg.answer("That code already exists.")
        return
    await admin_audit(admin, "coupon_create", "coupon", row["id"], {"code": code})
    await state.clear()
    await msg.answer(f"✅ Coupon {code} created at 10%. Adjust it below.")
    await view_coupon(msg, admin, row["id"])


@capture_applier("coupon_val")
async def _coupon_val(msg, state, admin, data):
    try:
        val = money(Decimal(msg.text.strip()), Q_CRYPTO)
        if val < 0:
            raise ValueError
    except (InvalidOperation, ValueError):
        await msg.answer("Send a non-negative number.")
        return
    await db.execute("UPDATE coupons SET value=$1 WHERE id=$2", val, int(data["cid"]))
    await state.clear()
    await msg.answer("✅ Value updated.")
    await view_coupon(msg, admin, int(data["cid"]))


@capture_applier("coupon_max")
async def _coupon_max(msg, state, admin, data):
    raw = msg.text.strip()
    val = None if raw == "-" else (int(raw) if raw.isdigit() else None)
    if raw != "-" and val is None:
        await msg.answer("Send a whole number or a dash.")
        return
    await db.execute("UPDATE coupons SET max_uses=$1 WHERE id=$2", val, int(data["cid"]))
    await state.clear()
    await msg.answer("✅ Max uses updated.")
    await view_coupon(msg, admin, int(data["cid"]))


@capture_applier("coupon_min")
async def _coupon_min(msg, state, admin, data):
    try:
        val = money(Decimal(msg.text.strip().replace("₹", "")), Q_CRYPTO)
    except (InvalidOperation, ValueError):
        await msg.answer("Send a number.")
        return
    await db.execute("UPDATE coupons SET min_order_amount=$1 WHERE id=$2",
                     val, int(data["cid"]))
    await state.clear()
    await msg.answer("✅ Minimum updated.")
    await view_coupon(msg, admin, int(data["cid"]))


@capture_applier("coupon_per")
async def _coupon_per(msg, state, admin, data):
    raw = msg.text.strip()
    val = None if raw == "-" else (int(raw) if raw.isdigit() else None)
    if raw != "-" and val is None:
        await msg.answer("Send a whole number or a dash.")
        return
    await db.execute("UPDATE coupons SET per_user_limit=$1 WHERE id=$2",
                     val, int(data["cid"]))
    await state.clear()
    await msg.answer("✅ Per-user limit updated.")
    await view_coupon(msg, admin, int(data["cid"]))


_ADMIN_OPENERS["coupons"] = lambda e, a, *_: open_coupons(e, a)


# =============================================================================
#  ADMIN — LANGUAGES & CURRENCY
# =============================================================================

async def open_langs(event, admin) -> None:
    enabled = (await settings.get("languages_enabled", "en")).split(",")
    default = await settings.get("default_language", "en")
    lines = ["🌍 <b>Languages</b>\n"]
    rows = []
    for code, name in UI_LABEL_LANGS:
        name = name[2:].strip()  # drop the flag prefix for this list
        on = code in enabled
        star = " ⭐default" if code == default else ""
        lines.append(f"{'✅' if on else '🚫'} {name}{star}")
        rows.append([abtn(f"{'🚫 Disable' if on else '✅ Enable'} {name}",
                          "langs", "toggle", code),
                     abtn("⭐ Default", "langs", "default", code)])
    lines.append("\nEdit the actual wording under 🎨 UI Editor → Button labels.")
    await admin_render(event, "\n".join(lines), kb_rows(
        *rows, [abtn("🔤 Edit translations", "ui", "labels")], admin_back_row("home")))


@router.callback_query(AdminCB.filter(F.section == "langs"))
async def cb_langs(cq: CallbackQuery) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    if cb.action == "open":
        await open_langs(cq, admin)
    elif cb.action == "toggle":
        enabled = [x for x in (await settings.get("languages_enabled", "en,hi")).split(",") if x]
        if cb.arg in enabled:
            if len(enabled) == 1:
                await cq.answer("At least one language must stay enabled.", show_alert=True)
                return
            enabled.remove(cb.arg)
        else:
            enabled.append(cb.arg)
        await settings.set("languages_enabled", ",".join(enabled))
        await open_langs(cq, admin)
    elif cb.action == "default":
        await settings.set("default_language", cb.arg)
        await cq.answer(f"Default set to {cb.arg}")
        await open_langs(cq, admin)
    await _ack(cq)


async def open_currency(event, admin) -> None:
    enabled = (await settings.get("currencies_enabled", "INR,USDT")).split(",")
    default = await settings.get("default_currency", "USDT")
    rate = await usdt_rate()
    lines = [
        "💱 <b>Currency</b>\n",
        f"Rate: <b>1 USDT = ₹{rate.normalize()}</b>\n",
    ]
    rows = []
    for code, name in (("INR", "₹ Indian Rupee"), ("USDT", "₮ Tether")):
        on = code in enabled
        star = " ⭐default" if code == default else ""
        lines.append(f"{'✅' if on else '🚫'} {name}{star}")
        rows.append([abtn(f"{'🚫' if on else '✅'} {code}", "currency", "toggle", code),
                     abtn("⭐ Default", "currency", "default", code)])
    await admin_render(event, "\n".join(lines), kb_rows(
        *rows, [abtn("💱 Change rate", "prices", "rate")], admin_back_row("home")))


@router.callback_query(AdminCB.filter(F.section == "currency"))
async def cb_currency(cq: CallbackQuery) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    if cb.action == "open":
        await open_currency(cq, admin)
    elif cb.action == "toggle":
        enabled = [x for x in (await settings.get("currencies_enabled", "INR,USDT")).split(",") if x]
        if cb.arg in enabled:
            if len(enabled) == 1:
                await cq.answer("At least one currency must stay enabled.", show_alert=True)
                return
            enabled.remove(cb.arg)
        else:
            enabled.append(cb.arg)
        await settings.set("currencies_enabled", ",".join(enabled))
        await open_currency(cq, admin)
    elif cb.action == "default":
        await settings.set("default_currency", cb.arg)
        await cq.answer(f"Default set to {cb.arg}")
        await open_currency(cq, admin)
    await _ack(cq)


_ADMIN_OPENERS["langs"] = lambda e, a, *_: open_langs(e, a)
_ADMIN_OPENERS["currency"] = lambda e, a, *_: open_currency(e, a)


# =============================================================================
#  ADMIN — BOT SETTINGS
# =============================================================================

async def open_settings_admin(event, admin) -> None:
    maint = await settings.get_bool("maintenance_mode", False)
    autov = await settings.get_bool("auto_verify_enabled", True)
    admins = int(await db.fetchval("SELECT COUNT(*) FROM admins WHERE active") or 0)

    text = (
        "⚙️ <b>Bot Settings</b>\n\n"
        f"🔧 Maintenance mode: <b>{'ON — shop closed' if maint else 'off'}</b>\n"
        f"🔗 Auto chain verification: <b>{'on' if autov else 'OFF'}</b>\n"
        f"⚙️ Active admins: {admins}\n"
        f"👤 Env allowlist: {len(CFG.admin_ids)} id(s)"
    )
    markup = kb_rows(
        [abtn(f"{'✅ Reopen shop' if maint else '🔧 Maintenance mode'}",
              "settings", "maint")],
        [abtn(f"{'🚫 Disable' if autov else '✅ Enable'} auto-verify",
              "settings", "autov")],
        [abtn("🎁 Tiers", "settings", "tiers")],
        [abtn("🔒 Force Join Channel", "settings", "fj")],
        [abtn("⚙️ Admin list", "settings", "admins")],
        [abtn("🧮 Wallet audit", "settings", "audit")],
        admin_back_row("home"),
    )
    await admin_render(event, text, markup)


async def open_force_join_admin(event, admin) -> None:
    fj_on = await settings.get_bool("force_join_enabled", False)
    url = await settings.get("force_join_channel_url", "")
    cid = await settings.get("force_join_channel_id", "")

    text = (
        "🔒 <b>Force Join Channel</b>\n\n"
        "Users must join this channel before they can open the menu or use "
        "any bot feature. They see Join Channel / Verify buttons until they do.\n\n"
        f"Status: <b>{'ON' if fj_on else 'OFF'}</b>\n"
        f"🔗 Channel link: {url or '— not set'}\n"
        f"🆔 Channel ID: <code>{cid or '— not set'}</code>\n\n"
        "The channel ID is what the bot checks membership against — e.g. "
        "<code>@yourchannel</code> or <code>-1001234567890</code>. The bot must "
        "be an admin of that channel to verify members."
    )
    markup = kb_rows(
        [abtn(f"{'🚫 Disable' if fj_on else '✅ Enable'}", "settings", "fjtoggle")],
        [abtn("🔗 Set channel link", "settings", "fjurl"),
         abtn("🆔 Set channel ID", "settings", "fjid")],
        admin_back_row("settings"),
    )
    await admin_render(event, text, markup)


@router.callback_query(AdminCB.filter(F.section == "settings"))
async def cb_settings_admin(cq: CallbackQuery, state: FSMContext) -> None:
    admin = await admin_guard(cq)
    if not admin:
        return
    cb = AdminCB.unpack(cq.data)
    act, arg = cb.action, cb.arg

    if act == "open":
        await open_settings_admin(cq, admin)
    elif act == "maint":
        cur = await settings.get_bool("maintenance_mode", False)
        await settings.set("maintenance_mode", "0" if cur else "1")
        await admin_audit(admin, "maintenance_toggle", "settings", None,
                          {"on": not cur})
        await open_settings_admin(cq, admin)
    elif act == "autov":
        cur = await settings.get_bool("auto_verify_enabled", True)
        await settings.set("auto_verify_enabled", "0" if cur else "1")
        await open_settings_admin(cq, admin)
    elif act == "fj":
        await open_force_join_admin(cq, admin)
    elif act == "fjtoggle":
        cur = await settings.get_bool("force_join_enabled", False)
        await settings.set("force_join_enabled", "0" if cur else "1")
        await admin_audit(admin, "force_join_toggle", "settings", None,
                          {"on": not cur})
        await open_force_join_admin(cq, admin)
    elif act == "fjurl":
        await begin_capture(cq, state, "set_fj_url",
            "🔗 Send the channel invite link (e.g. https://t.me/yourchannel), "
            "or a dash to clear it.", back="settings")
        return
    elif act == "fjid":
        await begin_capture(cq, state, "set_fj_id",
            "🆔 Send the channel ID the bot should check membership against "
            "— e.g. <code>@yourchannel</code> or <code>-1001234567890</code>, "
            "or a dash to clear it.\n\nThe bot must be an admin of that channel.",
            back="settings")
        return
    elif act == "tiers":
        silver = await settings.get("tier_silver_at", "5000")
        gold = await settings.get("tier_gold_at", "25000")
        sd = await settings.get("tier_silver_discount", "2")
        gd = await settings.get("tier_gold_discount", "5")
        await admin_render(cq,
            f"🎁 <b>Loyalty tiers</b>\n\n"
            f"🥈 Silver at ₹{silver} spend — {sd}% off\n"
            f"🥇 Gold at ₹{gold} spend — {gd}% off\n\n"
            f"Rank names are editable in 🎨 UI Editor.",
            kb_rows(
                [abtn("🥈 Silver threshold", "settings", "ts"),
                 abtn("🥈 Silver %", "settings", "tsd")],
                [abtn("🥇 Gold threshold", "settings", "tg"),
                 abtn("🥇 Gold %", "settings", "tgd")],
                admin_back_row("settings")))
    elif act in ("ts", "tsd", "tg", "tgd"):
        keymap = {"ts": ("tier_silver_at", "silver spend threshold (INR)"),
                  "tsd": ("tier_silver_discount", "silver discount percent"),
                  "tg": ("tier_gold_at", "gold spend threshold (INR)"),
                  "tgd": ("tier_gold_discount", "gold discount percent")}
        key, label = keymap[act]
        await begin_capture(cq, state, "set_number",
            f"Send the new {label}.", back="settings", key=key)
        return
    elif act == "admins":
        rows = await db.fetch(
            "SELECT username, role, active FROM admins ORDER BY id")
        lines = ["⚙️ <b>Admins</b>\n"]
        for r in rows:
            lines.append(f"{'✅' if r['active'] else '🚫'} <code>{r['username']}</code> — {r['role']}")
        if CFG.admin_ids:
            lines.append("\n<b>Env allowlist</b> (always owner)")
            for i in CFG.admin_ids:
                lines.append(f"👑 <code>{i}</code>")
        lines.append("\nPromote users from 👥 Users → pick a user → Make admin.")
        await admin_render(cq, "\n".join(lines), kb_rows(admin_back_row("settings")))
    elif act == "audit":
        drift = await wallet_audit()
        if not drift:
            body = "✅ Every balance matches the ledger."
        else:
            body = "⚠️ <b>Discrepancies</b>\n\n" + "\n".join(
                f"user {d['userId']} ({d['telegramId']}): "
                f"stored {d['stored']}, ledger {d['derived']}"
                for d in drift[:15])
        await admin_render(cq, f"🧮 <b>Wallet audit</b>\n\n{body}",
                           kb_rows(admin_back_row("settings")))
    await _ack(cq)


@capture_applier("set_fj_url")
async def _set_fj_url(msg, state, admin, data):
    raw = msg.text.strip()
    val = "" if raw == "-" else raw
    if val and not (val.startswith("https://") or val.startswith("http://")):
        await msg.answer("Send a valid URL (starting with https://), or a dash to clear it.")
        return
    await settings.set("force_join_channel_url", val)
    await admin_audit(admin, "setting_edit", "settings", None, {"key": "force_join_channel_url"})
    await state.clear()
    await msg.answer("✅ Saved.")
    await open_force_join_admin(msg, admin)


@capture_applier("set_fj_id")
async def _set_fj_id(msg, state, admin, data):
    raw = msg.text.strip()
    val = "" if raw == "-" else raw
    if val and not (val.startswith("@") or val.lstrip("-").isdigit()):
        await msg.answer(
            "Send a channel @username or numeric ID (e.g. -1001234567890), "
            "or a dash to clear it.")
        return
    await settings.set("force_join_channel_id", val)
    await admin_audit(admin, "setting_edit", "settings", None, {"key": "force_join_channel_id"})
    await state.clear()
    await msg.answer("✅ Saved.")
    await open_force_join_admin(msg, admin)


@capture_applier("set_number")
async def _set_number(msg, state, admin, data):
    try:
        val = money(Decimal(msg.text.strip().replace("₹", "").replace(",", "")), Q_CRYPTO)
        if val < 0:
            raise ValueError
    except (InvalidOperation, ValueError):
        await msg.answer("Send a non-negative number.")
        return
    await settings.set(data["key"], str(val.normalize()))
    await admin_audit(admin, "setting_edit", "settings", None, {"key": data["key"]})
    await state.clear()
    await msg.answer("✅ Saved.")
    await open_settings_admin(msg, admin)


_ADMIN_OPENERS["settings"] = lambda e, a, *_: open_settings_admin(e, a)

@router.message(Command("id"))
async def cmd_id(msg: Message) -> None:
    await msg.answer(f"Your Telegram ID: <code>{msg.from_user.id}</code>")


@router.message(F.text)
async def msg_fallback(msg: Message, state: FSMContext) -> None:
    if await state.get_state():
        return
    user = await guard(msg)
    if not user:
        return
    await show_menu(msg, user)

# =============================================================================
#  API — AUTH
# =============================================================================
#  JWT bearer, bcrypt password hashes, role hierarchy, login throttling.
#  Endpoint paths mirror the original OpenAPI spec so the existing React
#  admin panel works against this server without changes.

ROLE_RANK = {"support": 1, "manager": 2, "owner": 3}


def make_token(admin: asyncpg.Record) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(admin["id"]),
        "username": admin["username"],
        "role": admin["role"],
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(hours=CFG.jwt_ttl_hours)).timestamp()),
    }
    return jwt.encode(payload, CFG.session_secret, algorithm="HS256")


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, CFG.session_secret, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "Session expired")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Invalid token")


async def current_admin(request: Request) -> asyncpg.Record:
    token = ""
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        token = auth[7:].strip()
    if not token:
        # <img> tags cannot set headers, so screenshots accept ?token=
        token = request.query_params.get("token", "")
    if not token:
        raise HTTPException(401, "Not authenticated")

    payload = decode_token(token)
    admin = await db.fetchrow(
        "SELECT * FROM admins WHERE id=$1 AND active", int(payload["sub"]))
    if not admin:
        raise HTTPException(401, "Account disabled")
    return admin


def require_role(minimum: str):
    async def dep(admin: asyncpg.Record = Depends(current_admin)) -> asyncpg.Record:
        if ROLE_RANK.get(admin["role"], 0) < ROLE_RANK.get(minimum, 99):
            raise HTTPException(403, f"Requires {minimum} role or higher")
        return admin
    return dep


_login_attempts: dict[str, list[float]] = {}


def login_throttled(ip: str) -> bool:
    now = time.monotonic()
    window = _login_attempts.setdefault(ip, [])
    window[:] = [t0 for t0 in window if now - t0 < 900]
    return len(window) >= CFG.login_attempts_per_15min


def record_login_attempt(ip: str) -> None:
    _login_attempts.setdefault(ip, []).append(time.monotonic())


async def audit(
    admin: asyncpg.Record | None, action: str,
    entity: str = "", entity_id: int | None = None,
    detail: dict | None = None, ip: str = "",
) -> None:
    await db.execute(
        """INSERT INTO audit_logs(admin_id, admin_name, action, entity, entity_id, detail, ip)
           VALUES($1,$2,$3,$4,$5,$6,$7)""",
        admin["id"] if admin else None,
        admin["username"] if admin else "system",
        action, entity or None, entity_id, detail or {}, ip or None,
    )


def client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for", "")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else ""


# =============================================================================
#  API — SERIALISATION
# =============================================================================
#  Money leaves the API as a string, never a JSON float. JavaScript's Number
#  cannot hold 18-digit decimals exactly, so stringifying is what keeps the
#  panel's totals matching the database.

def ser(row: asyncpg.Record | None, money_fields: Sequence[str] = ()) -> dict | None:
    if row is None:
        return None
    out: dict[str, Any] = {}
    for k, v in dict(row).items():
        key = "".join(
            w if i == 0 else w.capitalize() for i, w in enumerate(k.split("_"))
        )
        if isinstance(v, datetime):
            out[key] = v.isoformat()
        elif isinstance(v, Decimal):
            out[key] = str(crypto(v).normalize())
        else:
            out[key] = v
    return out


def ser_all(rows: Sequence[asyncpg.Record]) -> list[dict]:
    return [ser(r) for r in rows]


# ---- request models ----------------------------------------------------------

# Every admin-authored field that ends up rendered as Telegram HTML (welcome
# text, product/category name+description) is stored and shown raw/unescaped
# on purpose -- that's what lets an admin write real <b>/<i>/spoiler/etc.
# formatting and have it come out exactly as typed (see show_menu, cb_product
# etc.). The failure mode that leaves is silent: a stray '<' or an unclosed
# tag makes Telegram reject the sendMessage/editMessageText call outright,
# and edit_or_send's fallback just retries the same broken text forever with
# nothing shown to the admin or the end user. Catch that here, at the one
# place it can enter the system, with a specific error instead of a 500 or a
# silently broken live screen.
_TG_ALLOWED_HTML_TAGS = {
    "b", "strong", "i", "em", "u", "ins", "s", "strike", "del",
    "span", "tg-spoiler", "a", "code", "pre", "blockquote", "tg-emoji",
}
_TG_TAG_RE = re.compile(r"<(/?)([a-zA-Z][a-zA-Z0-9-]*)(?:\s+[^<>]*)?>")
_TG_BAD_AMP_RE = re.compile(r"&(?!amp;|lt;|gt;|quot;|#39;|#\d+;|#x[0-9a-fA-F]+;)")


def validate_telegram_html(text: str) -> str | None:
    """None if `text` is safe to send with parse_mode=HTML; otherwise a
    human-readable description of the first problem found. Not a full HTML
    parser -- just enough to catch what would otherwise break a live screen:
    stray '<'/'&', unknown tags, and unclosed/mismatched tags."""
    if not text:
        return None
    stack: list[str] = []
    pos = 0
    for m in _TG_TAG_RE.finditer(text):
        chunk = text[pos:m.start()]
        if "<" in chunk:
            return "Stray '<' outside a tag — escape it as &lt; if it's meant literally"
        if _TG_BAD_AMP_RE.search(chunk):
            return "Unescaped '&' — use &amp; if it's meant literally"
        pos = m.end()

        closing, tag = m.group(1), m.group(2).lower()
        if tag not in _TG_ALLOWED_HTML_TAGS:
            return f"<{tag}> isn't a formatting tag Telegram supports here"
        if closing:
            if not stack or stack[-1] != tag:
                return f"</{tag}> has no matching opening tag"
            stack.pop()
        else:
            stack.append(tag)

    tail = text[pos:]
    if "<" in tail:
        return "Stray '<' outside a tag — escape it as &lt; if it's meant literally"
    if _TG_BAD_AMP_RE.search(tail):
        return "Unescaped '&' — use &amp; if it's meant literally"
    if stack:
        return f"<{stack[-1]}> was never closed"
    return None


class LoginIn(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1, max_length=256)


class CategoryIn(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    nameHi: str = ""
    emoji: str = "📦"
    sortOrder: int = 0
    active: bool = True

    @field_validator("name", "nameHi")
    @classmethod
    def valid_html(cls, v):
        err = validate_telegram_html(v)
        if err:
            raise ValueError(err)
        return v


class ProductIn(BaseModel):
    categoryId: int
    name: str = Field(min_length=1, max_length=200)
    nameHi: str = ""
    description: str = ""
    descriptionHi: str = ""
    price: Decimal
    originalPrice: Decimal | None = None
    deliveryType: str = "auto"
    imageUrl: str | None = None
    active: bool = True
    isDeal: bool = False
    deliverAsFile: bool = False
    manualStock: int | None = None
    maxPerUser: int | None = None

    @field_validator("price", "originalPrice")
    @classmethod
    def non_negative(cls, v):
        if v is not None and v < 0:
            raise ValueError("price cannot be negative")
        return v

    @field_validator("deliveryType")
    @classmethod
    def valid_delivery(cls, v):
        if v not in ("auto", "manual"):
            raise ValueError("deliveryType must be 'auto' or 'manual'")
        return v

    @field_validator("name", "nameHi", "description", "descriptionHi")
    @classmethod
    def valid_html(cls, v):
        err = validate_telegram_html(v)
        if err:
            raise ValueError(err)
        return v


class KeysIn(BaseModel):
    keys: list[str] = Field(min_length=1, max_length=10000)


class CouponIn(BaseModel):
    code: str = Field(min_length=2, max_length=40)
    discountType: str = "percent"
    value: Decimal
    maxUses: int | None = None
    minOrderAmount: Decimal = ZERO
    perUserLimit: int | None = None
    expiresAt: datetime | None = None
    active: bool = True

    @field_validator("discountType")
    @classmethod
    def valid_type(cls, v):
        if v not in ("percent", "fixed"):
            raise ValueError("discountType must be 'percent' or 'fixed'")
        return v


class UserUpdateIn(BaseModel):
    isBanned: bool | None = None
    banReason: str | None = None
    balanceAdjust: Decimal | None = None
    adjustNote: str = "Admin adjustment"


class OrderUpdateIn(BaseModel):
    status: str | None = None
    deliveredContent: str | None = None


class PaymentUpdateIn(BaseModel):
    status: str
    note: str = ""


class BroadcastIn(BaseModel):
    message: str = Field(min_length=1, max_length=4000)
    target: str = "all"

    @field_validator("message")
    @classmethod
    def valid_html(cls, v):
        err = validate_telegram_html(v)
        if err:
            raise ValueError(err)
        return v


class ProductBroadcastIn(BaseModel):
    productId: int
    message: str = Field(min_length=1, max_length=1024)  # photo/video caption limit
    mediaType: str = "photo"  # "photo" | "video" | "none"
    mediaFileId: str | None = None  # optional; "photo" with this blank falls back to the product's own image
    target: str = "all"

    @field_validator("message")
    @classmethod
    def valid_html(cls, v):
        err = validate_telegram_html(v)
        if err:
            raise ValueError(err)
        return v

    @field_validator("mediaType")
    @classmethod
    def valid_media_type(cls, v):
        if v not in ("photo", "video", "none"):
            raise ValueError("mediaType must be 'photo', 'video' or 'none'")
        return v


class EmojiMappingIn(BaseModel):
    unicodeEmoji: str = Field(min_length=1, max_length=16)
    premiumEmojiId: str = Field(min_length=1, max_length=64)


class SettingsIn(BaseModel):
    values: dict[str, str]


class AdminIn(BaseModel):
    username: str = Field(min_length=3, max_length=64)
    password: str = Field(min_length=8, max_length=256)
    role: str = "support"

    @field_validator("role")
    @classmethod
    def valid_role(cls, v):
        if v not in ROLE_RANK:
            raise ValueError(f"role must be one of {list(ROLE_RANK)}")
        return v


# =============================================================================
#  API — APP
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    await settings.seed()
    await seed_admin()

    if CFG.webhook_url:
        url = CFG.webhook_url.rstrip("/") + "/api/telegram/webhook"
        await bot.set_webhook(
            url, secret_token=CFG.webhook_secret, drop_pending_updates=True)
        log.info("webhook registered: %s", url)
        poller = None
    else:
        await bot.delete_webhook(drop_pending_updates=True)
        poller = asyncio.create_task(dp.start_polling(bot, handle_signals=False))
        log.info("long-polling started")

    worker = asyncio.create_task(verification_worker(bot))
    prune_worker = asyncio.create_task(prune_bot_messages_worker())
    sweep_worker = asyncio.create_task(memory_cache_sweep_worker())

    # Persistent bottom-left Menu button — lets users reach the bot menu
    # without typing /start, alongside the existing command.
    await bot.set_my_commands([
        BotCommand(command="start", description="Open the main menu"),
        BotCommand(command="menu", description="Open the main menu"),
        BotCommand(command="shop", description="Browse products"),
        BotCommand(command="wallet", description="Check your wallet balance"),
        BotCommand(command="profile", description="View your profile"),
        BotCommand(command="orders", description="View your order history"),
        BotCommand(command="support", description="Contact support"),
    ])
    await bot.set_chat_menu_button(menu_button=MenuButtonCommands())

    me = await bot.get_me()
    log.info("bot online as @%s", me.username)
    log.info("API listening on %s:%s", CFG.host, CFG.port)

    try:
        yield
    finally:
        worker.cancel()
        prune_worker.cancel()
        sweep_worker.cancel()
        if poller:
            poller.cancel()
        await verifier.close()
        await translator.close()
        await bot.session.close()
        await db.close()
        log.info("shutdown complete")


app = FastAPI(title="Digital Hub Bot API", version="2.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(CFG.cors_origins) or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# admin.html itself is ~80KB, and every list endpoint the dashboard polls
# (orders/payments/users/...) returns JSON that compresses very well --
# gzip cuts both substantially with zero behaviour change. Threshold skips
# compressing tiny responses where the gzip header overhead isn't worth it.
app.add_middleware(GZipMiddleware, minimum_size=1000)


async def seed_admin() -> None:
    existing = await db.fetchrow("SELECT * FROM admins WHERE username='admin'")
    hashed = bcrypt.hashpw(CFG.admin_password.encode(), bcrypt.gensalt()).decode()
    if existing:
        await db.execute(
            "UPDATE admins SET password_hash=$1, role='owner', active=TRUE WHERE id=$2",
            hashed, existing["id"])
        log.info("admin account refreshed")
    else:
        await db.execute(
            """INSERT INTO admins(username, password_hash, role)
               VALUES('admin', $1, 'owner')""",
            hashed)
        log.info("admin account created (username: admin)")


@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception):
    log.exception("unhandled error on %s", request.url.path)
    return JSONResponse({"message": "Internal server error"}, status_code=500)


# ---- health & telegram webhook ----------------------------------------------

@app.get("/api/healthz")
async def healthz():
    try:
        await db.fetchval("SELECT 1")
        return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}
    except Exception:
        raise HTTPException(503, "database unavailable")


@app.post("/api/telegram/webhook")
async def telegram_webhook(request: Request):
    if CFG.webhook_secret:
        got = request.headers.get("x-telegram-bot-api-secret-token", "")
        if not hmac.compare_digest(got, CFG.webhook_secret):
            raise HTTPException(403, "bad secret")
    data = await request.json()
    await dp.feed_update(bot, Update.model_validate(data, context={"bot": bot}))
    return {"ok": True}


@app.post("/api/cryptomus/webhook")
async def cryptomus_webhook(request: Request):
    """Cryptomus payment notification. Signature is verified first (403 on
    mismatch, same style as the Telegram webhook above); once verified, the
    webhook body itself is only ever used to look up *which* payment to
    re-check -- get_info(uuid) is the actual source of truth for status,
    per the same "never trust the claim, always confirm independently"
    philosophy ChainVerifier uses for on-chain payments."""
    payload = await request.json()
    if not await cryptomus.verify_webhook_signature(payload):
        log.warning("cryptomus webhook: bad signature (uuid=%s)", payload.get("uuid"))
        raise HTTPException(403, "bad signature")

    uuid = payload.get("uuid")
    log.info("cryptomus webhook received: uuid=%s status=%s order_id=%s",
              uuid, payload.get("status"), payload.get("order_id"))

    p = await db.fetchrow("SELECT * FROM payments WHERE cryptomus_uuid=$1", uuid)
    if not p:
        # Not money either way from our side (no matching payment row) --
        # avoid a retry storm for something that will never resolve.
        log.warning("cryptomus webhook: no payment found for uuid=%s", uuid)
        return {"ok": True}

    # Re-verify against the live API rather than trusting the webhook body.
    # Left to propagate uncaught on failure (network error, bad response) so
    # FastAPI's default handler returns non-200 and Cryptomus retries --
    # a transient blip here must never silently drop a real payment.
    info = await cryptomus.get_info(uuid)
    if not info.ok:
        log.error("cryptomus webhook: get_info failed for uuid=%s: %s", uuid, info.reason)
        raise HTTPException(502, "could not verify payment status")

    # _process_cryptomus_status already absorbs FulfilError internally (a
    # payment no longer 'pending' when we tried to approve/reject it) --
    # duplicate webhook deliveries for an already-approved/rejected payment
    # are a quiet no-op there; a genuinely anomalous residual status (most
    # notably 'expired' beating this webhook to the row on the 'paid'
    # branch) is logged and pushed to admins from inside that function, so
    # nothing further is needed here.
    await _process_cryptomus_status(bot, p, info)
    return {"ok": True}


@app.post("/api/oxapay/webhook")
async def oxapay_webhook(request: Request):
    """OxaPay payment notification. Signature is verified first (403 on
    mismatch, same style as the Cryptomus webhook above) over the raw
    request body -- HMAC-SHA512 keyed with the merchant API key, per
    OxaPay's docs. Once verified, the webhook body itself is only ever
    used to look up *which* payment to re-check -- get_status(track_id) is
    the actual source of truth, per the same "never trust the claim,
    always confirm independently" philosophy Cryptomus/ChainVerifier use."""
    raw = await request.body()
    if not await oxapay.verify_webhook_signature(raw, request.headers.get("hmac", "")):
        log.warning("oxapay webhook: bad signature")
        raise HTTPException(403, "bad signature")

    payload = json.loads(raw)
    track_id = str(payload.get("track_id") or "")
    log.info("oxapay webhook received: track_id=%s status=%s order_id=%s",
              track_id, payload.get("status"), payload.get("order_id"))

    p = await db.fetchrow("SELECT * FROM payments WHERE oxapay_track_id=$1", track_id)
    if not p:
        # Not money either way from our side (no matching payment row) --
        # avoid a retry storm for something that will never resolve.
        log.warning("oxapay webhook: no payment found for track_id=%s", track_id)
        return {"ok": True}

    # Re-verify against the live API rather than trusting the webhook body.
    # Left to propagate uncaught on failure (network error, bad response) so
    # FastAPI's default handler returns non-200 and OxaPay retries -- a
    # transient blip here must never silently drop a real payment.
    info = await oxapay.get_status(track_id)
    if not info.ok:
        log.error("oxapay webhook: get_status failed for track_id=%s: %s", track_id, info.reason)
        raise HTTPException(502, "could not verify payment status")

    # _process_oxapay_status already absorbs FulfilError internally (a
    # payment no longer 'pending' when we tried to approve/reject it) --
    # duplicate webhook deliveries for an already-approved/rejected payment
    # are a quiet no-op there.
    await _process_oxapay_status(bot, p, info)
    return {"ok": True}


# ---- auth --------------------------------------------------------------------

@app.post("/api/admin/login")
async def admin_login(body: LoginIn, request: Request):
    ip = client_ip(request)
    if login_throttled(ip):
        raise HTTPException(429, "Too many attempts. Try again in 15 minutes.")

    admin = await db.fetchrow(
        "SELECT * FROM admins WHERE username=$1 AND active", body.username)

    # Constant-ish work either way so timing doesn't reveal valid usernames.
    stored = admin["password_hash"] if admin else bcrypt.hashpw(b"x", bcrypt.gensalt()).decode()
    ok = bcrypt.checkpw(body.password.encode(), stored.encode())

    if not admin or not ok:
        record_login_attempt(ip)
        await audit(None, "login_failed", "admin", None, {"username": body.username}, ip)
        raise HTTPException(401, "Invalid credentials")

    await db.execute("UPDATE admins SET last_login_at=NOW() WHERE id=$1", admin["id"])
    await audit(admin, "login", "admin", admin["id"], {}, ip)
    return {
        "token": make_token(admin),
        "admin": {"id": admin["id"], "username": admin["username"], "role": admin["role"]},
    }


@app.get("/api/admin/me")
async def admin_me(admin=Depends(current_admin)):
    return {"id": admin["id"], "username": admin["username"], "role": admin["role"]}


# ---- dashboard ---------------------------------------------------------------

@app.get("/api/admin/stats")
async def admin_stats(admin=Depends(current_admin)):
    row = await db.fetchrow(
        """SELECT
             (SELECT COUNT(*) FROM store_users) AS total_users,
             (SELECT COUNT(*) FROM store_users WHERE created_at > NOW() - INTERVAL '24 hours') AS new_users,
             (SELECT COUNT(*) FROM orders) AS total_orders,
             (SELECT COUNT(*) FROM orders WHERE status='pending') AS pending_orders,
             (SELECT COALESCE(SUM(total),0) FROM orders WHERE status IN ('delivered','processing')) AS revenue,
             (SELECT COALESCE(SUM(total),0) FROM orders
               WHERE status IN ('delivered','processing')
                 AND created_at > NOW() - INTERVAL '24 hours') AS revenue_24h,
             (SELECT COUNT(*) FROM payments WHERE status='pending') AS pending_payments,
             (SELECT COUNT(*) FROM products WHERE active) AS active_products,
             (SELECT COALESCE(SUM(wallet_balance),0) FROM store_users) AS wallet_liability"""
    )
    revenue_series = await db.fetch(
        """SELECT DATE(created_at) AS day, COALESCE(SUM(total),0) AS amount
             FROM orders
            WHERE status IN ('delivered','processing')
              AND created_at > NOW() - INTERVAL '30 days'
            GROUP BY day ORDER BY day"""
    )
    top = await db.fetch(
        """SELECT p.id, p.name, p.sold_count,
                  COALESCE(SUM(o.total),0) AS revenue
             FROM products p LEFT JOIN orders o
               ON o.product_id=p.id AND o.status IN ('delivered','processing')
            GROUP BY p.id ORDER BY p.sold_count DESC LIMIT 10"""
    )
    low_stock = await db.fetch(
        """SELECT p.id, p.name,
                  (SELECT COUNT(*) FROM product_keys k
                    WHERE k.product_id=p.id AND NOT k.is_used) AS stock
             FROM products p
            WHERE p.active AND p.delivery_type='auto'
            ORDER BY stock ASC LIMIT 10"""
    )
    return {
        "totalUsers": row["total_users"],
        "newUsers24h": row["new_users"],
        "totalOrders": row["total_orders"],
        "pendingOrders": row["pending_orders"],
        "revenue": str(crypto(row["revenue"])),
        "revenue24h": str(crypto(row["revenue_24h"])),
        "pendingPayments": row["pending_payments"],
        "activeProducts": row["active_products"],
        "walletLiability": str(crypto(row["wallet_liability"])),
        "revenueSeries": [
            {"date": r["day"].isoformat(), "amount": str(crypto(r["amount"]))}
            for r in revenue_series
        ],
        "topProducts": [
            {"id": r["id"], "name": r["name"], "sold": r["sold_count"],
             "revenue": str(crypto(r["revenue"]))}
            for r in top
        ],
        "lowStock": [
            {"id": r["id"], "name": r["name"], "stock": r["stock"]} for r in low_stock
        ],
    }


# ---- categories --------------------------------------------------------------

@app.get("/api/admin/categories")
async def list_categories(admin=Depends(current_admin)):
    rows = await db.fetch(
        """SELECT c.*, (SELECT COUNT(*) FROM products p WHERE p.category_id=c.id) AS product_count
             FROM categories c ORDER BY c.sort_order, c.id"""
    )
    return ser_all(rows)


@app.post("/api/admin/categories", status_code=201)
async def create_category(body: CategoryIn, request: Request,
                          admin=Depends(require_role("manager"))):
    row = await db.fetchrow(
        """INSERT INTO categories(name, name_hi, emoji, sort_order, active)
           VALUES($1,$2,$3,$4,$5) RETURNING *""",
        body.name, body.nameHi, body.emoji, body.sortOrder, body.active,
    )
    await audit(admin, "category_create", "category", row["id"],
                {"name": body.name}, client_ip(request))
    return ser(row)


@app.patch("/api/admin/categories/{cid}")
async def update_category(cid: int, body: CategoryIn, request: Request,
                          admin=Depends(require_role("manager"))):
    row = await db.fetchrow(
        """UPDATE categories SET name=$2, name_hi=$3, emoji=$4, sort_order=$5, active=$6
            WHERE id=$1 RETURNING *""",
        cid, body.name, body.nameHi, body.emoji, body.sortOrder, body.active,
    )
    if not row:
        raise HTTPException(404, "Category not found")
    await audit(admin, "category_update", "category", cid, {}, client_ip(request))
    return ser(row)


@app.delete("/api/admin/categories/{cid}", status_code=204)
async def delete_category(cid: int, request: Request,
                          admin=Depends(require_role("manager"))):
    n = await db.fetchval("SELECT COUNT(*) FROM products WHERE category_id=$1", cid)
    if n:
        raise HTTPException(409, f"Category still has {n} product(s)")
    await db.execute("DELETE FROM categories WHERE id=$1", cid)
    await audit(admin, "category_delete", "category", cid, {}, client_ip(request))
    return Response(status_code=204)


# ---- products ----------------------------------------------------------------

@app.get("/api/admin/products")
async def list_products(
    admin=Depends(current_admin),
    page: int = Query(1, ge=1), limit: int = Query(50, ge=1, le=200),
    search: str = "", categoryId: int | None = None, active: bool | None = None,
):
    where, args = ["1=1"], []
    if search:
        args.append(f"%{search}%")
        where.append(f"p.name ILIKE ${len(args)}")
    if categoryId:
        args.append(categoryId)
        where.append(f"p.category_id = ${len(args)}")
    if active is not None:
        args.append(active)
        where.append(f"p.active = ${len(args)}")
    clause = " AND ".join(where)

    total = await db.fetchval(f"SELECT COUNT(*) FROM products p WHERE {clause}", *args)
    args.extend([limit, (page - 1) * limit])
    rows = await db.fetch(
        f"""SELECT p.*, c.name AS category_name,
                   (SELECT COUNT(*) FROM product_keys k
                     WHERE k.product_id=p.id AND NOT k.is_used) AS stock_count
              FROM products p JOIN categories c ON c.id=p.category_id
             WHERE {clause}
             ORDER BY p.id DESC LIMIT ${len(args)-1} OFFSET ${len(args)}""",
        *args,
    )
    return {"items": ser_all(rows), "total": total, "page": page, "limit": limit}


@app.get("/api/admin/products/{pid}")
async def get_product(pid: int, admin=Depends(current_admin)):
    row = await db.fetchrow(
        """SELECT p.*, (SELECT COUNT(*) FROM product_keys k
                         WHERE k.product_id=p.id AND NOT k.is_used) AS stock_count
             FROM products p WHERE p.id=$1""", pid)
    if not row:
        raise HTTPException(404, "Product not found")
    return ser(row)


@app.post("/api/admin/products", status_code=201)
async def create_product(body: ProductIn, request: Request,
                         admin=Depends(require_role("manager"))):
    row = await db.fetchrow(
        """INSERT INTO products
             (category_id, name, name_hi, description, description_hi, price,
              original_price, delivery_type, image_url, active, is_deal,
              deliver_as_file, manual_stock, max_per_user)
           VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
           RETURNING *""",
        body.categoryId, body.name, body.nameHi, body.description, body.descriptionHi,
        money(body.price, Q_CRYPTO),
        money(body.originalPrice, Q_CRYPTO) if body.originalPrice else None,
        body.deliveryType, body.imageUrl, body.active, body.isDeal,
        body.deliverAsFile, body.manualStock, body.maxPerUser,
    )
    await audit(admin, "product_create", "product", row["id"],
                {"name": body.name, "price": str(body.price)}, client_ip(request))
    return ser(row)


@app.patch("/api/admin/products/{pid}")
async def update_product(pid: int, body: ProductIn, request: Request,
                         admin=Depends(require_role("manager"))):
    before = await db.fetchrow("SELECT * FROM products WHERE id=$1", pid)
    if not before:
        raise HTTPException(404, "Product not found")

    row = await db.fetchrow(
        """UPDATE products SET
             category_id=$2, name=$3, name_hi=$4, description=$5, description_hi=$6,
             price=$7, original_price=$8, delivery_type=$9, image_url=$10,
             active=$11, is_deal=$12, deliver_as_file=$13, manual_stock=$14, max_per_user=$15
            WHERE id=$1 RETURNING *""",
        pid, body.categoryId, body.name, body.nameHi, body.description,
        body.descriptionHi, money(body.price, Q_CRYPTO),
        money(body.originalPrice, Q_CRYPTO) if body.originalPrice else None,
        body.deliveryType, body.imageUrl, body.active, body.isDeal,
        body.deliverAsFile, body.manualStock, body.maxPerUser,
    )
    await audit(admin, "product_update", "product", pid,
                {"priceBefore": str(before["price"]), "priceAfter": str(body.price)},
                client_ip(request))

    # Manual restock should wake up anyone waiting.
    if body.manualStock and (before["manual_stock"] or 0) == 0:
        asyncio.create_task(notify_restock(bot, pid))
    return ser(row)


@app.delete("/api/admin/products/{pid}", status_code=204)
async def delete_product(pid: int, request: Request,
                         admin=Depends(require_role("owner"))):
    await db.execute("DELETE FROM products WHERE id=$1", pid)
    await audit(admin, "product_delete", "product", pid, {}, client_ip(request))
    return Response(status_code=204)


class BulkDiscountIn(BaseModel):
    scope: str                    # "products" | "category"
    targetIds: list[int] = Field(min_length=1)
    discountType: str = "percent"  # "percent" | "fixed"
    value: Decimal = Decimal("0")
    action: str = "apply"          # "apply" | "remove"

    @field_validator("scope")
    @classmethod
    def valid_scope(cls, v):
        if v not in ("products", "category"):
            raise ValueError("scope must be 'products' or 'category'")
        return v

    @field_validator("discountType")
    @classmethod
    def valid_discount_type(cls, v):
        if v not in ("percent", "fixed"):
            raise ValueError("discountType must be 'percent' or 'fixed'")
        return v

    @field_validator("action")
    @classmethod
    def valid_action(cls, v):
        if v not in ("apply", "remove"):
            raise ValueError("action must be 'apply' or 'remove'")
        return v

    @field_validator("value")
    @classmethod
    def non_negative_value(cls, v):
        if v < 0:
            raise ValueError("value cannot be negative")
        return v


@app.post("/api/admin/products/bulk-discount")
async def bulk_discount(body: BulkDiscountIn, request: Request,
                        admin=Depends(require_role("manager"))):
    """Apply (or remove) a percent/fixed discount across a set of products or
    an entire category in one shot. `original_price` is always the pristine
    pre-discount price: re-applying a discount recomputes from it rather than
    compounding on an already-discounted price, and `remove` restores it
    exactly rather than just zeroing the discount out."""
    async with db.tx() as conn:
        updated = await apply_bulk_discount_tx(
            conn, body.scope, body.targetIds, body.discountType, body.value, body.action)
    if not updated:
        raise HTTPException(404, "No matching products")

    await audit(admin, "product_bulk_discount", "product", None,
                {"scope": body.scope, "targetIds": body.targetIds, "action": body.action,
                 "discountType": body.discountType, "value": str(body.value), "updated": len(updated)},
                client_ip(request))
    return {"updated": len(updated)}


# ---- keys --------------------------------------------------------------------

@app.get("/api/admin/products/{pid}/keys")
async def list_keys(pid: int, admin=Depends(current_admin),
                    used: bool | None = None, limit: int = Query(200, le=1000)):
    if used is None:
        rows = await db.fetch(
            "SELECT * FROM product_keys WHERE product_id=$1 ORDER BY id DESC LIMIT $2",
            pid, limit)
    else:
        rows = await db.fetch(
            """SELECT * FROM product_keys WHERE product_id=$1 AND is_used=$2
                ORDER BY id DESC LIMIT $3""", pid, used, limit)
    return ser_all(rows)


@app.post("/api/admin/products/{pid}/keys", status_code=201)
async def add_keys(pid: int, body: KeysIn, request: Request,
                   admin=Depends(require_role("manager"))):
    product = await db.fetchrow("SELECT * FROM products WHERE id=$1", pid)
    if not product:
        raise HTTPException(404, "Product not found")

    cleaned = [k.strip() for k in body.keys if k.strip()]
    if not cleaned:
        raise HTTPException(400, "No usable keys supplied")

    before = int(await db.fetchval(
        "SELECT COUNT(*) FROM product_keys WHERE product_id=$1 AND NOT is_used", pid) or 0)

    async with db.tx() as conn:
        rows = await conn.fetch(
            """INSERT INTO product_keys(product_id, key_value)
                 SELECT $1, k FROM UNNEST($2::text[]) AS k
               ON CONFLICT DO NOTHING
               RETURNING key_value""",
            pid, cleaned)
    inserted = len(rows)

    await audit(admin, "keys_import", "product", pid,
                {"submitted": len(cleaned), "inserted": inserted}, client_ip(request))

    if before == 0 and inserted > 0:
        asyncio.create_task(notify_restock(bot, pid))

    return {"submitted": len(cleaned), "inserted": inserted,
            "duplicates": len(cleaned) - inserted}


@app.delete("/api/admin/keys/{kid}", status_code=204)
async def delete_key(kid: int, request: Request, admin=Depends(require_role("manager"))):
    row = await db.fetchrow("SELECT * FROM product_keys WHERE id=$1", kid)
    if row and row["is_used"]:
        raise HTTPException(409, "Cannot delete a key that has been delivered")
    await db.execute("DELETE FROM product_keys WHERE id=$1", kid)
    await audit(admin, "key_delete", "key", kid, {}, client_ip(request))
    return Response(status_code=204)

# ---- users -------------------------------------------------------------------

@app.get("/api/admin/users")
async def list_users(admin=Depends(current_admin),
                     page: int = Query(1, ge=1), limit: int = Query(50, ge=1, le=200),
                     search: str = "", banned: bool | None = None):
    where, args = ["1=1"], []
    if search:
        args.append(f"%{search}%")
        where.append(
            f"(u.username ILIKE ${len(args)} OR u.first_name ILIKE ${len(args)} "
            f"OR u.telegram_id ILIKE ${len(args)})")
    if banned is not None:
        args.append(banned)
        where.append(f"u.is_banned = ${len(args)}")
    clause = " AND ".join(where)

    total = await db.fetchval(f"SELECT COUNT(*) FROM store_users u WHERE {clause}", *args)
    args.extend([limit, (page - 1) * limit])
    rows = await db.fetch(
        f"""SELECT u.*,
                   (SELECT COUNT(*) FROM orders o WHERE o.user_id=u.id) AS order_count
              FROM store_users u WHERE {clause}
             ORDER BY u.created_at DESC LIMIT ${len(args)-1} OFFSET ${len(args)}""",
        *args,
    )
    return {"items": ser_all(rows), "total": total, "page": page, "limit": limit}


@app.get("/api/admin/users/{uid}")
async def get_user(uid: int, admin=Depends(current_admin)):
    u = await db.fetchrow("SELECT * FROM store_users WHERE id=$1", uid)
    if not u:
        raise HTTPException(404, "User not found")
    orders = await db.fetch(
        """SELECT o.*, p.name FROM orders o JOIN products p ON p.id=o.product_id
            WHERE o.user_id=$1 ORDER BY o.created_at DESC LIMIT 20""", uid)
    txs = await db.fetch(
        """SELECT * FROM wallet_transactions WHERE user_id=$1
            ORDER BY created_at DESC LIMIT 20""", uid)
    return {"user": ser(u), "orders": ser_all(orders), "transactions": ser_all(txs)}


@app.patch("/api/admin/users/{uid}")
async def update_user(uid: int, body: UserUpdateIn, request: Request,
                      admin=Depends(require_role("manager"))):
    u = await db.fetchrow("SELECT * FROM store_users WHERE id=$1", uid)
    if not u:
        raise HTTPException(404, "User not found")

    if body.isBanned is not None:
        await db.execute(
            "UPDATE store_users SET is_banned=$1, ban_reason=$2 WHERE id=$3",
            body.isBanned, body.banReason, uid)
        await audit(admin, "user_ban" if body.isBanned else "user_unban",
                    "user", uid, {"reason": body.banReason}, client_ip(request))

    if body.balanceAdjust is not None and body.balanceAdjust != 0:
        amount = money(body.balanceAdjust, Q_CRYPTO)
        try:
            async with db.tx() as conn:
                await wallet_apply(conn, uid, amount, "admin_adjust",
                                   body.adjustNote or "Admin adjustment")
        except InsufficientFunds:
            raise HTTPException(409, "Adjustment would make the balance negative")
        await audit(admin, "balance_adjust", "user", uid,
                    {"amount": str(amount), "note": body.adjustNote}, client_ip(request))
        await notify_user(
            bot, uid,
            f"💰 Your wallet was adjusted by an admin.\n"
            f"{'+' if amount > 0 else ''}{amount} — {body.adjustNote}",
        )

    return ser(await db.fetchrow("SELECT * FROM store_users WHERE id=$1", uid))


# ---- orders ------------------------------------------------------------------

@app.get("/api/admin/orders")
async def list_orders(admin=Depends(current_admin),
                      page: int = Query(1, ge=1), limit: int = Query(50, ge=1, le=200),
                      status: str = "", userId: int | None = None):
    where, args = ["1=1"], []
    if status:
        args.append(status)
        where.append(f"o.status = ${len(args)}")
    if userId:
        args.append(userId)
        where.append(f"o.user_id = ${len(args)}")
    clause = " AND ".join(where)

    total = await db.fetchval(f"SELECT COUNT(*) FROM orders o WHERE {clause}", *args)
    args.extend([limit, (page - 1) * limit])
    rows = await db.fetch(
        f"""SELECT o.*, p.name AS product_name,
                   u.telegram_id, u.username, u.first_name
              FROM orders o
              JOIN products p ON p.id=o.product_id
              JOIN store_users u ON u.id=o.user_id
             WHERE {clause}
             ORDER BY o.created_at DESC LIMIT ${len(args)-1} OFFSET ${len(args)}""",
        *args,
    )
    return {"items": ser_all(rows), "total": total, "page": page, "limit": limit}


@app.get("/api/admin/orders/{oid}")
async def get_order(oid: int, admin=Depends(current_admin)):
    row = await db.fetchrow(
        """SELECT o.*, p.name AS product_name, u.telegram_id, u.username, u.first_name
             FROM orders o JOIN products p ON p.id=o.product_id
             JOIN store_users u ON u.id=o.user_id WHERE o.id=$1""", oid)
    if not row:
        raise HTTPException(404, "Order not found")
    payments = await db.fetch("SELECT * FROM payments WHERE order_id=$1", oid)
    return {"order": ser(row), "payments": ser_all(payments)}


@app.patch("/api/admin/orders/{oid}")
async def update_order(oid: int, body: OrderUpdateIn, request: Request,
                       admin=Depends(require_role("manager"))):
    o = await db.fetchrow("SELECT * FROM orders WHERE id=$1", oid)
    if not o:
        raise HTTPException(404, "Order not found")

    if body.deliveredContent is not None:
        await db.execute(
            """UPDATE orders SET delivered_content=$1, status='delivered',
                   delivered_at=NOW() WHERE id=$2""",
            body.deliveredContent, oid)
        keys = [k for k in body.deliveredContent.split("\n") if k.strip()]
        fresh = await db.fetchrow("SELECT * FROM orders WHERE id=$1", oid)
        await deliver_to_user(bot, fresh, keys)
        await audit(admin, "order_manual_deliver", "order", oid, {}, client_ip(request))
    elif body.status:
        if body.status not in ("pending", "paid", "processing", "delivered", "cancelled"):
            raise HTTPException(400, "Invalid status")
        await db.execute("UPDATE orders SET status=$1 WHERE id=$2", body.status, oid)
        await audit(admin, "order_status", "order", oid,
                    {"status": body.status}, client_ip(request))

    return ser(await db.fetchrow("SELECT * FROM orders WHERE id=$1", oid))


@app.post("/api/admin/orders/{oid}/activate")
async def activate_order(oid: int, request: Request, admin=Depends(require_role("manager"))):
    """Web-panel equivalent of the in-Telegram /admin 'Mark activated' button
    for manual-delivery orders -- see activate_manual_delivery for the
    shared logic (closes the loop with the buyer either way)."""
    row = await activate_manual_delivery(oid)
    if not row:
        raise HTTPException(409, "Already handled, or this order isn't awaiting manual delivery")
    await audit(admin, "manual_deliver_confirm", "order", oid, {}, client_ip(request))
    return {"ok": True}


# ---- payments ----------------------------------------------------------------

@app.get("/api/admin/payments")
async def list_payments(admin=Depends(current_admin),
                        page: int = Query(1, ge=1), limit: int = Query(50, ge=1, le=200),
                        status: str = ""):
    where, args = ["1=1"], []
    if status:
        args.append(status)
        where.append(f"p.status = ${len(args)}")
    clause = " AND ".join(where)

    total = await db.fetchval(f"SELECT COUNT(*) FROM payments p WHERE {clause}", *args)
    args.extend([limit, (page - 1) * limit])
    rows = await db.fetch(
        f"""SELECT p.*, u.telegram_id, u.username, u.first_name
              FROM payments p JOIN store_users u ON u.id=p.user_id
             WHERE {clause}
             ORDER BY p.created_at DESC LIMIT ${len(args)-1} OFFSET ${len(args)}""",
        *args,
    )
    return {"items": ser_all(rows), "total": total, "page": page, "limit": limit}


@app.patch("/api/admin/payments/{pid}")
async def update_payment(pid: int, body: PaymentUpdateIn, request: Request,
                         admin=Depends(require_role("manager"))):
    try:
        if body.status == "approved":
            result = await approve_payment(bot, pid, admin["id"], body.note)
            await audit(admin, "payment_approve", "payment", pid,
                        {"note": body.note}, client_ip(request))
            return {"ok": True, "delivered": len(result.get("keys", []))}
        if body.status == "rejected":
            await reject_payment(bot, pid, admin["id"], body.note)
            await audit(admin, "payment_reject", "payment", pid,
                        {"note": body.note}, client_ip(request))
            return {"ok": True}
        raise HTTPException(400, "status must be 'approved' or 'rejected'")
    except FulfilError as e:
        raise HTTPException(409, str(e))


@app.post("/api/admin/payments/{pid}/recheck")
async def recheck_payment(pid: int, admin=Depends(require_role("support"))):
    """Force an immediate on-chain re-check rather than waiting for the worker."""
    p = await db.fetchrow("SELECT * FROM payments WHERE id=$1", pid)
    if not p:
        raise HTTPException(404, "Payment not found")
    if p["status"] != "pending":
        raise HTTPException(409, f"Payment is {p['status']}")
    if not p["tx_id"]:
        raise HTTPException(400, "No transaction hash submitted")

    chain = p["chain"] or "TRC20"
    result = await verifier.verify(
        chain, p["tx_id"], p["expected_address"] or await chain_address(chain),
        crypto(p["expected_amount"] or p["amount"]), p["created_at"],
    )
    if result.ok:
        await approve_payment(bot, pid, admin["id"], "manual recheck — verified")
    return {
        "verified": result.ok, "reason": result.reason,
        "amount": str(result.amount), "confirmations": result.confirmations,
    }


@app.get("/api/admin/file/{file_id}")
async def proxy_telegram_file(file_id: str, admin=Depends(current_admin)):
    """Proxy any Telegram-hosted file (payment screenshots, ticket photos,
    manual-delivery photos, ...) so the panel can render it -- keyed purely
    on file_id, not tied to any one feature."""
    try:
        f = await bot.get_file(file_id)
        url = f"https://api.telegram.org/file/bot{CFG.bot_token}/{f.file_path}"
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get(url)
            if r.status_code != 200:
                raise HTTPException(404, "Image unavailable")
            return Response(content=r.content,
                            media_type=r.headers.get("content-type", "image/jpeg"))
    except HTTPException:
        raise
    except Exception:
        log.exception("file proxy failed")
        raise HTTPException(502, "Could not fetch image")


# ---- coupons -----------------------------------------------------------------

@app.get("/api/admin/coupons")
async def list_coupons(admin=Depends(current_admin)):
    return ser_all(await db.fetch("SELECT * FROM coupons ORDER BY id DESC"))


@app.post("/api/admin/coupons", status_code=201)
async def create_coupon(body: CouponIn, request: Request,
                        admin=Depends(require_role("manager"))):
    try:
        row = await db.fetchrow(
            """INSERT INTO coupons
                 (code, discount_type, value, max_uses, min_order_amount,
                  per_user_limit, expires_at, active)
               VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *""",
            body.code.upper(), body.discountType, money(body.value, Q_CRYPTO),
            body.maxUses, money(body.minOrderAmount, Q_CRYPTO),
            body.perUserLimit, body.expiresAt, body.active,
        )
    except asyncpg.UniqueViolationError:
        raise HTTPException(409, "That coupon code already exists")
    await audit(admin, "coupon_create", "coupon", row["id"],
                {"code": body.code}, client_ip(request))
    return ser(row)


@app.patch("/api/admin/coupons/{cid}")
async def update_coupon(cid: int, body: CouponIn, request: Request,
                        admin=Depends(require_role("manager"))):
    row = await db.fetchrow(
        """UPDATE coupons SET code=$2, discount_type=$3, value=$4, max_uses=$5,
               min_order_amount=$6, per_user_limit=$7, expires_at=$8, active=$9
            WHERE id=$1 RETURNING *""",
        cid, body.code.upper(), body.discountType, money(body.value, Q_CRYPTO),
        body.maxUses, money(body.minOrderAmount, Q_CRYPTO),
        body.perUserLimit, body.expiresAt, body.active,
    )
    if not row:
        raise HTTPException(404, "Coupon not found")
    await audit(admin, "coupon_update", "coupon", cid, {}, client_ip(request))
    return ser(row)


@app.delete("/api/admin/coupons/{cid}", status_code=204)
async def delete_coupon(cid: int, request: Request,
                        admin=Depends(require_role("manager"))):
    await db.execute("DELETE FROM coupons WHERE id=$1", cid)
    await audit(admin, "coupon_delete", "coupon", cid, {}, client_ip(request))
    return Response(status_code=204)


# ---- emoji mappings (Global Emoji Replacement Manager) ------------------------
#  Same emoji_map table the Telegram bot's own admin chat flow uses (see
#  bot.py "ADMIN — EMOJI MAPPINGS" section) — one shared dictionary, two
#  front doors. invalidate_emoji_map() takes effect immediately since the
#  bot and this API run in the same process.

@app.get("/api/admin/emoji-map")
async def list_emoji_map(admin=Depends(current_admin)):
    return ser_all(await db.fetch("SELECT * FROM emoji_map ORDER BY unicode_emoji"))


@app.post("/api/admin/emoji-map", status_code=201)
async def upsert_emoji_map(body: EmojiMappingIn, request: Request,
                           admin=Depends(require_role("manager"))):
    row = await db.fetchrow(
        """INSERT INTO emoji_map(unicode_emoji, premium_emoji_id) VALUES($1,$2)
           ON CONFLICT (unicode_emoji) DO UPDATE
             SET premium_emoji_id=$2, updated_at=now()
           RETURNING *""",
        body.unicodeEmoji, body.premiumEmojiId)
    invalidate_emoji_map()
    await audit(admin, "emoji_map_upsert", "emoji_map", None,
                {"emoji": body.unicodeEmoji}, client_ip(request))
    return ser(row)


@app.delete("/api/admin/emoji-map/{emoji}", status_code=204)
async def delete_emoji_map(emoji: str, request: Request,
                           admin=Depends(require_role("manager"))):
    await db.execute("DELETE FROM emoji_map WHERE unicode_emoji=$1", emoji)
    invalidate_emoji_map()
    await audit(admin, "emoji_map_delete", "emoji_map", None,
                {"emoji": emoji}, client_ip(request))
    return Response(status_code=204)


# ---- broadcasts --------------------------------------------------------------

@app.get("/api/admin/broadcasts")
async def list_broadcasts(admin=Depends(current_admin)):
    return ser_all(await db.fetch(
        "SELECT * FROM broadcasts ORDER BY created_at DESC LIMIT 50"))


@app.post("/api/admin/broadcasts", status_code=202)
async def create_broadcast(body: BroadcastIn, request: Request,
                           admin=Depends(require_role("manager"))):
    row = await db.fetchrow(
        """INSERT INTO broadcasts(message, target, created_by, status)
           VALUES($1,$2,$3,'running') RETURNING *""",
        body.message, body.target, admin["id"],
    )
    await audit(admin, "broadcast_start", "broadcast", row["id"],
                {"target": body.target}, client_ip(request))
    asyncio.create_task(run_broadcast(row["id"], body.message, body.target))
    return ser(row)


@app.post("/api/admin/broadcasts/product", status_code=202)
async def create_product_broadcast(body: ProductBroadcastIn, request: Request,
                                   admin=Depends(require_role("manager"))):
    """Promote one product to all users or a chosen audience: custom
    message, optional image or video (a "photo" attachment with no explicit
    file falls back to the product's own listing photo), and a Buy Now
    button that opens that product directly."""
    product = await db.fetchrow("SELECT * FROM products WHERE id=$1", body.productId)
    if not product:
        raise HTTPException(404, "Product not found")

    media_type = body.mediaType
    media_file_id = (body.mediaFileId or "").strip()
    if media_type == "photo" and not media_file_id:
        media_file_id = product["image_file_id"] or product["image_url"] or ""
    if not media_file_id:
        media_type = "none"

    row = await db.fetchrow(
        """INSERT INTO broadcasts(message, target, created_by, status, product_id,
                                   image_file_id, media_type)
           VALUES($1,$2,$3,'running',$4,$5,$6) RETURNING *""",
        body.message, body.target, admin["id"], body.productId,
        media_file_id or None, media_type,
    )
    await audit(admin, "product_broadcast_start", "broadcast", row["id"],
                {"target": body.target, "productId": body.productId, "mediaType": media_type},
                client_ip(request))
    asyncio.create_task(run_broadcast(
        row["id"], body.message, body.target,
        media_type if media_type != "none" else "", media_file_id,
        buy_product_id=body.productId,
    ))
    return ser(row)


def broadcast_audience_sql(target: str) -> tuple[str, list]:
    """Build the recipient query for a broadcast audience.
    Returns (sql, params). Banned users are always excluded."""
    q = "SELECT id, telegram_id FROM store_users WHERE NOT is_banned"
    params: list = []
    if target == "buyers":
        q += " AND EXISTS (SELECT 1 FROM orders o WHERE o.user_id=store_users.id)"
    elif target == "no_orders":
        q += " AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id=store_users.id)"
    elif target.startswith("tier~"):
        params.append(target.split("~", 1)[1])
        q += f" AND tier = ${len(params)}"
    elif target.startswith("lang~"):
        params.append(target.split("~", 1)[1])
        q += f" AND language = ${len(params)}"
    elif target.startswith("ids~"):
        raw = [x for x in target.split("~", 1)[1].split(",") if x.strip().isdigit()]
        params.append(raw)
        q += f" AND telegram_id = ANY(${len(params)}::text[])"
    return q, params


async def send_broadcast_item(telegram_id: str, message: str, media_type: str = "",
                              file_id: str = "", reply_markup=None) -> bool:
    """Send one broadcast item (text or media, optionally with a button) to
    a user. Never raises. Takes telegram_id directly -- the audience query
    already has it, so no per-recipient lookup is needed here."""
    token = _broadcast_send_ctx.set(True)
    try:
        if not telegram_id:
            return False
        chat = int(telegram_id)
        if media_type == "photo" and file_id:
            await bot.send_photo(chat, file_id, caption=message or None, reply_markup=reply_markup)
        elif media_type == "video" and file_id:
            await bot.send_video(chat, file_id, caption=message or None, reply_markup=reply_markup)
        elif media_type == "document" and file_id:
            await bot.send_document(chat, file_id, caption=message or None, reply_markup=reply_markup)
        elif media_type == "animation" and file_id:
            await bot.send_animation(chat, file_id, caption=message or None, reply_markup=reply_markup)
        else:
            if not message:
                return False
            await bot.send_message(chat, message, reply_markup=reply_markup)
        return True
    except (TelegramForbiddenError, TelegramBadRequest):
        # User blocked the bot or the chat is gone — expected at scale.
        return False
    except Exception:
        log.exception("broadcast send failed for user %s", telegram_id)
        return False
    finally:
        _broadcast_send_ctx.reset(token)


async def run_broadcast(bid: int, message: str, target: str,
                        media_type: str = "", file_id: str = "",
                        buy_product_id: int | None = None,
                        shop_button: bool = False) -> None:
    """Paced sender. Telegram tolerates roughly 30 msg/s; we stay well under.
    Supports text, photo, video, document and animation broadcasts, plus
    either a "🛒 Buy Now" button (product broadcasts) that reuses the exact
    same prod: callback the shop listing already uses -- no separate handler
    needed -- or, when there's no single product to point at (a category-
    wide Discount Sale broadcast), a generic "🛍 Shop the Sale" button that
    opens the catalogue via the existing shop: callback instead of leaving
    the broadcast buttonless."""
    q, params = broadcast_audience_sql(target)
    users = await db.fetch(q, *params)

    markup = None
    if buy_product_id is not None:
        markup = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🛒 Buy Now", callback_data=f"prod:{buy_product_id}")
        ]])
    elif shop_button:
        markup = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🛍 Shop the Sale", callback_data="shop:1")
        ]])

    sent = failed = 0
    for i, u in enumerate(users):
        ok = await send_broadcast_item(u["telegram_id"], message, media_type, file_id, markup)
        sent += 1 if ok else 0
        failed += 0 if ok else 1
        if i % 25 == 0:
            await db.execute(
                "UPDATE broadcasts SET sent_count=$1, failed_count=$2 WHERE id=$3",
                sent, failed, bid)
        await asyncio.sleep(0.05)

    await db.execute(
        "UPDATE broadcasts SET sent_count=$1, failed_count=$2, status='done' WHERE id=$3",
        sent, failed, bid)
    log.info("broadcast %s complete: %s sent, %s failed", bid, sent, failed)


# ---- pinned warning broadcasts ------------------------------------------------
#  Admin-driven replacement for the old auto-pinned-per-user payment warning:
#  compose once, send + pin to an audience, and unpin for everyone later --
#  see pinned_broadcasts / pinned_broadcast_messages in SCHEMA.

async def run_pinned_warning_broadcast(bid: int, message: str, target: str) -> None:
    """Same pacing as run_broadcast, but additionally pins the sent message in
    each recipient's chat and records (chat_id, message_id) -- Telegram has no
    "list what I've pinned across all my chats" API, so this table is the only
    way "unpin for everyone" can find those messages again later."""
    q, params = broadcast_audience_sql(target)
    users = await db.fetch(q, *params)

    sent = failed = 0
    for i, u in enumerate(users):
        chat_id = int(u["telegram_id"])
        token = _broadcast_send_ctx.set(True)
        try:
            sent_msg = await bot.send_message(chat_id, message)
        except (TelegramForbiddenError, TelegramBadRequest):
            sent_msg = None
        except Exception:
            log.exception("pinned broadcast: send failed for chat %s", chat_id)
            sent_msg = None
        finally:
            _broadcast_send_ctx.reset(token)

        if sent_msg:
            sent += 1
            await db.execute(
                """INSERT INTO pinned_broadcast_messages(broadcast_id, chat_id, message_id)
                   VALUES($1,$2,$3)""",
                bid, chat_id, sent_msg.message_id)
            asyncio.create_task(_pin_chat_message_background(chat_id, sent_msg.message_id))
        else:
            failed += 1

        if i % 25 == 0:
            await db.execute(
                "UPDATE pinned_broadcasts SET sent_count=$1, failed_count=$2 WHERE id=$3",
                sent, failed, bid)
        await asyncio.sleep(0.05)

    await db.execute(
        "UPDATE pinned_broadcasts SET sent_count=$1, failed_count=$2, status='done' WHERE id=$3",
        sent, failed, bid)
    log.info("pinned broadcast %s complete: %s sent, %s failed", bid, sent, failed)


async def run_unpin_broadcast(bid: int) -> None:
    """Best-effort unpin across every chat this broadcast reached. Each row
    is deleted as its unpin is attempted (not batched at the end) so a crash
    mid-run never redoes an unpin that already succeeded on the next try."""
    rows = await db.fetch(
        "SELECT id, chat_id, message_id FROM pinned_broadcast_messages WHERE broadcast_id=$1", bid)
    for r in rows:
        try:
            await bot.unpin_chat_message(chat_id=r["chat_id"], message_id=r["message_id"])
        except (TelegramForbiddenError, TelegramBadRequest):
            pass  # already unpinned, chat gone, or bot lacks rights -- not an error state
        except Exception:
            log.exception("pinned broadcast: unpin failed for chat %s", r["chat_id"])
        await db.execute("DELETE FROM pinned_broadcast_messages WHERE id=$1", r["id"])
        await asyncio.sleep(0.05)
    await db.execute("UPDATE pinned_broadcasts SET active=FALSE WHERE id=$1", bid)
    log.info("pinned broadcast %s: unpinned for %s chats", bid, len(rows))


@app.get("/api/admin/pinned-warnings")
async def list_pinned_warnings(admin=Depends(current_admin)):
    return ser_all(await db.fetch(
        "SELECT * FROM pinned_broadcasts ORDER BY created_at DESC LIMIT 50"))


@app.post("/api/admin/pinned-warnings", status_code=202)
async def create_pinned_warning(body: BroadcastIn, request: Request,
                                admin=Depends(require_role("manager"))):
    row = await db.fetchrow(
        """INSERT INTO pinned_broadcasts(message, target, created_by, status)
           VALUES($1,$2,$3,'running') RETURNING *""",
        body.message, body.target, admin["id"],
    )
    await audit(admin, "pinned_warning_start", "pinned_broadcast", row["id"],
                {"target": body.target}, client_ip(request))
    asyncio.create_task(run_pinned_warning_broadcast(row["id"], body.message, body.target))
    return ser(row)


@app.post("/api/admin/pinned-warnings/{bid}/unpin", status_code=202)
async def unpin_pinned_warning(bid: int, request: Request,
                               admin=Depends(require_role("manager"))):
    row = await db.fetchrow("SELECT * FROM pinned_broadcasts WHERE id=$1", bid)
    if not row:
        raise HTTPException(404, "Not found")
    await audit(admin, "pinned_warning_unpin", "pinned_broadcast", bid, {}, client_ip(request))
    asyncio.create_task(run_unpin_broadcast(bid))
    return {"ok": True}


# ---- support tickets -----------------------------------------------------------

@app.get("/api/admin/tickets")
async def list_tickets(admin=Depends(current_admin),
                       status: str = Query("open"), page: int = Query(1, ge=1),
                       limit: int = Query(50, le=200)):
    where = "1=1" if status == "all" else "t.status=$1"
    args = [] if status == "all" else [status]
    total = await db.fetchval(f"SELECT COUNT(*) FROM tickets t WHERE {where}", *args)
    args2 = list(args) + [limit, (page - 1) * limit]
    rows = await db.fetch(
        f"""SELECT t.*, u.telegram_id, u.username, u.first_name,
                   (SELECT message FROM ticket_messages m WHERE m.ticket_id=t.id
                     ORDER BY m.created_at DESC LIMIT 1) AS last_message
              FROM tickets t JOIN store_users u ON u.id=t.user_id
             WHERE {where}
             ORDER BY t.updated_at DESC LIMIT ${len(args2)-1} OFFSET ${len(args2)}""",
        *args2)
    return {"items": ser_all(rows), "total": total, "page": page, "limit": limit}


@app.get("/api/admin/tickets/{tid}")
async def get_ticket(tid: int, admin=Depends(current_admin)):
    ticket = await db.fetchrow(
        """SELECT t.*, u.telegram_id, u.username, u.first_name
             FROM tickets t JOIN store_users u ON u.id=t.user_id WHERE t.id=$1""", tid)
    if not ticket:
        raise HTTPException(404, "Ticket not found")
    msgs = await db.fetch(
        "SELECT * FROM ticket_messages WHERE ticket_id=$1 ORDER BY created_at", tid)
    return {"ticket": ser(ticket), "messages": ser_all(msgs)}


class TicketReplyIn(BaseModel):
    message: str = Field(min_length=1, max_length=2000)

    @field_validator("message")
    @classmethod
    def valid_html(cls, v):
        err = validate_telegram_html(v)
        if err:
            raise ValueError(err)
        return v


@app.post("/api/admin/tickets/{tid}/reply")
async def reply_ticket(tid: int, body: TicketReplyIn, request: Request,
                       admin=Depends(require_role("support"))):
    ticket = await db.fetchrow("SELECT * FROM tickets WHERE id=$1", tid)
    if not ticket:
        raise HTTPException(404, "Ticket not found")
    await db.execute(
        """INSERT INTO ticket_messages(ticket_id, sender, admin_id, message)
           VALUES($1,'admin',$2,$3)""",
        tid, admin["id"], body.message)
    await db.execute("UPDATE tickets SET updated_at=NOW() WHERE id=$1", tid)
    await notify_user(
        bot, ticket["user_id"], f"💬 <b>Support replied to ticket #{tid}</b>\n\n{body.message}")
    await audit(admin, "ticket_reply", "ticket", tid, {}, client_ip(request))
    return {"ok": True}


@app.post("/api/admin/tickets/{tid}/close")
async def close_ticket(tid: int, request: Request, admin=Depends(require_role("support"))):
    row = await db.fetchrow(
        "UPDATE tickets SET status='closed', updated_at=NOW() WHERE id=$1 RETURNING *", tid)
    if not row:
        raise HTTPException(404, "Ticket not found")
    await audit(admin, "ticket_close", "ticket", tid, {}, client_ip(request))
    return ser(row)


@app.post("/api/admin/tickets/{tid}/reopen")
async def reopen_ticket(tid: int, request: Request, admin=Depends(require_role("support"))):
    row = await db.fetchrow(
        "UPDATE tickets SET status='open', updated_at=NOW() WHERE id=$1 RETURNING *", tid)
    if not row:
        raise HTTPException(404, "Ticket not found")
    await audit(admin, "ticket_reopen", "ticket", tid, {}, client_ip(request))
    return ser(row)


# ---- transactions, audit, settings, admins ----------------------------------

@app.get("/api/admin/transactions")
async def list_transactions(admin=Depends(current_admin),
                            page: int = Query(1, ge=1), limit: int = Query(50, le=200),
                            userId: int | None = None):
    where, args = ["1=1"], []
    if userId:
        args.append(userId)
        where.append(f"w.user_id = ${len(args)}")
    clause = " AND ".join(where)
    total = await db.fetchval(
        f"SELECT COUNT(*) FROM wallet_transactions w WHERE {clause}", *args)
    args.extend([limit, (page - 1) * limit])
    rows = await db.fetch(
        f"""SELECT w.*, u.telegram_id, u.username FROM wallet_transactions w
              JOIN store_users u ON u.id=w.user_id WHERE {clause}
             ORDER BY w.created_at DESC LIMIT ${len(args)-1} OFFSET ${len(args)}""",
        *args)
    return {"items": ser_all(rows), "total": total, "page": page, "limit": limit}


@app.get("/api/admin/wallet/audit")
async def wallet_audit_endpoint(admin=Depends(require_role("owner"))):
    """Re-derive balances from the ledger. An empty list is a healthy system."""
    drift = await wallet_audit()
    return {"healthy": len(drift) == 0, "discrepancies": drift}


@app.get("/api/admin/audit-logs")
async def list_audit(admin=Depends(require_role("manager")),
                     page: int = Query(1, ge=1), limit: int = Query(50, le=200)):
    total = await db.fetchval("SELECT COUNT(*) FROM audit_logs")
    rows = await db.fetch(
        "SELECT * FROM audit_logs ORDER BY created_at DESC LIMIT $1 OFFSET $2",
        limit, (page - 1) * limit)
    return {"items": ser_all(rows), "total": total, "page": page, "limit": limit}


@app.get("/api/admin/settings")
async def get_settings(admin=Depends(current_admin)):
    return await settings.load(force=True)


_HTML_RENDERED_SETTINGS = {"welcome_text", "payment_warning_text", "store_header", "store_footer"}


@app.patch("/api/admin/settings")
async def update_settings(body: SettingsIn, request: Request,
                          admin=Depends(require_role("manager"))):
    for k, v in body.values.items():
        if k not in DEFAULT_SETTINGS:
            raise HTTPException(400, f"Unknown setting: {k}")
        if k in _HTML_RENDERED_SETTINGS:
            err = validate_telegram_html(v)
            if err:
                raise HTTPException(400, f"{k}: {err}")
    for k, v in body.values.items():
        await settings.set(k, v)
    await audit(admin, "settings_update", "settings", None,
                {"keys": list(body.values)}, client_ip(request))
    return await settings.load(force=True)


@app.get("/api/admin/admins")
async def list_admins(admin=Depends(require_role("owner"))):
    rows = await db.fetch(
        "SELECT id, username, role, active, last_login_at, created_at FROM admins ORDER BY id")
    return ser_all(rows)


@app.post("/api/admin/admins", status_code=201)
async def create_admin(body: AdminIn, request: Request,
                       admin=Depends(require_role("owner"))):
    hashed = bcrypt.hashpw(body.password.encode(), bcrypt.gensalt()).decode()
    try:
        row = await db.fetchrow(
            """INSERT INTO admins(username, password_hash, role) VALUES($1,$2,$3)
               RETURNING id, username, role, active, created_at""",
            body.username, hashed, body.role)
    except asyncpg.UniqueViolationError:
        raise HTTPException(409, "That username already exists")
    await _load_admin_usernames(force=True)
    await audit(admin, "admin_create", "admin", row["id"],
                {"username": body.username, "role": body.role}, client_ip(request))
    return ser(row)


@app.delete("/api/admin/admins/{aid}", status_code=204)
async def delete_admin(aid: int, request: Request, admin=Depends(require_role("owner"))):
    if aid == admin["id"]:
        raise HTTPException(400, "You cannot delete your own account")
    target = await db.fetchrow("SELECT * FROM admins WHERE id=$1", aid)
    if target and target["username"] == "admin":
        raise HTTPException(400, "The primary admin account cannot be deleted")
    await db.execute("UPDATE admins SET active=FALSE WHERE id=$1", aid)
    await _load_admin_usernames(force=True)
    await audit(admin, "admin_delete", "admin", aid, {}, client_ip(request))
    return Response(status_code=204)


# ---- CSV export --------------------------------------------------------------

def csv_stream(rows: Sequence[asyncpg.Record], columns: Sequence[str]):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(columns)
    yield buf.getvalue()
    buf.seek(0), buf.truncate(0)
    for r in rows:
        w.writerow([r.get(c, "") if isinstance(r, dict) else r[c] for c in columns])
        yield buf.getvalue()
        buf.seek(0), buf.truncate(0)


@app.get("/api/admin/export/users")
async def export_users(admin=Depends(require_role("manager"))):
    rows = await db.fetch(
        """SELECT id, telegram_id, username, first_name, wallet_balance,
                  total_spent, tier, is_banned, created_at
             FROM store_users ORDER BY id""")
    cols = ["id", "telegram_id", "username", "first_name", "wallet_balance",
            "total_spent", "tier", "is_banned", "created_at"]
    return StreamingResponse(
        csv_stream(rows, cols), media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=users.csv"})


@app.get("/api/admin/export/orders")
async def export_orders(admin=Depends(require_role("manager"))):
    rows = await db.fetch(
        """SELECT o.id, o.user_id, u.telegram_id, p.name AS product,
                  o.quantity, o.subtotal, o.discount, o.total, o.currency,
                  o.status, o.payment_method, o.created_at
             FROM orders o JOIN products p ON p.id=o.product_id
             JOIN store_users u ON u.id=o.user_id ORDER BY o.id""")
    cols = ["id", "user_id", "telegram_id", "product", "quantity", "subtotal",
            "discount", "total", "currency", "status", "payment_method", "created_at"]
    return StreamingResponse(
        csv_stream(rows, cols), media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=orders.csv"})


# =============================================================================
#  STATIC ADMIN PANEL
# =============================================================================
#  Serves admin.html from the same directory if present, so the panel needs no
#  separate web server. Registered last so it never shadows an /api route.

_ADMIN_HTML = os.path.join(os.path.dirname(os.path.abspath(__file__)), "admin.html")

# React/ReactDOM/Babel-standalone/Tailwind, self-hosted rather than pulled
# from unpkg.com/cdn.tailwindcss.com at page-load time -- some ISPs/mobile
# carriers and device-level DNS filters block those domains, which silently
# left the panel a blank white page (HTML loaded fine, but the scripts that
# actually render it never did) with no error visible to the admin.
_VENDOR_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "vendor")
if os.path.isdir(_VENDOR_DIR):
    app.mount("/admin-vendor", StaticFiles(directory=_VENDOR_DIR), name="admin-vendor")


_admin_html_cache: str | None = None


@app.get("/admin")
@app.get("/admin/")
async def serve_admin_panel():
    global _admin_html_cache
    # Read once and keep in memory rather than a blocking file read (which
    # stalls the event loop) plus a full re-decode of the ~80KB panel on
    # every page load -- deploys already require a service restart to pick
    # up any other code change, so a stale cache across an admin.html edit
    # isn't a new class of problem.
    if _admin_html_cache is None:
        if not os.path.exists(_ADMIN_HTML):
            raise HTTPException(404, "admin.html not found next to bot.py")
        with open(_ADMIN_HTML, "r", encoding="utf-8") as f:
            _admin_html_cache = f.read()
    return Response(content=_admin_html_cache, media_type="text/html")


@app.get("/")
async def root_redirect():
    return JSONResponse({
        "service": "digital-hub-bot",
        "version": "2.0",
        "admin": "/admin",
        "health": "/api/healthz",
    })


# =============================================================================
#  ENTRYPOINT
# =============================================================================

def main() -> None:
    banner = r"""
   ____  _       _ _        _   _   _       _
  |  _ \(_) __ _(_) |_ __ _| | | | | |_   _| |__
  | | | | |/ _` | | __/ _` | | | |_| | | | | '_ \
  | |_| | | (_| | | || (_| | | |  _  | |_| | |_) |
  |____/|_|\__, |_|\__\__,_|_| |_| |_|\__,_|_.__/
           |___/          advanced edition  v2.0
"""
    print(banner)
    log.info("mode: %s", "webhook" if CFG.webhook_url else "long-polling")
    log.info("auto-verify: %s confirmations, every %ss",
             CFG.min_confirmations, CFG.verify_interval_seconds)

    uvicorn.run(
        app,
        host=CFG.host,
        port=CFG.port,
        log_level=CFG.log_level.lower(),
        access_log=False,
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("interrupted")
