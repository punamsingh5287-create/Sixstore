# In-Bot Admin Panel

Everything is managed from Telegram. No website needed.

---

## Getting access

1. Send `/id` to your bot — it replies with your Telegram ID.
2. Put that ID in `.env`:
   ```
   ADMIN_TELEGRAM_IDS=123456789
   ```
   The first ID listed is **owner** (the only role that can promote others).
3. `sudo systemctl restart digitalhub`
4. Send `/admin`.

Non-admins who send `/admin` get no reply at all — the panel never reveals it exists.

To add more admins later: **👥 Users → pick a user → ⬆️ Make admin**. They get
`manager` rights, which cannot promote further admins.

---

## The panel

```
📊 Dashboard      🛍 Products
📂 Categories     📦 Stock / Keys
💳 Payments       💰 Prices
🎁 Deals          👥 Users
💳 Payment Methods 🎟 Coupons
📢 Broadcast      🌍 Languages
💱 Currency       ⭐ Premium Emojis
🎨 UI Editor      ⚙️ Bot Settings
```

Pending payment and order counts appear live on the home screen.

---

## Sections

**📊 Dashboard** — users, new signups, orders today, revenue (total + today),
pending/approved payments, keys in stock, wallet liability, top 5 sellers.

**🛍 Products** — add, rename, reprice, describe, set image, move category,
toggle deal, toggle file-delivery, hide, delete.

**📂 Categories** — add, rename (EN + Hindi), emoji, sort order, hide, delete.
Deleting is blocked while products still reference it.

**📦 Stock / Keys** — low-stock-first overview with ⚠️ under 5. Paste keys one
per line (up to 5000 per message); duplicates are skipped automatically. Going
from 0 → stocked notifies everyone with a restock alert.

**💳 Payments** — filter by pending/approved/rejected/expired. Per payment:
re-check on-chain, view screenshot, approve, or reject with a reason. Approval
delivers keys or credits the wallet and notifies the buyer.

**💳 Payment Methods** — TRC20/BEP20/ERC20 addresses, UPI ID, UPI QR image,
Binance Pay, min confirmations, amount tolerance, payment timeout, min top-ups.

**💰 Prices** — USDT→INR rate and referral bonus percent.

**🎁 Deals** — feature any product, set a struck-through original price.

**👥 Users** — search by name/@username/ID. Adjust balance, change rank, grant
coupons, grant referral bonus, view/cancel orders, DM the user, ban/unban,
promote to admin.

**🎟 Coupons** — create, set value, max uses, minimum order, per-user limit,
enable/disable, delete.

**📢 Broadcast** — text, photo, video, document or animation. Audiences:
everyone, buyers, never-ordered, gold, silver, English users, Hindi users, or a
specific list of IDs. Shows recipient count and a preview before sending.

**🌍 Languages / 💱 Currency** — enable/disable and set defaults. At least one
of each must stay enabled.

**⚙️ Bot Settings** — maintenance mode, auto chain verification on/off,
mini-app URL, loyalty tier thresholds and discounts, admin list, wallet audit.

---

## 🎨 UI Editor

Nothing the customer sees is hardcoded.

- **Branding & text** — store name, welcome message, header, footer, logo,
  shop banner, support username, channel link, terms link, mini-app button
  label, and the three rank names.
- **Button labels** — every menu and button string, per language. Overrides are
  stored separately from the built-in text, so **♻️ Reset** restores the
  original at any time.
- **Menu order** — send a comma-separated list to reorder.
- **Show / hide buttons** — tap to toggle any menu entry.

**Smaller buttons:** Telegram sizes buttons to their text. Shorter labels =
smaller buttons. Edit labels here and hide entries you don't need. The default
layout is already two per row:

```
🛍 Shop      | 🔥 Deals
💰 Wallet    | 📦 Orders
🎁 Referral  | 💬 Support
🌍 Language  | 💱 Currency
```

---

## ⭐ Premium Emojis

**Read this before setting them up — there's a platform limit worth knowing.**

Telegram renders custom emoji only through message *entities*. That means:

| Where | Renders? |
|---|---|
| Welcome message, header, footer | ✅ yes |
| Photo captions, any message body | ✅ yes |
| **Inline keyboard button labels** | ❌ **no — Telegram limitation** |

Buttons show the **fallback glyph** instead. That's a restriction in Telegram
itself, not something code can work around. Any bot claiming otherwise is
showing you a normal emoji.

Sending custom emoji also requires the bot to be attached to a **Premium or
Business account**. Without that, Telegram rejects the entity and the fallback
shows everywhere.

**To use them:**
1. **⭐ Premium Emojis → ➕ Add emoji**
2. Send the premium emoji directly (the ID is read automatically), or paste a
   numeric ID like `5368324170671202286`
3. Set a fallback glyph — this is what buttons and non-Premium clients show
4. **🎯 Assign to slot** — header, user line, balance line, welcome, or any
   menu button

The preview shows the real emoji if your setup supports it, and the plain glyph
if it doesn't — so you can tell immediately whether it's working.

---

## Notes

- Every admin action is written to the audit log with your username.
- Balance changes go through the wallet ledger; **⚙️ Bot Settings → 🧮 Wallet
  audit** re-derives every balance from that ledger and reports any drift.
- The web API is still running for the mini-app shop. `admin.html` still works
  if you ever want it, but it's no longer required.
