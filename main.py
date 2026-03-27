import asyncio
import re
import random
import gc
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError

# ================= CONFIG ================= #

API_ID = 21152230
API_HASH = "183d369e906b4f350f64d94d892dcb91"

SESSIONS = [
    "1BVtsOMcBu3OKdsAOLDzSSqY4D642NrFACvDqks2iAocYjZqC1tXDd-hRBLswjJ64ZYKAqV5vaVCdNqeClDvx2JwNxnXOKzdKlunqYjpYTD-HHtGlARHP1M--MRLOiaTdOGIgJvt6n2VtEqR5OGsOHJLYwkfc5h6k23udF3XObbCeoz8lRXdNgvLgnHnRhetfzQW-SQQx0GM8GtnrGYqHdxyaHgUZToT--EqUGjPXU9Dibf209_ZSJhOPXkw0o-JqFprxmI0AInNKfpvGLUCQ01Yuslil5bm4scULe8JKdFf4h4bwn9nF8-PMcBIw9O9taqW7UTiC3z9R40opuPXpk9DbuCXq9JE="
]

MESSAGE_LINK = "https://t.me/c/2432662361/15698"

GROUPS = [
    "https://t.me/buffestmarket/16",
    "https://t.me/shoreline/325",
    "https://t.me/c/2256623070/2",
    "https://t.me/sectormarket/6",
    "https://t.me/EscrowpIace/41",
    "https://t.me/marketogs/127870",
    "https://t.me/porkmarket/8",
    "https://t.me/SectorSocial/21",
    "https://t.me/decorated/8263",
    "https://t.me/stockless/45",
    "https://t.me/pluggerz/4",
    "https://t.me/oguflips/20",
    "https://t.me/GureMarketplace/5",
    "https://t.me/Social_M_Marketplace/57",
    "https://t.me/advartise/6303",
    "https://t.me/kimsocialMP/3",
    "https://t.me/LuxurMarket/11",
    "https://t.me/marketunlimited/74145",
    "https://t.me/GooMarketplace/14616",
    "https://t.me/iinvd/120084",
    "https://t.me/aizenmarket/19",
    "https://t.me/crypto_forums/1",
    "https://t.me/CreeperForum/15",
    "https://t.me/crisgalaxymarket/324",
    "https://t.me/RareHandle/85",
    "https://t.me/VipexMarket/17",
    "https://t.me/unknownmart/13",
    "https://t.me/SocialCove/10",
    "https://t.me/texted/3",
    "https://t.me/securedmarts",
    "https://t.me/mythicforum/2",
    "https://t.me/totalsmp/21"
]

LOG_GROUP = -1002432662361

FORWARD_DELAY = (90, 100)
ROUND_DELAY = (800, 900)

# ========================================== #

CACHED_MSG = None
CACHED_FILE = None


def rand_delay(r):
    return random.randint(*r)


def parse_msg(link):
    m = re.search(r"t.me/c/(\d+)/(\d+)", link)
    if m:
        return int(f"-100{m.group(1)}"), int(m.group(2))
    m = re.search(r"t.me/([^/]+)/(\d+)", link)
    return m.group(1), int(m.group(2))


def parse_group(link):
    m = re.search(r"t.me/c/(\d+)(?:/(\d+))?", link)
    if m:
        return int(f"-100{m.group(1)}"), int(m.group(2)) if m.group(2) else None
    m = re.search(r"t.me/([^/]+)(?:/(\d+))?", link)
    return m.group(1), int(m.group(2)) if m.group(2) else None


class Bot:
    def __init__(self, session):
        self.client = TelegramClient(
            StringSession(session),
            API_ID,
            API_HASH,
            device_model="Android",
            system_version="11",
            app_version="9.0"
        )
        self.running = False
        self.loop_task = None
        self.entities = []

    async def load_message(self):
        global CACHED_MSG, CACHED_FILE

        if CACHED_MSG:
            return

        chat, msg_id = parse_msg(MESSAGE_LINK)
        msg = await self.client.get_messages(chat, ids=msg_id)

        CACHED_MSG = msg.text or ""

        if msg.media:
            CACHED_FILE = msg.media  # 🔥 no re-upload

    async def load_entities(self):
        self.entities = []
        for g in GROUPS:
            try:
                chat, topic = parse_group(g)
                entity = await self.client.get_entity(chat)
                self.entities.append((entity, topic))
            except:
                continue

    async def send(self, entity, topic):
        try:
            if CACHED_FILE:
                await self.client.send_file(
                    entity,
                    CACHED_FILE,
                    caption=CACHED_MSG,
                    reply_to=topic
                )
            else:
                await self.client.send_message(
                    entity,
                    CACHED_MSG,
                    reply_to=topic
                )
            return True

        except FloodWaitError as e:
            await asyncio.sleep(e.seconds)
        except:
            await asyncio.sleep(2)

        return False

    async def loop(self):
        await self.load_message()

        while self.running:
            ok, fail = 0, 0

            for entity, topic in self.entities:
                if not self.running:
                    break

                if await self.send(entity, topic):
                    ok += 1
                else:
                    fail += 1

                await asyncio.sleep(rand_delay(FORWARD_DELAY))

            try:
                await self.client.send_message(
                    LOG_GROUP,
                    f"✅ {ok} | ❌ {fail} | 📦 {len(self.entities)}"
                )
            except:
                pass

            gc.collect()

            await asyncio.sleep(rand_delay(ROUND_DELAY))

    async def start(self):
        await self.client.start()
        me = await self.client.get_me()

        await self.load_entities()  # 🔥 load once

        @self.client.on(events.NewMessage(from_users=me.id))
        async def cmd(e):
            if e.raw_text == "/adstart":
                if not self.running:
                    self.running = True
                    if not self.loop_task or self.loop_task.done():
                        self.loop_task = asyncio.create_task(self.loop())
                    await e.reply("🚀 Started")

            elif e.raw_text == "/adstop":
                self.running = False
                await e.reply("🛑 Stopped")

        await asyncio.Event().wait()


async def main():
    bots = [Bot(s) for s in SESSIONS]
    await asyncio.gather(*[b.start() for b in bots])


if __name__ == "__main__":
    asyncio.run(main())
