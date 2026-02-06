import os

import discord
from discord import File


DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
BTC_KISS_PATH = "/root/anzang-discord-bot/btc_kiss.png"
ETH_KISS_PATH = "/root/anzang-discord-bot/eth_kiss.png"
MISSING_CHART_MESSAGE = "暂无相切图表，请稍候再试"

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)


@client.event
async def on_ready() -> None:
    print(f"Logged in as {client.user}")


async def send_kiss_chart(channel: discord.abc.Messageable, symbol: str) -> None:
    chart_paths = {
        "btc": BTC_KISS_PATH,
        "eth": ETH_KISS_PATH,
    }
    chart_path = chart_paths.get(symbol.lower())
    if chart_path is None:
        await channel.send("用法：!kiss btc 或 !kiss eth")
        return

    if not os.path.exists(chart_path):
        await channel.send(MISSING_CHART_MESSAGE)
        return

    await channel.send(file=File(chart_path))


@client.event
async def on_message(message: discord.Message) -> None:
    if message.author == client.user:
        return

    content = message.content.strip().lower()
    if not content.startswith("!kiss"):
        return

    parts = content.split(maxsplit=1)
    if len(parts) == 1:
        await message.channel.send("用法：!kiss btc 或 !kiss eth")
        return

    await send_kiss_chart(message.channel, parts[1])


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise RuntimeError("DISCORD_TOKEN environment variable is required")
    client.run(DISCORD_TOKEN)
