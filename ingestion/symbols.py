import asyncio
import aiohttp
from pprint import pprint


EXCHANGE_INFO_URL = "https://testnet.binancefuture.com/fapi/v1/exchangeInfo"

async def get_all_symbols():
    async with aiohttp.ClientSession() as session:
        async with session.get(EXCHANGE_INFO_URL) as resp:
            data = await resp.json()

    symbols = [
        s["symbol"].lower()
        for s in data["symbols"]
        if s["contractType"] == "PERPETUAL" and s["status"] == "TRADING"
    ]

    pprint(symbols)

asyncio.run(get_all_symbols())