import os
from aiohttp import web
from botbuilder.core import (
    BotFrameworkAdapterSettings,
    TurnContext,
    BotFrameworkAdapter,
)
from botbuilder.schema import Activity
from botbuilder.core.integration import aiohttp_error_middleware
from botbuilder.core import MemoryStorage, UserState, ConversationState

from bot_logic import MyBot  # Import your bot logic from a separate file

# Create adapter and bot
settings = BotFrameworkAdapterSettings(os.getenv("MICROSOFT_APP_ID"), os.getenv("MICROSOFT_APP_PASSWORD"))
adapter = BotFrameworkAdapter(settings)

# Catch-all for errors.
async def on_error(context: TurnContext, error: Exception):
    await context.send_activity("Sorry, it looks like something went wrong.")
    raise error

adapter.on_turn_error = on_error

# Create the bot
memory_storage = MemoryStorage()
user_state = UserState(memory_storage)
conversation_state = ConversationState(memory_storage)
bot = MyBot(conversation_state, user_state)

async def messages(req: web.Request) -> web.Response:
    # Main bot message handler.
    if "application/json" in req.headers["Content-Type"]:
        body = await req.json()
    else:
        return web.Response(status=415)

    activity = Activity().deserialize(body)
    auth_header = req.headers["Authorization"] if "Authorization" in req.headers else ""

    response = await adapter.process_activity(activity, auth_header, bot.on_turn)
    if response:
        return web.json_response(data=response.body, status=response.status)
    return web.Response(status=201)

app = web.Application(middlewares=[aiohttp_error_middleware])
app.router.add_post("/api/messages", messages)

if __name__ == "__main__":
    web.run_app(app, host="localhost", port=3978)
