import aio_pika
import asyncio
import json
import aiohttp
from app.config import RABBITMQ_URL
import pandas as pd
from app.routes.auth import check_reserve_number

async def consume_reserve_auth_queue():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue('reserve-auth', durable=True)

        async for message in queue:
            async with message.process():
                try:
                    print(f"Received message: {message.body}")
                    props = message.properties

                    message_data = json.loads(message.body)
                    message_data["props"] = {
                        "reply_to": props.reply_to,
                        "correlation_id": props.correlation_id
                    }

                    await check_reserve_number(message_data)
                    
                except Exception as e:
                  print(f"Error processing message: {str(e)}")
                  response_data = {
                      "authSuccess": False,
                      "error": str(e)
                  }
                  print(f"Response: {response_data}")
