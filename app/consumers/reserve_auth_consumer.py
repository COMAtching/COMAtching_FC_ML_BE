import aio_pika
import asyncio
import json
import aiohttp
from app.config import RABBITMQ_URL
import pandas as pd
from app.config import CSV_FILE_PATH

ticket_list = pd.read_csv(CSV_FILE_PATH)

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
                    reserve_number = message_data.get("reserveNumber")

                    if reserve_number in ticket_list['대표티켓번호'].values:
                        ticket_row = ticket_list[ticket_list['대표티켓번호'] == reserve_number].iloc[0]

                        # 총매수가 0보다 크면 티켓을 인증 처리
                        if ticket_row['총매수'] > 0:
                            # 총매수에서 1을 차감
                            ticket_list.loc[ticket_list['대표티켓번호'] == reserve_number, '총매수'] -= 1
                            print(f"Ticket {reserve_number} authenticated, remaining tickets: {ticket_row['총매수'] - 1}")

                            ticket_list.to_csv(CSV_FILE_PATH, index=False)

                            response_data = {
                                "authSuccess": True
                            }

                        else:
                            # 이미 모든 티켓이 인증된 경우 오류 처리
                            print(f"Error: All tickets for {reserve_number} have already been authenticated.")
                            response_data = {
                                "authSuccess": False,
                                "error": "All tickets already authenticated"
                            }
                    else:
                        # 티켓번호가 존재하지 않는 경우 오류 처리
                        print(f"Error: Ticket number {reserve_number} not found.")
                        response_data = {
                            "authSuccess": False,
                            "error": "Ticket number not found"
                        }

                    print(f"Response: {response_data}")

                except Exception as e:
                    print(f"Error processing message: {str(e)}")
                    response_data = {
                        "authSuccess": False,
                        "error": str(e)
                    }
                    print(f"Response: {response_data}")
