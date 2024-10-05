from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
import os
import csv
import json
import pandas as pd
from app.config import CSV_FILE_PATH
from app.utils.helpers import send_to_queue

ticket_list = pd.read_csv(CSV_FILE_PATH)

router = APIRouter()

async def check_reserve_number(request: Request):
    data = await request.json()
    
    # props가 데이터에 포함되어 있는지 확인
    props = data.get('props')

    # 만약 props에 reply_to나 correlation_id가 없으면 오류 반환
    if not props or not props.get('reply_to') or not props.get('correlation_id'):
        return JSONResponse(content={"error": "Missing properties (reply_to or correlation_id)"}, status_code=400)
    
    reserve_number = data.get("reserveNumber")
    
    if reserve_number in ticket_list['대표티켓번호'].values:
        response_content = { "authSuccess": True }
        response_content.update(data)
        print(f"Response: {response_content}")
        await send_to_queue(None, props, response_content)
        return JSONResponse(content=response_content, status_code=200)

    else:
        # 티켓번호가 존재하지 않는 경우 오류 처리
        print(f"Error: Ticket number {reserve_number} not found.")
        response_content = {
            "authSuccess": False,
            "error": "Ticket number not found"
        }
        response_content.update(data)
        print(f"Response: {response_content}")
        await send_to_queue(None, props, response_content)
        return JSONResponse(content=response_content, status_code=400)
    
  