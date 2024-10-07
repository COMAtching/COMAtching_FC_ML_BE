from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
import os
import csv
import json
import pandas as pd
from app.config import CSV_FILE_PATH_HOME, CSV_FILE_PATH_AWAY
from app.utils.helpers import send_to_queue

ticket_list_home = pd.read_csv(CSV_FILE_PATH_HOME)
ticket_list_away = pd.read_csv(CSV_FILE_PATH_AWAY)
router = APIRouter()

async def check_reserve_number(data: dict):
    
    # props가 데이터에 포함되어 있는지 확인
    props = data.get('props')
    reserve_number = data["reserveNumber"]
    
    # 만약 props에 reply_to나 correlation_id가 없으면 오류 반환
    if not props or not props.get('reply_to') or not props.get('correlation_id'):
        response_content = { "stateCode": "GEN-001", "authSuccess": False, "teamSide": "NONE" }
        response_content.update(data)
        print(f"Response: {response_content}")
        await send_to_queue(None, props, response_content)
        return JSONResponse(content=response_content, status_code=400)
    
    
    if reserve_number in ticket_list_home['대표티켓번호'].values:
        response_content = { "stateCode": "GEN-000", "authSuccess": True, "teamSide": "HOME" }
        response_content.update(data)
        print(f"Response: {response_content}")
        await send_to_queue(None, props, response_content)
        return JSONResponse(content=response_content, status_code=200)

    else:
        if reserve_number in ticket_list_away['대표티켓번호'].values:
            response_content = { "stateCode": "GEN-000", "authSuccess": True, "teamSide": "AWAY" }
            response_content.update(data)
            print(f"Response: {response_content}")
            await send_to_queue(None, props, response_content)
            return JSONResponse(content=response_content, status_code=200)
        
        else:
            # 티켓번호가 존재하지 않는 경우 오류 처리
            print(f"Error: Ticket number {reserve_number} not found.")
            response_content = {
                "stateCode": "GEN-002",
                "authSuccess": False,
                "teamSide": "NONE"
            }
            response_content.update(data)
            print(f"Response: {response_content}")
            await send_to_queue(None, props, response_content)
            return JSONResponse(content=response_content, status_code=400)
        
    