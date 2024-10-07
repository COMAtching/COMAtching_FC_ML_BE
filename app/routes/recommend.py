from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
import os
import csv
import subprocess
import json
import pandas as pd
from app.config import CSV_FILE_PATH_HOME, CSV_FILE_PATH_AWAY, ML_FILE_PATH_AWAY, ML_FILE_PATH_HOME
from app.utils.helpers import send_to_queue

router = APIRouter()

@router.post("/recommend")
async def recommend_user(request: Request):
    data = await request.json()
    csv_file_path = ""
    ml_file_path = ""

    # props가 데이터에 포함되어 있는지 확인
    props = data.get('props')

    # 만약 props에 reply_to나 correlation_id가 없으면 오류 반환
    if not props or not props.get('reply_to') or not props.get('correlation_id'):
        response_content = {"stateCode": "CRUD-001", "message": "Field Missing"}
        response_content.update(data)
        await send_to_queue(None, props, response_content)
        return JSONResponse(content={"error": "Missing properties (reply_to or correlation_id)"}, status_code=400)
    
    # 필수 필드 확인
    required_fields = ["matcherUuid", "myGender", "myAge", "myPropensity", "myPropensity1", "myPropensity2", "myPropensity3", "myPropensity4", "myPropensity5", "myPropensity6", "genderOption", "teamOption"]
    for field in required_fields:
        if field not in data:
            response_content = {"stateCode": "CRUD-001", "message": "Field Missing"}
            response_content.update(data)
            await send_to_queue(None, props, response_content)
            return JSONResponse(content=response_content, status_code=400)
    
    
    users = []
    updated = False
    
    if data['teamOption'] == 'HOME':
        csv_file_path = CSV_FILE_PATH_HOME
        ml_file_path = ML_FILE_PATH_HOME
    elif data['teamOption'] == 'AWAY':
        csv_file_path = CSV_FILE_PATH_AWAY
        ml_file_path = ML_FILE_PATH_AWAY
    else:
        response_content = {"stateCode": "CRUD-002", "message": "Invalid TeamOption"}
        response_content.update(data)
        await send_to_queue(None, props, response_content)
        return JSONResponse(content=response_content, status_code=400)
    
    try:
        df = pd.read_csv(csv_file_path)
        
        if df.empty:
            response_content = {"errorCode": "GEN-004", "errorMessage": "CSV file is empty"}
            response_content.update(data)
            await send_to_queue(None, props, response_content)
            return JSONResponse(content=response_content, status_code=404)
            
        # 데이터 업데이트 (두 번째 행에 데이터를 반영)
        df.at[1][0] = data['matcherUuid']
        df.at[1][1] = data['myGender']
        df.at[1][2] = data['myAge']
        df.at[1][3] = data['myPropensity']
        df.at[1][4] = data['myPropensity1']
        df.at[1][5] = data['myPropensity2']
        df.at[1][6] = data['myPropensity3']
        df.at[1][7] = data['myPropensity4']
        df.at[1][8] = data['myPropensity5']
        df.at[1][9] = data['myPropensity6']
        df.at[1][10] = data['genderOption']

        # 수정된 내용을 CSV 파일에 저장
        df.to_csv(csv_file_path, index=False)

    except Exception as e:
        response_content = {"errorCode": "GEN-001", "errorMessage": "File open fail"}
        response_content.update(data)
        await send_to_queue(None, props, response_content)
        return JSONResponse(content=response_content, status_code=500)

    # ./new/run.py 파일 실행
    try:
        result = subprocess.run(['python', ml_file_path], capture_output=True, text=True)
        if result.returncode != 0:
            response_content = {"error": "GEN-002", "message": "Error running main7.py", "details": result.stderr}
            response_content.update(data)
            await send_to_queue(None, props, response_content)
            return JSONResponse(content=response_content, status_code=500)

        recommended_candidate = result.stdout.strip()

        if 'Top 1 Similar Person:' in recommended_candidate:
            start_index = recommended_candidate.find('Top 1 Similar Person:') + len('Top 1 Similar Person:')
            recommended_user_data = recommended_candidate[start_index:].strip()
            
            lines = recommended_user_data.split('\n')
            user_info = {}
            for line in lines:
                if line.strip():
                    key_value = line.split(maxsplit=1)
                    if len(key_value) == 2:
                        key, value = key_value
                        user_info[key.strip()] = value.strip()

            user_index = user_info.get('matcherUuid')
            if user_index:
                recommended_user = {"enemyUuid": user_index}

            else:
                recommended_user = {}

        else:
            response_content = {"errorCode": "GEN-002", "errorMessage": "assert fail"}
            response_content.update(data)
            await send_to_queue(None, props, response_content)
            return JSONResponse(content=response_content, status_code=500)

    except Exception as e:
        response_content = {"errorCode": "GEN-002", "errorMessage": "assert fail"}
        response_content.update(data)
        await send_to_queue(None, props, response_content)
        return JSONResponse(content=response_content, status_code=500)
    
    # 추가
    response_content = {"errorCode": "GEN-000", "errorMessage": "Success"}
    # 수정
    response_content.update(recommended_user)
    await send_to_queue(None, props, response_content)
    
    return JSONResponse(content=response_content, status_code=200)