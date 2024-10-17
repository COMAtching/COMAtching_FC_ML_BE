from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
import os
import csv
import subprocess
import json
import pandas as pd
from app.config import ML_CSV_FILE_PATH_HOME, ML_CSV_FILE_PATH_AWAY, ML_FILE_PATH_AWAY, ML_FILE_PATH_HOME
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
        response_content = {"stateCode": "MTCH-001", "message": "Field Missing"}
        await send_to_queue(None, props, response_content)
        return JSONResponse(content={"error": "Missing properties (reply_to or correlation_id)"}, status_code=400)

    # 필수 필드 확인
    required_fields = ["matcherUuid", "myGender", "myAge", "myPropensity", "myPropensity1", "myPropensity2", "myPropensity3", "myPropensity4", "myPropensity5", "myPropensity6", "genderOption", "teamOption"]
    for field in required_fields:
        if field not in data:
            response_content = {"stateCode": "MTCH-001", "message": "Field Missing"}
            await send_to_queue(None, props, response_content)
            return JSONResponse(content=response_content, status_code=400)


    users = []
    updated = False

    if data['teamOption'] == 'HOME':
        csv_file_path = ML_CSV_FILE_PATH_HOME
        ml_file_path = ML_FILE_PATH_HOME
    elif data['teamOption'] == 'AWAY':
        csv_file_path = ML_CSV_FILE_PATH_AWAY
        ml_file_path = ML_FILE_PATH_AWAY
    else:
        response_content = {"stateCode": "MTCH-002", "message": "Invalid TeamOption"}
        await send_to_queue(None, props, response_content)
        return JSONResponse(content=response_content, status_code=400)

    try:
        df = pd.read_csv(csv_file_path, encoding='utf-8')

        if df.empty:
            response_content = {"stateCode": "MTCH-003", "message": "CSV file is empty"}
            await send_to_queue(None, props, response_content)
            return JSONResponse(content=response_content, status_code=404)

        # 데이터 업데이트 (두 번째 행에 데이터를 반영)
        df.iloc[0, 0] = data['matcherUuid']
        df.iloc[0, 1] = data['myGender']
        df.iloc[0, 2] = data['myAge']
        df.iloc[0, 3] = data['myPropensity']
        df.iloc[0, 4] = data['myPropensity1']
        df.iloc[0, 5] = data['myPropensity2']
        df.iloc[0, 6] = data['myPropensity3']
        df.iloc[0, 7] = data['myPropensity4']
        df.iloc[0, 8] = data['myPropensity5']
        df.iloc[0, 9] = data['myPropensity6']
        df.iloc[0, 10] = data['genderOption']

        # 수정된 내용을 CSV 파일에 저장
        df.to_csv(csv_file_path, index=False, encoding='utf-8')

    except Exception as e:
        response_content = {"stateCode": "MTCH-004", "message": "File open fail"}
        await send_to_queue(None, props, response_content)
        response_content.update({"details": str(e)})
        return JSONResponse(content=response_content, status_code=500)

    # ./new/run.py 파일 실행
    try:
        result = subprocess.run(['python', ml_file_path], capture_output=True, text=True)
        if result.returncode != 0:

            response_content = {"stateCode": "MTCH-005", "message": "Error running model"}
            await send_to_queue(None, props, response_content)
            response_content.update({"details": str(e)})
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
            response_content = {"stateCode": "MTCH-006", "message": "Model return error"}
            await send_to_queue(None, props, response_content)
            response_content.update({"details": str(e)})
            return JSONResponse(content=response_content, status_code=500)

    except Exception as e:
        response_content = {"stateCode": "MTCH-005", "message": "Error running model"}
        await send_to_queue(None, props, response_content)
        response_content.update({"details": str(e)})
        return JSONResponse(content=response_content, status_code=500)

    # 추가
    response_content = {"stateCode": "MTCH-000", "message": "Success"}
    # 수정
    response_content.update(recommended_user)
    await send_to_queue(None, props, response_content)

    return JSONResponse(content=response_content, status_code=200)
