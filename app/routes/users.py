from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
import os
import csv
import json
from app.config import ML_CSV_FILE_PATH_HOME, ML_CSV_FILE_PATH_AWAY
from app.utils.helpers import send_to_queue
import pandas as pd

router = APIRouter()

@router.post("/users")
async def create_user(user: dict):
    try:
        # 인코딩 문제 해결을 위해 JSON 데이터를 UTF-8로 처리
        if isinstance(user, bytes):
            user = json.loads(user.decode('utf-8'))
    except Exception as e:
        return JSONResponse(content={"error": f"Invalid JSON data: {str(e)}"}, status_code=400)
    # teamOption에 따라 해당하는 CSV 파일 경로를 선택
    teamSide = user.get("teamSide")
    csv_file_path = ML_CSV_FILE_PATH_HOME if teamSide == "HOME" else ML_CSV_FILE_PATH_AWAY

    # props가 데이터에 포함되어 있는지 확인
    props = user.get('props')
    if not props or not props.get('reply_to') or not props.get('correlation_id'):
        return JSONResponse(content={"error": "Missing properties (reply_to or correlation_id)"}, status_code=400)
    
    # 필수 필드 확인
    required_fields = ["type", "uuid", "gender", "age", "propensity", "propensity1", "propensity2", "propensity3", "propensity4", "propensity5", "propensity6"]
    for field in required_fields:
        if field not in user:
            response_content = {"errorCode": "CRUD-001", "enemyUuid": user["uuid"]}
            await send_to_queue(None, props, response_content)
            return JSONResponse(content=response_content, status_code=400)
    
    # type 필드가 "CREATE"인지 확인
    if user["type"] != "CREATE":
        response_content = {"stateCode": "CRUD-002", "errorMessage": "Wrong Method", "requestType": "CREATE", "userId": user["uuid"]}
        await send_to_queue(None, props, response_content)
        return JSONResponse(content=response_content, status_code=400)

    # type 필드를 제거한 후 나머지 필드만 저장
    user_data_to_save = {k: v for k, v in user.items() if k not in ["type", "props"]}
   
    try:
        # CSV 파일이 존재하는 경우, 중복된 uuid가 있는지 확인
        if os.path.exists(csv_file_path):
            try:
                with open(csv_file_path, mode='r', newline='', encoding='utf-8') as file:
                    reader = csv.DictReader(file)
                    # 4번째 행부터 조회
                    for index, row in enumerate(reader, start=1):
                        if index >= 3 and row["matcherUuid"] == user_data_to_save["uuid"]:
                            response_content = {"errorCode": "CRUD-003", "enemyUuid": user["uuid"]}
                            await send_to_queue(None, props, response_content)
                            return JSONResponse(content=response_content, status_code=400)
            except Exception as e:
                response_content = {"stateCode": "GEN-001", "enemyUuid": user["uuid"]}
                await send_to_queue(None, props, response_content)
                return JSONResponse(content=response_content, status_code=500)
            
    except AssertionError as e:
        response_content = {"stateCode": "GEN-002", "enemyUuid": user["uuid"]}
        await send_to_queue(None, props, response_content)
        return JSONResponse(content=response_content, status_code=500)
    except Exception as e:
        response_content = {"stateCode": "GEN-003", "enemyUuid": user["uuid"]}
        await send_to_queue(None, props, response_content)
        return JSONResponse(content=response_content, status_code=500)

    # UUID가 중복되지 않으면 데이터를 CSV 파일에 저장
    if not os.path.exists(csv_file_path):
        try:
            with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=user_data_to_save.keys())
                writer.writeheader()
                writer.writerow(user_data_to_save)
        except Exception as e:
            response_content = {"stateCode": "GEN-001", "enemyUuid": user["uuid"]}
            await send_to_queue(None, props, response_content)
            return JSONResponse(content=response_content, status_code=500)
    else:
        with open(csv_file_path, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=user_data_to_save.keys())
            writer.writerow(user_data_to_save)
    
    # 동작 성공 시 message_data 생성
    response_content = {"stateCode": "GEN-000", "enemyUuid": user["uuid"]}
    
    await send_to_queue(None, props, response_content)

    print(json.dumps(response_content, ensure_ascii=False, indent=4))
    return JSONResponse(content=response_content, status_code=201)

@router.put("/users")
async def update_user(user: dict):
    # teamOption에 따라 해당하는 CSV 파일 경로를 선택
    teamSide = user.get("teamSide")
    csv_file_path = ML_CSV_FILE_PATH_HOME if teamSide == "HOME" else ML_CSV_FILE_PATH_AWAY
    
    # props가 데이터에 포함되어 있는지 확인
    props = user.get('props')
    if not props or not props.get('reply_to') or not props.get('correlation_id'):
        return JSONResponse(content={"error": "Missing properties (reply_to or correlation_id)"}, status_code=400)
    
    required_fields = ["type", "uuid", "matcherUuid", "age", "gender", "propensity", "propensity1", "propensity2", "propensity3", "propensity4", "propensity5", "propensity6"]
    ## 에러 처리 예시
    for field in required_fields:
        if field not in user:
            response_content = {"stateCode": "CRUD-001", "enemyUuid": user["uuid"]}
            
            await send_to_queue(None, props, response_content)
            return JSONResponse(content=response_content, status_code=400)
        else:
            if user["type"] != "UPDATE":
                response_content = {"stateCode": "CRUD-002", "userId": user["uuid"]}
                
                await send_to_queue(None, props, response_content)
                return JSONResponse(content=response_content, status_code=400)
    
    if not os.path.exists(csv_file_path):
        response_content = {"stateCode": "GEN-001", "enemyUuid": user["uuid"]}
                
        await send_to_queue(None, props, response_content)
        raise HTTPException(status_code=404, detail="File not found")
    
    users = []
    updated = False
    fieldnames = []

    try:
        with open(csv_file_path, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)
            first_row = next(reader)
            second_row = next(reader)
            fieldnames = next(reader)

            reader = csv.DictReader(file, fieldnames=fieldnames)
            for row in reader:
                if row["uuid"] == user["uuid"]:
                    row.update(user)
                    updated = True
                users.append(row)
                
    except AssertionError as e:
        response_content = {"stateCode": "GEN-002", "enemyUuid": user["uuid"]}
        
        await send_to_queue(None, props, response_content)
        return JSONResponse(content=response_content, status_code=500)

    except Exception as e:
        response_content = {"stateCode": "GEN-001", "enemyUuid": user["uuid"]}
                
        await send_to_queue(None, props, response_content)
        return JSONResponse(content={"error": str(e)}, status_code=500)
    
    if not updated:
        response_content = {"stateCode": "CRUD-003", "enemyUuid": user["uuid"]}
                
        await send_to_queue(None, props, response_content)
        return JSONResponse(content=response_content, status_code=404)

    try:
        with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(first_row)
            writer.writerow(second_row)
            writer.writerow(fieldnames)
            for user_row in users:
                writer.writerow([user_row.get(fieldname, '') for fieldname in fieldnames])
    
    except Exception as e:
        response_content = {"stateCode": "GEN-003", "enemyUuid": user["uuid"]}
                
        await send_to_queue(None, props, response_content)
        return JSONResponse(content={"error": str(e)}, status_code=500)

    response_content = {"stateCode": "GEN-000", "enemyUuid": user["uuid"]}

    await send_to_queue(None, props, response_content)
    return JSONResponse(content=response_content, status_code=201)

@router.delete("/users")
async def delete_user(user: dict):
    # teamOption에 따라 해당하는 CSV 파일 경로를 선택
    teamSide = user.get("teamSide")
    csv_file_path = ML_CSV_FILE_PATH_HOME if teamSide == "HOME" else ML_CSV_FILE_PATH_AWAY

    props = user.get('props')
    if not props or not props.get('reply_to') or not props.get('correlation_id'):
        return JSONResponse(content={"error": "Missing properties (reply_to or correlation_id)"}, status_code=400)
    
    # 필수 필드 확인
    required_fields = ["type", "uuid", "matcherUuid", "age", "gender", "propensity", "propensity1", "propensity2", "propensity3", "propensity4", "propensity5", "propensity6"]
    for field in required_fields:
        if field not in user:
            response_content = {"stateCode": "CRUD-001", "enemyUuid": user["uuid"]}
                    
            await send_to_queue(None, props, response_content)
            return JSONResponse(content=response_content, status_code=400)

    # type 필드가 "DELETE"인지 확인
    if user["type"] != "DELETE":
        response_content = {"stateCode": "CRUD-002", "enemyUuid": user["uuid"]}
                
        await send_to_queue(None, props, response_content)
        return JSONResponse(content=response_content, status_code=400)
    
    # CSV 파일이 존재하는 경우, uuid가 일치하는 row를 삭제
    if os.path.exists(csv_file_path):
        try:
            users = []
            row_deleted = False
            with open(csv_file_path, mode='r', newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                fieldnames = reader.fieldnames  # 기존 필드명 저장
                # 4번째 행부터 조회
                for index, row in enumerate(reader, start=1):
                    if index < 3 or row["matcherUuid"] != user["uuid"]:
                        users.append(row)  # 삭제할 row가 아닌 경우 저장
                    else:
                        row_deleted = True  # 일치하는 row가 있음을 표시

            if not row_deleted:
                response_content = {"stateCode": "CRUD-003", "enemyUuid": user["uuid"]}
                        
                await send_to_queue(None, props, response_content)
                return JSONResponse(content=response_content, status_code=404)
            
            # 변경된 데이터를 CSV 파일에 다시 저장
            with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(users)
        except AssertionError as e:
            response_content = {"stateCode": "GEN-002", "enemyUuid": user["uuid"]}
            await send_to_queue(None, props, response_content)
            return JSONResponse(content=response_content, status_code=500)
        
        except Exception as e:
            response_content = {"stateCode": "GEN-001", "enemyUuid": user["uuid"]}
                    
            await send_to_queue(None, props, response_content)
            return JSONResponse(content={"error": str(e)}, status_code=500)
    
    else:
        response_content = {"stateCode": "GEN-001", "enemyUuid": user["uuid"]}
                
        await send_to_queue(None, props, response_content)
        raise HTTPException(status_code=404, detail="CSV file not found.")
    
    # 동작 성공 시 응답 생성
    response_content = {"stateCode": "GEN-000", "enemyUuid": user["uuid"]}

    await send_to_queue(None, props, response_content)
    
    return JSONResponse(response_content, status_code=201)