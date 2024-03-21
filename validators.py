from starlette import status
from starlette.exceptions import HTTPException
import json
from datetime import datetime


async def validate_mes(msg: str) -> dict | HTTPException:
    try:
        json_data: dict = json.loads(msg)
    except:
        return HTTPException(status_code=status.HTTP_406_NOT_ACCEPTABLE, detail="Сообщение должно быть в формате json")
    if 'dt_from' not in json_data.keys() or 'dt_upto' not in json_data.keys() or 'group_type' not in json_data.keys():
        return HTTPException(status_code=status.HTTP_406_NOT_ACCEPTABLE,
                             detail="В сообщении должны быть ключи dt_from, dt_upto, group_type")
    if not isinstance(json_data['dt_from'], str) or not isinstance(json_data['dt_upto'], str) or not isinstance(
            json_data['group_type'], str):
        return HTTPException(status_code=status.HTTP_406_NOT_ACCEPTABLE, detail="Все поля должны быть str")
    if json_data['group_type'] != 'month' and json_data['group_type'] != 'day' and json_data['group_type'] != 'hour':
        return HTTPException(status_code=status.HTTP_406_NOT_ACCEPTABLE,
                             detail="group_type может быть month, day, hour")
    try:
        isinstance(
            datetime.strptime(json_data['dt_from'], "%Y-%m-%dT%H:%M:%S"), str)
        isinstance(
            datetime.strptime(json_data['dt_upto'], "%Y-%m-%dT%H:%M:%S"), str)
    except:
        return HTTPException(status_code=status.HTTP_406_NOT_ACCEPTABLE,
                             detail="dt_from и dt_upto должны быть в формате по типу 2022-10-01T00:00:00")
    return json_data
