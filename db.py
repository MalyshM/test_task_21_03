import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dateutil.relativedelta import relativedelta

load_dotenv()
DATABASE_URL = os.getenv("MONGODB_CONN")


def fill_dict(group_type: str):
    id_dict = {
        "year": {"$year": "$dt"},
        "month": {"$month": "$dt"}
    }
    date_from_parts = {
        "year": "$_id.year",
        "month": "$_id.month",
        "day": 1,
        "hour": 0,
        "minute": 0,
        "second": 0,
        "millisecond": 0
    }
    if group_type == "day":
        id_dict["day"] = {"$dayOfMonth": "$dt"}
        date_from_parts["day"] = "$_id.day"
    elif group_type == "hour":
        id_dict["day"] = {"$dayOfMonth": "$dt"}
        id_dict["hour"] = {"$hour": "$dt"}
        date_from_parts["day"] = "$_id.day"
        date_from_parts["hour"] = "$_id.hour"
    else:
        pass
    return id_dict, date_from_parts


def res_scheme(date_from: datetime, date_to: datetime, group: str):
    date_range = []
    value_range = []
    current_date = date_from

    while current_date <= date_to:
        date_str = current_date.strftime('%Y-%m-%dT%H:%M:%S')
        date_range.append(date_str)
        value_range.append(0)
        if group == 'hour':
            current_date += timedelta(hours=1)
        elif group == 'day':
            current_date += timedelta(days=1)
        elif group == 'month':
            current_date += relativedelta(months=1)
    return {'dataset': value_range, 'labels': date_range}


async def agg_data(group_type: str, date_from: datetime, date_to: datetime) -> str:
    try:
        id_dict, date_from_parts = fill_dict(group_type)
        max_pipeline_test = [
            {
                '$match': {
                    '$and': [
                        {'dt': {'$gte': date_from}},
                        {'dt': {'$lte': date_to}}
                    ]
                }
            },
            {
                '$group': {
                    '_id': id_dict,
                    'sum_value': {'$sum': {'$ifNull': ['$value', 0]}}
                }
            },
            {
                '$sort': {
                    '_id': 1
                }
            },
            {
                '$group': {
                    '_id': None,
                    'dataset': {'$push': '$sum_value'},
                    'labels': {'$push': {'$dateToString': {'date': {'$dateFromParts': date_from_parts},
                                                           'format': '%Y-%m-%dT%H:%M:%S'}}},
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'dataset': 1,
                    'labels': 1
                }
            }
        ]
        client = AsyncIOMotorClient(DATABASE_URL)
        cursor = client.sampleDB.sample_collection.aggregate(max_pipeline_test)
        query = await cursor.to_list(length=None)
        result = res_scheme(date_from=date_from, date_to=date_to, group=group_type)
        for data, label in zip(query[0]['dataset'], query[0]['labels']):
            if label in result['labels']:
                result['dataset'][result['labels'].index(label)] = data
        client.close()
        return json.dumps(result)
    except Exception as e:
        print(e)
