from aiogram import Router
from aiogram.types import Message
from aiogram.filters import Command
from starlette.exceptions import HTTPException
from datetime import datetime
from db import agg_data
from validators import validate_mes

router = Router()


@router.message(Command("start"))
async def start_handler(msg: Message):
    await msg.answer("Привет! Я помогу тебе узнать твой ID, просто отправь мне любое сообщение")


@router.message()
async def message_handler(msg: Message):
    json_dict = await validate_mes(msg.text)
    if isinstance(json_dict, HTTPException):
        return msg.answer(json_dict.detail)
    resp = await agg_data(group_type=json_dict["group_type"],
                          date_from=datetime.strptime(json_dict["dt_from"], "%Y-%m-%dT%H:%M:%S"),
                          date_to=datetime.strptime(json_dict["dt_upto"], "%Y-%m-%dT%H:%M:%S"))
    await msg.answer(resp)
