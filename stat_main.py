# https://proglib.io/p/pishem-prostoy-grabber-dlya-telegram-chatov-na-python-2019-11-06

import configparser
import json

from telethon import TelegramClient, sync, events

# from telethon.sync import TelegramClient
from telethon import connection

from datetime import date, datetime
# import sqlite3

from model import main

# Считываем учетные данные
config = configparser.ConfigParser()
config.read("config.ini")

# Присваиваем значения внутренним переменным
api_id = config['Param']['api_id']
api_hash = config['Param']['api_hash']
username = config['Param']['session']
channel = config['Param']['channel']
phone = '+79788784857'
client = TelegramClient(username, api_id, api_hash)

client.connect()
if not client.is_user_authorized():
    client.send_code_request(phone)
    client.sign_in(phone, input('Enter code:'))

with client:
    client.loop.run_until_complete(main(client, channel))
