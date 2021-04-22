# классы для работы с каналами
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch
import postgresql2


async def main(client, url_channel):
    # url = 'my_habr_channel'  # input("Введите ссылку на канал или чат: ")
    channel = await client.get_entity(url_channel)

    all_p = await dump_all_participants(channel, client)
    db = postgresql2.Database()
    await db.create()
    all_del = await db.get_users_left_channel(all_p)
    print(all_del)
    # await db.renovate_channel_table(all_p)
    pass


async def dump_all_participants(channel, client):
    """Записывает json-файл с информацией о всех участниках канала/чата"""
    offset_user = 0  # номер участника, с которого начинается считывание
    limit_user = 100  # максимальное число записей, передаваемых за один раз

    all_participants = []  # список всех участников канала
    filter_user = ChannelParticipantsSearch('')

    while True:
        participants = await client(GetParticipantsRequest(channel,
                                                           filter_user, offset_user, limit_user, hash=0))
        if not participants.users:
            break
        all_participants.extend(participants.users)
        offset_user += len(participants.users)

    channel_user_id = list()
    for participant in all_participants:
        print({"id": participant.id,
                                  "first_name": participant.first_name,
                                  "last_name": participant.last_name,
                                  "user": participant.username,
                                  "phone": participant.phone,
                                  "is_bot": participant.bot})
        channel_user_id.append([participant.id, participant.first_name])
    print(channel_user_id)
    return channel_user_id
