from typing import Union, Any
import asyncio
import asyncpg
from asyncpg.pool import Pool
import configparser

class Database:
    def __init__(self) -> None:
        self.pool: Union[Pool, None] = None
        # self.pool = None
        pass

    async def create(self):
        config = configparser.ConfigParser()
        config.read("config.ini")
        pool = await asyncpg.create_pool(
            user=config['Db']['user'],
            password=config['Db']['password'],
            host=config['Db']['IP'],
            database=config['Db']['database']
        )
        self.pool = pool
        return pool

    async def select_all_users(self):
        # self.pool = await self.create()
        # Замки обычно используются для синхронизации доступа к общим ресурсам.Для каждого источника создается объект
        # Когда вам нужно получить доступ к ресурсу, вызовите acquire для того, чтобы поставить блок
        async with self.pool.acquire() as con:
            async with con.transaction():
                rez = await con.fetch('SELECT * FROM Usersn')
        print(rez)

    async def add_user(self, id: int, name: str):
        sql = "INSERT INTO users_channel (id, name) " \
              "SELECT $1, $2 WHERE NOT EXISTS (SELECT id FROM users_channel WHERE id = $1)"
        try:
            await self.pool.execute(sql, id, name)
        except asyncpg.exceptions.UniqueViolationError:
            pass

    @staticmethod
    def format_args(sql, parameters: dict):
        sql += ' AND '.join([f'{item} = ${num}' for num, item in enumerate(parameters, start=1)])
        return sql, tuple(parameters.values())

    async def select_user(self, **kwargs):
        sql = "SELECT id, name, email, status FROM Usersn WHERE "
        sql, params = self.format_args(sql, kwargs)
        print(sql)
        print(*params)
        return await self.pool.fetchrow(sql, *params)

    async def update_user_status(self, status, id):
        sql = 'UPDATE Usersn SET status = $1 WHERE id = $2'
        return await self.pool.execute(sql, status, id)

    async def delete_user_hubs(self, id):
        sql = 'DELETE FROM Users_hubs WHERE id_user = $1'
        return await self.pool.execute(sql, id)

    # у user изменились хабыб пишем в базу id_user - id_hub->(из hubs_list)
    async def add_user_hubs(self, id: int, hubs_list: list):
        sql = "INSERT INTO Users_hubs(id_user, id_hub) VALUES "
        sql += ', '.join([f'({id}, {num})' for num in hubs_list])
        await self.pool.execute(sql)

    async def add_user_hubs_by_name(self, id: int, hubs_list: list):
        sql = "INSERT INTO Users_hubs(id_user, id_hub) "
        sql += f"(SELECT {id}, id FROM hubs WHERE hub_name IN ("
        sql += ' , '.join([f'\'{num}\'' for num in hubs_list])
        sql += ")"
        sql += ")"
        # print(sql)
        await self.pool.execute(sql)

    # получить list(id - hub_name) из списка
    async def get_post_hubs(self, hubs_names: list):
        sql = "SELECT * FROM hubs WHERE hub_name IN ("
        sql += ' , '.join([f'\'{num}\'' for num in hubs_names])
        sql += ")"
        return await self.pool.fetch(sql)

    # получить спис подходящих юзеров для рассылки нового поста - внутри селект список id тегов поста
    async def get_users_for_post(self, hubs_names: list):
        sql = "SELECT DISTINCT id_user FROM users_hubs WHERE id_hub IN ("
        sql += "SELECT id FROM hubs WHERE hub_name IN ("
        sql += ' , '.join([f"'{num}'" for num in hubs_names])
        sql += ")"
        sql += ")"
        print(sql)
        return await self.pool.fetch(sql)

    async def subscr_all_posts(self, id):
        async with self.pool.acquire() as con:
            # async with con.transaction():
            await self.update_user_status('all_posts', id)
            await self.delete_user_hubs(id)
            await self.add_user_hubs(id, [999])

    # вынимаем покинувших канал
    async def get_users_left_channel(self, lis):
        # покинувших канал - in users_channel
        not_in = "SELECT DISTINCT id FROM users_channel WHERE id not IN ("
        not_in += ' , '.join([f"{num}" for num, nam in lis])
        not_in += ')'
        sql = 'INSERT INTO users_out_channel '
        sql += not_in
        sql += ' ON CONFLICT (id) DO NOTHING'
        print(sql)

        await self.pool.execute(sql)
        # удалить из users_channel покинувших канал
        sql = "DELETE FROM users_channel WHERE id NOT IN ("
        sql += ' , '.join([f"{num}" for num, nam in lis])
        sql += ")"
        print(sql)
        return await self.pool.fetch(sql)

    # добавляем подписчиков канала из lis - список [id, name], если уже есть такой, пропускаем его
    async def renovate_channel_table(self, lis):
        # sql = 'TRUNCATE TABLE users_channel'
        # await self.pool.execute(sql)
        sql = "INSERT INTO users_channel(id, name) VALUES "
        sql += ', '.join([f"({num}, '{nam}')" for num, nam in lis])
        sql += ' ON CONFLICT (id) DO NOTHING'
        print(sql)
        await self.pool.execute(sql)



