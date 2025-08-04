#!/usr/bin/python3

import argparse
import asyncio

import pymysql
import asyncpg

def connect_mysql(args):
  conn = pymysql.connect(
    host = args.fluxbb_host,
    user = args.fluxbb_user,
    password = args.fluxbb_password,
    database = args.fluxbb_db,
    charset = 'utf8mb4',
  )
  return conn

async def connect_pg(args):
  conn = await asyncpg.connect(
    host = args.discourse_host,
    user = args.discourse_user,
    password = args.discourse_password,
    database = args.discourse_db,
  )
  return conn

async def gather_user_data(pg):
  sql = '''
    INSERT INTO fluxbbredir.users
    (fluxbb_user_id, discourse_username)
    SELECT value::integer fluxbb_user_id, username discourse_username
    FROM user_custom_fields
      JOIN users ON (users.id = user_custom_fields.user_id)
    WHERE user_custom_fields.name = 'import_id'
  '''
  await pg.execute(sql)

async def gather_post_data(mysql, pg, fluxbb_prefix):
  sql = f'''
    SELECT id, topic_id FROM {fluxbb_prefix}posts
  '''
  cursor = mysql.cursor()
  cursor.execute(sql)
  fluxbb_data = cursor.fetchall()

  async with pg.transaction():
    sql = '''
      CREATE TEMP TABLE fluxbb_data (
        fluxbb_post_id integer primary key,
        fluxbb_topic_id integer not null
      )
    '''
    await pg.execute(sql)

    sql = '''
      INSERT INTO fluxbb_data
      (fluxbb_post_id, fluxbb_topic_id)
      VALUES ($1, $2)
    '''
    await pg.executemany(sql, fluxbb_data)

    sql = '''
    WITH import_info AS (
      SELECT value::integer fluxbb_post_id, post_id
      FROM post_custom_fields
      WHERE name = 'import_id'
    )
    INSERT INTO fluxbbredir.posts
      (fluxbb_post_id, fluxbb_topic_id,
        discourse_topic_id, discourse_topic_post_number)
      SELECT
        fluxbb_post_id, fluxbb_topic_id,
        posts.topic_id discourse_topic_id,
        posts.post_number discourse_topic_post_number
      FROM fluxbb_data
        JOIN import_info USING (fluxbb_post_id)
        JOIN posts ON (import_info.post_id = posts.id)
    '''
    await pg.execute(sql)

async def async_main(args):
  mysql = connect_mysql(args)
  pg = await connect_pg(args)
  await gather_user_data(pg)
  await gather_post_data(mysql, pg, args.fluxbb_prefix)

def main():
  parser = argparse.ArgumentParser()

  parser.add_argument('--fluxbb-host', required=True, type=str)
  parser.add_argument('--fluxbb-user', required=True, type=str)
  parser.add_argument('--fluxbb-password', required=True, type=str)
  parser.add_argument('--fluxbb-db', required=True, type=str)
  parser.add_argument('--fluxbb-prefix', required=False, type=str, default='')

  parser.add_argument('--discourse-host', required=True, type=str)
  parser.add_argument('--discourse-user', required=True, type=str)
  parser.add_argument('--discourse-password', required=True, type=str)
  parser.add_argument('--discourse-db', required=True, type=str)

  args = parser.parse_args()

  asyncio.run(async_main(args))

if __name__ == '__main__':
  main()
