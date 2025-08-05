#!/usr/bin/python3

import logging

from aiohttp import web
import asyncpg
from yarl import URL

import config

logger = logging.getLogger(__name__)
KEY_DB = web.AppKey('db', asyncpg.Pool)

async def viewtopic(request):
  db = request.app[KEY_DB]

  if tid := request.query.get('id'):
    topic = await get_topic_by_tid(db, int(tid))
    url = f'{config.forum_url}t/topic/{topic}'
  elif pid := request.query.get('pid'):
    topic, num = await get_topic_by_pid(db, int(pid))
    url = f'{config.forum_url}t/topic/{topic}/{num}'
  else:
    url = config.forum_url

  url = redirect_from(request.url, url)
  raise web.HTTPFound(url)

async def get_topic_by_tid(db, tid):
  async with db.acquire() as conn, conn.transaction():
    sql = '''SELECT discourse_topic_id FROM posts
             WHERE fluxbb_topic_id = $1 LIMIT 1'''
    rs = await conn.fetch(sql, tid)
    if not rs:
      raise web.HTTPNotFound()

    return rs[0][0]

async def get_topic_by_pid(db, pid):
  async with db.acquire() as conn, conn.transaction():
    sql = '''SELECT discourse_topic_id, discourse_topic_post_number
             FROM posts
             WHERE fluxbb_post_id = $1 LIMIT 1'''
    rs = await conn.fetch(sql, pid)
    if not rs:
      raise web.HTTPNotFound()

    return rs[0]

async def profile(request):
  db = request.app[KEY_DB]

  if uid := request.query.get('id'):
    username = await get_username_by_uid(db, int(uid))
    url = f'{config.forum_url}u/{username}'
  else:
    url = config.forum_url

  url = redirect_from(request.url, url)
  raise web.HTTPFound(url)

async def get_username_by_uid(db, uid):
  async with db.acquire() as conn, conn.transaction():
    sql = '''SELECT discourse_username FROM users
             WHERE fluxbb_user_id = $1 LIMIT 1'''
    rs = await conn.fetch(sql, uid)
    if not rs:
      raise web.HTTPNotFound()

    return rs[0][0]

async def noredir(request):
  r = web.Response(status=302)
  r.headers['Location'] = '/'
  r.set_cookie('noredir', '1', httponly=True, samesite='Strict', max_age=86400 * 7)
  return r

async def yesredir(request):
  r = web.Response(status=302)
  r.headers['Location'] = '/'
  r.del_cookie('noredir')
  return r

async def default(request):
  url = redirect_from(
    request.url,
    config.forum_url + request.path_qs.lstrip('/'),
  )
  raise web.HTTPFound(url)

def redirect_from(url, to):
  u = URL(to) % {
    # aiohttp doesn't use X-Forwarded-Proto and we only use https, so change
    # to https always
    'redirected_from': str(url.with_scheme('https')),
  }
  return str(u)

async def init_db(app):
  app[KEY_DB] = await asyncpg.create_pool(config.db_url, setup=conn_init, min_size=0)
  yield
  await app[KEY_DB].close()

async def conn_init(conn):
  await conn.execute("set search_path to 'fluxbbredir'")

def setup_app(app):
  app.cleanup_ctx.append(init_db)

  app.router.add_get('/viewtopic.php', viewtopic)
  app.router.add_get('/profile.php', profile)
  app.router.add_get('/noredir', noredir)
  app.router.add_get('/yesredir', yesredir)
  app.router.add_get('/{p:.*}', default)

def main():
  import argparse

  from nicelogger import enable_pretty_logging

  parser = argparse.ArgumentParser(
    description = 'fluxbb-to-discourse redirector',
  )
  parser.add_argument('--port', default=9009, type=int,
                      help='port to listen on')
  parser.add_argument('--ip', default='127.0.0.1',
                      help='address to listen on')
  parser.add_argument('--loglevel', default='info',
                      choices=['debug', 'info', 'warn', 'error'],
                      help='log level')
  args = parser.parse_args()

  enable_pretty_logging(args.loglevel.upper())

  app = web.Application()
  setup_app(app)

  web.run_app(app, host=args.ip, port=args.port)

if __name__ == '__main__':
  main()

