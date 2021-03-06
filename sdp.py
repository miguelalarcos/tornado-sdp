# SDP: Subscription Data Protocol

import tornado
import tornado.ioloop
import tornado.websocket
from tornado.queues import Queue
#import time
import rethinkdb as r
from tornado import gen
import json
from datetime import datetime
import pytz

r.set_loop_type("tornado")
#https://www.rethinkdb.com/docs/async-connections/


methods = []


def method(f):
    methods.append(f.__name__)
    return gen.coroutine(f)

subs = []


def sub(f):
    subs.append(f.__name__)
    return f

hooks = {'before_insert': [],
         'before_update': []
         }

def before_insert(collection=None):
  def decorator(f):
    def helper(self, coll, doc):
      if collection == coll or collection is None:
        f(self, doc)
    hooks['before_insert'].append(helper)
    return f # does not matter, it's not going to be used directly, but helper in hooks
  return decorator

def before_update(collection=None):
  def decorator(f):
    def helper(self, coll, doc):
      if collection == coll or collection is None:
        f(self, doc)
    hooks['before_update'].append(helper)
    return f # does not matter, it's not going to be used directly, but helper in hooks
  return decorator


class MethodError(Exception):
  pass

class CheckError(Exception):
  pass

can = {'update': [], 'insert': [], 'delete': []}

def can_insert(table):
    def decorate(f):
        def helper(self, t, doc):
            if t == table:
                return f(self, doc)
            else:
                return True
        can['insert'].append(helper)
        return f # does not matter f or helper, it's not going to be used directly
    return decorate

def can_update(table):
    def decorate(f):
        def helper(self, t, doc, old_doc):
            if t == table:
                return f(self, doc, old_doc)
            else:
                return True
        can['update'].append(helper)
        return f # does not matter f or helper, it's not going to be used directly
    return decorate

def can_delete(table):
    def decorate(f):
        def helper(self, t, old_doc):
            if t == table:
                return f(self, old_doc)
            else:
                return True
        can['delete'].append(helper)
        return f # does not matter f or helper, it's not going to be used directly
    return decorate

class Collection:
    def __init__(self, table):
        self.table = table
        self._filter = r.table(table)

    def __getattr__(self, name):
        def helper(*args, **kwargs):
            self._filter = getattr(self._filter, name)(*args, **kwargs)
            return self
        return helper


class SDP(tornado.websocket.WebSocketHandler):

    def check_origin(self, origin):
        return True

    def __init__(self, application, request):
        super().__init__(application, request)
        self.conn = r.connect(host='db', port=28015, db='test')
        self.registered_feeds = {}
        self.feeds_with_observers = []
        self.queue = Queue(maxsize=10)
        self.user_id = 'miguel.alarcos@gmail.com' #None
        self.remove_observer_from_item = {}
        tornado.ioloop.IOLoop.current().spawn_callback(self.consumer)

    def call_later(self, delay, f, *args, **kwargs):
        return tornado.ioloop.IOLoop.current().call_later(delay, f, *args, **kwargs)

    @gen.coroutine
    def uuid(self):
        conn = yield self.conn
        ui = yield r.uuid().run(conn)
        return ui    

    @gen.coroutine
    def run(self, query):
        conn = yield self.conn
        yield query.run(conn)

    def check(self, attr, type):
      if not isinstance(attr, type):
        raise CheckError(attr + ' is not of type ' + str(type))

    @gen.coroutine
    def feed(self, sub_id, query, name):
        #query = query.filter(~r.row.has_fields('deleted'))
        print('ini of feed')
        conn = yield self.conn
        print('connection getted')
        #feed = yield query.changes(include_initial=True, include_states=True)._filter.run(conn)
        feed = yield query._filter.changes(include_initial=True, include_states=True).run(conn)
        self.registered_feeds[sub_id] = feed
        while (yield feed.fetch_next()):
            item = yield feed.next()
            print('item >', item)
            state = item.get('state')
            #if state == 'ready' or state == 'initializing':
            if state == 'ready':
                self.send_ready(sub_id)
            elif state == 'initializing':
                self.send_initializing(sub_id, query.table)
            else:
                if item.get('old_val') is None:
                    if name in self.feeds_with_observers:
                        new_item_id = item['new_val']['id']
                        yield r.table(query.table).get(new_item_id).update({'__count': r.row['__count'].default(0) + 1}).run(conn)
                        def helper_remove(id):
                            def helper():
                                @gen.coroutine
                                def aux():
                                    yield r.table(query.table).get(id).update({'__count': r.row['__count'] - 1}).run(conn)
                                tornado.ioloop.IOLoop.current().spawn_callback(aux) 
                            return helper
                        self.remove_observer_from_item.setdefault(sub_id, {})[new_item_id] = helper_remove(new_item_id) 
                    self.send_added(query.table, sub_id, item['new_val'])
                elif item.get('new_val') is None:
                    old_item_id = item['old_val']['id']
                    remove = self.remove_observer_from_item[sub_id].pop(old_item_id, None)
                    if remove:
                        remove() 
                    self.send_removed(query.table, sub_id, item['old_val']['id'])
                else:
                    self.send_changed(query.table, sub_id, item['new_val'])

    def send(self, data):
        def helper(x):
            if(isinstance(x, datetime)):
                return {'$date': x.timestamp()*1000}
            else:
                return x
        self.write_message(json.dumps(data, default=helper))

    def send_result(self, id, result):
        self.send({'msg': 'result', 'id': id, 'result': result})

    def send_error(self, id, error):
        self.send({'msg': 'error', 'id': id, 'error': error})

    def send_added(self, table, sub_id, doc):
        self.send({'msg': 'added', 'table': table, 'id': sub_id, 'doc': doc})

    def send_changed(self, table, sub_id, doc):
        self.send({'msg': 'changed', 'table': table, 'id': sub_id, 'doc': doc})

    def send_removed(self, table, sub_id, doc_id):
        self.send({'msg': 'removed', 'table': table, 'id': sub_id, 'doc_id': doc_id})

    def send_ready(self, sub_id):
        self.send({'msg': 'ready', 'id': sub_id})

    def send_initializing(self, sub_id, table):
        self.send({'msg': 'initializing', 'id': sub_id, 'table': table})    

    def send_nosub(self, sub_id, error):
        self.send({'msg': 'nosub', 'id': sub_id, 'error': error})

    def send_nomethod(self, method_id, error):
        self.send({'msg': 'nomethod', 'id': method_id, 'error': error})

    def on_open(self):
        print('open')

    @gen.coroutine
    def on_message(self, msg):
        yield self.queue.put(msg)

    def on_message_(self, msg):
        #print('raw ->', msg)
        @gen.coroutine
        def helper(msg):
            yield self.queue.put(msg)
        tornado.ioloop.IOLoop.current().spawn_callback(helper, msg)

    # consumer can be recoded as:
    # http: // www.tornadoweb.org / en / stable / queues.html?highlight = queue
    @gen.coroutine
    def consumer(self): # all data gets must go inside a try
        while True:
            msg = yield self.queue.get()
            if msg == 'stop':
                return
            def helper(dct):
                if '$date' in dct.keys():
                    d = datetime.utcfromtimestamp(dct['$date']/1000.0)
                    return d.replace(tzinfo=pytz.UTC)
                return dct
            data = json.loads(msg, object_hook=helper)
            #print(data)
            try:
                message = data['msg']
                id = data['id']

                if message == 'method':
                    params = data['params']
                    method = data['method']
                    if method not in methods:
                        self.send_nomethod(id, 'method does not exist')
                    else:
                        #try:
                          method = getattr(self, method)
                          result = yield method(**params)
                          self.send_result(id, result)
                        #except Exception as e:
                        #  self.send_error(id, str(e) + ':' + str(e.__traceback__))
                elif message == 'sub':
                    name = data['name']
                    params = data['params']
                    if name not in subs:
                        self.send_nosub(id, 'sub does not exist')
                    else:
                        query = getattr(self, name)(**params)
                        tornado.ioloop.IOLoop.current().spawn_callback(self.feed, id, query, name)
                elif message == 'unsub':
                    feed = self.registered_feeds[id]
                    feed.close()
                    if self.remove_observer_from_item.get(id):
                        for remove in self.remove_observer_from_item[id].values():
                            remove()
                        del self.remove_observer_from_item[id]
                    del self.registered_feeds[id]
            except KeyError as e:
              self.send_error(id, str(e))
            finally:
              self.queue.task_done()

    def on_close(self):
        print('close')
        for k in self.remove_observer_from_item.keys():
            for remove in self.remove_observer_from_item[k].values():
                remove()
        for feed in self.registered_feeds.values():
            feed.close()

        @gen.coroutine
        def helper(): # is it possible to call self.queue.put directly?
            self.queue.put('stop')
        tornado.ioloop.IOLoop.current().spawn_callback(helper)

    @gen.coroutine
    def insert(self, table, doc):
        cans = [c(self, table, doc) for c in can['insert']]
        if not all(cans):
            raise MethodError('can not insert ' + table)
        else:
            self.before_insert(table, doc)
            conn = yield self.conn
            result = yield r.table(table).insert(doc).run(conn)
            # self.after_insert()

    def before_insert(self, collection, doc):
        for hook in hooks['before_insert']:
          hook(self, collection, doc)

    @gen.coroutine
    def update(self, table, id, doc):
        conn = yield self.conn
        old_doc = yield r.table(table).get(id).run(conn)
        cans = [c(self, table, doc, old_doc) for c in can['update']]
        if not all(cans):
            raise MethodError('can not update ' + table + ', id: ' + str(id))
        else:
            self.before_update(table, doc)
            result = yield r.table(table).get(id).update(doc).run(conn)
            #self.after_update()

    def before_update(self, collection, subdoc):
        for hook in hooks['before_update']:
          hook(self, collection, subdoc)

    @gen.coroutine
    def soft_delete(self, table, id):
      conn = yield self.conn
      old_doc = yield r.table(table).get(id).run(conn)
      cans = [c(self, table, old_doc) for c in can['delete']]
      if not all(cans):
        raise MethodError('can not delete ' + table + ', id: ' + str(id))
      else:
        result = yield r.table(table).get(id).update({'deleted': True}).run(conn)

    @gen.coroutine
    def update_many(self, table, f, u, limit=None):
        conn = yield self.conn
        result = 0
        if limit:
            result = yield r.table(table).filter(f).limit(limit).update(lambda item: r.branch(f(item), u, {})).run(conn)
        else:
            result = yield r.table(table).filter(f).update(lambda item: r.branch(f(item), u, {})).run(conn)
        return result['replaced']