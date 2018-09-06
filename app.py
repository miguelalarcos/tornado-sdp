import tornado.ioloop
import tornado.web
from sdp import SDP, method, sub, before_insert, can_update, can_insert, Collection
import rethinkdb as r
import time
from datetime import datetime, timezone
import jwt
from tornado import gen

class App(SDP):

    def __init__(self, application, request):
        super().__init__(application, request)
        #self.feeds_with_observers = ['cars_of_color']
        self.laters = {}
        self.shopping_cart = 'mundo-uuid' #r.uuid('mundo')

    @method
    def add(self, a, b):
        return a + b

    @method
    def login(self, encoded_jwt):
        payload = jwt.decode(encoded_jwt, 'secret', algorithms=['HS256'])
        user_id = payload.get('user_id')
        if user_id:
            self.user_id = user_id
        return user_id

    @method
    def logout(self):
        self.close()

    @method
    def reserve_item(self, class_id, quantity):
        uuid = yield self.uuid()
        
        f = lambda item: (item['ref'] == class_id) & (item['state'] == 'store')
        u = {'state': 'reserved', 'user_id': self.user_id, 'order_id': self.shopping_cart, 'reserve_id': uuid} 

        replaced = yield self.update_many('items', f, u, limit=quantity)
        yield self.run(r.table('items').get(class_id).update({'stock': r.row['stock'].default(100) - replaced}))
        self.laters[uuid] = self.call_later(60, self._quit_item, class_id, uuid)

    @method
    def quit_item(self, class_id, uuid):
        yield self._quit_item(class_id, uuid)
        self.laters.pop(uuid)

    @gen.coroutine
    def _quit_item(self, class_id, uuid):
        #yield self.run(r.table('items').filter({'reserve_id': uuid}).update({'state': 'store', 
        #    'user_id': None,'order_id': None, 'reserve_id': None}))
        f = lambda item: item['reserve_id'] == uuid
        u = {'state': 'store', 'user_id': None,'order_id': None, 'reserve_id': None}
        replaced = self.update_many('items', f, u)    
        yield self.run(r.table('items').get(class_id).update({'stock': r.row['stock'] + replaced}))            

    """
    @can_insert('cars')
    def is_logged(self, doc):
        return self.user_id is not None

    @can_update('cars')
    def is_owner(self, doc, old_doc):
        return old_doc['owner'] == self.user_id

    @before_insert('cars')
    def created_at(self, doc):
        doc['created_at'] = datetime.now(timezone.utc)
        doc['owner'] = self.user_id
    """

    @method
    def change_color(self, id, color):
        self.check(id, str)
        self.check(color, str)
        yield self.update('cars', id, {'color': color})

    @method
    def create_car_of_color(self, color, matricula):
        self.check(color, str)
        self.check(matricula, str)
        yield self.insert('cars', {'matricula': matricula, 'color': color})

    @sub
    def cars_of_color(self, color):
        return Collection('cars').filter({'color': color})

    @sub
    def items(self):
        return Collection('items').filter({'type': 'class'})
def make_app():
    return tornado.web.Application([
        (r"/ws", App),
    ], debug=True)


if __name__ == "__main__":
    print('init')
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()