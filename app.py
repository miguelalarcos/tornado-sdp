import tornado.ioloop
import tornado.web
from sdp import SDP, method, sub, before_insert, can_update, can_insert, Collection
import rethinkdb as r
import time
from datetime import datetime, timezone
import jwt

class App(SDP):

    def __init__(self, application, request):
        super().__init__(application, request)
        self.feeds_with_observers = ['cars_of_color']

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

def make_app():
    return tornado.web.Application([
        (r"/ws", App),
    ], debug=True)


if __name__ == "__main__":
    print('init')
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()