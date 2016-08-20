class Observable(object):

    def __init__(self):
        self.observers = set()
        self.changed = False

    def _subscribe(self, value):
        self.observers.add(value)

    def _unsubscribe(self, value):
        self.observers.remove(value)

    def _unsubscribe_all(self):
        self.observers.clear()

    def _notify(self, *args, **kwargs):

        if self.changed:

            for observer in self.observers:
                observer._update(self, *args, **kwargs)

            self.changed = False
