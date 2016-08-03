class Observable(object):

    def __init__(self):
        self.observers = set()
        self.changed = False

    def subscribe(self, value):
        self.observers.add(value)

    def unsubscribe(self, value):
        self.observers.remove(value)

    def unsubscribe_all(self):
        self.observers.clear()

    def notify(self, *args, **kwargs):

        if self.changed:

            for observer in self.observers:
                observer.update(self, *args, **kwargs)

            self.changed = False
