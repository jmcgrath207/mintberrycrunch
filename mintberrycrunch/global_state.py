from addict import Dict
from deepmerge import always_merger


class GlobalState:

    def __init__(self):
        events = ["Hosts", "Groups", "Global"]
        self.subscribers = {event: dict() for event in events}
        self._attrs = False

    def register(self, event, who):
        if not bool(self.subscribers.get(event)):
            self.subscribers[event] = []
        if not id(who) in [id(x) for x in self.subscribers[event]]:
            self.subscribers[event].append(who)

    def unregister(self, event, who):
        self.subscribers[event].remove(who)

    def dispatch(self, event, message):
        for who in self.subscribers[event]:
            who.receive(message)


    @property
    def attrs(self):
        """I'm the 'x' property."""
        return self._attrs

    @attrs.setter
    def attrs(self, dictionary):
        _attrs = always_merger.merge(dictionary, self._attrs)
        self._attrs = Dict(_attrs)
