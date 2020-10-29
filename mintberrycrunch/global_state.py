from addict import Dict
from deepmerge import always_merger
from mintberrycrunch.base import Base


class GlobalState(Base):

    def __init__(self):
        default_events = ["Hosts", "Groups", "Tasks", "Global"]
        self.subscribers = {event: dict() for event in default_events}
        self.tasks = None
        self.groups = None
        self.hosts = None

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
