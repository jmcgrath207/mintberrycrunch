from abc import ABC, abstractmethod
from mintberrycrunch.base import Base

from mintberrycrunch.global_state import GlobalState


class SubscribeAbstract(ABC):

    @abstractmethod
    def receive(self, message):
        raise NotImplementedError


class SubscriberBase(SubscribeAbstract, Base):

    def __init__(self, global_state: GlobalState, subscribe_events: list = False):

        self.subscribe_events = subscribe_events
        self.global_state = global_state
        
        if self.subscribe_events:
            for event in self.subscribe_events:
                self.global_state.register(event, self)

    def __del__(self):
        for event in self.subscribe_events:
            self.global_state.unregister(event, self)


    def new_subscribe_events(self, event):
        self.subscribe_events.append(event)
        self.global_state.register(event, self)