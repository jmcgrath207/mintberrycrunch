from abc import ABC, abstractmethod
from addict import Dict
from deepmerge import always_merger
from mintberrycrunch.global_state import GlobalState


class SubscribeAbstract(ABC):

    @abstractmethod
    def receive(self, message):
        raise NotImplementedError


class SubscriberBase(SubscribeAbstract):

    def __init__(self, global_state: GlobalState, subscribe_events: list = False):
        self._attrs = Dict()
        self.subscribe_events = subscribe_events
        self.global_state = global_state
        
        if self.subscribe_events:
            for event in self.subscribe_events:
                self.global_state.register(event, self)

    def __del__(self):
        for event in self.subscribe_events:
            self.global_state.unregister(event, self)

    @property
    def attrs(self):
        """I'm the 'x' property."""
        return self._attrs

    @attrs.setter
    def attrs(self, dictionary):
        _attrs = always_merger.merge(dictionary, self._attrs)
        self._attrs = Dict(_attrs)
