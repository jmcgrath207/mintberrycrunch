from mintberrycrunch.subscriber_base import SubscriberBase
from mintberrycrunch.global_state import GlobalState


class Host(SubscriberBase):

    def __init__(self, host_dict: dict, global_state: GlobalState):
        self.name = host_dict.pop('name')
        subscribe_events = ["Hosts", "Global", f"Host_{self.name}"]
        super().__init__(global_state, subscribe_events)
        self.attrs = host_dict
        self.groups = []


    def __repr__(self):
        return f'{self.name}'

    def receive(self, message):
        print(message)

