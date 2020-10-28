from mintberrycrunch.subscriber_base import SubscriberBase
from mintberrycrunch.global_state import GlobalState
from mintberrycrunch.host import Host

class Group(SubscriberBase):

    def __init__(self, name: str, global_state: GlobalState, group_dict: dict):
        self.name = name
        subscribe_events = ["Groups", "Global", f"Group_{self.name}"]
        super().__init__(global_state, subscribe_events)
        hosts_list = group_dict.pop('hosts')
        hosts = []
        self.attrs = group_dict
        for host in hosts_list:
            hosts.append(Host(host, global_state, self))


        print()


    def __repr__(self):
        return f'{self.name}'

    def receive(self, message):
        print(message)
