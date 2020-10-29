from mintberrycrunch.subscriber_base import SubscriberBase
from mintberrycrunch.global_state import GlobalState


class Group(SubscriberBase):

    def __init__(self, name: str, global_state: GlobalState, group_dict: dict):
        self.name = name
        subscribe_events = ["Groups", "Global", f"Group_{self.name}"]
        super().__init__(global_state, subscribe_events)
        hosts_list = group_dict.pop('hosts')
        self.hosts = []
        self.tasks = []
        self.attrs = group_dict
        all_hosts_names = {x.name: x for x in global_state.subscribers['Hosts']}
        for host in hosts_list:
            if bool(all_hosts_names.get(host['name'])):
                host = all_hosts_names.get(host['name'])
                self.hosts.append(host)
                host.groups.append(self)
                # host.new_subscribe_events(f"Group_{self.name}")

    def __repr__(self):
        return f'{self.name}'

    def receive(self, message):
        print(message)
