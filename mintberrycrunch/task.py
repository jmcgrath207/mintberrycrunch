from mintberrycrunch.subscriber_base import SubscriberBase
from mintberrycrunch.global_state import GlobalState


class Task(SubscriberBase):

    def __init__(self, name: str, global_state: GlobalState, task_dict: dict):
        self.name = name
        subscribe_events = ["Tasks", "Global", f"Task_{self.name}"]
        super().__init__(global_state, subscribe_events)
        groups_list = [task_dict['groups']] \
            if isinstance(task_dict['groups'], str) else task_dict['groups']

        task_dict.pop('groups')
        self.groups = []
        self.attrs = task_dict
        all_group_names = {x.name: x for x in global_state.subscribers['Groups']}
        for group in groups_list:
            if bool(all_group_names.get(group)):
                group = all_group_names.get(group)
                self.groups.append(group)
                # host.new_subscribe_events(f"Group_{self.name}")

    def __repr__(self):
        return f'{self.name}'

    def receive(self, message):
        print(message)
