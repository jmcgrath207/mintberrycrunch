from mintberrycrunch.subscriber_base import SubscriberBase
from rich.table import Table


class Task(SubscriberBase):

    def __init__(self, order: int, name: str, app: 'SimpleNamespace', task_dict: dict):
        self.order = order
        self.name = name
        self.app = app
        subscribe_events = ["Tasks", "Global", f"Task_{self.name}"]
        super().__init__(app.global_state, subscribe_events)
        self.description = task_dict.pop('description') if task_dict.get('description') else None
        self.script_path = task_dict.pop('script_path')
        self.exec_order = task_dict.pop('exec_order')
        self.conn_type = task_dict.pop('conn_type')
        self.concurrency = task_dict.pop('concurrency')
        self.parallelism = task_dict.pop('parallelism')
        self.host_complete = []
        self.host_skip = []
        self.host_failed = []

        self.global_state.tasks = self.global_state.subscribers['Tasks']
        groups_list = [task_dict['groups']] \
            if isinstance(task_dict['groups'], str) else task_dict['groups']

        task_dict.pop('groups')
        self.groups = []
        self.attrs = task_dict

        all_group_names = {x.name: x for x in app.global_state.subscribers['Groups']}
        for group in groups_list:
            if bool(all_group_names.get(group)):
                group = all_group_names.get(group)
                self.groups.append(group)
                # host.new_subscribe_events(f"Group_{self.name}")

    def render_task_output(self):
        table = Table(title=f"Task: {self.name} | Order: {self.order}", show_header=True, header_style="bold magenta")
        table.add_column("Host")
        table.add_column("Status", justify="left")
        table.add_column("Connection Type", justify="center")

        for x in self.host_complete:
            table.add_row(
                f"{x['host'].name}", "Successful", f"{x['conn_type']}", style="green"
            )

        for x in self.host_failed:
            table.add_row(
                f"{x['host'].name}", "Failed", f"{x['conn_type']}", style="red"
            )
        self.app.console.print(table)

    def __repr__(self):
        return f'{self.name}'

    def receive(self, message):
        print(message)
