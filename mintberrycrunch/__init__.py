from types import SimpleNamespace
import yaml
from pathlib import Path
from asyncio import BoundedSemaphore, gather
from mintberrycrunch.global_state import GlobalState
from mintberrycrunch.group import Group
from mintberrycrunch.host import Host
from mintberrycrunch.task import Task
from deepmerge import always_merger
import copy
from typing import List

app = SimpleNamespace()


async def init(*args) -> None:
    parser = ParserConfig()
    parser.create_states(args)
    await parser.route_tasks()


class ParserConfig:

    def __init__(self):
        self.normalized_task_list = []

    @staticmethod
    def get_resolve_file(path: str, ref_list: list) -> List[dict]:
        results = []
        for ref in ref_list:
            ref_path = (Path(path).parent / ref).resolve()
            with open(ref_path, 'r') as stream:
                results.append({**{'path': str(ref_path)}, **yaml.safe_load(stream)})
        return results

    def resolve_ref_config(self, config_list: list, no_ref_list: list = None) -> List[dict]:
        results_list = []
        if not no_ref_list:
            no_ref_list = []
        for conf in config_list:

            if bool(conf.get('ref')):
                resolved_list = self.get_resolve_file(conf['path'], conf['ref'])
                results_list.extend(resolved_list)
                conf.pop('ref')

            if bool(conf.get('groups')) and bool(conf.get('groups').get('ref')):
                resolved_list = self.get_resolve_file(conf['path'], conf['groups']['ref'])
                results_list.extend(resolved_list)
                conf['groups'].pop('ref')

            if bool(conf.get('global-state')) and bool(conf.get('global-state').get('ref')):
                resolved_list = self.get_resolve_file(conf['path'], conf['global-state']['ref'])
                results_list.extend(resolved_list)
                conf['global-state'].pop('ref')

            if bool(conf.get('tasks')):
                for task_order, task in enumerate(conf['tasks']):
                    if bool(task.get('ref')):
                        resolved_list = self.get_resolve_file(conf['path'], task['ref'])
                        [y.update({'parent_path': conf['path'], 'ref_location': task_order, 'ref_order': num}) for
                         num, y in enumerate(resolved_list)]
                        results_list.extend(resolved_list)
                        conf['tasks'].pop(task_order)

            no_ref_list.append(conf)

        if not results_list:
            return no_ref_list

        if len(no_ref_list) > 200 or len(results_list) > 200:
            raise RecursionError

        return self.resolve_ref_config(results_list, no_ref_list)

    def deep_merge(self, dict_lists: list) -> dict:
        if len(dict_lists) == 1:
            return dict_lists[0]
        else:
            dict_lists.append(always_merger.merge(dict_lists.pop(), dict_lists.pop()))
            return self.deep_merge(dict_lists)

    def normalize(self, config_list: list) -> dict:
        temp_list = []
        config_list = self.resolve_ref_config(config_list)
        for config in config_list:
            if bool(config.get('groups')):
                temp_list.append(self.normalize_groups(config))
            if bool(config.get('tasks')):
                self.collect_normalize_tasks(config)

        temp_list.append(self.order_normalize_task())

        config_dict = self.deep_merge(temp_list)
        del config_dict['path']
        return config_dict

    def collect_normalize_tasks(self, config_dict: dict):
        tasks_list = config_dict.pop('tasks')
        tasks_list = [x for x in tasks_list if type(x) == dict]
        temp_tasks_dict = {}
        for task in tasks_list:
            for key, value in task.items():
                temp_tasks_dict[key] = value
                script_path = (Path(config_dict['path']).parent / value.get('script_path')).resolve()
                if script_path.exists():
                    temp_tasks_dict[key]['script_path'] = script_path
                else:
                    raise FileNotFoundError
        # config_dict['tasks'] = temp_tasks_dict
        self.normalized_task_list.append({**config_dict, **temp_tasks_dict})

    def find_all_paths(self, y: list, results=None):
        if not results:
            results = []
        research = []

        found = False
        for search_list in y:
            search_list = [x['path'] for x in self.normalized_task_list if search_list == x.get('parent_path')]
            if search_list:
                found = True
                for z in search_list:
                    results.append(z)
                    research.append(z)

        if not found:
            return results
        else:
            return self.find_all_paths(research, results)

    def order_normalize_task(self) -> dict:
        remove_keys = ['path', 'parent_path', 'ref_location', 'ref_order', 'global-state']

        master_task = [x for x in self.normalized_task_list if not x.get('parent_path')][0]
        parent_dicts = {}
        count = 0

        task = master_task
        # Sort tasks by file path
        while True:

            parent_dicts[task['path']] = [x for x in self.normalized_task_list if task['path'] == x.get('parent_path')]
            if count < len(self.normalized_task_list):
                task = self.normalized_task_list[count]
                count += 1
            else:
                break

        parent_dicts = {key: value for (key, value) in parent_dicts.items() if value}
        order_file_task_list = []
        for key, value in parent_dicts.items():
            order_file_task_list.append(key)
            for y in value:
                order_file_task_list.append(y['path'])
                order_file_task_list.extend(self.find_all_paths([y['path']]))

        order_file_path_task_list = list(dict.fromkeys(order_file_task_list))

        # for x in order_file_path_task_list:
        #    order_file_task_list.append([y for y in self.normalized_task_list if y['path'] == x][0])

        # Sort by task ref location and order
        temp_order_list = []
        for task_path in order_file_path_task_list:
            task = [y for y in self.normalized_task_list if y['path'] == task_path][0]
            sort_ref_order_child_path = {}
            ref_loc_child_paths = {}
            if bool(task.get('ref_location')):
                if not bool(ref_loc_child_paths.get(task['ref_location'])):
                    ref_loc_child_paths[task['ref_location']] = []
                ref_loc_child_paths[task['ref_location']].append(task)
                for key, value in ref_loc_child_paths.items():
                    sort_ref_order_child_path[key] = sorted(value, key=lambda k: k['ref_order'])
                complete_sort = {k: v for k, v in sorted(sort_ref_order_child_path.items(), key=lambda item: item[1])}
                for x in complete_sort.values():
                    for y in x:
                        temp_order_list.append(copy.copy(y))
                        self.normalized_task_list.remove(y)
            else:
                temp_order_list.append(task)

        global_state_list = [x['global-state'] for x in temp_order_list if bool(x.get('global-state'))]

        # Reduce keys dicts in list to remove un needed keys
        results = [{key: value for (key, value) in x.items()
                    if key not in remove_keys} for x in temp_order_list]

        temp_dict = {}
        temp_dict['tasks'] = results
        temp_dict['global-state'] = self.deep_merge(global_state_list)

        return temp_dict

    @staticmethod
    def normalize_groups(config_dict: dict):
        group_dict = config_dict.pop('groups')
        temp_group_dict = {}
        for key, value in group_dict.items():
            hosts = value.pop('hosts')
            temp_group_dict[key] = value
            temp_hosts_list = []
            for host in hosts:
                if isinstance(host, str):
                    if bool(temp_group_dict.get(host)):
                        temp_hosts_list.extend(temp_group_dict[host]['hosts'])
                    else:
                        temp_hosts_list.append({'name': host, 'address': host})
                else:
                    temp_hosts_list.append(host)
            temp_group_dict[key]['hosts'] = temp_hosts_list

        config_dict['groups'] = temp_group_dict
        return config_dict

    def create_states(self, args):
        app.global_state = GlobalState()

        temp_list = []
        for x in args:
            with open(x, 'r') as stream:
                temp_list.append({**{'path': x}, **yaml.safe_load(stream)})
        config_dict = self.normalize(temp_list)
        app.global_state.attrs = config_dict.pop('global-state')
        all_hosts = []
        for key, value in config_dict["groups"].items():
            all_hosts.extend(value['hosts'])

        reduce_duplicates_hosts = copy.deepcopy([i for n, i in enumerate(all_hosts) if i not in all_hosts[n + 1:]])

        for host in reduce_duplicates_hosts:
            Host(host, app.global_state)

        for key, value in config_dict["groups"].items():
            Group(key, app.global_state, value)

        task_order = 1
        for tasks in config_dict["tasks"]:
            for key, value in tasks.items():
                Task(task_order, key, app.global_state, value)
                task_order += 1

        print()

    @staticmethod
    async def concurrency_limit_semaphore(semaphore: BoundedSemaphore, func: callable):
        async with semaphore:
            return await func

    def build_func_parms(self, task_name: str, task_meta: dict, run_func: callable) -> callable:
        for host_name, host_address in app.hosts.get(task_meta['host_group']).items():
            host = {"host_name": host_name, "host_address": host_address}
            if app.env_vars:
                task_vars = app.env_vars.get('tasks') or {}
                task_vars = task_vars.get(task_name)
                host_vars = app.env_vars.get('hosts') or {}
                host_vars = host_vars.get(task_meta['host_group']) or {}
                global_vars = app.env_vars.get('global') or {}

                env_vars = {**task_vars, **host_vars, **global_vars}
            else:
                env_vars = {}

            yield self.concurrency_limit_semaphore(semaphore=BoundedSemaphore(task_meta["conn_limit"]),
                                                   func=run_func(script_path=task_meta["script_path"],
                                                                 host=host, env_vars=env_vars))

    async def route_tasks(self):
        for task_name, task_meta in app.tasks.items():
            task_meta['script_path'] = Path(app.tasks_path).joinpath(task_meta['script_path']).resolve()
            run_func = getattr(app, f'{task_meta["conn_type"]}_{task_meta["exec_order"]}')
            results = [x for x in await gather(*self.build_func_parms(task_name, task_meta, run_func))]
            print()


async def close_init():
    pass
