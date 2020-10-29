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

app = SimpleNamespace()


async def init(*args) -> None:
    create_states(args)
    await route_tasks()


def normalize(config_list: list) -> dict:
    temp_list = []
    for config in config_list:
        if bool(config.get('groups')):
            config = normalize_groups(config)
        if bool(config.get('tasks')):
            config = normalize_tasks(config)
        temp_list.append(config)

    config_dict = always_merger.merge(*temp_list)
    del config_dict['path']
    return config_dict


def normalize_tasks(config_dict: dict):
    tasks_dict = config_dict.pop('tasks')
    temp_tasks_dict = {}
    for key, value in tasks_dict.items():
        temp_tasks_dict[key] = value
        script_path = (Path(config_dict['path']).parent / value.get('script_path')).resolve()
        if script_path.exists():
            temp_tasks_dict[key]['script_path'] = script_path
        else:
            raise FileNotFoundError
    config_dict['tasks'] = temp_tasks_dict
    return config_dict



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


def create_states(args):
    app.global_state = GlobalState()

    temp_list = []
    for x in args:
        with open(x, 'r') as stream:
            temp_list.append({**{'path': x}, **yaml.safe_load(stream)})
    config_dict = normalize(temp_list)
    app.global_state.attrs = config_dict.pop('global-state')
    all_hosts = []
    for key, value in config_dict["groups"].items():
        all_hosts.extend(value['hosts'])

    reduce_duplicates_hosts = copy.deepcopy([i for n, i in enumerate(all_hosts) if i not in all_hosts[n + 1:]])

    for host in reduce_duplicates_hosts:
        Host(host, app.global_state)

    for key, value in config_dict["groups"].items():
        Group(key, app.global_state, value)

    for key, value in config_dict["tasks"].items():
        Task(key, app.global_state, value)

    print()


async def concurrency_limit_semaphore(semaphore: BoundedSemaphore, func: callable):
    async with semaphore:
        return await func


def build_func_parms(task_name: str, task_meta: dict, run_func: callable) -> callable:
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

        yield concurrency_limit_semaphore(semaphore=BoundedSemaphore(task_meta["conn_limit"]),
                                          func=run_func(script_path=task_meta["script_path"],
                                                        host=host, env_vars=env_vars))


async def route_tasks():
    for task_name, task_meta in app.tasks.items():
        task_meta['script_path'] = Path(app.tasks_path).joinpath(task_meta['script_path']).resolve()
        run_func = getattr(app, f'{task_meta["conn_type"]}_{task_meta["exec_order"]}')
        results = [x for x in await gather(*build_func_parms(task_name, task_meta, run_func))]
        print()


async def close_init():
    pass
