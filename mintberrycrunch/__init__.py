from types import SimpleNamespace
import yaml
from pathlib import Path
from asyncio import BoundedSemaphore, gather
import asyncssh
from mintberrycrunch.host import Host
from mintberrycrunch.group import Group
from mintberrycrunch.global_state import GlobalState

app = SimpleNamespace()

from mintberrycrunch.ssh import ssh_sequential

app.ssh_sequential = ssh_sequential


async def init(hosts: str, tasks: str, env_vars: str = None) -> None:
    app.global_state = GlobalState()
    app.connection_task_filter_keys = ['host_group', 'conn_type']
    process_parms(hosts, tasks, env_vars)
    await route_tasks()


def process_parms(hosts: str, tasks: str, env_vars: str = None):
    with open(hosts, 'r') as stream:
        host_yaml = yaml.safe_load(stream)
    app.hosts = build_host_lookup(host_yaml)
    with open(tasks, 'r') as stream:
        app.tasks = yaml.safe_load(stream)['tasks']
        app.tasks_path = tasks

    if env_vars:
        with open(env_vars, 'r') as stream:
            app.env_vars = yaml.safe_load(stream)['env_vars']
            app.env_vars_path = tasks


async def do_work_semaphore(semaphore: BoundedSemaphore, func: callable):
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

        yield do_work_semaphore(semaphore=BoundedSemaphore(task_meta["conn_limit"]),
                                func=run_func(script_path=task_meta["script_path"],
                                              host=host, env_vars=env_vars))


def flatten(input_dict, separator='_', prefix=''):
    output_dict = {}
    for key, value in input_dict.items():
        if isinstance(value, dict) and value:
            deeper = flatten(value, separator, prefix + key + separator)
            output_dict.update({key2: val2 for key2, val2 in deeper.items()})
        elif isinstance(value, list) and value:
            for index, sublist in enumerate(value, start=1):
                if isinstance(sublist, dict) and sublist:
                    deeper = flatten(sublist, separator, prefix + key + separator + str(index) + separator)
                    output_dict.update({key2: val2 for key2, val2 in deeper.items()})
                else:
                    output_dict[prefix + key + separator + str(index)] = value
        else:
            output_dict[prefix + key] = value
    return output_dict


def build_host_lookup(host_yaml: dict):

    app.global_state.update(host_yaml['global-state'])
    for key, value in host_yaml.items():
        for group_name, group_meta in value.items():
            print()







    print('asdfasdf')



async def route_tasks():
    for task_name, task_meta in app.tasks.items():
        task_meta['script_path'] = Path(app.tasks_path).joinpath(task_meta['script_path']).resolve()
        run_func = getattr(app, f'{task_meta["conn_type"]}_{task_meta["exec_order"]}')
        results = [x for x in await gather(*build_func_parms(task_name, task_meta, run_func))]
        print()


async def close_init():
    pass
