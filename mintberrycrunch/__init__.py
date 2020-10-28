from types import SimpleNamespace
import yaml
from pathlib import Path
from asyncio import BoundedSemaphore, gather
from mintberrycrunch.global_state import GlobalState
from mintberrycrunch.group import Group
from mintberrycrunch.host import Host
from deepmerge import always_merger

app = SimpleNamespace()

from mintberrycrunch.ssh import ssh_sequential

app.ssh_sequential = ssh_sequential


async def init(*args, **kwargs) -> None:
    create_states(*args, **kwargs)
    await route_tasks()



def create_states(*args, **kwargs):

    parms = process_parms(args)


    app.global_state = GlobalState()



def process_parms(args):
    app.global_state = GlobalState()
    temp_list = []
    for x in args:
        with open(x, 'r') as stream:
            temp_list.append(yaml.safe_load(stream))
    merged_dict = always_merger.merge(*temp_list)
    group_names = [ x for x in merged_dict["groups"].keys() ]
    for key, value in merged_dict["groups"].items():
        Group(key, app.global_state, value)

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
