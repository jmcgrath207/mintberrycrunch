from mintberrycrunch import app, Path
import asyncssh


async def ssh_sequential(script_path: Path, host: str, env_vars: dict) -> None:
    print(script_path)
    print(host)
    print(env_vars)
