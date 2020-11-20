import asyncssh

from mintberrycrunch.base import Base



class SSH(Base):

    def __init__(self, attrs):
        self.attrs = attrs

    async def sequential(self, script_path: str, host: 'Host', env_vars: dict) -> None:
        #print(host.name)
        #print(env_vars)
        #print(script_path)
        pass
