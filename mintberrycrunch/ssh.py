import asyncssh
from mintberrycrunch.base import Base


class SSH(Base):

    def __init__(self, attrs):
        self.attrs = attrs

    async def ssh_sequential(self) -> None:
        pass
