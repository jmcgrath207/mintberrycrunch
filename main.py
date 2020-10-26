from asyncio import run
from mintberrycrunch import init


async def main():
    await init(hosts='/home/john/PycharmProjects/StockTrading/mintberrycrunch/hosts.yaml',
               tasks='/home/john/PycharmProjects/StockTrading/mintberrycrunch/tasks.yaml',
               env_vars='/home/john/PycharmProjects/StockTrading/mintberrycrunch/vars.yaml')


if __name__ == '__main__':
    run(main())