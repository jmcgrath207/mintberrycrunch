from asyncio import run
from mintberrycrunch import init


async def main():
    await init('/home/john/PycharmProjects/StockTrading/mintberrycrunch/hosts.yaml',
               '/home/john/PycharmProjects/StockTrading/mintberrycrunch/tasks.yaml')


if __name__ == '__main__':
    run(main())