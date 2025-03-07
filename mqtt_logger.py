import asyncio
import logging
import signal

from config import get_first_config
from mqtt_handler import MqttHandler

__version__ = '0.0.1'


class MqttLogger:
    def __init__(self) -> None:
        self.config = get_first_config()
        self.setup_logging()
        self.mqtt_handler: MqttHandler = MqttHandler(self.config, self.config['topics'], self.handle_mqtt_message)

    def setup_logging(self):
        if 'logging' in self.config:
            logging_level_name: str = self.config['logging'].upper()
            logging_level: int = logging.getLevelNamesMapping().get(logging_level_name, logging.NOTSET)
            if logging_level != logging.NOTSET:
                logging.getLogger().setLevel(logging_level)
            else:
                logging.warning(f'unknown logging level: %s.', logging_level)

    async def connect(self):
        while not await self.mqtt_handler.connect():
            await asyncio.sleep(1)

    @staticmethod
    async def handle_mqtt_message(topic: str, payload: str):
        logging.info('%s > %s', topic, payload)

    async def exit(self):
        await self.mqtt_handler.disconnect()


async def main():
    mqtt_logger = MqttLogger()
    await mqtt_logger.connect()
    loop = asyncio.get_running_loop()
    main_task = asyncio.current_task()

    def shutdown_handler():
        if not main_task.done():
            main_task.cancel()

    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, shutdown_handler)
    except NotImplementedError:
        pass
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        logging.info('exiting.')
    finally:
        await mqtt_logger.exit()
        logging.info('exited.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.getLogger('gmqtt').setLevel(logging.ERROR)
    logging.info(f'starting MQTT-Logger v%s.', __version__)
    asyncio.run(main())
