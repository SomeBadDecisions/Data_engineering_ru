import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from cdm_loader.cdm_message_processor_job import CdmMessageProcessor
from cdm_loader.repository import (Migrator,
                                   RestaurantCategoryCounterRepository,
                                   UserCategoryCounterRepository,
                                   UserProductCounterRepository)

app = Flask(__name__)

config = AppConfig()

if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)

    migrator = Migrator(config.pg_warehouse_db())
    migrator.up()

    proc = CdmMessageProcessor(
        config.kafka_consumer(),
        UserProductCounterRepository(config.pg_warehouse_db()),
        UserCategoryCounterRepository(config.pg_warehouse_db()),
        RestaurantCategoryCounterRepository(config.pg_warehouse_db()),
        app.logger,
    )

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=25)
    scheduler.start()

    app.run(debug=True, host='0.0.0.0', use_reloader=False)
