from kombu import Connection
from tapiriik.settings import RABBITMQ_BROKER_URL
mq = Connection(RABBITMQ_BROKER_URL)
mq.connect()