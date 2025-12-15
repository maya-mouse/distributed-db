import faust
import logging
import random
import time
from datetime import datetime, timedelta

from cassandra.cluster import Cluster, Session

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')

APP_NAME = 'reactor-saga-app'
KAFKA_BROKER = 'kafka://127.0.0.1:9092'
CASSANDRA_CONTACT_POINTS = ['127.0.0.1']
CASSANDRA_KEYSPACE = 'energy_monitoring'


CASSANDRA_SESSION: Session = None

app = faust.App(
    APP_NAME,
    broker=KAFKA_BROKER,
    store='memory://',
    topic_partitions=3,
)


NOMINAL_POWER_PER_UNIT = 980.0
POWER_DEVIATION_LIMIT = NOMINAL_POWER_PER_UNIT * 0.05

class PowerReading(faust.Record, serializer='json'):
    reactor_id: str
    power_mw: float
    timestamp: float

class SagaCommand(faust.Record, serializer='json'):
    command_id: str
    target_reactor: str
    new_power_level: float
    compensation_needed: bool = False


raw_telemetry_topic = app.topic('telemetry-raw', key_type=str, value_type=PowerReading)
saga_commands_topic = app.topic('saga-load-command', key_type=str, value_type=SagaCommand)


hopping_power_table = app.Table(
    'hopping-power-store',
    default=float,
    key_type=str,
    value_type=float
).hopping(
    step=timedelta(minutes=1),
    size=timedelta(minutes=5),
)

@app.task
async def init_cassandra_task():
    global CASSANDRA_SESSION
    cluster = Cluster(CASSANDRA_CONTACT_POINTS)
    session = cluster.connect()
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE} 
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}
    """)
    session.set_keyspace(CASSANDRA_KEYSPACE)
    CASSANDRA_SESSION = session
    logging.info("Cassandra initialized successfully.")


def cassandra_write_aggregate(reactor_id: str, window_end: datetime, power: float):
    if CASSANDRA_SESSION is None:
        logging.warning("Cassandra session not initialized!")
        return
    try:
        query = """
            INSERT INTO load_following_aggregates
            (reactor_id, window_end_time, total_power_mw, window_start_time)
            VALUES (%s, %s, %s, %s)
        """
        window_start = window_end - timedelta(minutes=5)
        CASSANDRA_SESSION.execute(query, (reactor_id, window_end, power, window_start))
        logging.info(f"Cassandra insert SUCCESS: {reactor_id} @ {window_end}")
    except Exception as e:
        logging.error(f"Cassandra insert FAILED: {e}")


def cassandra_write_saga_log(saga_id: str, step: int, action: str, reactor: str, details: str):
    """Реальний запис у журнал Saga."""
    if CASSANDRA_SESSION:
        query = """
            INSERT INTO saga_log 
            (saga_id, timestamp, step_number, action_type, reactor_id, details)
            VALUES (%s, now(), %s, %s, %s, %s)
        """
        CASSANDRA_SESSION.execute(query, (saga_id, step, action, reactor, details))
        logging.info(f" CASSANDRA [SAGA LOG]: Запис кроку {step} ({action}) для Saga {saga_id}.")

@app.agent(raw_telemetry_topic)
async def process_power(readings):
    logging.info("PROCESS_POWER agent started!")
    async for reading in readings.group_by(PowerReading.reactor_id):
        logging.info(f"Received: {reading.reactor_id} - {reading.power_mw:.1f} MW")
        
        hopping_power_table[reading.reactor_id] = reading.power_mw
        logging.info(f"HOPPING WINDOW | {reading.reactor_id}: {reading.power_mw:.1f} MW")
        
        cassandra_write_aggregate(reading.reactor_id, datetime.utcnow(), reading.power_mw)
        

        if abs(reading.power_mw - NOMINAL_POWER_PER_UNIT) > POWER_DEVIATION_LIMIT:
            saga_command = SagaCommand(
                command_id=f"{reading.reactor_id}-{int(time.time())}",
                target_reactor=reading.reactor_id,
                new_power_level=NOMINAL_POWER_PER_UNIT
            )
            await saga_commands_topic.send(key=reading.reactor_id, value=saga_command)
            logging.warning(f"SAGA COMMAND SENT | {reading.reactor_id} - power deviation detected: {reading.power_mw:.1f} MW")
            cassandra_write_saga_log(saga_command.command_id, 1, 'STARTED', reading.reactor_id, 'Calculation completed')
            
if __name__ == '__main__':
    app.main()
