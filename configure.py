#!/usr/bin/env python

# Start script for Kafka to enable environment variable config injection.

from kazoo.client import KazooClient
import logging
import os
import sys

# Setup logging for Kazoo.
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

os.chdir(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..'))

KAFKA_CONFIG_FILE = os.path.join('config', 'server.properties')
KAFKA_LOGGING_CONFIG = os.path.join('config', 'log4j.properties')


KAFKA_CONFIG_TEMPLATE = """# Kafka configuration for {node_name}
broker.id={broker_id:d}
port={broker_port:d}

num.network.threads={num_threads:d}
num.io.threads={num_threads:d}

socket.send.buffer.bytes=1048576
socket.receive.buffer.bytes=1048576
socket.request.max.bytes=104857600

log.dirs={log_dirs}
num.partitions={num_partitions:d}

log.flush.interval.messages={flush_interval_msgs}
log.flush.interval.ms={flush_interval_ms:d}
log.retention.hours={retention_hours:d}
log.retention.bytes={retention_bytes:d}
log.segment.bytes={log_segment_bytes:d}
log.roll.hours={log_roll_hours:d}
log.cleanup.interval.mins=1

default.replication.factor={replication_factor:d}
num.replica.fetchers={num_replica_fetchers:d}
replica.fetch.max.bytes=1048576
replica.fetch.wait.max.ms=500
replica.high.watermark.checkpoint.interval.ms=5000
replica.socket.timeout.ms={replica_socket_timeout_ms:d}
replica.socket.receive.buffer.bytes=65536

replica.lag.time.max.ms={replica_lag_max_ms:d}
replica.lag.max.messages={replica_lag_max_msgs:d}

auto.leader.rebalance.enable={leader_rebalance}
controlled.shutdown.enable=true

zookeeper.connect={zookeeper_nodes}
zookeeper.connection.timeout.ms=6000
zookeeper.session.timeout.ms=6000
zookeeper.sync.time.ms=2000

"""

kafka_zookeeper_base = os.environ.get('kafka_zookeeper_base', '/databus/kafka')
kafka_zookeeper_nodes = os.environ.get('KAFKA_ZOOKEEPER_NODES', 'zookeeper:2181')

# Generate the Kafka configuration from the defined environment variables.
config_model = {
    'node_name':                    str(os.environ.get('KAFKA_NODE_NAME', 'kafka')),
    'broker_id':                    int(os.environ.get('KAFKA_BROKER_ID', 0)),
    'broker_port':                  int(os.environ.get('KAFKA_BROKER_PORT', 9092)),
    # Default log directory is /var/lib/kafka/logs.
    'log_dirs':                     str(os.environ.get('KAFKA_LOG_DIRS', '/var/lib/kafka/logs')),
    'num_partitions':               int(os.environ.get('KAFKA_NUM_PARTITIONS', 8)),
    # Default retention is 7 days (168 hours).
    'retention_hours':              int(os.environ.get('KAFKA_RETENTION_HOURS', 168)),
    # Default retention is only based on time.
    'retention_bytes':              int(os.environ.get('KAFKA_RETENTION_BYTES', -1)),
    # Segment size (default is 0.5GB)
    'log_segment_bytes':            int(os.environ.get('KAFKA_LOG_SEGMENT_BYTES', 536870912)),
    # Minimum interval between rolling new log segments (default 1 week)
    'log_roll_hours':               int(os.environ.get('KAFKA_LOG_ROLL_HOURS', 24 * 7)),
    'zookeeper_nodes':              kafka_zookeeper_nodes + kafka_zookeeper_base,
    'flush_interval_ms':            int(os.environ.get('KAFKA_FLUSH_INTERVAL_MS', 3000)),
    'flush_interval_msgs':          int(os.environ.get('KAFKA_FLUSH_INTERVAL_MSGS', 10000)),
    'num_threads':                  int(os.environ.get('KAFKA_NUM_THREADS', 8)),
    'replication_factor':           int(os.environ.get('KAFKA_REPLICATION_FACTOR', 2)),
    'num_replica_fetchers':         int(os.environ.get('KAFKA_NUM_REPLICA_FETCHERS', 4)),
    'replica_socket_timeout_ms':    int(os.environ.get('KAFKA_REPLICA_SOCKET_TIMEOUT_MS', 2500)),
    'replica_lag_max_ms':           int(os.environ.get('KAFKA_REPLICA_LAG_MAX_MS', 5000)),
    'replica_lag_max_msgs':         int(os.environ.get('KAFKA_REPLICA_LAG_MAX_MSGS', 1000)),
    'leader_rebalance':             str(os.environ.get('KAFKA_AUTO_LEADER_REBALANCE', 'false').lower() == 'true').lower()
}

with open(KAFKA_CONFIG_FILE, 'w+') as conf:
    print 'writing configuration file to', KAFKA_CONFIG_FILE
    conf.write(KAFKA_CONFIG_TEMPLATE.format(**config_model))

print 'Ensuring existance of the ZooKeeper zNode chroot path', kafka_zookeeper_base

def ensure_kafka_zk_path(retries=3):
    while retries >= 0:
        # Connect to the ZooKeeper nodes. Use a pretty large timeout in case they were
        # just started. We should wait for them for a little while.
        zk = KazooClient(hosts=kafka_zookeeper_nodes, timeout=30000)
        try:
            zk.start()
            zk.ensure_path(kafka_zookeeper_base)
            return True
        except:
            retries -= 1
        finally:
            zk.stop()
    return False

if not ensure_kafka_zk_path():
    sys.stderr.write('Could not create the base ZooKeeper path for Kafka!\n')
    sys.exit(1)

# Setup the JMX Java agent and various JVM options.
jvm_opts = [
    '-server',
    '-showversion',
]

jmx_port = int(os.environ.get('JMX_PORT', -1))

if jmx_port != -1:
    jvm_opts += [
        '-Dcom.sun.management.jmxremote.port={}'.format(jmx_port),
        '-Dcom.sun.management.jmxremote.rmi.port={}'.format(jmx_port),
        '-Dcom.sun.management.jmxremote.authenticate=false',
        '-Dcom.sun.management.jmxremote.local.only=false',
        '-Dcom.sun.management.jmxremote.ssl=false',
    ]

kafka_jvm_opts = os.environ.get('KAFKA_JVM_OPTS')
if kafka_jvm_opts:
    ' '.join(jvm_opts + [kafka_jvm_opts])
else:
    ' '.join(jvm_opts)
