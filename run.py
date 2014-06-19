#!/usr/bin/env python

# Copyright (C) 2013 SignalFuse, Inc.

# Start script for the Kafka service.
# Requires kazoo, a pure-Python ZooKeeper client.

from kazoo.client import KazooClient
import logging
import os
import sys

from maestro.guestutils import *

# Setup logging for Kazoo.
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

os.chdir(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..'))

KAFKA_CONFIG_FILE = os.path.join('config', 'server.properties')
KAFKA_LOGGING_CONFIG = os.path.join('config', 'log4j.properties')
KAFKA_ZOOKEEPER_BASE = os.environ.get('ZOOKEEPER_BASE',
                                      '/{}/kafka'.format(get_environment_name()))

LOG_PATTERN = "%d{yyyy'-'MM'-'dd'T'HH:mm:ss.SSSXXX} %-5p [%-35.35t] [%-36.36c]: %m%n"

ZOOKEEPER_NODE_LIST = ','.join(get_node_list('zookeeper', ports=['client']))

# Generate the Kafka configuration from the defined environment variables.
with open(KAFKA_CONFIG_FILE, 'w+') as conf:
    conf.write("""# Kafka configuration for %(node_name)s
broker.id=%(broker_id)d
advertised.host.name=%(host_address)s
port=%(broker_port)d

num.network.threads=2
num.io.threads=2

socket.send.buffer.bytes=1048576
socket.receive.buffer.bytes=1048576
socket.request.max.bytes=104857600

log.dir=/var/lib/kafka/logs
num.partitions=%(num_partitions)d

log.flush.interval.messages=10000
log.flush.interval.ms=100
log.retention.hours=%(retention_hours)d
log.segment.bytes=536870912
log.cleanup.interval.mins=1

zookeeper.connect=%(zookeeper_nodes)s%(zookeeper_base)s
zookeeper.connection.timeout.ms=300000
zookeeper.session.timeout.ms=10000

kafka.metrics.polling.interval.secs=5
kafka.metrics.reporters=kafka.metrics.KafkaCSVMetricsReporter
kafka.csv.metrics.dir=/var/lib/kafka/metrics/
kafka.csv.metrics.reporter.enabled=false
""" % {
        'node_name': get_container_name(),
        'broker_id': int(os.environ.get('BROKER_ID', 0)),
        'host_address': get_container_host_address(),
        'broker_port': get_port('broker', 9092),
        'num_partitions': int(os.environ.get('NUM_PARTITIONS', 8)),
        'retention_hours': int(os.environ.get('RETENTION_HOURS', 168)),
        'zookeeper_nodes': ZOOKEEPER_NODE_LIST,
        'zookeeper_base': KAFKA_ZOOKEEPER_BASE,
    })

# Setup the logging configuration.
with open(KAFKA_LOGGING_CONFIG, 'w+') as f:
    f.write("""# Log4j configuration, logs to rotating file
log4j.rootLogger=INFO,R

log4j.appender.R=org.apache.log4j.RollingFileAppender
log4j.appender.R.File=/var/log/%s/%s.log
log4j.appender.R.MaxFileSize=100MB
log4j.appender.R.MaxBackupIndex=10
log4j.appender.R.layout=org.apache.log4j.PatternLayout
log4j.appender.R.layout.ConversionPattern=%s
""" % (get_service_name(), get_container_name(), LOG_PATTERN))

# Ensure the existence of the ZooKeeper root node for Kafka
print 'Ensuring existance of the ZooKeeper zNode chroot path %s...' % \
        KAFKA_ZOOKEEPER_BASE
def ensure_kafka_zk_path(retries=3):
    while retries >= 0:
        # Connect to the ZooKeeper nodes. Use a pretty large timeout in case they were
        # just started. We should wait for them for a little while.
        zk = KazooClient(hosts=ZOOKEEPER_NODE_LIST, timeout=30000)
        try:
            zk.start()
            zk.ensure_path(KAFKA_ZOOKEEPER_BASE)
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
    '-Dvisualvm.display.name="{}/{}"'.format(
        get_environment_name(), get_container_name()),
]

jmx_port = get_port('jmx', -1)
if jmx_port != -1:
    os.environ['JMX_PORT'] = str(jmx_port)
    jvm_opts += [
        '-Djava.rmi.server.hostname={}'.format(get_container_host_address()),
        '-Dcom.sun.management.jmxremote.port={}'.format(jmx_port),
        '-Dcom.sun.management.jmxremote.rmi.port={}'.format(jmx_port),
        '-Dcom.sun.management.jmxremote.authenticate=false',
        '-Dcom.sun.management.jmxremote.local.only=false',
        '-Dcom.sun.management.jmxremote.ssl=false',
    ]

os.environ['KAFKA_OPTS'] = ' '.join(jvm_opts) + os.environ.get('JVM_OPTS', '')

# Start the Kafka broker.
os.execl('bin/kafka-server-start.sh', 'kafka', KAFKA_CONFIG_FILE)
