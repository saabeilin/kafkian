import json
import typing
from functools import partial

from kafkian.metrics.consts import TOPIC_STATS_LAG, TOPIC_STATS_OFFSETS, TOPIC_STATS_DEBUG


class KafkaMetrics:
    """
    Send librdkafka statistics to Datadog/statsd.

    For more information see:
    https://github.com/edenhill/librdkafka/wiki/Statistics
    """
    def __init__(self, statsd, basename='', tags=None):
        assert statsd, "KafkaMetrics needs a configured statsd instance"
        self.statsd = statsd
        self.base = basename or 'kafkian'
        self.base_tags = tags or []
        self.topic_stats_level = TOPIC_STATS_LAG

    def send(self, stats: str):
        stats = json.loads(stats)

        self._send_stats(stats)
        self._send_broker_stats(stats)
        self._send_topic_partition_stats(stats)
        self._send_cgrp_stats(stats)
        self._send_eos_stats(stats)

    def _send_stats(self, stats: typing.Dict):
        base = f'{self.base}.librdkafka'
        gauge = partial(self.statsd.gauge, tags=self.base_tags)
        gauge(f'{base}.replyq', stats['replyq'])
        gauge(f'{base}.msg_cnt', stats['msg_cnt'])
        gauge(f'{base}.msg_size', stats['msg_size'])
        gauge(f'{base}.msg_max', stats['msg_max'])
        gauge(f'{base}.msg_size_max', stats['msg_size_max'])
        
    def _send_broker_stats(self, stats: typing.Dict):
        pass

    def _send_topic_partition_stats(self, stats: typing.Dict):
        if stats['type'] != 'consumer':
            return

        base = f'{self.base}.librdkafka.topic'

        for topic, topic_stats in stats['topics'].items():
            for partition, partition_stats in topic_stats['partitions'].items():
                # TODO: why is partition a string? @edenhill
                partition = int(partition)
                if partition < 0:
                    # TODO: what does partition -1 mean? @edenhill
                    continue

                tags = self.base_tags + [f'topic:{topic}', f'partition:{partition}']
                gauge = partial(self.statsd.gauge, tags=tags)

                gauge(f'{base}.consumer_lag', partition_stats['consumer_lag'])

                if self.topic_stats_level >= TOPIC_STATS_OFFSETS:
                    gauge(f'{base}.query_offset', partition_stats['query_offset'])
                    gauge(f'{base}.next_offset', partition_stats['next_offset'])
                    gauge(f'{base}.app_offset', partition_stats['app_offset'])
                    gauge(f'{base}.stored_offset', partition_stats['stored_offset'])
                    gauge(f'{base}.committed_offset', partition_stats['committed_offset'])
                    gauge(f'{base}.lo_offset', partition_stats['lo_offset'])
                    gauge(f'{base}.hi_offset', partition_stats['hi_offset'])

                if self.topic_stats_level >= TOPIC_STATS_DEBUG:
                    gauge(f'{base}.msgq_cnt', partition_stats['msgq_cnt'])
                    gauge(f'{base}.msgq_bytes', partition_stats['msgq_bytes'])
                    gauge(f'{base}.xmit_msgq_cnt', partition_stats['xmit_msgq_cnt'])
                    gauge(f'{base}.xmit_msgq_bytes', partition_stats['xmit_msgq_bytes'])
                    gauge(f'{base}.fetchq_cnt', partition_stats['fetchq_cnt'])
                    gauge(f'{base}.fetchq_size', partition_stats['fetchq_size'])
                    gauge(f'{base}.txmsgs', partition_stats['txmsgs'])
                    gauge(f'{base}.txbytes', partition_stats['txbytes'])
                    gauge(f'{base}.msgs', partition_stats['msgs'])
                    gauge(f'{base}.rx_ver_drops', partition_stats['rx_ver_drops'])

    def _send_cgrp_stats(self, stats: typing.Dict):
        if stats['type'] != 'consumer' or 'cgrp' not in stats:
            return
        cgrp = stats['cgrp']
        base = f'{self.base}.librdkafka.cgrp'
        gauge = partial(self.statsd.gauge, tags=self.base_tags)
        gauge(f'{base}.rebalance_age', cgrp['rebalance_age'])
        gauge(f'{base}.rebalance_cnt', cgrp['rebalance_cnt'])
        gauge(f'{base}.assignment_size', cgrp['assignment_size'])

    def _send_eos_stats(self, stats: typing.Dict):
        if stats['type'] != 'producer' or 'eos' not in stats:
            return
