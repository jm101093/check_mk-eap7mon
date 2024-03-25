from __future__ import print_function
import json
from config.config import Config
from monitors.common import get_node_list, ask_jboss

config = Config()

# Define threshold configurations for each CLUSTER_NUMBER
# Example: {1: {'THREAD_COUNT_WARN': 1300, 'THREAD_COUNT_CRITICAL': 1500}, 2: {'THREAD_COUNT_WARN': 1200, 'THREAD_COUNT_CRITICAL': 1400}}
CLUSTER_THRESHOLDS = {
    1: {'THREAD_COUNT_WARN': 1300, 'THREAD_COUNT_CRITICAL': 1500},
    8: {'THREAD_COUNT_WARN': 1400, 'THREAD_COUNT_CRITICAL': 1600}
}

LOG_LEVEL_MAPPING = {
    'OK': 0,
    'WARN': 1,
    'CRITICAL': 2
}

def get_thread_stats(nodes, level, output_type):
    for node in nodes:
        data = {
            'address': [
                {'host': '{0}jbc{1}n{2}-hc'.format(config.ENV, config.CLUSTER_NUMBER, node)},
                {'server': 'node{0}'.format(node)},
                {'core-service': 'platform-mbean'},
                {'type': 'threading'}
            ],
            'operation': 'read-resource',
            'include-runtime': True
        }

        response = ask_jboss(data)
        response_data = response['result']
        current_thread_count = response_data['thread-count']

        cluster_thresholds = CLUSTER_THRESHOLDS.get(config.CLUSTER_NUMBER, {})
        THREAD_COUNT_WARN = cluster_thresholds.get('THREAD_COUNT_WARN', config.get('THREAD_COUNT_WARN', 1300))
        THREAD_COUNT_CRITICAL = cluster_thresholds.get('THREAD_COUNT_CRITICAL', config.get('THREAD_COUNT_CRITICAL', 1500))

        if current_thread_count >= THREAD_COUNT_CRITICAL and LOG_LEVEL_MAPPING['CRITICAL'] >= LOG_LEVEL_MAPPING[level]:
            output(output_type, node, 'CRITICAL', 'ThreadCount', current_thread_count)
        elif current_thread_count >= THREAD_COUNT_WARN and LOG_LEVEL_MAPPING['WARN'] >= LOG_LEVEL_MAPPING[level]:
            output(output_type, node, 'WARN', 'ThreadCount', current_thread_count)
        elif LOG_LEVEL_MAPPING['OK'] >= LOG_LEVEL_MAPPING[level]:
            output(output_type, node, 'OK', 'ThreadCount', current_thread_count)

def output(output_type, node, level, metric_name, metric_value):
    status = LOG_LEVEL_MAPPING[level]
    name = '{0}-jbc{1}_{2}eap7c{1}n{3}lxv'.format(metric_name, config.CLUSTER_NUMBER, config.ENV, node)
    metric = '{0}={1}'.format(metric_name, metric_value)
    message = '{0} - {1}: {2}'.format(level, metric_name, metric_value)

    if output_type == 'nagios':
        print('{0} {1} {2} {3}'.format(status, name, metric, message))
    elif output_type == 'json':
        json_output = {'status': status, 'name': name, 'metric': metric, 'message': metric}
        print(json.dumps(json_output))

def process(level, output_type, rule_file='threads.json'):
    node_list = get_node_list() if len(config.NODES) == 0 else config.NODES
    get_thread_stats(node_list, level, output_type)
