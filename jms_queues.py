from __future__ import print_function
import json
from config.config import Config
from monitors.common import get_node_list, load_rules, ask_jboss
#import datetime

config = Config()

MESSAGE_CRITICAL_THRESHOLD = config.get('MESSAGE_CRITICAL_THRESHOLD', 10)
MESSAGE_WARN_THRESHOLD = config.get('MESSAGE_WARN_THRESHOLD', 10)
CONSUMER_CRITICAL_THRESHOLD = config.get('CONSUMER_CRITICAL_THRESHOLD', 0)
CONSUMER_WARN_THRESHOLD = config.get('CONSUMER_WARN_THRESHOLD', 0)

LOG_LEVEL_MAPPING = {
    'OK': 0,
    'WARN': 1,
    'CRITICAL': 2
}

def get_all_queue_stats(nodes, rules, level, output_type):
    for node in nodes:
        data = {
            'address': [
                {'host': '{0}jbc{1}n{2}-hc'.format(config.ENV, config.CLUSTER_NUMBER, node)},
                {'server': 'node{0}'.format(node)},
                {'subsystem': 'messaging-activemq'},
                {'server': 'default'}
            ],
            'operation': 'read-children-resources',
            'child-type': 'jms-queue',
            'include-runtime': True
        }

        response = ask_jboss(data)
        response_data = response['result']
        for queue in response_data:
            message_count = response_data[queue]['message-count']
            consumer_count = response_data[queue]['consumer-count']

            check_message_count(queue, message_count, nodes, rules, level, output_type)
            check_consumer_count(queue, consumer_count, nodes, rules, level, output_type)

def check_message_count(queue, message_count, nodes, rules, level, output_type):
    if queue in rules and 'message_thresholds' in rules[queue]:
        thresholds = rules[queue]['message_thresholds']
    else:
        thresholds = {'WARNING': MESSAGE_WARN_THRESHOLD, 'CRITICAL': MESSAGE_CRITICAL_THRESHOLD}

    if message_count > thresholds['CRITICAL'] and LOG_LEVEL_MAPPING['CRITICAL'] >= LOG_LEVEL_MAPPING[level]:
        output(output_type, queue, message_count, 'message-count', nodes, 'CRITICAL')
    elif message_count > thresholds['WARNING'] and LOG_LEVEL_MAPPING['WARN'] >= LOG_LEVEL_MAPPING[level]:
        output(output_type, queue, message_count, 'message-count', nodes, 'WARN')
    elif LOG_LEVEL_MAPPING['OK'] >= LOG_LEVEL_MAPPING[level]:
        output(output_type, queue, message_count, 'message-count', nodes, 'OK')

def check_consumer_count(queue, consumer_count, nodes, rules, level, output_type):
    if queue in rules and 'consumer_thresholds' in rules[queue]:
        thresholds = rules[queue]['consumer_thresholds']
    else:
        thresholds = {'WARNING': CONSUMER_WARN_THRESHOLD, 'CRITICAL': CONSUMER_CRITICAL_THRESHOLD}

    if consumer_count == 0 and LOG_LEVEL_MAPPING['CRITICAL'] >= LOG_LEVEL_MAPPING[level]:
        output(output_type, queue, consumer_count, 'consumer-count', nodes, 'CRITICAL')
    elif consumer_count > 0 and LOG_LEVEL_MAPPING['OK'] >= LOG_LEVEL_MAPPING[level]:
        output(output_type, queue, consumer_count, 'consumer-count', nodes, 'OK')

def output(output_type, queue, value, metric_type, nodes, level):
    status = LOG_LEVEL_MAPPING[level]
    for node in nodes:
        name = 'JMSQ_jbc{0}_{1}eap7c{0}n{2}lxv_{3}'.format(config.CLUSTER_NUMBER, config.ENV, node, queue)
        if metric_type == 'consumer-count':
            name = 'Consumer_' + name

        message = '{0} - {1} in {2}'.format(level, value, name)

        if output_type == 'nagios':
            print('{0} {1} {2}={3} {4}'.format(status, name, metric_type, value, message))

        elif output_type == 'json':
            json_output = {'status': status, 'name': name, metric_type: value, 'message': message}
            print(json.dumps(json_output))
        # log file for troubleshooting outupt
        #now = datetime.datetime.now()
        #Log1 = open("/tmp/Log.txt", "a")  # append mode
        #Log1.write('{0} {1} {2} {3} \n'.format(status, name, metric, message))
        #Log1.write(now.strftime("%Y-%m-%d %H:%M:%S \n"))
        #Log1.close()

def process(level, output_type, rule_file):
    node_list = get_node_list() if len(config.NODES) == 0 else config.NODES
    if rule_file is None:
        rule_file = 'jms-queues.json'
    rules = load_rules(rule_file)
    get_all_queue_stats(node_list, rules, level, output_type)
