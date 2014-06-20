#!/usr/bin/env python
"""Graphite Babble

Usage:
    gbabble.py [options]

Example:
    gbabble.py

Options:
    -h <host>, --host <host>                Hostname for statsd
    -p <port>, --port <port>                Port for statsd
    -s <socket>, --socket <socket>          The socket used to connect to statsd
    -n, --dryrun                            Prints out what would normally be sent to the server.
    -d <days>, --days <days>                Amount of days to generate data.
    -i <seconds>, --interval <seconds>      Generates new data every X seconds.
    -c <file>, --config <file>              Will generate metrics based on a YAML file.
    --help                                  Show this help message.
"""
import random
import time

import yaml
from docopt import docopt
import graphitesend


DEFAULT_FROM = 0.0
DEFAULT_UNTIL = 10.0
DEFAULT_STEP = 0.1
DEFAULT_VARIANCE = 0.3
DEFAULT_INTERVAL = 60

CONNECTION = None

## GraphiteSend voodoo

class MyGraphiteClient(graphitesend.GraphiteClient):

    def send(self, metric, value, timestamp=None):
        """
        Overrides the send method to fully manage the metric field.
        """
        if timestamp is None:
            timestamp = int(time.time())
        else:
            timestamp = int(timestamp)

        if type(value).__name__ in ['str', 'unicode']:
            value = float(value)

        metric = self.clean_metric_name(metric)

        message = "%s %f %d\n" % (metric, value, timestamp)
        message = self. _presend(message)

        if self.dryrun:
            return message

        return self._send(message)

def init(init_type='plaintext_tcp', *args, **kwargs):
    """
    Create the module instance of the GraphiteClient.
    """
    global _module_instance
    graphitesend.reset()

    validate_init_types = ['plaintext_tcp', 'plaintext', 'pickle_tcp',
                           'pickle', 'plain']

    if init_type not in validate_init_types:
        raise graphitesend.GraphiteSendException(
            "Invalid init_type '%s', must be one of: %s" %
            (init_type, ", ".join(validate_init_types)))

    # Use TCP to send data to the plain text receiver on the graphite server.
    if init_type in ['plaintext_tcp', 'plaintext', 'plain']:
        _module_instance = MyGraphiteClient(*args, **kwargs)

    # Use TCP to send pickled data to the pickle receiver on the graphite
    # server.
    if init_type in ['pickle_tcp', 'pickle']:
        _module_instance = MyGraphiteClient(*args, **kwargs)

    return _module_instance



### file ops


def load_dummy_data(file):
    """Opens a file and parses its content as YAML.
    """
    print "[debug] Opening file {0}".format(file)
    stream = open(file, 'r')
    dummy_data = yaml.load(stream)
    return dummy_data


### Metric generation


class Metric(object):

    def __init__(self, name):
        self.name = name
        self.current_value = None



def create_metric_data(metric, defaults):
    """Generates a metric value string. It will randomly step or down from a previous value.
    """
    default_from = defaults.get('range', {}).get('from', DEFAULT_FROM)
    default_until = defaults.get('range', {}).get('until', DEFAULT_UNTIL)
    default_step = defaults.get('range', {}).get('step', DEFAULT_STEP)
    default_resolution = abs(default_until - default_from) / default_step

    # Calculate a previous value if not available
    if not metric.current_value:
        metric.current_value = round(
            random.uniform(
                default_from,
                default_until
            ),
            2)

    # Calculate next value
    start = max(
        default_from,
        metric.current_value - round(
            random.uniform(
                0,
                default_step * default_resolution * DEFAULT_VARIANCE
            ),
            2))
    end = min(default_until,
            metric.current_value + round(
            random.uniform(
                0,
                default_step * default_resolution * DEFAULT_VARIANCE
            ),
            2))

    new_value = round(
        random.uniform(
            start,
            end),
        2
    )

    metric.current_value = new_value

    return (metric, metric.current_value, int(time.time()))


### Metric sending

def send_metric(metric, value, timestamp):
    """Performs the actual sending of the metric data
    """
    global CONNECTION

    print "[info] Sending: {0} {1} {2}".format(metric.name, value, timestamp)
    g = init(graphite_server=arguments.get('--host'),
             graphite_port=int(arguments.get('--port', 2003)))
    g.send(metric.name, value, timestamp)


def send_metrics_batch(metric_list, path, defaults, arguments):
    """Sends a value for each of the defined metrics to graphite.
    """
    while True:
        for metric in metric_list:
            metric, value, timestamp = create_metric_data(metric, defaults)
            send_metric(metric, value, timestamp)
        time.sleep(int(arguments.get('--interval', DEFAULT_INTERVAL)))


if __name__ == '__main__':
    arguments = docopt(__doc__, version='0.1.0')
    print arguments

    dummy_data_file = arguments.get('--input') or "./dummydata.yaml"
    dummy_data = load_dummy_data(dummy_data_file)

    metrics_list = [Metric(m) for m in dummy_data.get('metrics', [])]

    send_metrics_batch(metrics_list,
                 arguments.get('<path>'),
                 dummy_data.get('defaults', {}),
                 arguments)

