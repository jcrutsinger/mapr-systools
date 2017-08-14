
#!/usr/bin/python3

import argparse
import base64
import json
import logging
import os
import requests
import sys
import multiprocessing
import time
import urllib

class Counter(object):
    def __init__(self, initval=0):
        self.val = multiprocessing.Value('i', initval)
        self.lock = multiprocessing.Lock()

    def increment(self):
        with self.lock:
            self.val.value += 1

    def value(self):
        with self.lock:
            return self.val.value

def send_messages(url, payload, successes, errors, state):
    session = requests.Session()
    headers = {'Content-Type':'application/vnd.kafka.binary.v1+json'}
    while state.value < 2:
        try:
            response = session.post(url, headers=headers, data=payload)
            if response.status_code == requests.codes.ok:
                successes.increment()
            else:
                errors.increment()
#                logging.error('{} {}'.format(response.status_code, response.reason))
        except requests.exceptions.ConnectionError as e:
            errors.increment()
#            logging.error('{}'.format(e))

def monitor(successes, errors, state):
    (s_curr, e_curr, t_curr, state_curr) = (0, 0, time.time(), 0)
    while state.value < 3:
        time.sleep(1)
        (s_prev, e_prev, t_prev, state_prev) = (s_curr, e_curr, t_curr, state_curr)
        (s_curr, e_curr, t_curr, state_curr) = (successes.value(), errors.value(), time.time(), state.value)
        flag = '*' if state_curr == 1 and state_prev != 0 else ' '
        print('{} {:.1f} msgs/s, {} errors'.format(flag, (s_curr - s_prev)/(t_curr - t_prev), e_curr - e_prev))

if __name__ == "__main__":
#    logging.basicConfig(level=logging.ERROR,
#                        format='[%(levelname)s] %(asctime)s %(message)s')

    parser = argparse.ArgumentParser(description='Kafka REST load generator')
    parser.add_argument('--host', default='localhost',
                        help='rest proxy hostname (default: localhost)')
    parser.add_argument('--port', default='8082',
                        help='rest proxy port (default: 8082)')
    parser.add_argument('-c', '--concurrency', type=int, default=1,
                        help='number of threads (default: 1)')
    parser.add_argument('-s', '--size', type=int, default=100,
                        help='message size in bytes (default: 100)')
    parser.add_argument('-d', '--duration', type=float, default=300,
                        help='duration in seconds (default: 300)')
    parser.add_argument('topic', help='topic name')
    args = parser.parse_args()

    url = 'http://{}:{}/topics/{}'.format(args.host, args.port, urllib.parse.quote(args.topic, ''))
    payload = '{"records":[{"value":"' + base64.b64encode(os.urandom(args.size)).decode() + '"}]}'
    successes = Counter()
    errors = Counter()

    # initializing monitor and sender processes
    state = multiprocessing.Value('i', 0)
    mon_process = multiprocessing.Process(target=monitor, args=(successes, errors, state))
    mon_process.start()
    processes = []
    for i in range(args.concurrency):
        process = multiprocessing.Process(target=send_messages,
                                          args=(url, payload, successes, errors, state))
        process.start()
        processes.append(process)

    # measuring throughput
    state.value = 1
    s_start = successes.value()
    time.sleep(args.duration)
    total_successes = successes.value() - s_start

    # terminating sender processes
    state.value = 2
    for process in processes:
        process.join()

    # terminating monitor process
    state.value = 3
    mon_process.join()
    print('---')
    print('Total throughput: {:.1f} msgs/s'.format(total_successes/args.duration))
    print('Total successes: {}'.format(successes.value()))
    print('Total errors: {}'.format(errors.value()))