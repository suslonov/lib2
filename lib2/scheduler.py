#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, multiple-statements, missing-function-docstring, missing-class-docstring, fixme.
"""
Paper-trading and live-trading algorithm stages scheduler
It is very primitive, without branches
All scheduled functions should return 0 if succeeded

Event data description:
    event_operations = {'Stage': function, ...}
    event_restarts = {"Stage": N, ...}
    event_restart_delays = {"InitStage": N intervals, ...}
    event_list = [(+/- time shift from the start, 'Stage', 'Required Previous Stage'), ...]

'Required Previous Stage' is not implemented yet

"""

from time import sleep
from datetime import datetime
from enum import Enum
import threading
import queue
import pytz
import pandas as pd

EASTERN = pytz.timezone('US/Eastern'); JERUSALEM = pytz.timezone('Asia/Jerusalem'); UTC = pytz.UTC
START_SESSION_HOUR = 9; START_SESSION_MIN = 30; END_SESSION_HOUR = 16; END_SESSION_MIN = 0

class CLOCK_MESSAGES(Enum):
    BAR = 0
    EVENT = 1
    FINISH = 2

class RealTimeClock():
    def __init__(self,
                 clock_id,
                 session_stop,
                 session_start,
                 scheduled_event_list,
                 clock_queue,
                 interval):

        if interval is None:
            self.interval = pd.Timedelta('1 minute')
        else:
            self.interval = interval
        self.clock_id = clock_id
        self.session_stop = session_stop
        self.session_start = session_start
        self.clock_queue = clock_queue
        self._run_clock = True
        self.scheduled_event_list = scheduled_event_list.copy()
        self.scheduled_event_list.sort(key=lambda x: x[0])
        self.clock_thread = None

    def __enter__(self):
        clock_thread = threading.Thread(target=self.event_loop, daemon=True)
        clock_thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.scheduled_event_list = []
        self._run_clock = False

    def event_loop(self):
        last_emit = None
        current_time = pd.to_datetime('now', utc=True)

        while self.scheduled_event_list:
            if current_time >= self.session_start + self.scheduled_event_list[0][0]:
                del self.scheduled_event_list[0]
            else:
                break

        while self._run_clock and current_time <= self.session_stop:
            current_time = pd.to_datetime('now', utc=True)
            if last_emit is None or (current_time - last_emit >= self.interval):
                self.clock_queue.put((current_time, CLOCK_MESSAGES.BAR))
                last_emit = current_time

            while self.scheduled_event_list and current_time >= ((self.session_start + self.scheduled_event_list[0][0])
                                                                 if self.scheduled_event_list[0][0] # if event time shift is zero or empty start it at the beginning of the session
                                                                 else self.session_start):
                self.clock_queue.put((current_time, CLOCK_MESSAGES.EVENT, self.scheduled_event_list[0][1]))
                del self.scheduled_event_list[0]

            sleep(1)

        self.clock_queue.put((current_time, CLOCK_MESSAGES.FINISH))

# TODO deal with a situation when the scheduler is started after an event

class IntraDayScheduler():

    def __init__(self, event_list,
                 event_operations,
                 event_restarts,
                 event_restart_delays,
                 session_start=None,
                 session_end=None,
                 interval=None):
        now_time = pd.Timestamp(pd.to_datetime('now', utc=True)).astimezone(EASTERN)
        if session_start is None:
            self.session_start = now_time.replace(hour=START_SESSION_HOUR, minute=START_SESSION_MIN, second=0)
        else:
            self.session_start = session_start
        if session_end is None:
            self.session_end = now_time.replace(hour=END_SESSION_HOUR, minute=END_SESSION_MIN, second=0)
        else:
            self.session_end = session_end
        self.interval = interval
        self.event_queue = queue.Queue()
        self.event_list = event_list
        self.event_operations = event_operations
        self.event_restarts = event_restarts
        self.event_restart_delays = event_restart_delays

    def run(self):
        current_stage = ""; last_stage = ""  # not used
        restart = 0; restart_count = 0
        my_context = None
        with RealTimeClock(1, self.session_end, self.session_start, self.event_list, self.event_queue, self.interval) as clock:
            print("started at: ", datetime.now(tz=EASTERN))

            while True:
                if current_stage:
                    if not restart:
                        try:
                            error_code, my_context = self.event_operations[current_stage](my_context)
                        except Exception as e:
                            print('exception', e, flush=True)
                            current_stage = ""
                            continue
                        print("restart", restart_count, error_code, flush=True)
                        if not error_code:
                            current_stage = ""
                            last_stage = current_stage
                        else:
                            if restart_count:
                                restart = self.event_restart_delays[current_stage]
                                restart_count -= 1
                            else:
                                print("failed at", current_stage, flush=True)
                                continue  # even if failed
#                                break  # there is no finishing stage if some stage never succeed
                    else:
                        restart -= 1
                    continue

                if not self.event_queue.empty():
                    current_event = self.event_queue.get()

                    while current_event[1] == CLOCK_MESSAGES.BAR and not self.event_queue.empty(): # roll on if there was a delay
                        current_event = self.event_queue.get()

                    if current_event[1] == CLOCK_MESSAGES.FINISH:
                        break

                    if current_event[1] == CLOCK_MESSAGES.EVENT:
                        print(current_event[2] + " at: " + str(current_event[0]), flush=True)
                        current_stage = current_event[2]
                        try:
                            error_code, my_context = self.event_operations[current_event[2]](my_context)
                        except Exception as e:
                            print('exception', e, flush=True)
                            current_stage = ""
                            continue
                        print('first run', error_code, flush=True)
                        if not error_code:
                            current_stage = ""
                            last_stage = current_event[2]
                        else:
                            restart = self.event_restart_delays[current_event[2]]
                            restart_count = self.event_restarts[current_event[2]]
                        continue
                sleep(1)

            print("finished at: " + str(datetime.now(tz=EASTERN)))
