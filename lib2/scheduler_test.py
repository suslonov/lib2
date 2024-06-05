#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import pytz
import pandas as pd

from lib2.scheduler import IntraDayScheduler

EASTERN = pytz.timezone('US/Eastern'); JERUSALEM = pytz.timezone('Asia/Jerusalem'); UTC = pytz.UTC

now_time = pd.Timestamp(pd.to_datetime('now')).tz_localize(tz=UTC).astimezone(EASTERN)

session_start = now_time + pd.Timedelta(20, 's')
session_end = now_time + pd.Timedelta(100, 's')

def init_stage_paper(my_context):
    print("init_stage_paper")
    return 0, my_context
    
def prepare_stage(my_context):
    print("prepare_stage")
    return 0, my_context

def trade_stage(my_context):
    print("trade_stage")
    return 0, my_context

def final_stage(my_context):
    print("final_stage")
    return 0, my_context

def killall_stage(my_context):
    print("killall_stage")
    return 0, my_context

event_operations = {"InitStage": init_stage_paper,
                    "PrepareStage": prepare_stage,
                    "TradeStage": trade_stage,
                    "FinalStage": final_stage,
                    "KillALLStage": killall_stage}

event_restarts = {"InitStage": 3, "PrepareStage": 3, "TradeStage": 3, "FinalStage": 3, 'KillALLStage': 3}
event_restart_delays = {"InitStage": 1, "PrepareStage": 1, "TradeStage": 1, "FinalStage": 1, 'KillALLStage': 1}

event_list = []
event_list.append((-pd.Timedelta('1 second') * 7, "InitStage", None))
event_list.append((pd.Timedelta('1 second') * 1, "PrepareStage", "InitStage"))
event_list.append((pd.Timedelta('1 second') * 2, "TradeStage", "PrepareStage"))
event_list.append((pd.Timedelta('1 second') * 3, "FinalStage", None))
event_list.append((pd.Timedelta('1 second') * 6, "KillALLStage", None))

scheduler = IntraDayScheduler(event_list,
                              event_operations,
                              event_restarts,
                              event_restart_delays,
                              session_start,
                              session_end,
                              interval=pd.Timedelta('1 second'))

scheduler.run()
