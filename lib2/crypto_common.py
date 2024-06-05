#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from enum import Enum

ORDER_BOOK_DEPTH = 5
CVI_URL = "http://defi.r-synergy.com/V004/cvijson"
CVI_URL1 = "http://defi1.r-synergy.com/V004/cvijson"
HEADERS = {'Content-Type': "application/json"}

class crypto_currencies(Enum):
    BTC = "BTC"
    ETH = "ETH"
    # SOL = "SOL"

def crypto_currencies_cardinal(c):
    if c == "BTC":
        return 0
    elif c == "ETH":
        return 1
    return None

def get_cvi():
    try:
        res = requests.get(CVI_URL, headers=HEADERS, timeout=5)
    except:
        res = None
    if not res or res.status_code != 200:
        try:
            res = requests.get(CVI_URL1, headers=HEADERS, timeout=5)
        except:
            res = None
        if not res or res.status_code != 200:
            return 0
    c = res.json()
    return c['cvi-ema']
