#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, multiple-statements, missing-function-docstring, missing-class-docstring, fixme. bare-except
"""
Database operations for backtesting
+ winloss metrics

"""

import time
import pickle
import zlib
import json
import pandas as pd
import MySQLdb

DB_HOST = "127.0.0.1"
DB_USER = "zipline"
DB_PASSWD = "zipline_pass"
DB_NAME = "zipline_runs"

def load_journal_from_db(last_row=1000):
    db = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=DB_NAME)
    mycur = db.cursor()
    s2 = "SELECT * FROM journal_records ORDER BY record_datetime DESC LIMIT " + str(last_row)
    mycur.execute(s2, )
    l = list(mycur.fetchall())
    db.close()
    return l

def save_signals_to_db(alg_name, input_date, signals_comment, algorithm_params, signals, market_data,
                       params_extractor, signals_extractor, port=None):
    s1 = """SELECT algorithm_id FROM algorithms_table WHERE name = %s"""
    s2 = """INSERT INTO algorithms_table (name, extractor) VALUES (%s, "%s")"""
    s3 = """INSERT INTO signals_table (algorithm_id, input_date, signals_comment, algorithm_parameters, signals_extractor, market_data) VALUES (%s, %s, "%s", "%s", "%s", %s)"""
    s5 = """INSERT INTO signals_data_table (signals_id, signals) VALUES (%s, _binary "%s")"""

    if port:
        tries = 0
        while tries < 5:
            try:
                db = MySQLdb.connect(host=DB_HOST, port=port, user=DB_USER, passwd=DB_PASSWD, db=DB_NAME)
                break
            except:
                tries += 1
                time.sleep(10)
        else:
            return
    else:
        db = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=DB_NAME)
    mycur = db.cursor()

    i = mycur.execute(s1, (alg_name, ))
    if i == 0:
        mycur.execute(s2, (alg_name, json.dumps(params_extractor)))
        mycur.execute("SELECT LAST_INSERT_ID();")
        algorithm_id = mycur.fetchall()[0][0]
    else:
        algorithm_id = mycur.fetchall()[0][0]

    mycur.execute(s3, (algorithm_id, input_date.strftime('%Y-%m-%d %H:%M:%S'), signals_comment, json.dumps(algorithm_params, default=str), json.dumps(signals_extractor, default=str), market_data))
    mycur.execute("SELECT LAST_INSERT_ID();")
    signals_id = mycur.fetchall()[0][0]
    db.commit()

    mycur.execute(s5, (signals_id, zlib.compress(json.dumps(signals, default=str).encode())))
    db.commit()
    db.close()


def get_last_signal_from_db():
    db = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=DB_NAME)
    mycur = db.cursor()

    s2 = """SELECT algorithm_id, signals_id FROM signals_table order by signals_datetime desc"""
    i = mycur.execute(s2)
    if i == 0:
        db.close()
        return None
    row = mycur.fetchone()
    s1 = """SELECT name FROM algorithms_table WHERE algorithm_id = %s"""
    mycur.execute(s1, (row[0], ))
    name = mycur.fetchone()[0]
    db.close()
    return (row[0], row[1], name)

def load_signals_from_db(alg_name=None, algorithm_id=None, signals_id=None, last_signals=100):
    db = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=DB_NAME)
    mycur = db.cursor()

    if signals_id:
        s2 = """SELECT * FROM signals_table WHERE signals_id = %s"""
        mycur.execute(s2, (signals_id, ))
        l = list(mycur.fetchall())
        db.close()
        return l

    if not algorithm_id and alg_name:
        s1 = """SELECT algorithm_id FROM algorithms_table WHERE name = %s"""
        i = mycur.execute(s1, (alg_name, ))
        if i == 0:
            db.close()
            return []
        algorithm_id = mycur.fetchall()[0][0]

    s2 = "SELECT * FROM signals_table WHERE algorithm_id = %s ORDER BY signals_datetime DESC LIMIT " + str(last_signals)
    mycur.execute(s2, (algorithm_id, ))
    l = list(mycur.fetchall())
    db.close()
    return l

def load_signals_data_from_db(signals_id, port=None):
    if port:
        db = MySQLdb.connect(host=DB_HOST, port=port, user=DB_USER, passwd=DB_PASSWD, db=DB_NAME)
    else:
        db = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=DB_NAME)
    mycur = db.cursor()
    s1 = """SELECT signals FROM signals_data_table WHERE signals_id = %s"""
    i = mycur.execute(s1, (signals_id, ))
    if i == 0:
        db.close()
        return None
    signals = zlib.decompress(mycur.fetchall()[0][0][1:-1]).decode("utf-8")
    db.close()
    return signals


def save_run_to_db(alg_name, run_comment, text_output, run_params, algorithm_params, metrics, params_extractor, x=None, port=None):

    s1 = """SELECT algorithm_id FROM algorithms_table WHERE name = %s"""
    s2 = """INSERT INTO algorithms_table (name, extractor) VALUES (%s, "%s")"""
    s3 = """INSERT INTO saved_runs_table (algorithm_id, run_parameters, algorithm_parameters, metrics, is_xdata, run_comment) VALUES (%s, "%s", "%s", "%s", "%s", "%s")"""
    s4 = """INSERT INTO xdata_table (saved_run_id, xdata1, xdata2) VALUES (%s, _binary "%s", _binary "%s")"""
    s5 = """INSERT INTO text_output_table (saved_run_id, text_output) VALUES (%s, _binary "%s")"""
#    x_columns1 = ('algo_volatility', 'algorithm_period_return', 'alpha',
#       'benchmark_period_return', 'benchmark_volatility', 'beta',
#       'capital_used', 'ending_cash', 'ending_exposure', 'ending_value',
#       'excess_return', 'gross_leverage', 'long_exposure', 'long_value',
#       'longs_count', 'max_drawdown', 'max_leverage', 'net_leverage',
#       'period_close', 'period_label', 'period_open', 'pnl', 'portfolio_value',
#       'returns', 'sharpe', 'short_exposure', 'short_value',
#       'shorts_count', 'sortino', 'starting_cash', 'starting_exposure',
#       'starting_value', 'trading_days',
#       'treasury_period_return', 'universe_size')
    x_columns2 = ('orders', 'positions', 'transactions')
    x_columns1 = tuple(set(x.columns)- set(x_columns2))

    if port:
        tries = 0
        while tries < 5:
            try:
                db = MySQLdb.connect(host=DB_HOST, port=port, user=DB_USER, passwd=DB_PASSWD, db=DB_NAME)
                break
            except:
                tries += 1
                time.sleep(10)
        else:
            return
    else:
        db = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=DB_NAME)
    mycur = db.cursor()

    i = mycur.execute(s1, (alg_name, ))
    if i == 0:
        mycur.execute(s2, (alg_name, json.dumps(params_extractor)))
        mycur.execute("SELECT LAST_INSERT_ID();")
        algorithm_id = mycur.fetchall()[0][0]
    else:
        algorithm_id = mycur.fetchall()[0][0]

    if x is None:
        mycur.execute(s3, (algorithm_id, json.dumps(run_params, default=str), json.dumps(algorithm_params, default=str), json.dumps(metrics, default=str), 0, run_comment))
    else:
        mycur.execute(s3, (algorithm_id, json.dumps(run_params, default=str), json.dumps(algorithm_params, default=str), json.dumps(metrics, default=str), 1, run_comment))
        mycur.execute("SELECT LAST_INSERT_ID();")
        saved_run_id = mycur.fetchall()[0][0]
        db.commit()
        x1 = x.loc[:, x_columns1]
        x2 = x.loc[:, x_columns2]
        mycur.execute(s4, (saved_run_id, zlib.compress(pickle.dumps(x1)), zlib.compress(pickle.dumps(x2))))

    mycur.execute(s5, (saved_run_id, zlib.compress(text_output.encode())))
    db.commit()
    db.close()

def load_algs_from_db(alg_name=None, algorithm_id=None):
    db = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=DB_NAME)
    mycur = db.cursor()
    if alg_name:
        s1 = """SELECT * FROM algorithms_table WHERE name = %s"""
        mycur.execute(s1, (alg_name, ))
    elif algorithm_id:
        s1 = """SELECT * FROM algorithms_table WHERE algorithm_id = %s"""
        mycur.execute(s1, (algorithm_id, ))
    else:
        s1 = """SELECT * FROM algorithms_table ORDER BY name"""
        mycur.execute(s1)
    l = list(mycur.fetchall())
    db.close()
    return l

def load_runs_from_db(alg_name=None, algorithm_id=None, saved_run_id=None):
    db = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=DB_NAME)
    mycur = db.cursor()

    if saved_run_id:
        s2 = """SELECT * FROM saved_runs_table WHERE saved_run_id = %s"""
        mycur.execute(s2, (saved_run_id, ))
        l = list(mycur.fetchall())
        db.close()
        return l

    if not algorithm_id and alg_name:
        s1 = """SELECT algorithm_id FROM algorithms_table WHERE name = %s"""
        i = mycur.execute(s1, (alg_name, ))
        if i == 0:
            db.close()
            return []
        algorithm_id = mycur.fetchall()[0][0]

    s2 = """SELECT * FROM saved_runs_table WHERE algorithm_id = %s"""
    mycur.execute(s2, (algorithm_id, ))
    l = list(mycur.fetchall())
    db.close()
    return l

def load_xdata1_from_db(saved_run_id):
    db = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=DB_NAME)
    mycur = db.cursor()
    s1 = """SELECT xdata1 FROM xdata_table WHERE saved_run_id = %s"""
    i = mycur.execute(s1, (saved_run_id, ))
    if i == 0:
        db.close()
        return None
    x1 = pickle.loads(zlib.decompress(mycur.fetchall()[0][0][1:-1]))
    db.close()
    return x1

def load_xdata2_from_db(saved_run_id):
    db = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=DB_NAME)
    mycur = db.cursor()
    s1 = """SELECT xdata2 FROM xdata_table WHERE saved_run_id = %s"""
    i = mycur.execute(s1, (saved_run_id, ))
    if i == 0:
        db.close()
        return None
    x2 = pickle.loads(zlib.decompress(mycur.fetchall()[0][0][1:-1]))
    db.close()
    return x2

def load_text_output_from_db(saved_run_id):
    db = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=DB_NAME)
    mycur = db.cursor()
    s1 = """SELECT text_output FROM text_output_table WHERE saved_run_id = %s"""
    i = mycur.execute(s1, (saved_run_id, ))
    if i == 0:
        db.close()
        return None
    text_output = zlib.decompress(mycur.fetchall()[0][0][1:-1]).decode("utf-8")
    db.close()
    return text_output

def clean_db(alg_name=None, algorithm_id=None, saved_run_id=None):
    db = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=DB_NAME)
    mycur = db.cursor()
    if not algorithm_id and alg_name:
        s1 = """SELECT algorithm_id FROM algorithms_table WHERE name = %s"""
        i = mycur.execute(s1, (alg_name, ))
        if i == 0:
            db.close()
            return
        algorithm_id = mycur.fetchall()[0][0]

    if saved_run_id:
        s3 = """DELETE FROM xdata_table WHERE saved_run_id = %s"""
        s4 = """DELETE FROM text_output_table WHERE saved_run_id = %s"""
        s5 = """DELETE FROM saved_runs_table WHERE saved_run_id = %s"""
        mycur.execute(s3, (saved_run_id, ))
        mycur.execute(s5, (saved_run_id, ))
        mycur.execute(s4, (saved_run_id, ))
    elif algorithm_id:
        s2 = """DELETE FROM algorithms_table WHERE algorithm_id = %s"""
        s3 = """DELETE xdata_table FROM xdata_table INNER JOIN saved_runs_table ON saved_runs_table.saved_run_id = xdata_table.saved_run_id WHERE saved_runs_table.algorithm_id = %s"""
        s5 = """DELETE text_output_table FROM text_output_table INNER JOIN saved_runs_table ON saved_runs_table.saved_run_id = text_output_table.saved_run_id WHERE saved_runs_table.algorithm_id = %s"""
        s4 = """DELETE FROM saved_runs_table WHERE algorithm_id = %s"""
        s5 = """DELETE FROM signals_table WHERE algorithm_id = %s"""
        mycur.execute(s2, (algorithm_id, ))
        mycur.execute(s3, (algorithm_id, ))
        mycur.execute(s5, (algorithm_id, ))
        mycur.execute(s4, (algorithm_id, ))
        mycur.execute(s5, (algorithm_id, ))
    else:
        s2 = """DELETE FROM algorithms_table"""
        s3 = """DELETE FROM xdata_table"""
        s5 = """DELETE FROM text_output_table"""
        s4 = """DELETE FROM saved_runs_table"""
        s5 = """DELETE FROM signals_table"""
        mycur.execute(s2)
        mycur.execute(s3)
        mycur.execute(s5)
        mycur.execute(s4)
        mycur.execute(s5)

    db.commit()
    db.close()

def slice_EOD_data(csvfile="~/.zipline/EOD_all/EOD_20200129.csv", outputdir="~/.zipline/EOD_all/daily"):

    skiprows = 0
    sid = 0
    step = 1000000 #0

    csv_names = ['Symbol', 'Date', 'open', 'high', 'low', 'close', 'volume', 'ex_dividend', 'split_ratio', 'Adj. Open', 'Adj. High', 'Adj. Low', 'Adj. Close', 'Adj. Volume']
    pa = pd.read_csv(csvfile, skiprows=skiprows, nrows=step, header=None, names=csv_names, parse_dates=['Date'])

    while len(pa) != 0:
        sid += 1
        symbol = pa.Symbol.iloc[0]
        pa_current = pa.loc[pa.Symbol == symbol]
        pa = pa.loc[pa.Symbol != symbol]
        if len(pa) == 0:
            skiprows = skiprows + step
            if sid < 500000:
                print('read next ' + str(step) + ' rows')
                pa = pd.read_csv(csvfile, skiprows=skiprows, nrows=step, header=None, names=csv_names, parse_dates=['Date'])
                pa_current = pa_current.append(pa.loc[pa.Symbol == symbol])
                pa = pa.loc[pa.Symbol != symbol]

#        start_date = pa_current.Date.iloc[0]
#        end_date = pa_current.Date.iloc[-1]
#        ac_date = end_date + pd.Timedelta(days=1)
#        metadata.loc[sid] = start_date, end_date, ac_date, symbol

        pa_current.set_index(keys=['Date'], drop=True, inplace=True)
        pa_data = pa_current.drop(columns=["Symbol", 'Adj. Open', 'Adj. High', 'Adj. Low', 'Adj. Close', 'Adj. Volume'])

#        sessions = calendar.sessions_in_range(start_session, end_session)
#        pa_data = pa_data.reindex(
#            sessions.tz_localize(None),
#            copy=False,
#            fill_value=0.0,
#        ).fillna(0.0)

        pa_data.to_csv(outputdir+"/"+symbol+".csv")


def _metrics_winloss(x, exclude):
    xx = x.loc[x['transactions'].apply(lambda t: len(t)) > 0].transactions
    position_list = []
    for d in xx:
        for transaction in d:
            if transaction['sid'].symbol in exclude:
                continue
            position = [p for p in position_list if p[0] == transaction['sid'].symbol]
            if position:
                # print(position)
                if not position[0][3]:
                    position[0][3].append((transaction['amount'], transaction['price'], transaction['commission']))
                else:
                    if position[0][3][0][0] * transaction['amount'] > 0:
                        position[0][3].append((transaction['amount'], transaction['price'], transaction['commission']))
                    else:
                        num = 0
                        pos = 0.0
                        invest_total = 0.0
                        comm = transaction['commission'] if 'commission' in transaction else 0
                        while abs(num) < abs(transaction['amount']):
                            # print(position[0][3][0], num, pos, transaction['amount'])
                            if abs(num + position[0][3][0][0]) >= abs(transaction['amount']):
                                amount = position[0][3][0][0] + transaction['amount'] + num
                                price = position[0][3][0][1]
                                comm += position[0][3][0][2]
                                invest_total += position[0][3][0][0] * price
                                num += position[0][3][0][0]
                                pos -= position[0][3][0][0] * price
                                pos_price = pos / num
                                pos = -transaction['amount'] * (transaction['price'] + pos_price)
                                del position[0][3][0]
                                if amount != 0:
                                    position[0][3].insert(0, (amount, -pos_price, 0))
                                else:
                                    break
                            else:
                                price = position[0][3][0][1]
                                comm += position[0][3][0][2]
                                num += position[0][3][0][0]
                                pos -= position[0][3][0][0] * price
                                del  position[0][3][0]
                                if not position[0][3]:
                                    break

                        # print(num, pos)
                        if abs(num) == abs(transaction['amount']):
                            if pos >= 0:
                                position[0][1] += 1
                                position[0][6] += pos - comm
                                position[0][4] += invest_total
                            else:
                                position[0][2] += 1
                                position[0][7] += pos - comm
                                position[0][5] += invest_total
                        elif abs(num) < abs(transaction['amount']):
                            position[0][3].append((transaction['amount'] + num, transaction['price'], transaction['commission']))
                            if (num > 0 and pos/num <= transaction['price']) or (num < 0 and -pos/num >= transaction['price']):
                                position[0][1] += 1
                                position[0][6] += pos - comm
                                position[0][4] += invest_total
                            else:
                                position[0][2] += 1
                                position[0][7] += pos - comm
                                position[0][5] += invest_total
                        else:
                            if (num > 0 and pos >= 0) or (num < 0 and pos <= 0):
                                position[0][1] += 1
                                position[0][6] += pos - comm
                                position[0][4] += invest_total
                            else:
                                position[0][2] += 1
                                position[0][7] += pos - comm
                                position[0][5] += invest_total
                        # print(position)

            else:
                position_list.append([transaction['sid'].symbol, 0, 0, [(transaction['amount'], transaction['price'], transaction['commission'])], 0, 0, 0, 0])

    # print(position_list)
    n_win = 0; n_loss = 0
    in_win = 0; in_loss = 0; profit = 0; loss = 0
    for position in position_list:
        num = 0
        pos = 0.0
        if position[3]:
            for (p_amount, p_price, p_commission) in position[3]:
                num += p_amount
                pos += p_amount * p_price
            pr = [q['last_sale_price'] for q in x.positions[-1] if position[0] == q['sid'].symbol][0]
            if (num > 0 and pos < pr * num) or (num < 0 and pos > pr*num):
                position[1] += 1
                position[6] += pos
            else:
                position[2] += 1
                position[7] += pos
        n_win += position[1]
        n_loss += position[2]
        profit += position[6]
        loss += position[7]
        in_win += position[4]
        in_loss += position[5]
    if n_loss != 0:
        return n_win, n_loss, in_win, in_loss, profit, loss, n_win/n_loss
    return n_win, n_loss, in_win, in_loss, profit, loss, "no loss"

def metrics_winloss(x, exclude):
    n_win, n_loss, in_win, in_loss, profit, loss, ratio = _metrics_winloss(x, exclude)
    return ratio

    
    
