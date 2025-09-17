# -*- coding: utf-8 -*-
"""
Generate and save log file readable by user
"""
from global_cfg import logger
from common import OUTPUT_FOLDER
from common import E2E_LOG
import sys, os, re
from datetime import datetime
import pandas as pd

class SQLLogger:
    """Writes a log file out of provided messages. 
    Needs to be initilised BEFORE the queries are executed, so that
    start time and duration are calculated correctly.
    """
    lenth = 100
    llenth = lenth - 5
    time_format = "%Y-%m-%d %H:%M:%S"
    
    def __init__(self, logName='ExecutionLog') -> None:
        self.time_started = datetime.now()
        self.logName = logName
        self.temp_table_info = {}
        logger.info(f'SQLLogger "{self.logName}" has been initialized')

    def printDateTime(self):
        self.time_finished = datetime.now()
        duration = str(self.time_finished - self.time_started).split('.')[0]
        start_time = datetime.strftime(self.time_started, self.time_format)
        finish_time = datetime.strftime(self.time_finished, self.time_format)

        print(f"Execution started: {start_time}")
        print(f"Execution finished: {finish_time}")
        print(f"Duration: {duration}\n")

    def processLog(self, log: list) -> list:
        log = [[i+1]+x[1:] for i, x in enumerate(log)]
        return log
    
    def printLog(self, log: list) -> None:
        if len(log)>0:
            print('Execution result:\n')
            for l in log:
                print(f'{l[0]} - Affected Rows: {l[2]}')
                print(f'\n{l[1]}')
                print('-' * self.lenth)

    def printPLog(self, plog: list) -> None:
        plog = [x for x in plog if not x in [None, '']]
        if len(plog)>0:
            print('WARNINGS:')
            n = 1
            for l in plog:
                print(f'{n} - {self.shortenText(l)}')
                n += 1
            print('='*self.lenth)
            print()

    def print_temp_table_info(self):
        """
        !!!FOR DEDUP ONLY!!!
        Informs user about how 
        temp tables were created"""
        parts = self.temp_table_info
        if parts:
            if len(parts) > 1:
                print("Following parts were created:")
                for part in parts.values():
                    table = part['table']
                    size = part['total']
                    split_by = part['split_by']
                    items = part['items']
                    print(f"'{table}': size = {size}, '{split_by}' = {items}")
            print('='*self.lenth)

    def newLine(self, line: str):
        s = [m.start() for m in re.finditer('\s', line)]
        r = [x % self.llenth for x in s if x<self.llenth][-1] 
        line = [line[:r], line[r+1:]]
        return line

    def shortenText(self, txt: str):
        txt = txt.split('\n')
        while len(txt[-1]) > self.llenth:
            line = txt[-1]
            del txt[-1]
            txt += self.newLine(line)
        txt = '\n'.join(map(str, txt))
        return txt

    def saveLog(self, log: list, plog: list, logName: str) -> None:
        file_name = logName + '.log'
        logger.info('Saving log file...')
        log = self.processLog(log)
        path = os.path.join(OUTPUT_FOLDER, file_name)
        if os.path.isfile(path):
            os.remove(path)
        old_stdout = sys.stdout
        with open(path,'w', encoding="utf-8") as log_file:
            sys.stdout = log_file
            self.printDateTime()
            self.printPLog(plog)
            self.print_temp_table_info()
            self.printLog(log)
            sys.stdout = old_stdout
        logger.info(f'The file {file_name} has been saved.')

    def writeLog(self, log: list=[], plog: list=[]) -> None:
        self.saveLog(log, plog, self.logName)

class E2ETempLog:
    """Manage temp and final end to end log"""
    
    def __init__(self):
        self.tmp_log_path = E2E_LOG
        self.output_folder = OUTPUT_FOLDER
        self._NA = 'N.A.'
        self._status = 'Status'
        self._duration = 'Run Time'
        self._main_input = 'Main Input'
        self._final_output = 'Final Output'
        self._input_count = 'Input Count'
        self._output_count = 'Output Count'
        self._pending = 'Pending'
        self._running = 'Running...'
        self._secceeded = 'Done'
        self._failed = 'Failed'
        self._dropped = ' (dropped)'
        
    def delete_temp_log(self):
        if os.path.isfile(self.tmp_log_path):
            os.remove(self.tmp_log_path)
            
    def delete_all_logs(self):
        files = [x for x in os.listdir(self.output_folder) if x.startswith('e2e') and x.endswith('.log')]
        for file in files:
            os.remove(os.path.join(self.output_folder, file))
            
    def read_log(self):
        if os.path.isfile(self.tmp_log_path):
            log = pd.read_csv(self.tmp_log_path, index_col=0)
            mode = log.index.tolist()[-2]
            init_table = log.index.tolist()[-1]
            log = log.iloc[:-2,:]
            return log, mode, init_table
        
    def pprint_log(self, log, running = True):
        """pretty print temp log"""
        if not running:
            log.loc[log[self._status]==self._running, self._status] = self._failed
        print(log.fillna('N.A.').to_markdown(tablefmt="psql"))
    