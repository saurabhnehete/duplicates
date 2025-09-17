from global_cfg import logger
from global_cfg import version
from global_cfg import OUTPUT_FOLDER
from common import UUID
from common import query_toolbox as toolbox
from common import database as db
from enum import Enum
from datetime import datetime
import pandas as pd
import os


class Result(Enum):
    FAILED = 0
    SUCCESSFUL = 1


class CentralMonitoring:
    """Monitors the runs and logs the attributes in two tables: one table containing details and one table only the logs
    The tables should be updated only when the tool is running by users not developers.
    """
    def __init__(self, monitor: bool = True):
        """If monitor = False, then the monitor tables are not updated. It is recommended to put to False during
        development
        """
        self.monitor = monitor
        self.temp_path_runs = os.path.join(OUTPUT_FOLDER, '_runs_to_upload_do_not_delete_.csv')
        self.temp_path_logs = os.path.join(OUTPUT_FOLDER, '_logs_to_upload_do_not_delete_.csv')
        self.schema = 'pde_source_evaluation_tool'
        self.runs_table = 'runs'
        self.logs_table = 'logs'

        # Attribute names
        self._run_id = 'run_id'
        self._user = 'user'
        self._tool_name = 'tool_name'
        self._input_table_name = 'table'
        self._input_table_size = 'table_size'
        self._dt_start = 'start_date_time'
        self._dt_end = 'end_date_time'
        self._duration = 'duration'
        self._version = 'version'
        self._result = 'result'
        self._message = 'message'
        self._pid = 'pid'
        self._log = 'log'

        # Other variables
        self.tool_name = None
        self.result = None
        self.message = None
        self.log_path = None
        self.log = None
        self.start_date_time = None
        self.input_table_size = None
        self.user = ''

        if self.monitor:
            self.set_start_date_time()
            self._set_version()
            self._set_run_id()
            self._set_pid()
            self._set_input_table_name()

    def set_start_date_time(self):
        self.start_date_time = datetime.now()
        self.start_date_time = datetime.strftime(self.start_date_time, '%Y-%m-%d %H:%M:%S')
        self.start_date_time = datetime.strptime(self.start_date_time, '%Y-%m-%d %H:%M:%S')
        # self.date_time = datetime.strftime(self.date_time, '%Y-%m-%d %H:%M:%S')

    def _set_input_table_name(self):
        self.input_table_name = f'{db.schema}.{db.table}'

    def set_input_table_size(self):
        if self.monitor:
            logger.info('Getting input table size')
            self.input_table_size = toolbox.get_table_size(db.table)

    def _set_end_date_time_and_duration(self):
        self.end_date_time = datetime.now()
        duration = self.end_date_time - self.start_date_time
        seconds = duration.seconds
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60
        self.duration = datetime.strptime(f'{hours}:{minutes}:{seconds}', '%H:%M:%S').time()

    def _set_version(self):
        self.version = version

    def _set_run_id(self):
        self.run_id = UUID

    def _set_pid(self):
        self.pid = os.getpid()

    def upload_result(self):
        if self.monitor:
            self._set_end_date_time_and_duration()
            df_run = pd.DataFrame([{
                self._run_id: self.run_id,
                # self._user: self.user,
                self._pid: self.pid,
                self._tool_name: self.tool_name,
                self._input_table_name: self.input_table_name,
                self._input_table_size: self.input_table_size,
                self._dt_start: self.start_date_time,
                self._duration: self.duration,
                self._version: self.version,
                self._result: self.result,
                self._message: self.message,
            }])
            self._read_log()
            df_log = pd.DataFrame([{
                self._run_id: self.run_id,
                self._log: self.log,
            }])
            self._upload_or_save(df_run, self.runs_table, self.temp_path_runs)
            self._upload_or_save(df_log, self.logs_table, self.temp_path_logs)

    def _upload(self, df, table):
        logger.info(f'Updating monitoring table at {self.schema}.{table}')
        toolbox.dataFrameToDB(df, name=table, schema=self.schema, if_exists='append')

    def _upload_or_save(self, df, table, local_path):
        try:
            self._upload(df, table)
            self._upload_temp_if_exists(table, local_path)
        except:
            logger.info(f'Cannot store {table} on db so storing them locally.')
            df.to_csv(local_path, mode='a', sep='|', index=False)

    def _upload_temp_if_exists(self, table, local_path):
        if os.path.isfile(local_path):
            logger.info(f'Updating monitoring table from temp at {self.schema}.{table}')
            df = pd.read_csv(local_path, sep='|')
            self._upload(df, table)
            os.remove(local_path)

    def _read_log(self):
        with open(self.log_path, 'r', encoding='utf-8') as file:
            log = file.read()
        self.log = log

    def set_tool_name(self, tool_name):
        self.tool_name = tool_name

    def set_log_path(self, log_path):
        self.log_path = log_path

    def set_user(self):
        if self.monitor:
            while not self.user:
                text = 'Please type your user name (same as Windows login user name): '
                self.user = input(text).strip().lower()

    def success(self):
        self.result = Result(1).name
        self.message = Result(1).name

    def fail(self, error_msg):
        self.result = Result(0).name
        self.message = error_msg


if __name__ == '__main__':
    self = CentralMonitoring()
    self.set_tool_name('E2E')
    self.set_log_path('logs/main-9120.log')
