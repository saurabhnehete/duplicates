# -*- coding: utf-8 -*-
"""
Various tools for reading and parsing configuration files
"""
from global_cfg import logger

import re
from typing import Iterable


class ConfigReader:
    """A base class for all config-file parsers.

    This class can be used on its own or 
    it can be inherited by any other specific config
    parser to add some additional functionality.
    """

    __not_specified_flag = '__not_specified_by_the_user__'

    def __init__(self, cfg_path: str):
        self._parse_config_file(cfg_path)
        self._log_config(cfg_path)

    def _parse_config_file(self, cfg_path: str = None, cfg: dict = None) -> None:
        """Reads config file provided as 'config_path' or 'config'
        into the class instance. All parameters listed in
        the config files are executed into instance variables.
        
        Either 'cfg_path' or 'cfg' is needed. If provide both, 'cfg_path' will
        be used

        In case a parameter is already set in the class instance, raises
        a "KeyError".
        """
        if cfg_path is not None:
            cfg = self.read_variables(cfg_path)
            logger.debug(f"Parsing '{cfg_path}'...")
        for key, value in cfg.items():
            if key in dir(self):
                raise KeyError(
                    f"Parameter name '{key}' is already used in '{cfg_path}'."
                    )
            else:
                exec(f"self.{key} = '{value}'")

    def read_variables(self, path: str) -> dict:
        """Reads config files line-by-line and parses them into
        key-value pair returning a dictionary of content;

        Raises a ValueError in case ":" is missing in a line.
        """

        try:
            with open(path, encoding='utf-8') as f:
                c = f.readlines()
            c = [x.strip('\n') for x in c]                                    # removing trailing \n
            c = [x for x in c if not x.strip().startswith('#')]               # ignoring comments
            c = [x for x in c if not x.strip() in ('','\n')]                  # ignoring empty lines
            c = '\n'.join(c).replace('\n\t', '').split('\n')
            c = {x.split(':')[0].strip():                                     # taking 1st part
                                    ':'.join(
                                        [e.strip() for e in x.split(':')[1:]] # joining remaining parts
                                        ) for x in c}
            cfg = {(k):(self.__not_specified_flag if v=='' else v) for k,v in c.items()}
        except:
            name = path.split('\\')[-1]
            raise ValueError(f'Cannot read the config file {name}. '\
                            'Make sure to seperate pairs of values with ":"')
        return cfg

    @property
    def parameters(self) -> dict:
        """Returns all configuration parameters
        """
        return self.__dict__

    @property
    def populated(self) -> dict:
        """Returns only populated configuration parameters
        """
        return {key:value 
                    for key, value 
                        in self.parameters.items()
                            if value != self.__not_specified_flag}
    
    @property
    def populated_tuples(self) -> list:
        """Returns list of tuples of populated configuration parameters
        """
        return list(self.populated.items())
    
    @property
    def not_specified_flag(self) -> str:
        return self.__not_specified_flag
    
    def _log_config(self, cfg_path: str) -> None:
        """Logs configuration values"""
        logger.debug(f"Values of '{cfg_path}':")
        for key, value in self.parameters.items():
            logger.debug(f'{key:>20} : "{value}"')


class DatabaseConfigReader(ConfigReader):
    """A special config reader for "database.txt".
    
    Note that self.table and the related names like self.Ttable are of the type
    property and not a variable. because it is intended to be dynamic for the
    E2E tool.

    Added functionality:
     - constructing various table names
    """

    def __init__(self, cfg_path: str, table_prefixes: Iterable):
        self.cfg_path = cfg_path
        self.table_prefixes = table_prefixes
        self.table_key = 'table'
        
        cfg = self.read_variables(self.cfg_path)
        cfg.pop(self.table_key)
        self._parse_config_file(cfg = cfg)
        
        self._define_extra_variables()
        self._log_config(self.cfg_path)

    def _define_extra_variables(self):
        self.original_table = self.table        # for loggign purposes
        # self.NTtable = self.getTempName(self.table_not_geocode)
        self.OLDTtable = self.getTempName(self.table_old)
        # self.NOtable = self.NTtable.replace('tmp_','out_')

    @property
    def table(self):
        cfg = self.read_variables(self.cfg_path)
        table_nm = cfg[self.table_key]
        # logger.debug(f'database.table = {table_nm}')
        return table_nm
    
    @property
    def Ttable(self): return self.getTempName(self.table)

    @property
    def Stable(self): return self.Ttable.replace('tmp_','sts_')
    
    @property
    def Otable(self): return self.Ttable.replace('tmp_','out_')
    
    @property
    def OLDOtable(self): return self.OLDTtable.replace('tmp_','out_')

    @property
    def Rtable(self): return self.Ttable.replace('tmp_','')

    @property
    def VTtable(self): return self.getTempName(self.vls_table + self.table)

    def getTempName(self, table: str, max_allowed: int=59) -> str:
        """Adds a 'tmp_' prefix to the source table name.
        - removes all of the known prefixes from the table name.
        - fixes the length of the table name if greater than
        "max_allowed" value.

        NOTE: Changes behaviour of the previous version of this function!
        """
        temp_prefix = 'tmp_'
        prefixes = [f'^{p}' for p in self.table_prefixes]
        prefixes_pattern = '|'.join(prefixes)
        temp = table
        for i in range(len(self.table_prefixes)):
            temp = re.sub(prefixes_pattern, '', temp, re.I)
        if not table.startswith(temp_prefix):
            temp = temp_prefix + temp

        temp = self.fix_table_name_length(temp, max_allowed)
        return temp

    def get_output_table_name(self, table: str=None, prefix: str='out_') -> str:
        """
        Returns the output table name having the input name
        prefix will be added and replaced by tmp_
        deafult table input is the self.Ttable which is the temp table
        """
        if table is None:
            table = self.Ttable
        if table.startswith('tmp_'):
            new_table_name = table.replace('tmp_', prefix)
        else:
            new_table_name = prefix + table
        new_table_name = self.fix_table_name_length(new_table_name)
        return new_table_name
    
    def get_output_dict(self):
        """
        Returns the standard final output table created by each tool
        """
        out_dict = {
            1: self.get_output_table_name(prefix = 'chr_'), 
            2: self.get_output_table_name(prefix = 'gen_'),
            3: self.get_output_table_name(prefix = 'add_'),
            4: self.get_output_table_name(prefix = 'dup_'),
            5: self.get_output_table_name(prefix = 'frq_'),
            # 6: location_accuracy,
            # 7: f'loc_vls_merged_{self.table}',
            7: self.get_output_table_name(prefix = 'loc_'),
            8: self.get_output_table_name(prefix = 'fst_'),
            9: self.get_output_table_name(prefix = 'mch_'),
            10: None,
            12: None,
            13: None,
            14: None,
            15: None,
            16: self.get_output_table_name(prefix = 'ath_'),
            17: self.get_output_table_name(prefix = 'bld_'),
            18: self.get_output_table_name(prefix = 'ggl_')
            }
        return out_dict
    
    def get_output_table_by_tool(self, tool_id: int):
        """
        Returns the standard final output table for one tool
        """
        out_dict = self.get_output_dict()
        return out_dict[tool_id]

    @staticmethod
    def fix_table_name_length(name: str, max_allowed: int=59) -> str:
        """Fixes the length of the string if its length is greater than
        "max_allowed" value.
        """
        name_mod = name
        if len(name) > max_allowed:
            name_mod = name.replace('_etc_','')[:max_allowed-5] + '_etc_'
        return name_mod


class statCols:
    query = 'Query'
    result = 'Result'
    reject = 'Reject'
    percent = 'Percentage'
    

class ConfigParser:
    """A toolbox for parsing configuration files"""

    @staticmethod
    def str_to_list(item: str) -> list:
        """
        Converts a list-like string into a list object.
        Raises ValueError if string cannot be converted into list.

        NOTE: Elements in the input list must not have quotations!

        EXAMPLE:
        "[A, B, C]" -> ['A', 'B', 'C'];
        "['A', 'B', 'C']" -> ["'A'", "'B'", "'C'"]; 
        """
        if item == '[]' or not item:
            return []
        try:
            converted = item.strip('[]').split(', ')
        except:
            raise ValueError("Value must be a list-like string!")
        return converted

    @staticmethod
    def str_to_int(input: str) -> int:
        """Converts a string into int.
        If string is not convertable, raises ValueError
        """
        try:
            number = int(input)
            return number
        except ValueError:
            raise ValueError(f"Cannot convert '{input}' to int!")

    @staticmethod
    def validate_available(input: str, available: Iterable) -> None:
        """Raises ValueError if provided input 
        value is not in the list of available ones.
        NOTE: validation is case-insensitive!
        """
        available_upper = {value.upper() for value in available}
        if input.upper() not in available_upper:
            str_available = ', '.join(available)
            raise ValueError(f"Unknown value: '{input}'. Available values: {str_available}")

    @staticmethod
    def validate_regex(pattern: str) -> None:
        """Validates a regex by trying to compile it.
        Throws ValueError if regex is incorrect."""
        try:
            re.compile(pattern)
        except re.error as msg:
            raise ValueError(f"Invalid regex '{pattern}': {msg}")

    @staticmethod
    def search_str(pattern: str, string: str) -> str:
        """Executes re.search and returns first match"""
        try:
            output = re.search(pattern, string)[0]
            return output
        except re.error:
            raise ValueError(f'Incorrect regular expression: {pattern}')