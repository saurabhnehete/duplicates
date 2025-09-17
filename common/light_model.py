# -*- coding: utf-8 -*-
"""
Collection of functions and classes used by the light tool
"""
from global_cfg import logger
from common import database as db
from common import run as light
from common import user_fieldname as f
from common import query_toolbox as toolbox
from common.configs import statCols as st

import re
from pandas import DataFrame
import pandas as pd
import copy

class PreRun:
    """Parent class containing general methods and variables"""
    
    def __init__(self) -> None:
        self.run_light = light.light_mode.lower()
        self._check_separator()

    def _check_separator(self):
        col = light.separator
        if col in f.parameters:
            self.col = f.parameters[col]
        else:
            raise ValueError(f'Incorrect "separator" value in run.txt config "{col}". Please check.')
        
    def can_run_light(self, table) -> bool:
        """Check if the requirements are met.
        Requirements are:
            1- light_mode set to true in the configuration.
            2- Separator column (supplier name) exists.
            3- Having 2 or more supplier names populated.
        """
        can_run = False
        if self.run_light == 'true':
            columns = toolbox.list_available_columns(table = table)
            if self.col in columns:
                logger.debug(f'{table}: Separator column exists')
                query = f'select distinct({self.col}) from {db.schema}.{table}'
                unique_vals = toolbox.toDataFrame(query).iloc[:,0].tolist()
                if any(x is None for x in unique_vals):
                    raise Exception(f'{table} on light mode: NULL value is not allowed in {self.col}')
                        
                no_of_unique_vals = len(unique_vals)
                if no_of_unique_vals > 1:
                    logger.debug(f'{table}: More than one unique value found in {self.col}')
                    logger.debug('Running light mode')
                    can_run = True
                else:
                    logger.debug(f'{table}: Separator exists but only one value populated')
            else:
                logger.debug(f'{table}: Separator column does not exist')
        else:
            logger.debug('Light mode is set to False')
        return can_run

    def can_run_light_one_condition(self) -> bool:
        """Checks if light_mode set to true"""
        can_run = False
        if self.run_light == 'true':
            logger.debug('Light mode is set to True')
            can_run = True
        else:
            logger.debug('Light mode is set to False')
        return can_run
            

class StatsPerSupplier(PreRun):
    """Get the original stats queries from a tool and generate stats queries
    per supplier in a dataframe
    """
    
    def __init__(self,
                 queries,
                 executor,
                 funcs: list = [],
                 tables: list = [db.Ttable],
                 indexed = False,
                 post_processor = None,
                 concatenate_results = True) -> None:
        """queries can be string or a list. If it is a list, it can be list of queries or
        list of sub-lists. first element of each sub-list is the 
        query explanation and the second element is the actual query - or queries
        are just a list of queries. Depends on the executor what it accepts.
        funcs is list of tuples to be run on the generated dataframe. First 
        element of each tuple is the function second are args and third are kwargs
        executor is the function that executes the queries
        """
        super().__init__()
        self.tables = tables
        if isinstance(queries, (list, dict)):
            self.queries = queries.copy()
        elif isinstance(queries, str):
            self.queries = queries
        else:
            self.queries = queries.copy()
        self.null = '__NULL__'
        self.all = 'ALL'
        self.funcs = funcs
        self.executor = executor
        self.indexed = indexed
        self.post = post_processor
        self.concatenate_results = concatenate_results
    
    def get_unique_values(self) -> None:
        """Get list of unique separators"""
        self.unique_vals = set()
        for table in self.tables:
            query = f'select distinct({self.col}) from {db.schema}.{table}'
            self.unique_vals = self.unique_vals | set(toolbox.toDataFrame(query).iloc[:,0].fillna(self.null).tolist())
    
    def generate_original_stats(self) -> DataFrame:
        """Generate the original stats dataframe (like there is no separator)
        """
        q = copy.deepcopy(self.queries)
        df = self.executor(q)
        for func, args, kwargs in self.funcs:
            df = func(df, *args, **kwargs)
        # df = df.set_index(st.query)
        if not self.indexed:
            df = df.reset_index(drop = True)
        return df
   
    def modify_column_names(self, df: DataFrame, val: str) -> DataFrame:
        if len(df.columns) > 0:
            if not self.indexed:
                if st.query in df:
                    self.index_col = st.query
                else:
                    self.index_col = df.columns.tolist()[0]
                df = df.set_index(self.index_col)
            cols = df.columns.tolist()
            cols = [*map(lambda x: f'{x}_{val}', cols)]
            df.columns = cols
            if not self.indexed:
                df = df.reset_index()
        return df
    
    def split_tables(self) -> None:
        self.sliced_tables = {val: [] for val in self.unique_vals}
        for val in self.unique_vals:
            val_in_query = 'IS NULL' if val == self.null else f"= '{val}'"
            for table in self.tables:
                tbl_nm = val.replace(' ','_').lower() + '_' + table
                tbl_nm = toolbox.fix_table_name_length(tbl_nm)
                toolbox.drop_table(tbl_nm)
                q = f"""CREATE TABLE {db.schema}.{tbl_nm} AS
                        SELECT * FROM {db.schema}.{table}
                        WHERE {self.col} {val_in_query}"""
                toolbox.execute(q)
                logger.debug(f'Table {tbl_nm} has been created')
                self.sliced_tables[val] += [tbl_nm]
    
    def modify_queries_list(self) -> dict:
        queries_mod = {val: [] for val in self.unique_vals}
        for query in self.queries:
            if isinstance(query, list):
                q = query[1]
                q_key = query[0] # light_model_bug_fix
                for supplier, tables in self.sliced_tables.items():
                    qmode = q
                    qmode = self.replace_table_names(qmode, tables)
                    queries_mod[supplier].append([q_key, qmode])
            elif isinstance(query, str):
                q = query
                q_key=q # light_model_bug_fix
                for supplier, tables in self.sliced_tables.items():
                    qmode = q
                    qmode = self.replace_table_names(qmode, tables)
                    queries_mod[supplier].append(qmode)
            else:
                logger.error('Invalid query type %s' %type(query))
            # for supplier, tables in self.sliced_tables.items():
            #     qmode = q
            #     qmode = self.replace_table_names(qmode, tables)
            #     queries_mod[supplier].append([q_key,qmode]) # light_model_bug_fix
        return queries_mod
    
    def modify_queries_str(self) -> dict:
        queries_mod = {val: '' for val in self.unique_vals}
        for supplier, tables in self.sliced_tables.items():
            qmode = self.queries
            qmode = self.replace_table_names(qmode, tables)
            queries_mod[supplier] = qmode
        return queries_mod
    
    def modify_queries_dict(self) -> dict:
        queries_mod = {val: [] for val in self.unique_vals}
        for supplier, tables in self.sliced_tables.items():
            sub_queries_mod = {k: [] for k in self.queries}
            for k, query in self.queries.items():
                if isinstance(query, list):
                    qmode = query.copy()
                    for q in range(len(qmode)):
                        qmode[q] = self.replace_table_names(qmode[q], tables)
                    sub_queries_mod[k] = qmode
                elif isinstance(query, str):
                    qmode = query
                    qmode = self.replace_table_names(qmode, tables)
                    sub_queries_mod[k] = qmode
                elif isinstance(query, dict):
                    qmode = query.copy()
                    for x in qmode:
                       qmode[x] = self.replace_table_names(qmode[x], tables)
                    sub_queries_mod[k] = qmode
                else:
                    logger.error('Invalid query type %s' %type(query))
                queries_mod[supplier] = sub_queries_mod
        return queries_mod
    
    def replace_table_names(self, query, tables: list):
        """tables are table names for a specific supplier"""
        if query:
            # print(type(query))
            for i in range(len(self.tables)):
                query = re.sub(r"\.%s\b" % self.tables[i] , f'.{tables[i]}', query)
        return query

    def modify_queries(self) -> dict:
        if isinstance(self.queries, list):
            queries_mod= self.modify_queries_list()
        elif isinstance(self.queries, str):
            queries_mod = self.modify_queries_str()
        elif isinstance(self.queries, dict):
            queries_mod = self.modify_queries_dict()
        else:
            raise TypeError('Invalid query type %s' %type(self.queries))
        return queries_mod
    
    def concat_results(self, dfs):
       df_final = dfs[self.all].copy()
       rest = [v for k,v in dfs.items() if not k == self.all]
       for i in range(len(rest)):
           if not self.indexed:
               # cols = [x for x in rest[i] if not x == self.index_col]     #PDE-9889
               cols = [x for x in rest[i]]
               col = [elem for elem in cols if 'Query' in elem]

           else:
               cols = rest[i].columns.tolist()
               col = [elem for elem in cols if "Query" in elem]
           # df_final = pd.merge(df_final, rest[i][cols], left_index = True, right_index = True, how = 'outer')
           df_final = pd.merge(df_final, rest[i][cols], left_on='Query', right_on=col[0], how='outer')         #PDE-9889
           # df_final = df_final.set_index(st.query)

       if self.post:
           df_final = self.post(df_final)
       return df_final
                        
    def run(self):
        """Main method to generate the stats.
        If concatenate_results is set to true in __init__, then the result is
        a dataframe. Otherwise the result is a dictionary with keys as supplier
        names and values as datframes for each supplier"""
        dfs = {}
        dfs[self.all] = self.generate_original_stats()
        run_it = True
        for table in self.tables:
            run_it = bool(self.can_run_light(table) * run_it)
        if run_it:
            self.get_unique_values() # self.unique_vals is defined
            self.split_tables()
            queries_mod = self.modify_queries()
            for supplier, queries in queries_mod.items():
                df = self.executor(queries)
                for func, args, kwargs in self.funcs:
                    df = func(df, *args, **kwargs)
                df = self.modify_column_names(df, supplier)
                dfs[supplier] = df
            # Dropping created tables
            for supplier, tables in self.sliced_tables.items():
                for table in tables:
                    toolbox.drop_table(table)
        if self.concatenate_results:
            dfs = self.concat_results(dfs)
        # dfs.fillna(0, inplace=True)         #PDE-9889
        return dfs

class AddSeparator(PreRun):
    """Add the separator column to by joining based on a key column"""
    def __init__(self) -> None:
        super().__init__()
    
    def run(self, source_table: str, target_table:str, key:str) -> None:
        cols = toolbox.list_available_columns(target_table)
        if self.can_run_light(source_table) and not self.col in cols: # we can run on source and does not exist in target
            logger.info(f'Adding {self.col} from "{source_table}" to "{target_table}"')
            toolbox.add_column(target_table, target_table, self.col, 'text')
            query = f"""
                UPDATE {db.schema}.{target_table} t
                SET {self.col} = s.{self.col}
                FROM {db.schema}.{source_table} s
                WHERE t.{key}::text = s.{key}::text
                """
            toolbox.execute(query)
            logger.info(f'{self.col} Added from "{source_table}" to "{target_table}"')
