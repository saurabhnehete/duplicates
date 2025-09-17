from global_cfg import logger
from common import database as db
from common import user_fieldname as fn
from common import tool_fieldname as tf
from common import query_toolbox as toolbox
from common import db_connection
from common.light_model import PreRun
from pack_duplicates import duplicate_scenarios
from pack_duplicates import clusters, group_creator
from pandas import DataFrame, Series
import pandas as pd
from fuzzywuzzy import fuzz
# fuzzywuzzy import process
import numpy as np


class Duplicates:
    """Parent class for all processors
    """
    def __init__(self):
        self.light_mode = False
        self.temp_data = 'temp__data__'
        
        self.orig_table = db.table
        self.schema = db.schema
        self.table = db.Ttable
        self.tablespace = db_connection.db_name
        self.full_table_name = f'{db.schema}.{db.Ttable}'

        self.rowid = '__rowid__'
        # exposed config attributes
        self.supplier_poiid = fn.src_supplier_poiid

        # to be created
        self.reject = tf.reject_dedup
        self.reject_group = tf.reject_group_dedup
        self.sample_type_code = tf.sample_type_code_dedup
        self.group_id = tf.dedup_group_id
        self.stacking_group_id = tf.stacking_group_id
        self.Stack_Count = tf.Stack_Count
        self.Stack_Type = tf.Stack_Type
        self.address_score_percentage = tf.address_score_percentage

        self.location = duplicate_scenarios.get_location()
        self.address = duplicate_scenarios.get_address()
        self.coordinates = duplicate_scenarios.get_xy()
        self.dedup_scenarios = duplicate_scenarios.get_dedupl_scenarios()
        logger.debug("Init Duplicates class")
        
    def var_chk(self, row, var):
        """ Checking mapped attributes from User Filed and return value if mapped else return None"""
        if var != '__not_specified_by_the_user__':
            result = getattr(row, var)
        else:
            result = ''
        return result
    
    def create_temp_table(self, temp_table_name: str=None, 
                                    where_clause: str='',
                                    execute: bool=True) -> list:
        """Creates a single temp table"""
        if not temp_table_name:
            temp_table_name = self.table
        if toolbox.checkIfTableExists(self.orig_table):
            query = toolbox.create_temp_table(self.orig_table, 
                                    temp_table_name, 
                                    where_clause=where_clause,
                                    execute_query=execute)
            return query
        else:    
            raise NameError(f'Cannot find the table name {self.orig_table} in the schema {self.schema}')


class Preprocessor(Duplicates):
    """The preprocess controller:
    - checks the input data, gathers necessary statistics
    - decides if the input data needs to be split into parts
    - runs the jobs for each part
    """
    def __init__(self):
        super().__init__()
        self.split_by_doable = duplicate_scenarios.check_split_by()
        self.split_if_greater = duplicate_scenarios.split_if_greater
        self.part_size = duplicate_scenarios.part_size
        self.check_for_light_mode() 
        self.create_table_queries = []
        self.group_creator = group_creator(self.part_size)
    
    def check_if_split_needed(self) -> tuple:
        """Checks if table split is needed and doable"""
        query = f"SELECT count(*) FROM {self.schema}.{self.orig_table};"
        logger.info("Analyzing the input table...")
        rows_count = toolbox.toDataFrame(query).to_dict()['count'][0]
        if self.split_by_doable:
            if rows_count >= self.split_if_greater:
                if self.light_mode:
                    self.split_by = self.split_light
                else:
                    self.split_by = duplicate_scenarios.get_split_by()
                num_groups_query = f"""
                SELECT count(*) FROM (
                SELECT DISTINCT {self.split_by}
                FROM {self.schema}.{self.orig_table}) a;
                """
                unique_values = toolbox.toDataFrame(num_groups_query).to_dict()['count'][0]
                if unique_values > 1:
                    logger.debug(f"Split is needed as the input table size is {rows_count}")
                    return True, rows_count
        logger.debug("Split is not needed or not doable")
        return False, rows_count

    def _query_split_by(self) -> DataFrame:
        query = f"""
        SELECT {self.split_by}, count({self.split_by}) AS _count
        FROM {self.schema}.{self.orig_table}
        GROUP BY {self.split_by}
        ORDER BY _count DESC;
        """
        df = toolbox.toDataFrame(query)
        return df

    def generate_split_groups(self) -> dict:
        """Generates almost equal groups
        """
        df = self._query_split_by()
        raw_groups = Series(df['_count'].values, index=df[f'{self.split_by}']).to_dict()
        combined_groups = self.group_creator.get_groups(raw_groups)
        return combined_groups

    def create_temp_tables(self, groups: dict) -> dict:
        """Creates multiple temp tables"""
        parts_info = {}
        logger.debug(f"Creating temp tables for {len(groups)} groups")
        for key, value in groups.items():
            items = value['items']
            if len(items) > 1:
                where_clause = f"WHERE {self.split_by} IN {tuple(items)}"
            else:
                where_clause = f"WHERE {self.split_by} = '{items[0]}'"
                
            temp_table_name = f'tmp{key}_{self.orig_table}'
            self.create_table_queries.append(self.create_temp_table(temp_table_name, 
                                                                    where_clause,
                                                                    execute=False))
            parts_info[key] = value
            parts_info[key]['table'] = temp_table_name
            parts_info[key]['split_by'] = self.split_by
        return parts_info
    
    def check_for_light_mode(self):
        """Check if the requirements for light mode are met. Only in case
        the light_mode is set to True
        If run in light mode, then self.split_if_greater and self.part_size = 1
        are set to 1
        """
        pre = PreRun()
        self.run_light = pre.can_run_light(self.orig_table)
        if self.run_light: # We want and can run the lighe mode
            # sep_dup = duplicate_scenarios.get_split_by()
            # sep_light = light.separator
            self.split_if_greater = 1
            self.part_size = 1
            self.split_light = pre.col
            self.light_mode = True
            self.split_by_doable = True
    
    def process(self) -> list:
        """Creates either multiple or single tmp tables,
        depending on the configuration"""
        logger.debug("Running duplicates preprocessing")
        split_needed = self.check_if_split_needed()
        if split_needed[0]:
            logger.info(f"The input table will be split by '{self.split_by}'.")
            groups = self.generate_split_groups()
            parts = self.create_temp_tables(groups)
            return parts
        else:
            self.create_table_queries.append(
                self.create_temp_table(
                    self.table, 
                    execute=False)
                    )
            return {1: {'items': ['ALL'], 
                        'total': split_needed[1],
                        'split_by': None, 
                        'table': self.table}}

class DedupQueryGenerator(Duplicates):
    def __init__(self):
        super().__init__()
        self.part = 1
        self.original_columns = toolbox.list_available_columns(self.orig_table)

    def get_dedup_attributes(self, scenario: str, location: str='none'):
        try:
            return duplicate_scenarios.parse(location)[scenario]
        except KeyError:
            logger.error(f"Unknown scenario: '{scenario}'!")
            # How to handle errors?

    def create_columns(self):
        """Creates neccessary columns if don't exist"""
        columns = [
            (self.reject, 'text'),
            (self.reject_group, 'text'),
            (self.sample_type_code, 'text'),
            (self.group_id, 'text'),
            (self.stacking_group_id, 'text'),
            (self.Stack_Count, 'double precision'),
            (self.address_score_percentage, 'double precision'),
            (self.Stack_Type, 'text')
            ]
        columns_to_create = [f'ADD {column} {dtype}' for column, dtype in columns if column not in self.original_columns]
        if columns_to_create:
            query = f"""
            ALTER TABLE {self.full_table_name}
            {', '.join(columns_to_create)};
            """
            return query
        else:
            return ''

    def convert_to_text(self):
        """Converts all the existing columns into text"""
        columns_to_convert = [f'ALTER COLUMN {column} TYPE text' for column in self.original_columns]
        query = f"""
        ALTER TABLE {self.full_table_name}
        {', '.join(columns_to_convert)};
        """
        return query

    def create_index(self, index_field_name: str, temp_table: str):
        return f"""
        CREATE INDEX {temp_table}_rowid_index
        ON {self.full_table_name} 
        USING btree
        ({index_field_name} ASC NULLS LAST)
        TABLESPACE {self.tablespace}
        """

    def analyze(self):
        return f"""
        ANALYZE {self.full_table_name};
        """

    def add_prefix(self, prefix: str, fields: list):
        """Adds prefixes to field names"""
        return (f'{prefix}.{field}' for field in fields)

    def list_fields(self, fields: list, prefix: str=None):
        """
        Generates comma-separated list of fields used
        in SELECT or GROUP BY statements. Returns string.
        """
        fields = self.add_prefix(prefix, fields) if prefix else fields
        return ',\n'.join(fields)

    @staticmethod
    def join_on_fields(fields: list, first_prefix: str, second_prefix: str,
                                                                operator: str):
        """
        Constructs matching keys for JOIN ON query, 
        separates all with fixed operator. 
        Assuming that fields are named the same for both tables.
        """
        conditions = [f'{first_prefix}.{field} = {second_prefix}.{field}' for field in fields]
        return f'\n{operator} '.join(conditions)
    
    @staticmethod
    def build_where_conditions(prefix: str='', fields_not_null: list=[], 
                                    fields_null: list=[], empty_or_null: list=[], 
                                    operator: str='AND'):
        """
        To use in WHERE clause where listed fiilds must not be empty or null
        """
        prefix += '.' if prefix else ''
        equal_null = [f"{prefix}{field} = ''" for field in fields_null]
        not_null = [f"{prefix}{field} != ''\n{operator} {prefix}{field} IS NOT NULL" for field in fields_not_null]
        null_or_empty = [f"({prefix}{field} = ''\nOR {prefix}{field} IS NULL)" for field in empty_or_null]
        conditions = equal_null + not_null + null_or_empty
        return f'\n{operator} '.join(conditions)

    @staticmethod
    def build_simple_condition(l_prefix: str='', r_prefix: str='', equal: bool=True,
                                fields: list=[], operator: str='AND'):
        l_prefix += '.' if l_prefix else ''
        r_prefix += '.' if r_prefix else ''
        equal = '=' if equal else '!='
        conditions = [f"{l_prefix}{field} {equal} {r_prefix}{field}" for field in fields]
        return f'\n{operator} '.join(conditions)

    def wrap_update_set(self, set_value: str, 
                        reject: bool=False, 
                        where_clause: str=None,
                        select_clause: str=None):
        """
        Wraps the query with the "UPDATE-SET" statement and returns the complete query. 
        Thakes "WHERE" clause as a string.
        If reject is False - sets the sample type instead of reject code
        """
        set_expression = f"{self.reject_group} = '{set_value}',\
                           {self.reject} = 'true'" if reject else f"{self.sample_type_code} = '{set_value}'"
        
        
        tbls = toolbox.list_available_tables()
        temps = [x for x in tbls if x.startswith(self.temp_data)]
        n = 0
        new_temp = f'{self.temp_data}{n}'
        while new_temp in temps:
            n += 1
            new_temp = f'{self.temp_data}{n}'
        
        q = f"""
        CREATE TABLE {db.schema}.{new_temp} AS {select_clause}
        """
        toolbox.execute(q)
        col = toolbox.list_available_columns(new_temp)[0]
        new_select_clause = f"""
        SELECT {col} FROM {db.schema}.{new_temp}
        """
        wrapped = f"""
        UPDATE {self.full_table_name}
        SET {set_expression}
        WHERE {where_clause}
        IN ({new_select_clause});
        """
        
        # wrapped = f"""
        # UPDATE {self.full_table_name}
        # SET {set_expression}
        # WHERE {where_clause}
        # IN ({select_clause});
        # """
        return wrapped

    def delete_temps(self):
        tbls = toolbox.list_available_tables()
        temps = [x for x in tbls if x.startswith(self.temp_data)]
        for temp in temps:
            toolbox.drop_table(temp)
    
    def find_duplicates(self, reject: bool, scenario: str, location: str='none',
                         subselect_where: str='', pre_condition: str=''):
        """
        Base dedup query constructor.
        'pre_condition' must end with AND or OR keyword
        """
        where_null = [self.reject] if reject else [self.reject, self.sample_type_code]
        dedup_by = list(self.get_dedup_attributes(scenario, location))
        subselect_where = self.build_where_conditions(prefix='s', fields_not_null=dedup_by, 
                                                        empty_or_null=where_null, operator='AND')
        where_clause = f"""
        {pre_condition} 
        {self.build_where_conditions(empty_or_null=where_null)}
        AND {self.rowid}
        """

        select_clause = f"""
        SELECT t.{self.rowid}
        FROM {self.full_table_name} t
        INNER JOIN 
                    (SELECT count(*), 
                        {self.list_fields(dedup_by, 's')}
                    FROM {self.full_table_name} s 
                    WHERE {subselect_where}
                    GROUP BY {self.list_fields(dedup_by, 's')}
                    HAVING count(*) > 1) u
        ON {self.join_on_fields(dedup_by, 'u', 't', 'AND')}
        """
        wrapped = self.wrap_update_set(scenario, reject, where_clause, select_clause)
        return wrapped

    def find_dif_loc_duplicates(self, reject: bool, scenario: str, location: str='none',
                                 subselect_where: str='', pre_condition: str=''):
        """
        Advanced dedup query constructor for 'dupl_phone_dif_loc' scenario.
        'pre_condition' must end with AND or OR keyword
        """
        where_null = [self.reject] if reject else [self.reject, self.sample_type_code]
        dedup_by = list(self.get_dedup_attributes(scenario, 'none')+self.location)
        dedup_by_no_loc = list(self.get_dedup_attributes(scenario, 'none'))
        subselect_where = self.build_where_conditions(prefix='s', fields_not_null=dedup_by, 
                                                        empty_or_null=where_null, operator='AND')
        where_clause = f"""
        {pre_condition} 
        {self.build_where_conditions(empty_or_null=where_null)} 
        AND {self.rowid}
        """
        
        select_clause = f"""
        SELECT DISTINCT v.{self.rowid}
        FROM {self.full_table_name} v
        INNER JOIN 
            (SELECT t.{self.rowid}, 
                {self.list_fields(dedup_by, 't')}
            FROM {self.full_table_name} t
            INNER JOIN
                (SELECT count(*),
                        {self.list_fields(dedup_by_no_loc, 's')}
                FROM {self.full_table_name} s
                WHERE {subselect_where}
                GROUP BY {self.list_fields(dedup_by_no_loc, 's')}
            HAVING count(*) > 1) u
            ON {self.build_simple_condition(l_prefix='u', r_prefix='t', 
                                            equal=True, fields=dedup_by_no_loc)}) w
        ON {self.build_simple_condition(l_prefix='v', r_prefix='w', 
                                            equal=True, fields=dedup_by_no_loc)}
        AND ({self.build_simple_condition(l_prefix='v', r_prefix='w', 
                                            equal=False, fields=self.location, 
                                            operator='OR')})
        """
        wrapped = self.wrap_update_set(scenario, reject, where_clause, select_clause)
        return wrapped

    def add_group_id(self, reject: bool, scenario: str, location: str='none'):
        """
        Add group ids per sample_type_code or reject_group for duplicates 
        (required for sampling an entire group of records for review)
        """
        order_by = list(self.get_dedup_attributes(scenario, location))
        dupl_type = self.reject_group if reject else self.sample_type_code
        
        if location == 'coordinates':
            sub_rnk = f"'x{self.part}'||subquery.rnk"  
        else:
            sub_rnk = f"'{self.part}'||subquery.rnk"

        query = f"""
        WITH subquery AS
            (SELECT {self.rowid},
                dense_rank() OVER (
                             ORDER BY {self.list_fields(order_by)}) AS rnk
            FROM {self.full_table_name}
            WHERE {dupl_type} = '{scenario}' 
            AND {self.group_id} IS NULL)
        UPDATE {self.full_table_name}
        SET {self.group_id} = {sub_rnk}
        FROM subquery
        WHERE {self.full_table_name}.{self.rowid} = subquery.{self.rowid};
        """
        return query
    
    def add_stacking_group_id(self,reject:bool ,location: str='none'):
        """
        Add group ids for identical coordinates 
        (required for grouping of records for stacked location review)
        """
        #order_by = list(self.coordinates)
        #dupl_type = self.reject_group if reject else self.sample_type_code
        where_null = [self.reject]
        not_null = list(self.coordinates)
        subselect_where = self.build_where_conditions(prefix='', fields_not_null=not_null, 
                                                        empty_or_null=where_null, operator='AND')
        if location == 'coordinates':
            order_by = list(self.coordinates)
            sub_rnk = f"'x{self.part}'||subquery.rnk"  
        else:
            order_by = ''
            sub_rnk = ''

        query = f"""
        WITH subquery AS
            (SELECT {self.rowid},
                dense_rank() OVER (
                             ORDER BY {self.list_fields(order_by)}) AS rnk
            FROM {self.full_table_name})
            
        UPDATE {self.full_table_name}
        SET {self.stacking_group_id} = {sub_rnk}
        FROM subquery
        WHERE {self.full_table_name}.{self.rowid} = subquery.{self.rowid}
        AND ({subselect_where});
        """
        return query
 
    def dereject_group(self, group: str):
        """
        From each rejected duplicate group, one record needs to be kept as non-rejected.
        Evaluate duplicate reject groups and reset reject flag on one record per group
        """
        query = f"""
        WITH subquery AS
            (SELECT DISTINCT ON ({self.group_id}) {self.rowid}
            FROM {self.full_table_name}
            WHERE {self.reject_group} = '{group}'
            ORDER BY {self.group_id})
        UPDATE {self.full_table_name}
        SET {self.reject} = ''
        FROM subquery
        WHERE {self.full_table_name}.{self.rowid}  = subquery.{self.rowid};
        """
        return query
    
    def reject_group_populate(self):
        
        query = f"""
        Update {self.full_table_name} SET {fn.org_reject_group}={self.reject_group} WHERE 
        {self.reject_group} is not null and ({self.reject} = 'true');
        """
        return query

    def generate_basic_queries(self):
        """
        Generates deduplication queries depending on
        location attributes availabity
        """
        duplicate_queries = []
        dereject_queries = []
        group_id_queries = []
        stacking_group_id_queries = []
        for scenario in self.dedup_scenarios:
            has_reject = duplicate_scenarios.check_flag(scenario, '_reject_')
            is_advanced = duplicate_scenarios.check_flag(scenario, '_advanced_')
            uses_location = duplicate_scenarios.check_flag(scenario, '_location_')
            uses_dif_location = duplicate_scenarios.check_flag(scenario, '_dif_location_')
            if not is_advanced:
                if uses_location:
                    if self.address:
                        duplicate_queries.append(self.find_duplicates(reject=has_reject, 
                                                                      scenario=scenario,
                                                                      location='address'))
                        duplicate_queries.append(self.add_group_id(reject=has_reject,
                                                                    scenario=scenario, 
                                                                    location='address'))
                    if self.coordinates:
                        duplicate_queries.append(self.find_duplicates(reject=has_reject, 
                                                                     scenario=scenario,
                                                                     location='coordinates'))
                        duplicate_queries.append(self.add_group_id(reject=has_reject,
                                                                    scenario=scenario, 
                                                                    location='coordinates'))
                        stacking_group_id_queries.append(self.add_stacking_group_id(reject=False,
                                                                                location='coordinates'))
                elif uses_dif_location:
                    duplicate_queries.append(self.find_dif_loc_duplicates(reject=has_reject,
                                                                          scenario=scenario,
                                                                          location='none'))
                    duplicate_queries.append(self.add_group_id(reject=has_reject,
                                                              scenario=scenario, 
                                                              location='none'))
                else:
                    duplicate_queries.append(self.find_duplicates(reject=has_reject, 
                                                                  scenario=scenario,
                                                                  location='none'))
                    duplicate_queries.append(self.add_group_id(reject=has_reject,
                                                              scenario=scenario, 
                                                              location='none'))

                dereject_queries.append(self.dereject_group(scenario))
        dereject_queries.append(self.reject_group_populate())
        
        return {'duplicate_queries': duplicate_queries, 
                'dereject_queries': dereject_queries, 
                'group_id_queries': group_id_queries,
                'stacking_group_id_queries':stacking_group_id_queries}

    def generate_advanced_queries(self):
        duplicate_queries = []
        dereject_queries = []
        group_id_queries = []
        stacking_group_id_queries = []

        # call custom query functions from here

        return {'duplicate_queries': duplicate_queries,
                'dereject_queries': dereject_queries, 
                'group_id_queries': group_id_queries,
                'stacking_group_id_queries': stacking_group_id_queries}

    def generate_queries(self, temp_table: str='', part: int=1, analyze: bool=False) -> list:
        """Generates query sequence"""
        logger.debug("Generating pre-query sequence for dedup scenarios")
        if temp_table:
            self.part = part
            self.table = temp_table
            self.full_table_name = f'{db.schema}.{temp_table}'
        checkin_queries = [
        self.create_columns(),
        self.convert_to_text(),
        self.create_index(index_field_name=self.rowid, temp_table=temp_table)
        ]
        log = toolbox.execute_query_list(checkin_queries) # we need tables to create the queries
        logger.debug("Generating query sequence for dedup scenarios")
        basic_queries = self.generate_basic_queries()
        advanced_queries = self.generate_advanced_queries()
        queries = [
            # *checkin_queries,
            *basic_queries['duplicate_queries'], *advanced_queries['duplicate_queries'],
            *basic_queries['group_id_queries'], *advanced_queries['group_id_queries'],
            *basic_queries['dereject_queries'], *advanced_queries['dereject_queries'],
            *basic_queries['stacking_group_id_queries'], *advanced_queries['stacking_group_id_queries'],
            ]
        if analyze:
            logger.debug("Injecting ANALYZE queries")
            full_queries = []
            for query in queries:
                full_queries.append(query)
                full_queries.append(self.analyze())
            full_queries = full_queries[:-1]
        else:
            full_queries = queries
        logger.debug("Queries generated")
        return full_queries, log


    
        
class Postprocessor(Duplicates):
    """The postprocess controller:
    - defines the scope for postprocessing
    - runs corresponding jobs
    - merges multiple temp tables into one
    """
    def __init__(self):
        super().__init__()

    def _find_scope(self) -> dict:
        """Defines which scenarios need to be posprocessed"""
        found = {}
        logger.debug("Postprocessing scope definition")
        postprocesses = duplicate_scenarios.require_postprocessing
        scenarios = self.dedup_scenarios
        for scenario, flags in scenarios.items():
            flags = flags.strip('[]').split(', ')
            for process in postprocesses:
                if process in flags:
                    found[scenario] = process
        return found

    def _controller(self, scenario: str, flag: str, temp_table: str='') -> list:
        """Runs required postprocess"""
        if flag == '_sim_name_':
            queries = SimilarNames(scenario).process(temp_table)
        else:
            raise ValueError(f"Flag '{flag}' is not defined!")
        return queries

    def run_additional_scripts(self, temp_table: str=''):
        """Runs the postprocessing"""
        logger.debug("Running dedup posprocessing")
        scope = self._find_scope()
        queries = []
        for scenario, flag in scope.items():
            queries += self._controller(scenario, flag, temp_table)
        return queries

    def merge_temp_tables(self, temp_tables: list, cleanup: bool=False) -> list:
        """Creates queries for merging temp partitions 
        into the final temp table.
        If cleanup=True, deletes the temp partitions.
        """
        queries = []
        if toolbox.checkIfTableExists(self.table):
            delete_query = f"""DROP TABLE {self.full_table_name};"""
            queries.append(delete_query)
        prepared = [f'SELECT * FROM {self.schema}.{temp_table}' 
                        for temp_table in temp_tables]
        union = '\nUNION ALL\n'.join(prepared)
        create_from = f"{union}"
        merge_query = f"""
        CREATE TABLE {self.full_table_name} AS
        SELECT * FROM (
        {create_from}
        ) a;
        """
        queries.append(merge_query)
        if cleanup:
            cleanup_queries = self.cleanup(temp_tables)
            queries += cleanup_queries
        return queries
    
    def cleanup(self, tables: list) -> list:
        """Creates drop table queries"""
        logger.debug("Postprocess cleanup queries creation")
        queries = [f"""DROP TABLE {self.schema}.{table};"""
                        for table in tables]
        return queries


class SimilarNames(Duplicates):
    """Logic for '_sim_name_' flagged scenarios.
    Main method 'process' returns list of queries for execution
    """
    def __init__(self, scenario: str):
        super().__init__()
        self.scenario = scenario
        self.poi_nm = fn.src_poi_nm
    
    @staticmethod
    def select_from_db(
                    from_table: str, 
                    where: str=None, 
                    attributes: str='*') -> str:
        """Constructs basic SELECT-FROM-WHERE query"""
        where = f'WHERE {where}' if where else None
        query = f"""
        SELECT {attributes}
        FROM {from_table}
        {where}
        """
        return query

    def _clear_group_query(self,
                            group_id: str, 
                            group_name: str, 
                            scenario: str) -> str:
        query = f"""
        UPDATE {self.full_table_name}
        SET {group_id} = '',
            {group_name} = ''
        WHERE {group_name} = '{scenario}';
        """
        return query

    @staticmethod
    def _build_transaction(queries: list) -> str:
        """Wraps queries into a transaction"""
        str_queries = '\n'.join(queries)
        transaction = f"""
        BEGIN;
        {str_queries}
        COMMIT;
        """
        return transaction

    def _update_sim_names_query(self, df: DataFrame, 
                                group_name: str,
                                scenario: str) -> str:
        """Makes a list of UPDATE queries for 
        '_sim_name_' scenario and puts them into transaction
        function
        """
        groups = df.groupby([self.group_id])['__rowid__'].apply(list).to_dict()
        queries = []
        for group, rowids in groups.items():
            rowids = [f"'{rowid}'" for rowid in rowids]
            query = f"""
            UPDATE {self.full_table_name}
            SET {self.group_id} = '{group}',
                {group_name} = '{scenario}'
            WHERE {self.full_table_name}.__rowid__ IN ({', '.join(rowids)});
            """
            queries.append(query)
        
        query = self._build_transaction(queries)
        return query

    def process(self, temp_table: str='') -> list:
        """Runs _sim_name_ scenario:
        1. Query data from db with given scenario as df
        2. Run fuzzyclustering
        3. Construct query list
        """
        logger.debug("Running SimilarNames scenario")
        if temp_table:
            self.full_table_name = f'{db.schema}.{temp_table}'
        rejected = duplicate_scenarios.check_flag(self.scenario, '_reject_')
        group_name = self.reject_group if rejected else self.sample_type_code

        select_attributes = f"""
                __rowid__,
                {self.poi_nm},
                {self.group_id}
        """
        from_table = self.full_table_name
        where = f"{group_name} = '{self.scenario}'"
        select_query = self.select_from_db(from_table, where, select_attributes)

        df = toolbox.toDataFrame(select_query)
        unique_ids = df[self.group_id].unique()
        logger.info(f"{self.scenario}: looking for similar names in {len(unique_ids)} groups...")
        df['new_group_id'] = ''

        counter = xcounter = 1
        global_count = 1
        for old_id in unique_ids:
            filt = (df[self.group_id] == old_id)
            data = df.loc[filt, self.poi_nm].values.tolist()
            clustered = clusters.create_clusters(data)
            for cluster in clustered:
                if len(cluster) > 1:
                    if 'x' in old_id:
                        new_id = f'x{xcounter}'
                        xcounter += 1
                    else:
                        new_id = counter
                        counter += 1
                    for name in cluster:
                        filt = ((df[self.group_id] == old_id) & (df[self.poi_nm] == name))
                        df.loc[filt, 'new_group_id'] = new_id
            if global_count%1000 == 0:
                logger.info(f"Groups done: {global_count}/{len(unique_ids)}")
            global_count += 1
        
        filt = (df['new_group_id'] != '')
        to_update = df.loc[filt]
        
        cleanup = self._clear_group_query(self.group_id, 
                                            group_name, 
                                                self.scenario)
        updates = self._update_sim_names_query(to_update, 
                                                group_name, 
                                                    self.scenario)
        queries = [cleanup, updates]
        return queries
    
class Stacking(Duplicates):
    def __init__(self):
        super().__init__()
        
        
    def address_score(self,DF):
        #print('1111',DF)
        DF[fn.src_full_addr] = DF[fn.src_full_addr].fillna('')
        DF[fn.src_full_addr] = DF[fn.src_full_addr].str.lower()
        DF[fn.src_full_addr] = DF[fn.src_full_addr].str.replace('\d+', '')
        DF[fn.src_full_addr] = DF[fn.src_full_addr].str.strip()
        for d in DF[fn.src_full_addr].unique():
            # compute Levensthein distance
            # and set to True if >= a limit
            # (you may have to play around with it)
            DF[d] = DF[fn.src_full_addr].apply(
                lambda x : fuzz.partial_ratio(x, d)>=60
            )
            # set a name for the group
            # here, simply the shortest
            m = np.min(DF[DF[d]==True][fn.src_full_addr])
            # assign the group
            DF.loc[DF[fn.src_full_addr]==d, 'group'] = m
        #print('DFDF',DF)
        #print(DF.groupby('group').group.count())
        summary=DF.groupby('group').group.count().to_frame(name="Counts")
        #print(summary)
        total=summary['Counts'].sum()
        summary_subset=int(summary[summary['Counts']!=''].max())
        #print('summary_subsets\n',summary_subset)
        percentage = summary_subset/total*100
        #print('DFDF',DF)
        DF['address_score_percentage']=percentage
        return DF
    def run_stacks(self, temp_table: str=''):
        if fn.src_full_addr=='__not_specified_by_the_user__' or fn.src_display_lat=='__not_specified_by_the_user__' or fn.src_display_long=='__not_specified_by_the_user__':
            return
        #print('hi')
        self.src_full_addr = fn.src_full_addr
        self.src_display_lat= fn.src_display_lat
        self.src_display_long = fn.src_display_long
        
        if temp_table:
            self.full_table_name = f'{db.schema}.{temp_table}'
            print('self.full_table_name1111',self.full_table_name)
        print('self.full_table_name',self.full_table_name)
        select_query = f"""
            SELECT {self.rowid},
                {self.src_full_addr},
                {self.src_display_lat},
                {self.src_display_long},
                {self.stacking_group_id} FROM {self.full_table_name}"""
#         self.stacking_df=pd.read_sql(""" 
#         SELECT display_lat,display_long,src_full_addr,__rowid__,stacking_group_id
#         FROM test.stacking_samples
#         """, con = conn)

        self.stacking_df=toolbox.toDataFrame(select_query)
        query = f"""update {self.full_table_name} set {self.stacking_group_id} = NULL"""
        toolbox.execute(query)
        #toolbox.drop_column(self.table,'stacking_group_id')
        self.stacking_df['Stack_Count']=''
        self.stacking_df['Stack_Count']= self.stacking_df.groupby(['stacking_group_id'])['stacking_group_id'].transform('count')
        self.stacking_df=self.stacking_df[(self.stacking_df['Stack_Count']>=10) & (self.stacking_df['Stack_Count']<=100)]
        
        if(len(self.stacking_df)>0):
            self.flag=True
            self.p=pd.unique(self.stacking_df['stacking_group_id']).tolist()
            self.p = list(filter(None, self.p))
            self.final_df=pd.DataFrame()
            print(self.stacking_df)
            print(self.p)
        else:
            self.flag=False
        
        if self.flag==True:
            for i in range(0,len(self.p)):
                self.groupwise_df=self.stacking_df[self.stacking_df['stacking_group_id']==self.p[i]]
                # print('111',groupwise_df)
                self.groupwise_df=self.groupwise_df.reset_index(drop=True)
    
                self.temp_df=self.address_score(self.groupwise_df)
                self.temp_df=self.temp_df[['stacking_group_id','address_score_percentage']]
                # print('222',temp_df)
                # self.final_df=self.final_df.append(self.temp_df)
                self.final_df = pd.concat([self.final_df, self.temp_df])
            self.stacking_df=pd.merge(self.stacking_df,self.final_df[['stacking_group_id','address_score_percentage']],on='stacking_group_id',how='left')
            self.df2 = self.stacking_df[fn.src_full_addr].isnull().groupby([self.stacking_df['stacking_group_id']]).sum().astype(int).reset_index(name='null counts')
            self.df3 = self.stacking_df[fn.src_full_addr].notnull().groupby([self.stacking_df['stacking_group_id']]).sum().astype(int).reset_index(name='Not null counts')
            self.df2=pd.merge(self.df2,self.df3[['stacking_group_id','Not null counts']],on='stacking_group_id',how='left')
            #self.df2['Drop'] = np.where(self.df2['null counts']> self.df2['Not null counts'], True, False)
            self.df2['Drop'] = self.df2['null counts']*100/(self.df2['null counts']+self.df2['Not null counts'])
            #col1_is_Zero = (self.df2['Drop']>80)
            
            self.stacking_df=pd.merge(self.stacking_df,self.df2[['stacking_group_id','Drop']],on='stacking_group_id',how='left')
            col1_is_Zero = (self.stacking_df['Drop']>80)
            self.stacking_df.loc[col1_is_Zero, 'address_score_percentage']=0
            self.stacking_df['Stack_Type'] = np.where((self.stacking_df['address_score_percentage']>48) & (self.stacking_df['Drop']<40), 'Valid Stack', 'Invalid Stack')
            score_is_Zero = (self.stacking_df['address_score_percentage']==0)
            self.stacking_df.loc[score_is_Zero, 'Stack_Type']='Inconclusive'
            #self.stacking_df['Stack_Type'] = np.where(self.stacking_df['Stack_Count']>=10, 'Valid Stack', 'Invalid Stack')
            self.stacking_df=self.stacking_df.drop_duplicates('__rowid__').reset_index(drop=True)
            #print(self.stacking_df.head(10))
            #a=toolbox.create_empty_table('stacking__df__db')
            toolbox.dataFrameToDB(self.stacking_df,'stacking__df__db')
            #toolbox.drop_table(self.temp_data)
           
            
            query = f"""
            UPDATE {self.full_table_name} AS v SET {self.stacking_group_id} = s.{self.stacking_group_id} FROM {self.schema}.stacking__df__db AS s WHERE v.{self.rowid} = s.{self.rowid};
            """
            toolbox.execute(query)
            query = f"""
            UPDATE {self.full_table_name} AS v SET {self.Stack_Count} = s."Stack_Count" FROM {self.schema}.stacking__df__db AS s WHERE v.{self.rowid} = s.{self.rowid};
            """
            toolbox.execute(query)
            query= f"""
            UPDATE {self.full_table_name} AS v SET {self.address_score_percentage} = s."address_score_percentage" FROM {self.schema}.stacking__df__db AS s WHERE v.{self.rowid} = s.{self.rowid};
            """
            toolbox.execute(query)
            query= f"""
            UPDATE {self.full_table_name} AS v SET {self.Stack_Type} = s."Stack_Type" FROM {self.schema}.stacking__df__db AS s WHERE v.{self.rowid} = s.{self.rowid};
            """
            toolbox.execute(query)
            
            toolbox.drop_table('stacking__df__db')
            #toolbox.merge_two_tables_on_db_withoutdroppingtable2('stacking__df__db',self.full_table_name)
            #print(a)
    #         self.stacking_df=self.stacking_df.reset_index(drop=True)
    #         print(self.stacking_df)
    #         print(self.p)
        return self.stacking_df

