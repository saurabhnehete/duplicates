# -*- coding: utf-8 -*-
"""
Modules used to process tables in batches
"""
from global_cfg import logger
from common import database as db
from common import query_toolbox as toolbox
from math import ceil

class batch_processor:
    """This class can run the generated queries in batches.
    It works the same as running list of queries on a table.
    But can slice the table and run in batches if the total number of rows
    exceed a specified number.
    
    You need to have a "class" of your specific tool which generates
    queries to be performed on a "table".
    batch_processor will slice the table, take your generated queries
    re-generate your queries but for each sliced table.
    Example: If you have 4 queries to be performed on a table and the
    table is sliced to 5 sub-tables, the result will be 20 tables.
    After executing the queries, the tables will be stacked together again.
    The output table name is the same db.Ttable temp table.
    
    - Instruction how to use it:
        1. Initialize the class: bp = batch_processor()
        2. generate queries:
            queries = bp.batch_query_generator(table, class)
        3. Union the sliced tables after executing the generated queries:
            log = bp.union_batch_tables()
    """
    
    def __init__(self):
        self.batch = False
        
    def add_index(self, table:str, index_col:str = '__index__') -> str:
        """Method to add a column to be used for slicing the table"""
        columns = toolbox.list_available_columns(table)
        if not index_col in columns:
            logger.info('Adding index...')
            query = f'alter table {db.schema}.{table} add {index_col} serial'
            toolbox.execute(query)
        # query = f'create index {index} on {db.schema}.{table}({index_col})'
        # main.execute(query)
        return index_col
    
    def read_batch_size(self) -> int:
        """Method to read batch size form configs file
        The variable is in database.txt called "batch_size"
        """
        try:
            batch_size = int(db.batch_size)
        except:
            raise ValueError('Batch size should be specified. %s is not valid' %db.batch_size)
        return batch_size
    
    def partition_table(self, table:str) -> None:
        """Method to slice the table based on batch size."""
        logger.info('Spliting the table...')
        self.sliced_tables = []
        index_col = self.add_index(table)
        batch_size = self.read_batch_size()
        # query = f'alter table {db.schema}.{table} PARTITION BY RANGE ({index_col})'
        # main.execute(query)
        num_of_partitions = ceil(self.size/int(db.batch_size))
        for i in range(num_of_partitions):
            fr = i*batch_size+1
            to = min([(i+1)*batch_size,self.size])+1
            table_name = f'{table}_{fr}_{to-1}'
            table_name = toolbox.fix_table_name_length(table_name)
            # self.check_table_name(table_name)
            toolbox.drop_table(table_name)
            query = f"""CREATE TABLE {db.schema}.{table_name} AS
                    SELECT * FROM {db.schema}.{table}
                    WHERE {index_col} >= {fr} and {index_col} < {to}"""
            toolbox.execute(query)
            logger.debug(f'Table {table_name} has been created')
            self.sliced_tables.append(table_name)
            
    def batch_query_generator(self, table:str, _class:object, *args:tuple) -> list:
        """Main method to create sliced tables and generate queries.
        _class is the object that generates the queries for a tool
        _class should have a .run(table, *args) method to generate queries
        if db.batch_size is false, then no batch processing is performed.
        tabel names are stored in self.sliced_tables and can be acceesed late
        to union tables
         """
        
        self.size = toolbox.get_table_size(table)
        if self.size > self.read_batch_size() and db.batch_processing.lower() == 'true':
            self.batch = True
            logger.info(f'The table will be processed in batches of {db.batch_size}')
            self.partition_table(table)
            queries = []
            for table_name in self.sliced_tables:
                queries += _class(table_name, *args).run()
        else:
            queries = _class(*args).run()
        return queries
    
    def union_batch_tables(self) -> list:
        """Method to union the sliced tables.
        The sliced tables names are taken from self.sliced_tables
        The final table will replace the old temp table.
        The method returns the log as a list.
        """
        txt = ''
        if self.batch:
            logger.info('Merging...')
            cols = toolbox.getDataTypes(table = db.Ttable)
            # Drop the current temp table
            toolbox.drop_table(db.Ttable)
            # Recreate temp table as an empty table
            toolbox.create_empty_table(db.Ttable, columns = cols)
            # Union processed tables as the new temp table
            txt = toolbox.union_multiple_tables(self.sliced_tables, db.Ttable)
            for table in self.sliced_tables:
                toolbox.drop_table(table)
        return [txt]