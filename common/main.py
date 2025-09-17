# -*- coding: utf-8 -*-
"""
Collection of functions and classes widely used in the tool
"""
from global_cfg import logger
from common import OUTPUT_FOLDER
from common import database as db
from common.configs import statCols as st
from common.log import E2ETempLog
from common import aws_secret_manager as asm
from io import StringIO
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy import create_engine
from psycopg2 import connect
from pandas import read_sql, DataFrame, ExcelWriter
from itertools import chain
import sqlparse, os, re
from typing import Generator
from datetime import datetime
from copy import deepcopy


class DatabaseConnection:
    """Class responsible for establishing
    connection to the DB.

    Can be inherited by other classes that use
    the connection.
    """
    def __init__(self) -> None:
        self.engine = None
        self.connection = None
        self._get_credentials()
        logger.debug('DatabaseConnection object initialized successfully')

    def _get_credentials(self):
        secret = asm.get_secret("gispde1prd1/postgres")
        self.db_username = secret.get("username")
        self.db_password = secret.get("password")
        self.db_host = secret.get("host")
        self.db_port = secret.get("port")
        self.db_name = secret.get("dbname")

    def get_engine(self) -> Engine:
        """Creates and returns a DB engine."""
        if not self.engine:
            user_password = f"{self.db_username}:{self.db_password}"
            host_port = f"{self.db_host}:{self.db_port}"
            connect_args={"keepalives": 1, "keepalives_idle": 60, "keepalives_interval": 60}
            self.engine = create_engine(
                f"postgresql+psycopg2://{user_password}@{host_port}/{self.db_name}",
                connect_args = connect_args,
                pool_size = 10,
                max_overflow = 2,
                pool_recycle = 300,
                pool_pre_ping = True,
                pool_use_lifo = True
            )
        return self.engine

    def get_connection(self) -> Connection:
        """Establishes a new DB connection 
        if doesn't exist or closed. Otherwise, 
        returns the existing connection.
        """
        if not self.connection or self.connection.closed:
            logger.info('Establishing a new DB connection...')
            self.connection = connect(
                user=self.db_username,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port,
                database=self.db_name)
            logger.debug("Connection established successfully")
            return self.connection
        return self.connection


class QueryExecutor:
    """Query executor class for executing SQL queries.
    
    Methods:
    --------
        execute - executes a single query;
        sql_data_fetcher - fetches the data returned 
                            by an SQL query in chunks;
        execute_query_list - executes a list of queries.
    """

    def __init__(self, db_connection: DatabaseConnection) -> None:
        self.db_connection = db_connection
        self.db_schema = db.schema
        self.e2e = E2ETempLog()

    def execute(self, query: str, commit: bool=True) -> int:
        """Executes a single SQL query and 
        returns the number of rows affected.
        """
        conn = self.db_connection.get_connection()
        with conn.cursor() as cursor:
            time_start = datetime.now()
            cursor.execute(query)
            time_finish = datetime.now()
            duration = str(time_finish - time_start)
            rows = cursor.rowcount
            if commit:
                conn.commit()
        logger.debug(f"The query above took {duration} to execute")
        return rows

    def sql_data_fetcher(
        self,
        query: str, 
        chunksize: int=10000, 
        get_columns: bool=True
        ) -> Generator:
        """Fetches the data returned by an SQL query in chunks.
        - query: an SQL query;
        - chunksize: number of records yilded per chunk;
        - get_columns: if True, yields column names of the query result prior data.
        """
        logger.debug(f"Creating SQL fetcher for:\n{query}")
        conn = self.db_connection.get_connection()
        with conn.cursor('pde-se-tool') as cursor:
            cursor.execute(query)
            while True:
                data = cursor.fetchmany(chunksize)
                if not data:
                    logger.debug("Fetching finished")
                    break
                else:
                    if get_columns:
                        yield [desc[0] for desc in cursor.description]
                        get_columns = False
                    logger.debug(f"Fetching {chunksize} records")
                    yield data
                    
    @staticmethod
    def clean_query_list(queries: list) -> list:
        """Get a list of queries as input. If it contains list of lists, they will
        be converted to a single list - also these values will be dropped:
            [], [''], None, ''
        """
        queries = [[x] if type(x) != list else x for x in queries]
        queries = list(chain(*queries))
        queries = [x for x in queries if not x in [[], [''], None, '']]
        return queries
    
    @staticmethod
    def pretty_query(query: str) -> str:
        """Makes the query pretty"""
        query = sqlparse.format(query.strip(), 
                                reindent=True, 
                                keyword_case='upper')
        return query
    
    def execute_query_list(
        self,
        queries: list, 
        prettify: bool=True
        ) -> list:
        """Executes SQL queries over the database.
        If 'prettify' is set to 'True' - makes 
        the queries pretty before execution.
        This parameter was added as optional because
        'sqlparse' is long on big queries.
        """
        
        log = []
        queries = self.clean_query_list(queries)
        total = len(queries)
        logger.debug(f"Executing {total} queries")
        for i in range(len(queries)):
            pquery = self.pretty_query(queries[i]) if prettify else queries[i]
            self.clean_console()
            logger.info(f"Executing query: {i+1}/{total}\n\n{pquery}\n")
            rows = self.execute(queries[i])
            log.append([i+1, pquery, rows])
        return log
    
    def clean_console(self):
        os.system('cls')
        log = self.e2e.read_log()
        if log is not None:
            print(f'Running on {log[1]}:')
            self.e2e.pprint_log(log[0])
            print()

class PandasQueryExecutor(QueryExecutor):
    """Collection of queriy executors 
    to/from Pandas DataFrame
    """
    def __init__(self, db_connection: DatabaseConnection) -> None:
        super().__init__(db_connection)

    def toDataFrame(self, query: str, chunksize: int=None) -> DataFrame:
        """Exectutes the SQL query over the initial db 
        and returns the result as a dataframe
        """
        logger.debug(f"Executing query to DataFrame:\n\n{query}\n")
        engine = self.db_connection.get_engine()
        df = read_sql(query, con=engine, chunksize=chunksize)
        return df

    def to_dataframe_fetching(
        self,
        query: str,
        chunksize: int=10000,
        cache: bool=False
        ) -> Generator:
        """Generates a number of Pandas DataFrames out of the SQL query results.
        - query: an SQL query;
        - chunksize: defines the amount of 
        records in each chunk.
        - cache: defines whether the results of the 
        SQL query are cached before the DataFrames are generated. 
        It may help to let go of the database connection faster, 
        however if the results are huge it may result in MemoryError exception.
        """
        if cache:
            data = [rows for rows 
                    in self.sql_data_fetcher(query, chunksize)] # caching the query results in memory
            columns = data.pop(0)
        else:
            data = self.sql_data_fetcher(query, chunksize)
            columns = next(data)
        for chunk in data:
            df = DataFrame(chunk, columns=columns)
            yield df

    def toSingleNumber(self, query: str):
        """TODO: Add description, output type"""
        return self.toDataFrame(query).iloc[0,0]

    def dataFrameToDB(
        self, 
        df: DataFrame, 
        name: str,
        if_exists='replace',
        schema: str = ''
        ) -> None:
        """Writes a DataFrame to DB
        as a table with 'name' provided
        """
        if schema == '':
            schema = self.db_schema
        logger.debug(f"Loading a dataframe to the table '{schema}.{name}'")
        engine = self.db_connection.get_engine()
        df.to_sql(
            name, 
            con=engine, 
            if_exists=if_exists,
            index=False, 
            schema=schema
            )

    def dfToDB(self,df,name: str, if_exists='replace') -> None:
        """Writes a DataFrame to DB
        as a table with 'name' provided
        """
        logger.debug(f"Loading a dataframe to the table '{name}'")
        df = df.loc[:,~df.columns.duplicated()].replace(r'\\|\n|\t|\r|\f',' ', regex=True)
        engine = self.db_connection.get_engine()
        df.head(0).to_sql(name, engine, if_exists=if_exists, index=False, schema=f'{db.schema}')
        size = 100000
        df_lst = [df[i:i+size] for i in range(0,df.shape[0],size)]
        for df in df_lst:
            con = engine.raw_connection()
            cursor = con.cursor()
            output = StringIO()
            df.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)
            cursor.execute(f'SET search_path TO {db.schema}')
            cursor.copy_from(output, f'{name}',null='')
            con.commit()
            con.close()

class QueryToolBox(PandasQueryExecutor):
    """A collection of various queries and 
    DB operations commonly used in the
    tool in different modules.
    """
    def __init__(self, db_connection: DatabaseConnection) -> None:
        super().__init__(db_connection)

    def getDataTypes(self, schema: str=db.schema, table: str=db.table) -> dict:
        """Returns {column_name: data_type} dict 
        for a provided table.
        """
        query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{table}';
        """
        typ = self.toDataFrame(query)
        tdict = {x: y for x, y in zip(typ['column_name'], typ['data_type'])}
        return tdict

    def checkIfTableExists(self, table: str) -> bool:
        """Returns True if specified table exists in the 
        current schema.
        """
        tables = self.list_available_tables()
        return table in tables

    def create_temp_table(
        self,
        org_table: str, 
        tmp_table: str, 
        add_rowid: bool=True,
        where_clause: str='',
        execute_query: bool=True
        ) -> list:
        """Creates a temporary table out of the orignal
        source table.
        """

        orig_table = f'{self.db_schema}.{org_table}'
        temp_table = f'{self.db_schema}.{tmp_table}'
        queries = []
        columns = self.list_available_columns(table=org_table)

        if add_rowid and '__rowid__' not in columns:
            query = f"""
            CREATE TABLE {temp_table} AS
            SELECT row_number() OVER () AS __rowid__, * FROM
            (SELECT * FROM {orig_table}) t
            {where_clause};
            """
        else:
            query = f"""
            CREATE TABLE {temp_table} AS
            SELECT * FROM {orig_table}
            {where_clause};
            """
        tables = self.list_available_tables()
        if tmp_table in tables:
            txt = 'Replacing temp table %s...' %tmp_table
            queries.append(self.drop_table(tmp_table, execute_query))
        else:
            txt = 'Creating temp table %s...' %tmp_table
        queries.append(query)
        if execute_query:
            logger.info(txt)
            self.execute(query)
        return queries

    def list_available_tables(self, schema: str = '') -> list:
        """Returns a list of tables that exist in a schema"""
        if schema == '':
            schema = self.db_schema
        logger.debug(f"Getting the list of tables for schema: '{schema}'")
        exist_query = f"""
        SELECT table_name 
        FROM information_schema.tables
        WHERE table_schema = '{schema}';
        """
        chunks = self.sql_data_fetcher(exist_query, get_columns=False)
        tables = [table[0] for tables in chunks for table in tables]
        return tables

    def list_available_columns(self, table=None, schema=None) -> list:
        """Returns a list of columns in the specified table."""

        table = table if table else db.table
        schema = schema if schema else self.db_schema
        logger.debug(f"Getting the list of columns for: '{schema}.{table}'")
        engine = self.db_connection.get_engine()
        query = f"""
        SELECT * 
        FROM {schema}.{table}
        LIMIT 1;
        """
        return read_sql(query, con=engine).columns.tolist()

    def add_column(
        self, 
        table: str,
        source: str,
        column: str,
        dtype: str,
        echo: bool=True,
        ignore_if_exists: bool = False,
        schema = None
        ) -> str:
        """Creates a column in the specified database table
        if ignore_if_exists set to True, the existing column will be ignored,
        otherwise it returns an exception.
        if echo, the message showing the column name and data type created is
        printed."""
        
        schema = schema if schema else self.db_schema
        logger.debug(f"Adding column '{column}' to '{schema}.{table}'")
        columns = self.list_available_columns(table=source, schema=schema)
        if column in columns and not ignore_if_exists:
            raise NameError('Column "%s" already exists in %s, '\
                            'please drop/rename in the source table '\
                            'and try again.' %(column, source))
        elif column in columns and ignore_if_exists:
            txt = 'Column "%s" already exists so it was not created' %(column)
        else:
            query = "\
            alter table %s.%s add %s %s\
            " %(schema, table, column, dtype)
            self.execute(query)
            txt = f'Column "{column}" with data type "{dtype}" was created'        
        if echo:
            logger.info(txt)
        return txt

    def drop_column(self, table: str, column: str) -> None:
        """Drops specified column from the table"""

        logger.debug(f"Dropping column '{column}' from '{table}'")
        columns = self.list_available_columns(table)
        if not column in columns:
            raise NameError(f'The column {column} does not exist in {table}')
        query = f"""
        ALTER TABLE {self.db_schema}.{table} 
        DROP {column};
        """
        self.execute(query)
    
    def drop_table(self, table: str, execute_query: bool=True) -> str:
        """Deletes specified table from the current schema if exists.
        """
        query = f'DROP TABLE IF EXISTS {self.db_schema}.{table}'
        if execute_query:
            logger.debug(f"Dropping '{table}'")
            self.execute(query)
        return query

    def save_output_table(
        self,
        table: str=db.Ttable,
        prefix: str='out_',
        tool_id: int=None,
        ) -> str:
        """
        Saves a table with the specified tool_id or prefix.
        If tool_id is specified, then prefix will not be used.
        Overrides if exists, creates if doesn't
        """
        tables = self.list_available_tables()
        if tool_id:
            new_table_name = db.get_output_table_by_tool(tool_id)
        else:
            new_table_name = db.get_output_table_name(table=table, prefix=prefix)
        if table in tables:
            if new_table_name in tables:
                message = f"Replacing output table '{new_table_name}'"
                self.drop_table(new_table_name)
            else:
                message = f"Creating output table '{new_table_name}'"
            logger.info(message)
            query = f"""
            CREATE TABLE {self.db_schema}.{new_table_name}
            AS (SELECT * FROM
                {self.db_schema}.{table})
            """
            self.execute(query)
            self.drop_table(table)
            return f"The output table '{new_table_name}' has been stored to the database."
        else:
            return f"'{table}' was not found in the database."

    def saveOutTable(self, temp_table: str, output_table: str) -> str:
        """Makes a full copy of temp_table as output_table and drops
        temp_table once done.
        """

        tables = self.list_available_tables()
        if temp_table in tables:
            if output_table in tables:
                txt = f'Replacing output table {output_table}...'
                self.drop_table(output_table)
            else:
                txt = f'Creating output table {output_table}...'
            logger.info(txt)
            query = f"""
            CREATE TABLE {self.db_schema}.{output_table} 
            AS
            (SELECT * FROM {self.db_schema}.{temp_table});
            """
            self.execute(query)
            self.drop_table(temp_table)
            return f'The output table "{output_table}" has been stored to the database.'

    def create_empty_table(self, table: str, columns: dict) -> str:
        """Creates an empty table with the name provided as
        "table" attribute and with the columns provided as 
        a dict as {column name:data type}.
        """
        tables = self.list_available_tables()
        if table in tables:
            self.drop_table(table)
            txt = 'Table %s has been replaced' %table
        else:
            txt = 'Table %s has been created' %table
        columns = [f'"{column}" {data_type}' 
                        for column, data_type 
                            in columns.items()]
        query = f"""
        CREATE TABLE {self.db_schema}.{table}
        ({', '.join(columns)});
        """
        self.execute(query)
        return txt

    def merge_two_tables_on_db(self,
                               table1: str,
                               table2: str,
                               drop_table1: bool = True,
                               drop_table2: bool = True) -> tuple:
        """Performs UNION operation of two table provided as 
        table1 with table2 if they exist into one table.
        Drops both input tables at the end.
        Name of the final table will be merged_table1
        Outputs are table name and the log
        """

        txt = []
        if self.checkIfTableExists(table1) and self.checkIfTableExists(table2):
            table = self.define_merge_table_name(table1)
            logger.info(f'Merging "{table1}" and "{table2}" into "{table}"...')
            txt.append(self.areColumnsTheSame(table1, table2))
            mergeColumns, tmp1, tmp2 = self.sync_table_columns(table1, table2)
            self.create_empty_table(table, columns = mergeColumns)
            txt.append(self.union_multiple_tables([tmp1, tmp2], table))
            if drop_table1:
                self.drop_table(tmp1)
            if drop_table2:
                self.drop_table(tmp2)
        else:
            table = table1
            txt.append(f'Cannot find both {table1} and {table2} to merge')
        return table, txt

    def define_merge_table_name(self, table1: str) -> str:
        """Generates an output table name for 
        merging tables out of the table name provided.
        If the output table name already exists - drops it.
        """      
        table_name = f'merged_{table1}'
        self.drop_table(table_name)
        return table_name

    def sync_table_columns(self, table1: str, table2: str) -> tuple:
        """Syncs the columns of the two input tables so the output tables
        have the same attributes. All the missing attributes in any of the
        tables will be added with the same data type.
        Output table names are __temp__{table2} and __temp__{table2}
        Output mergeColumns is the dictionary of the attributes and data types
        of the created tables
        """
        
        tmp1 = self.fix_table_name_length(f'__temp__{table1}')
        tmp2 = self.fix_table_name_length(f'__temp__{table2}')
        t1Columns = self.getDataTypes(table1)
        t2Columns = self.getDataTypes(table2)
        mergeColumns = t1Columns.copy()
        mergeColumns.update(t2Columns)
        toCreate1 = {k:v for k,v in mergeColumns.items() if not k in t1Columns}
        toCreate2 = {k:v for k,v in mergeColumns.items() if not k in t2Columns}
        self.create_temp_table(table1, tmp1, add_rowid=False)
        self.create_temp_table(table2, tmp2, add_rowid=False)

        for col, dtype in toCreate1.items():
            self.add_column(tmp1, tmp1, f'"{col}"', dtype, echo=False)
        for col, dtype in toCreate2.items():
            self.add_column(tmp2, tmp2, f'"{col}"', dtype, echo=False)
        return mergeColumns, tmp1, tmp2
    
    def union_multiple_tables(self, table_list: list, destination: str) -> str:
        """Performs union on multiple tables under the same schema.
        Inputs:
            - table_list: list of table names under the same schema
            Tables should have the same column names and dtypes - can be
            done using sync_table_columns method.
            - destination: An empty table with proper attributes and data
            types. An empty table can be created using the create_empty_table
            method
        """

        cols = list(self.getDataTypes(table = destination).keys())
        cols = [f'"{x}"' for x in cols]
        cols = ', '.join(cols)
        ts = [f'SELECT {cols} FROM {self.db_schema}.{t}' for t in table_list]
        ts = ' UNION '.join(ts)

        query = f"""
        INSERT INTO
        {self.db_schema}.{destination} {ts};
        """
        tsx = [f'"{t}"' for t in table_list]
        tsx = ', '.join(tsx)
        txt = f'Tables {tsx} merged into "{destination}"'

        self.execute(query)
        logger.info(txt)
        return txt

    def areColumnsTheSame(self, table1: str, table2: str) -> str:
        """TODO: Add description"""
        txt = ''
        t1 = self.getDataTypes(table = table1)
        t2 = self.getDataTypes(table = table2)
        conflict = [x for x in t1 if x in t2 and t1[x]!=t2[x]]
        if len(conflict) > 0:
            for col in conflict:
                self.convertToType(col, t1[col], 'text', table = table1, with_backup = False)
                self.convertToType(col, t2[col], 'text', table = table2, with_backup = False)
            txt = 'To merge the tables "%s" and "%s", '\
                    'dtype of the following columns have been chnaged to text '\
                        'as they exist in both tables but data types differ:\n'\
                        '%s' %(table1, table2, str(conflict))
        return txt

    def get_table_size(self, table: str) -> int:
        """Returns the number of rows of the table"""
        query = f'SELECT count(*) FROM {self.db_schema}.{table}'
        size = self.toSingleNumber(query)
        return size

    def fix_table_name_length(
        self,
        name: str,
        unique: bool=False,
        max_allowed: int=63
        ) -> str:
        """Fixes the table name length.
        If the input name is shorter than max_allowed, the name will remain
        intanct. If the name length exceeds max_allowed, then the name will
        be the same name but the extra characters will be removed from the end
        and _etc_ will be added at the end.
        Example:
            input "a_long_name...more_than_max_allowed"
            output "a_long_name...more_th_etc_" now the length is max_allowed
        If unique is set to true and the shortened name exists in the schema
        a unique name will be generated.
        Example:
            shortened "a_long_name...more_th_etc_" already exists so the output
            name will be "a_long_name...more_th_001_" or 
            "a_long_name...more_th_002_" etc.
        """

        name_mod = name
        if len(name) > max_allowed:
            name_mod = name.replace('_etc_','')[:max_allowed-5] + '_etc_'
            if unique:
                tables = self.list_available_tables()
                nums = [*map(lambda x: '{:0>3}'.format(x),range(1000))]
                i = 0
                while name_mod in tables:
                    to_replace = name_mod[58:]+'$'
                    name_mod = re.sub(to_replace, f'_{nums[i]}_', name_mod)
                    i += 1
        return name_mod

    def defineBakColumn(
        self, 
        column: str, 
        table: str=db.table
        ) -> str:
        """Given a column name, it returns a unique new column name.
        First name will be column_bak, then column_bak_bak etc.
        The column can be used later to backup the input column"""
        cols = self.list_available_columns(table)
        bak = column
        while bak in cols:
            bak += '_bak'
        return bak

    @staticmethod
    def defineORGfromBak(bak: str) -> str:
        """Given a backup column generated by defineBakColumn, it returns the
        original column name
        """
        if not '_bak' in bak:
            raise NameError(f'Backup column should contain _bak in its name but it is {bak}')
        return '_'.join(map(str,bak.split('_')[:-1]))

    def convertToType(
        self,
        column: str,
        typ: str,
        toType: str,
        table: str=db.Ttable,
        with_backup: bool=True
        ) -> str:
        """Convert an input column to a specified type.
        Inputs:
            - column: the column name
            - typ: current column data type
            - toType: target column data type
            - table: table name
            - with_backup: if True, a backup column wil be created to preserve
            the original values
        Returns log as string
        """

        if with_backup:
            bak = self.defineBakColumn(column)
            self.add_column(table, table, bak, typ)
            query = f"""
            UPDATE {self.db_schema}.{table} 
            SET {bak} = {column};
            """
            self.execute(query)
            
        if toType == 'numeric':
            query = f"""
            ALTER TABLE {self.db_schema}.{table}
            ALTER COLUMN {column} TYPE {toType} 
            USING cast(nullif({column},'') as {toType});
            """
        else:
            query = f"""
            ALTER TABLE {self.db_schema}.{table}
            ALTER COLUMN {column} TYPE {toType}
            USING {column}::{toType};
            """
        self.execute(query)
        
        if with_backup:
            text = 'Column "%s" was created as the backup of "%s". '\
                'Data type of the column "%s" was changed to %s' %(bak, column, column, toType)
            logger.info(text)
            return text, bak
        else:
            text = 'Data type of the column "%s" was changed to %s' %(column, toType)
            logger.info(text)
            return text

    def revertToText(self, bak: str, table: str=db.Ttable) -> str:
        """Having a manipulated column and a backup column to preserve the
        original values, the method replaces the manipulated values with the
        original values. Also, the backup column will be dropped.
        The final column data type will be text.
        The only requirement input is the backup column name, the column
        original name will be derived using defineORGfromBak.
        """

        column = self.defineORGfromBak(bak)
        self.drop_column(table, column)
        query = f"""
        ALTER TABLE {self.db_schema}.{table}
        ALTER COLUMN {bak} type text;
        """
        self.execute(query)
        self.add_column(table, table, column, 'text', echo=False)

        query = f"""
        UPDATE {self.db_schema}.{table}
        SET {column} = {bak};
        """
        self.execute(query)
        self.drop_column(table, bak)

        text = 'The backup column %s has been dropped and the column %s has been recreated with the type text'\
            ''%(bak, column) 
        logger.info(text)
        return text
    
    def save_queries(
        self,
        queries: list, 
        filename: str='Queries', 
        prettify: bool=True
        ) -> None:
        """Save the queries that are executed locally
        """
        queries = self.clean_query_list(queries)
        queries = [*map(self.pretty_query, queries)] if prettify else queries
        queries = '\n' + ';\n\n'.join(queries) + ';'
        queries = queries.replace(';;', ';')
        path = os.path.join(OUTPUT_FOLDER, (filename + '.sql'))
        if os.path.isfile(path):
            os.remove(path)
        with open(path, 'w', encoding="utf-8") as log_file:
            log_file.writelines(queries)

        logger.info(f'Queries have been saved to {filename}.')
        
    def set_a_general_field(
        self,
        queries: list,
        colname: str,
        val: str
        ) -> list:
        """Having a list of queries, the function can add an argument to the
        SET part of the query.
        For example, if you want to set an attribute to a value in all your
        queries, you can simply pass your queries to this function.
        NOTE: the queries should contain SET part already
        Inputs:
            - queries: list of queries
            - colname: column name you want to set something
            - val: value you want to set to the colname
        """
        queries = self.clean_query_list(queries)
        queries_new = []
        for query in queries:
            query = query.split('set ')
            query = query[0] + 'set %s = %s,\n' %(colname, val)+query[1]
            queries_new.append(query)
        return queries_new
    
    def add_a_general_criteria(
        self,
        queries: list,
        colname: str,
        val: str,
        noequal: bool = False
        ) -> list:
        """Having a list of queries, the function can add an argument to the WHERE
        part of the query.
        For example, if you want to add a condition to all your queries, you can
        simply pass your queries to this function.
        NOTE: the queries should contain WHERE part already
        Inputs:
            - queries: list of queries
            - colname: column name you want to use as a condition
            - val: value you want to put in front of the colname
            - noequal: if False, the created query will be like:
                WHERE colname = val
                if True, the created query will be like:
                WHERE colname val. so in this case you can add other operators
                like >. you can set val to >val in this case
                
        """
        if noequal:
            term = ' where (%s %s)' %(colname, val)
        else:
            term = ' where (%s = %s)' %(colname, val)
        queries = self.clean_query_list(queries)
        queries_new = []
        for query in queries:
            query  = query.lower().strip(' ;\n')
            if 'where' in query:
                query = query.split('where',1)
            else:
                query = [query, '']
            query = [x.strip() for x in query]
            if not query[1] == '':
                query = query[0] + term + '\nand ' + query[1]
            else:
                query = query[0] + term
            queries_new.append(query)
        return queries_new
    

class StatisticsQueries(PandasQueryExecutor):
    """A set of queries used for statistics calculation"""

    def __init__(self, db_connection: DatabaseConnection) -> None:
        super().__init__(db_connection)

    @staticmethod
    def addPercentColumn(df: DataFrame, col: str = st.result, total: int = None) -> DataFrame:
        if not total:
            total = df.iloc[0][col]
        df[st.percent] = (df[col]/total) *100
        df[st.percent] = df[st.percent].apply(lambda x: round(x, 2))
        return df

    def simpleCount(
        self, 
        key: str='', 
        table: str=db.Ttable, 
        **kwargs
        ) -> list:
        """Creates a simple count query and returns a key, query pair:
            - key: the description of what is being counted. if no key provided
            as input, then the query itself will be the key.
            - query: the actual query to be executed and get the counts.
        You can also provide other criteria as kwargs of (column, value) to
        the method. if values is a list, then the query will append values with
        AND in between. Ther rest of values will be appended with OR.
        Example:
            kwargs = {"col1": "='val1'", "col2": [">'val21'", "='val22'"]}
            query will be:
                SELECT count(*)
                FROM {self.db_schema}.{table}
                WHERE col1 ='val1'
                OR (col2 >'val21' AND col2='val22')
        """

        query = f"""
        SELECT count(*)
        FROM {self.db_schema}.{table}
        """
        crit = []
        for col, val in kwargs.items():
            if type(val) == str:
                crit.append(f'{col} {val}')
            elif type(val) == list:
                p = []
                for va in val:
                    p.append(f'{col} {va}')
                crit.append('(' + '\nOR '.join(map(str, p)) + ')')
        if len(crit) > 0:
            toAdd = ' WHERE ' + '\nAND '.join(map(str, crit))
            query += toAdd
        if not key:
            key = toAdd.replace('\n',' ')
        return [key, query]

    def saveStatsLocally(
        self, 
        df: DataFrame,
        table: str=db.Stable,
        prefix: str=''
        ) -> str:
        """Saves the statistics stored in a dataframe locally in the OUTPUT
        folder as csv file.
        The methods uses the db.Stable name as deafult to name the file. If
        prefix is specified, it will be added to the name as well.
        Returns log as string
        
        """

        if prefix and table.startswith('sts_'):
            table =  re.sub('^sts_', f'sts_{prefix}', table)
        path = os.path.join(OUTPUT_FOLDER, table + '.csv')
        df.to_csv(path, index=False)
        return f'Stats file has been saved in the directory: {table}.csv'
    
    @staticmethod
    def dfs_tabs(df_list: list, sheet_list: list, file_name: str, index: bool = True, engine: str = None) -> str:
        """Saves list of pandas dataframes into an Excel file in different sheets
        Inputs:
            - df_list: list of data frames
            - sheet_list: list of sheet names as strings
            - file_name: name of the output file - excluding file format .xlsx
            - index: specify if index should be included
        """
        file_name += '.xlsx'
        path = os.path.join(OUTPUT_FOLDER, file_name)
        writer = ExcelWriter(path,engine='xlsxwriter')   
        for dataframe, sheet in zip(df_list, sheet_list):
            if engine:
                dataframe.to_excel(writer, sheet_name = sheet, index = index, engine = engine)
            else:
                dataframe.to_excel(writer, sheet_name = sheet, index = index)    
        # writer.save()
        writer.close()
        txt = 'Stats file has been saved in the directory: %s' %file_name
        logger.debug(txt)
        return txt

    def saveStatsOnDB(
        self,
        df: DataFrame,
        table: str=db.Stable,
        prefix: str=''
        ) -> str:
        """Stores the statistics stored in a dataframe in the database.
        The methods uses the db.Stable name as deafult to name the file. If
        prefix is specified, it will be added to the name as well.
        Returns log as string
        """
        if prefix and table.startswith('sts_'):
            table =  re.sub('^sts_', f'sts_{prefix}', table)
        self.dataFrameToDB(df, table)
        return f'Stats table has been saved under the same schema: {table}'



    def execute_query_stats(self, queries: list) -> DataFrame:
        """Executes statistical queries each returning a single number as
        result. Then it returns all the queries along with the single numbers
        each generated as a dataframe.
        If input is a list of lists with length 2. First element should be the
        key, showing what is going to be calculated by this query, and the
        second element should be the actual query itself.
        If the query is a list of queries, then the query itself will be used
        as the key
        """

        total = len(queries)
        queries_mod = deepcopy(queries)
        for i in range(len(queries_mod)):
            if type(queries_mod[i]) in (list, tuple):
                # queries[i][0] is the key -> description for the query

                pquerry = self.pretty_query(queries_mod[i][1])

            else:
                # key will be same as the query

                pquerry = self.pretty_query(queries_mod[i])
                key = queries_mod[i].replace('\n',' ')
                queries_mod[i] = [key, queries_mod[i]]
            self.clean_console()
            logger.info('Calculating stats: %d/%d\n\n%s' %(i+1, total, pquerry))
            queries_mod[i][1] = self.toSingleNumber(queries_mod[i][1])
        df = DataFrame(queries_mod)
        df.columns = [st.query, st.result]
        return df