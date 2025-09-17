# -*- coding: utf-8 -*-
"""
Functions to merge attributes created by different tools
"""
from common import query_toolbox as toolbox
# from common import tool_fieldname as tf
# from common import user_fieldname as f
from common import database as db
from global_cfg import logger

def aggregate_column(table: str, column: str, target: str, *args) -> list:
    """Combine values of two columns. Usually can be used at the end of a tool,
    for example to populate "reject" with "reject_temp" values.
    The functions will do nothing if the target column is missing in the table,
    or the target column and the column name are the same.
    Inputs:
        - table: Table name
        - column: Column name we want to use to populate a target column
        - target: Target column name
        - *args: Any additional argument to be added to the WHERE part of the
        query. args will be appended with AND together if it is more than one.
    Returns log.
    """
    logger.info('Aggregating column %s and %s...' %(column, target))
    log = []
    cols = toolbox.list_available_columns(table)
    if target in cols and column in cols and not column == target: # make sure it exists and was not created by the tool
        query = f"""UPDATE {db.schema}.{table}
                SET {target}={column} where ({target}='' or {target} is null)"""
        for a in args:
            query += 'and ' + a
        logger.info('\n'+toolbox.pretty_query(query)+'\n')
        toolbox.execute(query)
        txt = f'The Column "{target}" has been updated with the values in "{column}"'
        logger.info(txt)
        log.append(txt)
    elif column == target or not target in cols:
        txt = f'The column "{target}" was not aggregated as it did not exist in the input table'
        logger.info(txt)
        log.append(txt)
    return log
    