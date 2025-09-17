# -*- coding: utf-8 -*-
"""
Chekcing user filed data types.
"""
from global_cfg import logger
from global_cfg import tools
from common import tool_fieldname as tf
from common import user_fieldname as f
from common import query_toolbox as toolbox


class column_dtype_convertor:
    """Convert data types, backup columns and revert the original values.
    If for any tool we expect specific columns with specific data types, then
    it can be defined here. If the module finds that the data type is different
    from what is expected, then it will backup the column and converts the data.
    You can also use revertCols to revert back the original values.
    Note that the reverted values will be in text data type.
    """
    def __init__(self, tool:str) -> None:
        """Given the tool name, it will define the key, value pair of column 
        names and expected data types
        """
        if tool == tools[3]:        # Address Completeness
            self.colList = {    
                }
        elif tool == tools[6]:      # Location Accuracy
            self.colList = {
                tf.complete_address: 'boolean',
                f.distance: 'numeric'
                }
        elif tool == tools[1]:      # Change Rate
            self.colList = {
                f.src_display_lat: 'text',
                f.src_display_long: 'text',
                f.src_routing_lat: 'text',
                f.src_routing_long: 'text',
                }
        elif tool == tools[7]:      # Location Accuracy VLS
            self.colList = {
                tf.reject_vls: 'boolean',
                tf.reject_group_vls: 'text',
                tf.sample_label_vls: 'text',
                f.src_display_lat: 'text',
                f.src_display_long: 'text',
                f.src_routing_lat: 'text',
                f.src_routing_long: 'text',
                }
        elif tool == tools[8]:      # Final Stats
            self.colList = {
                f.org_reject: 'text',
                }
        else:
            raise NameError('The tool name is not correct: %s' %tool)
    
    def convertCols(self) -> list:
        """Given the tool name,the method checks if the listed columns in
        getColList have the expected data types. If they don't, it will create
        backup columns, copy the original values there, and convert the data type
        of the original columns to the expected data type.
        Returns log.
        """
        logs = []
        self.baks = []
        currentCols = toolbox.getDataTypes()
        toCheck = [x for x in self.colList if x in currentCols]
        for col in toCheck:
            if self.colList[col] == 'boolean' and not currentCols[col] == 'boolean':
                logger.info('COLUMN DATA TYPE WARNING:\nColumn "%s" expected to be the type "%s" but it is "%s". The data type will be converted'\
                      %(col, self.colList[col], currentCols[col]))
                log, bak = toolbox.convertToType(col, currentCols[col], 'boolean')
                logs.append(log)
                self.baks.append(bak)
            elif self.colList[col] == 'numeric' and not currentCols[col] in ['numeric', 'float64', 'double precision']:
                logger.info('COLUMN DATA TYPE WARNING:\nColumn "%s" expected to be the type "%s" but it is "%s". The data type will be converted'\
                      %(col, self.colList[col], currentCols[col]))
                log, bak = toolbox.convertToType(col, currentCols[col], 'numeric')
                logs.append(log)
                self.baks.append(bak)
            elif self.colList[col] == 'text' and not currentCols[col] == 'text':
                logger.info('COLUMN DATA TYPE WARNING:\nColumn "%s" expected to be the type "%s" but it is "%s". The data type will be converted'\
                      %(col, self.colList[col], currentCols[col]))
                log = toolbox.convertToType(col, currentCols[col], 'text', with_backup = False)
                logs.append(log)
        return logs
    
    def revertCols(self) -> list:
        """Revert back the converted column. The reverted column data types
        will be text.
        Returns log.
        """
        logs = []
        if len(self.baks)>0:
            logger.info('Reverting the converted columns...')
        for bak in self.baks:
            logs.append(toolbox.revertToText(bak))
        return logs
            