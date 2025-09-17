# -*- coding: utf-8 -*-
"""
Module to create required columns for each tool
"""
from common import query_toolbox as toolbox
from common import tool_fieldname as tf
from common import user_fieldname as f
from common import database as db

class column_creator:
    """Create required columns needed for each tool"""
    
    @staticmethod
    def analyse_address() -> list:
        """Create columns for analyse_address tool and return logs specifying
        changes have been made to the columns
        """
        columns = toolbox.list_available_columns()
        logs = []
        notes = []
        colList = [
            (tf.incomplete_address, 'text'),
            (tf.complete_address, 'bool'),
            (tf.reject, 'bool'),
            (tf.reject_group, 'text'),
            (tf.cat, 'text')
            ]
        if not f.src_house_nr in columns and f.src_full_addr in columns:
            colList.append((tf.hn_temp, 'text'))
            notes.append('%s was used to extract House Numbers from the column %s' %(tf.hn_temp, f.src_full_addr))
        if not f.src_street_nm in columns and f.src_full_addr in columns:
            colList.append((tf.street_temp, 'text'))
            notes.append('%s was used to extract Street Names from the column %s' %(tf.street_temp, f.src_full_addr))
        for col, dtype in colList:
            logs.append(toolbox.add_column(db.Ttable, db.table, col, dtype))
        return logs + notes

    @staticmethod
    def location_accuracy_geocode() -> list:
        """Create columns for location_accuracy_geocode tool and return logs specifying
        changes have been made to the columns
        """
        logs = []
        colList = [
            (tf.confidence_geocode, 'text'),
            (tf.improvements, 'bool'),
            (tf.remove_coordinates, 'bool'),
            (tf.sample_label, 'text'),
            (tf.trust_address, 'bool'),
            (tf.trust_xy, 'bool'),
            (tf.low_conf_level, 'text'),
            ]
        for col, dtype in colList:
            logs.append(toolbox.add_column(db.Ttable, db.table, col, dtype))
        return logs
       
    @staticmethod
    def location_accuracy_not_geocode() -> list:
        """Create columns for location_accuracy_not_geocode tool and return logs specifying
        changes have been made to the columns
        """
        logs = []
        colList = [
            (tf.sample_label, 'text')
            ]
        for col, dtype in colList:
            logs.append(toolbox.add_column(db.NTtable, db.table_not_geocode, col, dtype))
        return logs

    @staticmethod    
    def frequency() -> list:
        """Create columns for frequency tool and return logs specifying
        changes have been made to the columns
        """
        logs = []
        colList = [
            (tf.sample_label_freq, 'text')
            ]
        for col, dtype in colList:
            logs.append(toolbox.add_column(db.Ttable, db.table, col, dtype))
        # Add org_sample_type_code as optional attribute
        colList = [
            (f.org_sample_type_code, 'text')
            ]
        for col, dtype in colList:
            logs.append(toolbox.add_column(db.Ttable, db.table, col, dtype, ignore_if_exists = True))
        return logs

    @staticmethod    
    def change_rate() -> list:
        """Create columns for change_rate tool and return logs specifying
        changes have been made to the columns
        """
        logs = []
        colList = [
            (tf.sample_label_chr, 'text'),
            (tf.sample_label_chr_top, 'text')
            ]
        for col, dtype in colList:
            logs.append(toolbox.add_column(db.Ttable, db.table, col, dtype))
        # Add org_sample_type_code as optional attribute
        colList = [
            (f.org_sample_type_code, 'text')
            ]
        for col, dtype in colList:
            logs.append(toolbox.add_column(db.Ttable, db.table, col, dtype, ignore_if_exists = True))
        return logs
    
   
# =============================================================================
#     def stacked_location_duplicates() -> list:
#         """
#         Create columns for location_accuracy_vls tool and return logs specifying
#         changes have been made to the columns
#         """
#         logs = []
#         colList = [
#             (tf.Stack_Count, 'double precision'),
#             (tf.address_score_percentage, 'double precision'),
#             (tf.Stack_Type, 'text'),
#         ]
#         for col, dtype in colList:
#             logs.append(toolbox.add_column(db.Ttable, db.table, col, dtype))
#         return logs
# =============================================================================

    @staticmethod
    def location_accuracy_vls() -> list:
        """Create columns for location_accuracy_vls tool and return logs specifying
        changes have been made to the columns
        """
        logs = []
        colList = [
            (tf.reject_vls, 'bool'),
            (tf.reject_group_vls, 'text'),
            (tf.sample_label_vls, 'text'),
            (tf.stack_reject_vls, 'text')
        ]
        for col, dtype in colList:
            logs.append(toolbox.add_column(db.VTtable, db.table, col, dtype))
        return logs

    @staticmethod
    def location_accuracy_api_call() -> list:
        """Create columns for location_accuracy_api_call tool and return logs specifying
        changes have been made to the columns
        """
        logs = []
        colList = [
            (tf.g_status, 'text'),
            (tf.g_lat, 'text'),
            (tf.g_lon, 'text'),
            (tf.g_street, 'text'),
            (tf.g_hno, 'text'),
            (tf.g_pc, 'text'),
            (tf.g_city, 'text'),
            (tf.sc_street, 'text'),
            (tf.sc_hno, 'text'),
            (tf.sc_postalcode, 'text'),
            (tf.sc_city, 'text'),
            (tf.sc_district, 'text'),
            (tf.g_distance, 'text'),
            (tf.g_rule, 'text'),
            (tf.g_match_typ, 'text'),
        ]
        for col, dtype in colList:
            logs.append(toolbox.add_column(db.Ttable, db.table, col, dtype))
        return logs
    
    @staticmethod
    def final_statistics() -> list:
        """Create columns for final statistics tool and return logs specifying
        changes have been made to the columns
        """
        from pack_final_statistics.stats_rich_final_statistics import FinalStatsRich
        final_stats_rich = FinalStatsRich()
        logs = []
        cols = final_stats_rich.get_available_raw_and_temp_attr_names()
        cols = list(cols.values())
        colList = [(c, 'boolean') for c in cols]
        for col, dtype in colList:
            logs.append(toolbox.add_column(db.Ttable, db.table, col, dtype))
        return logs