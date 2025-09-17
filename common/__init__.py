"""Common modules and variables used across all the tools"""

import os
from global_cfg import OUTPUT_FOLDER, UUID, E2E_LOG
from common.configs import ConfigReader, DatabaseConfigReader

OUTPUT_FOLDER = OUTPUT_FOLDER
UUID = UUID
E2E_LOG = E2E_LOG

# Setting up configuration readers:
table_prefixes = {   # set of all known table prefixes. Considered reserved.
    "src_", "out_",
    "int_", "gen_",
    "dup_", "tmp_",
    "loc_", "frq_",
    "chr_", "fst_",
    "add_","vls_",
    "loc_vls_", "mch_",
    "merged_","rich_",
    "ath_", "bld_",
    "ggl_"
    }
database = DatabaseConfigReader(r'configs\database.txt', table_prefixes)
tool_fieldname = ConfigReader(r'configs\tool_fieldname.txt')
user_fieldname = ConfigReader(r'configs\user_fieldname.txt')
values = ConfigReader(r'configs\values.txt')
source = ConfigReader(r'configs\source.txt')
duplicate_scenarios = ConfigReader(r'configs\duplicate_scenarios.txt')
change_rate = ConfigReader(r'configs\change_rate.txt')
basic_checks = ConfigReader(r'configs\basic_checks.txt')
run = ConfigReader(r'configs\run.txt')
richcontent = ConfigReader(r'configs\richcontent_attributes.txt')
rich_attributes_mapping = ConfigReader(r'configs\rich_attributes_mapping.txt')

from common.main import DatabaseConnection
from common.main import QueryToolBox
from common.main import StatisticsQueries

# Setting up db connection:
db_connection = DatabaseConnection()

# Setting up query helpers:
query_toolbox = QueryToolBox(db_connection)
stat_queries = StatisticsQueries(db_connection)

from common.run_tools import Tools

# Setting up tool entry points
run_dict = Tools().run_dict

# Setting up tool monitoring
from common.tool_monitor import CentralMonitoring
CentralMonitoring = CentralMonitoring
