from pack_duplicates.duplicate_analysis import Postprocessor, DedupQueryGenerator, Preprocessor, Stacking
from pack_duplicates.stats_duplicates import DuplicateStatistics
from common.log import SQLLogger
from common import query_toolbox as toolbox
from global_cfg import tools


tool_id = 4

def run():
    additional_scripts = []
    plog = []
    log = []

    sql_logger = SQLLogger(logName=tools[tool_id].replace(' ', '_'))
    pre = Preprocessor()
    dup = DedupQueryGenerator()
    post = Postprocessor()
    stats = DuplicateStatistics()

    parts = pre.process()
    sql_logger.temp_table_info = parts
    for key, value in parts.items():
        size = value['total']
        temp_table_name = value['table']
        analyze = True if size>2000000 else False
        log.extend(toolbox.execute_query_list(pre.create_table_queries[key-1])) #create temp table query
        queries = []
        q, l = dup.generate_queries(temp_table_name,
                                        part=key, 
                                        analyze=analyze)                    
        queries.extend(q)     #adding main queries
        log.extend(l)
        toolbox.save_queries(queries, 
                    filename=f'{key}_{tools[tool_id].replace(" ","_")}_Queries') #saving queries
        log.extend(toolbox.execute_query_list(queries))
        additional_scripts = post.run_additional_scripts(temp_table_name)
        log.extend(toolbox.execute_query_list(additional_scripts))
        stack = Stacking()
        stack.run_stacks(temp_table_name)
        dup.delete_temps()
    if len(parts) > 1:
        tables = [value['table'] for value in parts.values()]
        wrap_up = post.merge_temp_tables(tables, cleanup=True)
        log.extend(toolbox.execute_query_list(wrap_up))

    plog.extend(stats.run())
    toolbox.clean_console()
    plog.append(toolbox.save_output_table(tool_id = tool_id))
    sql_logger.writeLog(log, plog)