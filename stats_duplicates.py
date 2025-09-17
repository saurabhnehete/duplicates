from global_cfg import logger
from common import tool_fieldname as tf
from common import stat_queries as stats
from common.light_model import StatsPerSupplier
from pack_duplicates import duplicate_scenarios as ds


class DuplicateStatistics():
    def __init__(self):
        self.dupl_scenarios = ds.get_dedupl_scenarios()

    def scenarios_count(self):
        """Calculationg numbers per scenario"""
        queries = []
        for scenario, attrs in self.dupl_scenarios.items():
            if '_reject_' in attrs:
                queries.append(
                    stats.simpleCount(
                    f"Rejected as '{scenario}' ",
                    **{tf.reject_group_dedup: f"= '{scenario}'"}))
            else:
                queries.append(
                    stats.simpleCount(
                    f"Sampled as '{scenario}' ",
                    **{tf.sample_type_code_dedup: f"= '{scenario}'"}))
                
        return queries
    
    @staticmethod
    def stack_count():
        """Calculationg numbers per scenario"""
        queries=[]
        queries.append(
                stats.simpleCount(
                'Invalid Stack Records',
                **{tf.Stack_Type: f"= 'Invalid Stack'"}))
        
        queries.append(
                stats.simpleCount(
                'Valid Stack Records',
                **{tf.Stack_Type: f"= 'Valid Stack'"}))
        
        queries.append(
                stats.simpleCount(
                'Inconclusive Stack Records',
                **{tf.Stack_Type: f"= 'Inconclusive'"}))
                
        return queries

    @staticmethod
    def counts():
        """Calculating totals"""
        queries = [
            stats.simpleCount(
            'Total number of records'
            ),
            stats.simpleCount(
            'Total rejected',
            **{tf.reject_group_dedup: " <> ''"}),
            stats.simpleCount(
            'Total sampled',
            **{tf.sample_type_code_dedup: " <> ''"})
        ]
        return queries

    def return_queries(self):
        """Returns query list without execution"""
        queries = self.counts() + self.scenarios_count() +self.stack_count()
        return queries

    def run(self):
        plog = []
        queries = self.counts() + self.scenarios_count() + self.stack_count()
        logger.debug(queries)
        funcs = [(stats.addPercentColumn, (), {})]
        executor = stats.execute_query_stats
        sts_per_sup = StatsPerSupplier(queries, executor, funcs)
        df = sts_per_sup.run()
        # df = stats.execute_query_stats(queries)
        # df = stats.addPercentColumn(df)
        logger.debug(df)
        plog.append(stats.saveStatsOnDB(df, prefix="dup_"))
        plog.append(stats.saveStatsLocally(df, prefix="dup_"))
        return plog