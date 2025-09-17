# -*- coding: utf-8 -*-
"""
Generate entry point to each tool.
It should be in synce with tools dictionary from global_cfg
"""
import subprocess

class Tools:
    def __init__(self):
        """This dictionary should be in sync with tools from global_cfg"""
        self.run_dict = {
            1: self.change_rate,
            2: self.basic_checks,
            3: self.address_completeness,
            4: self.duplicates,
            5: self.frequency,
            # 6: self.location_accuracy,
            7: self.location_accuracy_vls,
            8: self.final_statistics,
            9: self.matching_block,
            10: self.duplicates_sample_generation,
            11: self.end_to_end,
            12: self.upload_to_tableau,
            13: self.supplier_scoring,
            14: self.upload_summary,
            15: self.privacy_vvs_rule_check,
            16: self.matching_and_vls_block_athena,
            17: self.building_data,
            18: self.google_api,
            99: self.goodbye
            }

    @staticmethod
    def change_rate():
        from pack_change_rate import tool_change_rate
        tool_change_rate.run()

    @staticmethod
    def basic_checks():
        process = subprocess.Popen(r"dist\run_basic_checks\run_basic_checks.exe") # run separately as it uses multiprocess
        process.wait()
        # from pack_basic_checks import tool_basic_checks
        # tool_basic_checks.run()

    @staticmethod
    def address_completeness():
        from pack_analyse_address import tool_analyse_address
        tool_analyse_address.run()

    @staticmethod
    def matching_block():
        from pack_matching_block import tool_matching_block
        tool_matching_block.run()

    @staticmethod
    def matching_and_vls_block_athena():
        from pack_re_evaluation import tool_re_evaluation_block
        tool_re_evaluation_block.run()

    @staticmethod
    def building_data():
        from pack_building_data import tool_building_data_block
        tool_building_data_block.run()

    @staticmethod
    def google_api():
        from pack_google_api import tool_google_api_block
        tool_google_api_block.run()

    @staticmethod
    def duplicates():
        from pack_duplicates import tool_analyze_duplicates
        tool_analyze_duplicates.run()

    @staticmethod
    def duplicates_sample_generation():
        from pack_duplicates_sample_generation import tool_duplicates_sample_generation
        tool_duplicates_sample_generation.run()

    @staticmethod
    def frequency():
        from pack_frequency import tool_frequency
        tool_frequency.run()

    # @staticmethod  # Removed from tool not in use
    # def location_accuracy():
    #     from pack_location_accuracy import tool_location_accuracy
    #     tool_location_accuracy.run()

    @staticmethod
    def location_accuracy_vls():
        from pack_location_accuracy_vls import tool_location_accuracy_vls
        tool_location_accuracy_vls.run()

    @staticmethod
    def final_statistics():
        from pack_final_statistics import tool_final_statistics
        tool_final_statistics.run()

    @staticmethod
    def upload_to_tableau():
        #from pack_tableau import upload_to_s3
        from pack_tableau import data_aggregation
        data_aggregation.run()
        #upload_to_s3.run()

    @staticmethod
    def supplier_scoring():
        from pack_tableau import tool_supplier_scores
        tool_supplier_scores.run()

    @staticmethod
    def upload_summary():
        from pack_tableau import summary_template
        summary_template.run()

    @staticmethod
    def end_to_end():
        from pack_end_to_end import tool_end_to_end
        tool_end_to_end.run()

    @staticmethod
    def privacy_vvs_rule_check():
        from pack_privacy_rule import tool_privacy_rule
        tool_privacy_rule.run()

    @staticmethod
    def goodbye():
        print("Have a good day!")