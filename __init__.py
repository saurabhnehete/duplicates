"""The Duplicates module"""
from pack_duplicates import scenarios
from pack_duplicates import custom


duplicate_scenarios = scenarios.DuplicateScenarioParser()

clusters = custom.FuzzyCluster(0.75)

# group_creator = custom.GroupCreator(duplicate_scenarios.part_size)
group_creator = custom.GroupCreator

