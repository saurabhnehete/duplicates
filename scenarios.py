from global_cfg import logger
from common import duplicate_scenarios as ds
from common import user_fieldname as fn
from common.configs import ConfigParser


class DuplicateScenarioParser(ConfigParser):
    """
    Duplicate scenarios parsing and values validation
    """
    not_included_attrs = [
        '_reject_', 
        '_advanced_',
        '_dif_location_', 
        '_sim_name_'
        ]
    conflicting_flags = [
        '_location_', 
        '_dif_location_'
        ]
    require_postprocessing = [
        '_sim_name_'
        ]

    def __init__(self):
        self.user_field_names = fn.parameters

        self.address = ds._address_
        self.coordinates = ds._coordinates_
        
        self.split_if_greater = int(ds.split_if_greater)
        self.split_by = ds.split_by
        self.part_size = int(ds.part_size)
        logger.debug('DuplicateScenarioParser has been initialized')

    def get_dedupl_scenarios(self):
        logger.debug('Getting dedup scenarios')
        scenarios = {parameter: ds.parameters[parameter] for 
                        parameter in ds.parameters if parameter.startswith('dupl_')}
        return scenarios

    def format_with_location(self, values: list, add: str='both'):
        """
        Updates the list with the location elements if specified.
        Address info or xy or both can be added.
        Parameter 'add' takes following values:
        both - both address and coordinates are added
        coordinates = only the coordinates are added
        address = only address is added
        """
        if '_location_' in values:
            values.remove('_location_')
            location = []
            if add == 'address' or add == 'both':
                location += self.str_to_list(self.address)
            if add == 'coordinates' or add == 'both':
                location += self.str_to_list(self.coordinates)
            if not location and add !='none':
                raise ValueError("Either _address_ or _coordinates_ must be populated!")
            else:
                values += location
    
        return values
    
    def get_formatted(self, key: str, scenario: str):
        if key not in self.user_field_names:
            raise ValueError(f"Attribute {key} is not found in the 'user_fieldname' file!")
        value = self.user_field_names[key]
        if value == '__not_specified_by_the_user__':
            raise ValueError(f"Value '{key}' must be populated to be used with '{scenario}' scenario!")
        return value

    def get_address(self):
        """Get address info attributes without xy"""
        address = self.str_to_list(self.address)
        return [self.get_formatted(attr, '_address_') 
                    for attr in address] if address else []

    def get_xy(self):
        """Get xy attributes without address info"""
        coordinates = self.str_to_list(self.coordinates)
        return [self.get_formatted(attr, '_coordinates_') 
                    for attr in coordinates] if coordinates else []

    def get_location(self):
        """Get all location attributes"""
        location = self.get_address() + self.get_xy()
        return location
    
    def check_split_by(self) -> bool:
        """Checks if split_by attribute populated"""
        if self.split_by != '__not_specified_by_the_user__':
            try:
                self.get_split_by()
                return True
            except:
                pass
        return False

    def get_split_by(self):
        """Get split by attribute if specified by user"""
        value = self.get_formatted(self.split_by, None)
        return value

    def check_flag(self, scenario: str, flag: str):
        """Checks if given scenario has a specific flag"""
        scenarios = self.get_dedupl_scenarios()
        has_flag = flag in self.str_to_list(scenarios[scenario])
        return True if has_flag else False

    @staticmethod
    def check_conflicting_flags(scenario: str,
                                 attributes: list, 
                                 will_conflict: list):
        """
        Raises an error if all of the conflicting values 
        are found in the attribute list.
        """
        count = 0
        for value in will_conflict:
            if value in attributes:
                count+=1
        if count == len(will_conflict):
            raise ValueError(f"Flags: {' and '.join(will_conflict)} can't be used in '{scenario}' at the same time!")

    def parse(self, used_location: str='both'):
        """Main function"""
        logger.debug(f"Parsing duplicate scenarios, used_location='{used_location}'")
        scenarios = self.get_dedupl_scenarios()
        new_dict = {}
        for key in scenarios:
            values = self.str_to_list(scenarios[key])
            self.check_conflicting_flags(key, values, self.conflicting_flags)
            locations = self.format_with_location(values, add=used_location)
            populated = [self.get_formatted(value, key) 
                            for value in locations 
                                if value not in self.not_included_attrs]
            new_dict.update({key: populated})
        return new_dict