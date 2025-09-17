from global_cfg import logger
from Levenshtein.StringMatcher import ratio
from itertools import combinations
import re

class FuzzyCluster:
    """Class responsible for creating clusters.
    """
    def __init__(self, threshold: float=0.8) -> None:
        self.threshold = threshold

    @staticmethod
    def read_file(filepath: str) -> str:
        """Reads stopwords as regex"""
        with open(filepath, 'r', encoding='utf-8') as f:
            data = f.read()
        return data
    
    @staticmethod
    def _apply_rules(item: str, pattern: str) -> str:
        """Applies string replacements to the input string 
        along with other cleanup operations
        """ 
        item = re.sub("[,\.\"'\|\(\)-]+", ' ', item)
        item = re.sub(pattern,' ', item, flags=re.IGNORECASE)
        item = re.sub("\s+", " ", item)
        item = item.strip().lower()
        return item

    def _remove_stopwords(self, items: list) -> list:
        stopwords = self.read_file(r'meta\stopwords.txt')
        pattern = f"\s+({stopwords})(?=\s)|^({stopwords})\s|\s({stopwords})$"
        replaced = [(item, self._apply_rules(item, pattern)) for item in items]
        return replaced

    def _match_pairs(self, docs: list) -> list:
        """Builds unique pairs of items to calculate 
        levenshtein distance between them.
        If the distance is greater or equal the threshold,
        collapses them into clusters of 2 elements.
        Elements that didn't match will get their own clusters.
        """
        first_round = [comb for comb in combinations(docs, 2)
                            if ratio(comb[0], comb[1]) >= self.threshold]
        unique_items = {item for tupes in first_round 
                                for item in tupes}
        left_alone = [[doc] for doc in docs 
                            if doc not in unique_items]
        clusters = first_round + left_alone
        return clusters

    @staticmethod
    def _combine_clusters(clusters: list) -> list:
        """Combines clusters that share the same items.
        [[1, 2], [2, 3], [4, 5], [6]] 
        --> [[1, 2, 3], [4, 5], [6]]
        """
        out = []
        while len(clusters)>0:
            first, *rest = clusters
            first = set(first)
            lf = -1
            while len(first)>lf:
                lf = len(first)
                rest2 = []
                for r in rest:
                    if len(first.intersection(set(r)))>0:
                        first |= set(r)
                    else:
                        rest2.append(r)     
                rest = rest2
            out.append(list(first))
            clusters = rest
        return out

    @staticmethod
    def _restore_original(clusters: list, dictionary: dict) -> list:
        """Restores original items using dictionary
        """
        new_clusters = []
        for cluster in clusters:
            new_values = []
            for item in cluster:
                for element in dictionary:
                    if item == element[1]:
                        new_values.append(element[0])
            new_clusters.append(new_values)
        return new_clusters
        
    def create_clusters(self, items: list) -> list:
        dictionary = self._remove_stopwords(items)
        items = [element[1] for element in dictionary]
        matched = self._match_pairs(items)
        clusters = self._combine_clusters(matched)
        new_clusters = self._restore_original(clusters, dictionary)
        return new_clusters


class GroupCreator:
    """Generates as few groups as possible 
    while keeping their sizes as big 
    as possible
    """
    def __init__(self, limit: int):
        self.limit = int(limit)
    
    def _regroup(self, data: dict, min_limit: int) -> dict:
        counter = 1
        groups = {}
        unpacked = [(value['items'], value['total']) for _, value in data.items()]

        bucket = list(unpacked)
        for key, value in unpacked:
            if value > min_limit:
                groups[counter] = {'items': key, 'total': value}
                counter += 1
                bucket.remove((key, value))

        bucket2 = list(bucket)
        for key, value in bucket:
            if len(bucket2) == 1:
                leftover, left_num = bucket2[0]
                groups[counter] = {'items': leftover, 'total': left_num}
                counter += 1
                bucket2.remove((leftover, left_num))
            bucket2_items = [item[0] for item in bucket2]
            if key in bucket2_items:
                delta = min_limit-value
                bucket2.remove((key, value))
                values = [item[1] for item in bucket2]
                closest_value = min(values, key=lambda x:abs(x-delta))
                for name, num in bucket2:
                    if closest_value == num:
                        closest_key = name
                groups[counter] = {'items': key + closest_key, 'total': value+closest_value}
                counter+=1
                bucket2.remove((closest_key, closest_value))
        return groups

    def get_groups(self, raw_data: dict) -> dict:
        """Converts dict of raw data into dict of almost
        equal by size groups. Sizes of groups are defined in config"""

        logger.debug(
            f"Splitting raw data of {len(raw_data)} into almost equal groups of {self.limit}"
            )
        counter = 1
        data = {}
        for k, v in raw_data.items():
            data[counter] = {'items': [k], 'total': v}
            counter += 1

        less_than_limit = [0, 0]
        while len(less_than_limit) > 1:
            data = self._regroup(data, self.limit)
            less_than_limit = [value['total'] 
                for value in data.values() 
                    if value['total'] < self.limit]
        return data