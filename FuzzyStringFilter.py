from enum import Enum
from fuzzywuzzy import fuzz


class FilterType(Enum):
    """
    Used for a more descriptive way of identifying filter type options for fuzzyStringFilterMatch()
    """
    LEVENSHTEIN_RATIO = 0  # Levenshtein Ratio
    SUB_STRING_RATIO = 1   # Substring Ratio
    TOKEN_SORT_RATIO = 2   # Partial Sort Ratio
    TOKEN_SET_RATIO = 3    # Partial Set Ratio


def fuzzyStringFilterMatch(companyName, resultName, filterType = FilterType.LEVENSHTEIN_RATIO, threshold = 90.0):
    """
    Based on tutorial: https://www.datacamp.com/community/tutorials/fuzzy-string-python
    Determines whether a query result's name is within an acceptable similarity threshold to the actual company's name.

    The user can choose to apply a simple Levenshtein ratio filter, a substring filter, a partial sort ratio, or a
    partial set ratio. In general, these filters go from most "strict" to least "strict", but please refer to above
    tutorial for a more detailed explanation of each.

    :param companyName: The company name used to query Google Places API
    :type companyName: str

    :param resultName: The name of the result from the query
    :type resultName: str

    :param filterType: The type of comparison algorithm to be applied
    :type filterType: FilterType

    :param threshold: Percentage threshold results will be filtered by
    :type threshold: float

    :rtype bool
    """

    if filterType == FilterType.LEVENSHTEIN_RATIO:
        return fuzz.ratio(companyName, resultName) > threshold
    elif filterType == FilterType.SUB_STRING_RATIO:
        return fuzz.partial_ratio(companyName, resultName) > threshold
    elif filterType == FilterType.TOKEN_SORT_RATIO:
        return fuzz.token_sort_ratio(companyName, resultName) > threshold
    else:
        return fuzz.token_set_ratio(companyName, resultName) > threshold
