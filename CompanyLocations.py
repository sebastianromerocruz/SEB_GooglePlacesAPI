import jsonpickle
import logging

logging.basicConfig(level = logging.DEBUG, format = '%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger()


class CompanyLocations:
    """
    Represents a collection of query results for a given company. As results return from a Google Places query call,
    they are store in a list respective of the company being searched.å
    """

    def __init__(self, companyName = "null"):
        """
        :param companyName: Name of the company to be used in query
        :type companyName: str
        """
        self.companyName = companyName
        self.queryResultList = []

    def getCompanyName(self):
        if self.companyName == "":
            LOGGER.warning("Company Name field is blank")
        if self.companyName is None:
            LOGGER.error("Company Name field is null")
            raise TypeError

        return self.companyName

    def getQueryResultList(self):
        if self.queryResultList is None:
            LOGGER.error("Query result list for", self.getCompanyName(), "is null!")
            raise TypeError

        return self.queryResultList

    def addQueryResult(self, newQueryResult = None):
        """
        If this result is not already part of our result list, it will add it to the list

        :param newQueryResult: The query result to be considered and added
        :type newQueryResult: QueryResult
        """
        if newQueryResult is None:
            LOGGER.error("addQueryResult failed because new query result is null.")
            raise TypeError

        if newQueryResult not in self.getQueryResultList():
            self.getQueryResultList().append(newQueryResult.getDictionaryRepresentation())

    def getDictionaryRepresentation(self, unpicklable = False):
        """
        Pickles the object into JSON format. Uses jsonpickle module. See: https://jsonpickle.github.io

        :param unpicklable: If it is desired to return JSON to CompanyLocations form, set flag to True
        :type unpicklable: bool


        :return: JSON representation of object
        :rtype: JSON
        """
        return jsonpickle.encode(self, unpicklable = unpicklable)

    def __eq__(self, other):
        """
        Equivalence check based on company name and its query result list

        :param other: Object to be compared
        :type other: object

        :return: Whether self and other are equivalent
        :rtype: bool
        """
        if isinstance(other, CompanyLocations):
            areCompanyNamesEqual: bool = (self.companyName == other.companyName)
            areQueryResultListsEqual: bool = (self.queryResultList == other.queryResultList)

            return areCompanyNamesEqual and areQueryResultListsEqual

        return False
