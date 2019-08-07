import logging
from jsonConvertToDict import convertObjectToDictionary

logging.basicConfig(level = logging.DEBUG, format = '%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger()


class QueryResult:
    """
    Represents a query result from Goople Places API call using the places_nearby() function. Is helpful in avoiding
    keeping duplicate results through its equality check.
    """

    def __init__(self, resultName, latitude, longitude, vicinity ="N/A", companyKeyword ="", types = None):
        """
        :param resultName: The name of the query result, extracted from the html response
        :type resultName: str

        :param latitude: The latitude value of the query result, extracted from the html response
        :type latitude: float

        :param longitude: The longitude value of the query result, extracted from the html response
        :type longitude: float

        :param vicinity: Added only if html response contains a 'vicinity' field. Usually an address or neighbourhood
        near the query result
        :type vicinity: str

        :param companyKeyword: The keyword used when querying places_nearby(). Usually the name of the company
        :type companyKeyword: str

        :param types: The types of the query result, extracted from the html response (e.g. "bank", "bar", etc.)
        :type types: [str]
        """
        if types is None:
            types = []
        self.resultName = resultName
        self.types = types
        self.geometry = {
            "lat": latitude,
            "lon": longitude,
        }
        self.companyKeyword = companyKeyword
        self.vicinity = vicinity

    def getResultName(self):
        if self.resultName == "":
            LOGGER.warning("Result Name field is blank")
        if self.resultName is None:
            LOGGER.error("Result Name field is null")
            raise TypeError

        return self.resultName

    def getTypes(self):
        if len(self.types) == 0:
            LOGGER.warning("Types list is empty")
        if self.types is None:
            LOGGER.error("Types field is null")
            raise TypeError

        return self.types

    def getLatitude(self):
        if self.geometry['lat'] is None:
            LOGGER.error("WLatitude field is null")
            raise TypeError

        return self.geometry['lat']

    def getLongitude(self):
        if self.geometry['lon'] is None:
            LOGGER.error("Longitude field is null")
            raise TypeError

        return self.geometry['lat']

    def getGeometry(self):
        """
        :return: Dictionary of coordinates of query result
        :rtype: {str : float}
        """
        return self.geometry

    def getCompanyKeyword(self):
        if self.companyKeyword == "":
            LOGGER.warning("Company keyword field is blank")
        if self.companyKeyword is None:
            LOGGER.error("Company keyword field is null")
            raise TypeError

        return self.companyKeyword

    def getCityName(self):
        if self.vicinity is None:
            LOGGER.error("City name keyword field is null")
            raise TypeError

        return self.vicinity

    def getDictionaryRepresentation(self):
        """
        Converts itself to dictionary form (see: convertObjectToDictionary() documentation)

        :return: Dictionary representation of object
        :rtype: {str: str, str: [str], str: {str: float, str: float}, str: str, str: str}
        """
        return convertObjectToDictionary(self)

    def __str__(self):
        stringRep = "RESULT NAME: " + self.getResultName() + "\nTYPES: " + str(self.getTypes()) + "\nCOORDINATES: (" + \
                    str(self.getLatitude()) + ", " + str(self.getLongitude()) + ")\nCITY: " + self.getCityName() + "\n"

        return stringRep

    def __eq__(self, other):
        """
        Equivalency check based on name, types, and geometry.

        :param other: Object to be compared
        :type other: object

        :rtype: bool
        """
        if isinstance(other, QueryResult):
            areNamesEqual = (self.resultName == other.resultName)
            areTypesEqual = (self.types == other.types)
            areLatitudesEqual = (self.geometry['lat'] == other.geometry['lat'])
            areLongitudesEqual = (self.geometry['lon'] == other.geometry['lon'])

            return areNamesEqual and areTypesEqual and areLatitudesEqual and areLongitudesEqual

        return False
