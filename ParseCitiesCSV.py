import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger()


def parseCitiesCSV(filename, hasHeader = False, state = None):
    """
    A quick back-of-the-envelope reader of a CSV I found of the 1,000 largest American cities for testing purposes.
    Could be modified to be more modular.

    :param filename: Address of the CSV file to be read
    :type filename: str

    :param hasHeader: Whether CSV file has a header or not
    :type hasHeader: bool

    :param state: Specific state being considered for testing purposes
    :type state: str

    :return: Dictionary of form {cityName : latitudeAndLongitude} of cities from CSV file. Please note that coordinates
    are in string form (e.g. "1.2345,6.789")
    :rtype: {str : str}
    """

    if filename is None:
        LOGGER.error("filename is null")
        raise TypeError

    if hasHeader is None:
        LOGGER.warning("hasHeader is null")
        raise TypeError

    cities = {}

    with open(filename, 'r') as file:
        LOGGER.debug(filename + " OPENED SUCCESSFULLY")
        if hasHeader:
            file.readline()

        for line in file:
            line = line.strip().split(";")

            if line[0] in cities:
                # Should not happen but just in case
                continue

            if state:
                if line[2] == state:
                    cityName = line[0]
                    cities[cityName] = line[5]
            else:
                cityName = line[0]
                cities[cityName] = line[5]

    LOGGER.debug(filename + " READ AND CLOSED SUCCESSFULLY")

    return cities
