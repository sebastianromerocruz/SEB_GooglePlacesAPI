import logging
import googlemaps
from QueryResult import QueryResult
from CompanyLocations import CompanyLocations
from FuzzyStringFilter import fuzzyStringFilterMatch, FilterType

# NOTE: GOOGLE PLACES API KEY REQUIRED HERE!
API_KEY = 'SOME API KEY'


# This list may not be exhaustive, or may not be strict enough
GOOGLE_PLACES_IRRELEVANT_TYPES = ['amusement_park', 'aquarium', 'art_gallery', 'bakery', 'bar', 'beauty_salon',
                                  'book_store', 'bowling_alley', 'cafe', 'campground', 'casino', 'cemetery', 'church',
                                  'clothing_store', 'convenience_store', 'courthouse', 'dentist', 'department_store',
                                  'doctor', 'embassy', 'fire_station', 'florist', 'funeral_home', 'furniture_store',
                                  'gym', 'hair_care', 'hardware_store', 'hindu_temple', 'insurance_agency', 'zoo',
                                  'travel_agency', 'taxi_stand', 'synagogue', 'supermarket', 'store', 'spa',
                                  'shopping_mall', 'shoe_store', 'school', 'rv_park', 'restaurant',
                                  'real_estate_agency', 'post_office', 'police', 'plumber', 'physiotherapist',
                                  'pharmacy', 'pet_store', 'park', 'painter', 'night_club', 'museum', 'movie_theater',
                                  'movie_rental', 'mosque', 'meal_takeaway', 'meal_delivery', 'lodging', 'locksmith',
                                  'local_government_office', 'liquor_store', 'library', 'lawyer', 'laundry',
                                  'jewelry_store', 'electrician', 'bus_station', 'car_wash', 'car_repair',
                                  'natural_feature', 'university', 'parking', 'neighborhood', 'political',
                                  'general_contractor', 'gas_station', 'accounting', 'food', 'transit_station', "atm",
                                  'finance']
logging.basicConfig(level = logging.DEBUG, format = '%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger()
RADIUS_OF_SEARCH = 50000  # metres
LOGGER.info("Starting...")
LOGGER.info("API acquired...")
FUZZY_FILTER_TYPE = FilterType.TOKEN_SET_RATIO
FUZZY_FILTER_THRESHOLD = 80
FIELDS = ['geometry', 'name', 'type', 'permanently_closed', 'vicinity']  # Define the fields we want sent back to us
GMAPS = googlemaps.Client(key = API_KEY)  # Define our client
LOGGER.info("Client defined...")


def placesNearbyQuery(companyLocations, locationEpicentre, radiusFromEpicentre = 100, hasToBeOpen = False,
                      companyKeyword = "", coordinates = {}):
    """
    For a company keyword (i.e. their official name), performs a search using Google Places' API around a location. This
    location is a set of coordinates in string form, and the radius of search is maxed out at 50,000 metres. Provided
    that a query result passes through a series of filters, it will then be added to a dictionary of company locations.
    In order to avoid duplicates, a second dictionary is passed to keep track of latitudes and longitudes already seen
    in a particular search.

    :param companyLocations: dictionary of company locations collected thus far
    :type companyLocations: {str : CompanyLocations}

    :param locationEpicentre: Latitude and longitude where search should be centered
    :type locationEpicentre: str

    :param radiusFromEpicentre: Radius of search centered at locationEpicenter
    :type radiusFromEpicentre: float

    :param hasToBeOpen: Optional filter applied into the Places API call
    :type hasToBeOpen: bool

    :param companyKeyword: The actual name of the company to be queried into Google Places API
    :type companyKeyword: str

    :param coordinates: Dictionary to keep track of already-seen coordinates
    :type coordinates: {str : [(float, float]}

    :return: None
    """

    # Define our search
    placesResult = GMAPS.places_nearby(location = locationEpicentre,
                                       radius = radiusFromEpicentre,
                                       open_now = hasToBeOpen,
                                       keyword = companyKeyword)

    # If query fails
    if placesResult["status"] != "OK":
        LOGGER.warning("Error geocoding {}: {}".format(companyKeyword, placesResult["status"]))
        return

    if len(placesResult['results']) > 0:
        # Loop through each place in results:
        for place in placesResult['results']:
            # Define my place id
            placeID = place['place_id']

            if not fuzzyStringFilterMatch(companyKeyword, place['name'], FUZZY_FILTER_TYPE, FUZZY_FILTER_THRESHOLD):
                LOGGER.info("Fuzzy string non-match {}: {}".format(companyKeyword, place['name']))
                return

            # Make a request for the details
            placeInformation = GMAPS.place(place_id = placeID, fields = FIELDS)

            if placeInformation['status'] != "OK":
                LOGGER.warning("Error extracting details of {}: {}".format(companyKeyword, placesResult["status"]))
                LOGGER.warning("Skipping!")
                continue

            # Extract relevant information from JSON
            placeName = placeInformation['result']['name'].strip().lower()
            placeTypes = placeInformation['result']['types']
            placeLatitude = float(placeInformation['result']['geometry']['location']['lat'])
            placeLongitude = float(placeInformation['result']['geometry']['location']['lng'])
            if 'vicinity' in placeInformation['result']:
                placeVicinity = placeInformation['result']['vicinity']
            else:
                placeVicinity = "N/A"
            # Is this business permanently closed? If so, we want to filter that out
            permanentlyClosed = getIsPermanentlyClosed(placeInformation = placeInformation)

            if permanentlyClosed is False and set(placeTypes).isdisjoint(GOOGLE_PLACES_IRRELEVANT_TYPES):
                # If this location is not categorised as any of our 'irrelevant' types
                newQueryResult = QueryResult(resultName= placeName,
                                             types = placeTypes,
                                             latitude = placeLatitude,
                                             longitude = placeLongitude,
                                             companyKeyword = companyKeyword.replace('"', ''),
                                             vicinity = placeVicinity)

                print(coordinates)
                if (newQueryResult.getLatitude(), newQueryResult.getLongitude()) not \
                        in coordinates[companyKeyword]:
                    # If this set of coordinates has been not seen before
                    companyLocations.addQueryResult(newQueryResult)
                    coordinates[companyKeyword].append((newQueryResult.getLatitude(), newQueryResult.getLongitude()))


def getCompanyLocationsNearLocationList(companyNameList, locationsDictionary, limitOfAmountOfCities = 50):
    """
    Generates a dictionary of CompanyLocations objects based on a set of company names and their respective coordinates.
    One can limit the amount of cities to be searched.

    :param companyNameList: A list of names of companies to be search
    :type companyNameList: [str]

    :param locationsDictionary: A set of City names and epicentre coordinates to be searched
    :type locationsDictionary: {str : str}

    :param limitOfAmountOfCities: Optional limit of amount of cities to be searched
    :type limitOfAmountOfCities: int

    :return: Dictionary of company locations for every company passed in
    :rtype {str : CompanyLocations}
    """
    companyLocationsMaster = {}
    currentCoordinates = {}

    amountOfCities = 0  # Initialising city counter
    for city, epicentre in locationsDictionary.items():
        LOGGER.info("Searching " + city + "...")
        for companyName in companyNameList:
            if companyName not in companyLocationsMaster:
                companyLocations = CompanyLocations(companyName = companyName.replace('"', ''))
                companyLocationsMaster[companyName] = companyLocations
                currentCoordinates[companyName] = []

            # logger.info("Company: " + companyName + "...")
            try:
                placesNearbyQuery(companyLocations = companyLocationsMaster[companyName], locationEpicentre = epicentre,
                                  radiusFromEpicentre = RADIUS_OF_SEARCH, hasToBeOpen = False,
                                  companyKeyword = companyName, coordinates = currentCoordinates)
            except Exception as e:
                LOGGER.exception(e)
                LOGGER.error("Major error with {} in {}".format(companyName, city))
                LOGGER.error("Skipping!")
                continue

        if amountOfCities == limitOfAmountOfCities:
            break
        amountOfCities += 1

        if amountOfCities > len(locationsDictionary):
            break

    return companyLocationsMaster


def getIsPermanentlyClosed(placeInformation = None):
    """
    Simple check if current query result is permanently close, in which case it should not be included in final results

    :param placeInformation: Response from Google Places API
    :type placeInformation: JSON

    :rtype: bool
    """
    isPermanentlyClosed = ('permanently_closed' in placeInformation['result']
                           and placeInformation['result']['permanently_closed'])

    return isPermanentlyClosed


def getAPIKey():
    """
    If API key exists, it returns it

    :return: API Key
    :rtype: str
    """
    if API_KEY:
        return API_KEY
    else:
        LOGGER.error("API_KEY is null")
        raise TypeError


def getIrrelevantTypes():
    """
    If a list of irrelevant types to be search exists, it returns it

    :return: List of irrelevant types
    :rtype: [str]
    """
    if GOOGLE_PLACES_IRRELEVANT_TYPES:
        return GOOGLE_PLACES_IRRELEVANT_TYPES
    else:
        LOGGER.error("Irrelevant type list is null")
        raise TypeError
