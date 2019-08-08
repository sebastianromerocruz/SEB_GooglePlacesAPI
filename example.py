# -*- coding: utf-8 -*-
import time
import logging
from pprint import pprint
from ParseCitiesCSV import parseCitiesCSV
from PySparkPreprocessing import getListOfCompanyNames
from GooglePlacesSEB import getCompanyLocationsNearLocationList

"""
The following is a demonstration of the capabilities of the Google Places API when searching for locations of companies.

The pipeline acquires a sample of company names from a given country and sector (see PySparkPreProcessing.py for 
pre-processing details), along with a list of the coordinates of the 1,000 largest American cities, and performs queries
for each company name in each city.

Query result names are first run through a fuzzy string filter to measure their similarity to the actual company name.
This filter is customizable—-please refer to FuzzyStringFilter.py for details. Results are further filtered to not 
include duplicates, permanently-closed locations (which are also returned by the API), and places of irrelevant types 
(e.g. 'hindu_temple', 'rv_park', etc.).

Finally, the results are stored in JSON format with the following schema:
root
 |-- companyName: string (nullable = true)
 |-- queryResultList: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- companyKeyword: string (nullable = true)
 |    |    |-- geometry: struct (nullable = true)
 |    |    |    |-- lat: double (nullable = true)
 |    |    |    |-- lon: double (nullable = true)
 |    |    |-- resultName: string (nullable = true)
 |    |    |-- types: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- vicinity: string (nullable = true)
 
I.e., every company's JSON object will contain an array of JSON objects for every result returned by the Google Places
API query.

This program is not meant to be an exhaustive representation of the full capabilities of the Google Places API, but
rather a proof-of-concept exploration of the API's free-tier capabilities and how it could be potentially scaled up.

        -- S. Romero Cruz, July 2019, S.E.B. New York
"""

logging.basicConfig(level = logging.DEBUG, format = '%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger()
COMPANY_NAME_SAMPLE_QUOTES = getListOfCompanyNames(fileName = "All_comp2019May.csv", sizeOfList = 20 , country = "USA",
                                                   numberOfStopWordsToRemove = 0, sector ="Materials")

CITY_AMOUNT_LIMIT = 20

# SOURCE:
# https://public.opendatasoft.com/explore/dataset/1000-largest-us-cities-by-population-with-geographic-coordinates
AMERICAN_CITIES = parseCitiesCSV(filename ="1000-largest-us-cities-by-population-with-geographic-coordinates.csv",
                                 hasHeader = True, state = "New Jersey")


if __name__ == "__main__":
    print(COMPANY_NAME_SAMPLE_QUOTES)
    startTime = time.time()
    companyLocationsMaster = getCompanyLocationsNearLocationList(companyNameList = COMPANY_NAME_SAMPLE_QUOTES,
                                                                 locationsDictionary = AMERICAN_CITIES,
                                                                 limitOfAmountOfCities = CITY_AMOUNT_LIMIT)

    print("\nResults for this sample: ")
    with open("sampleResults_tok80.json", "w") as outputFile:
        for companyName, companyQueryList in companyLocationsMaster.items():
            if len(companyQueryList.getQueryResultList()) == 0:
                LOGGER.info("0 RESULTS FOR: " + companyName.upper())
                print()
                continue

            print(str(len(companyQueryList.getQueryResultList())) + " RESULTS FOR: " + companyName.upper())
            pprint(companyQueryList.getDictionaryRepresentation())

            print(companyQueryList.getDictionaryRepresentation(), file = outputFile, end = "\n")
            print()
    print("--- %s seconds ---" % (time.time() - startTime))

