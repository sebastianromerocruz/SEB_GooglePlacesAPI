import re
import logging
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import length, explode, split, concat_ws, rand
from pyspark.sql import SparkSession
from pyspark.ml.feature import StopWordsRemover

"""
        This was my pre-processing pipeline. I chose to use Apache Spark, but this isn't necessary if the amount of data
        isn't too big. This is the order of operations of the pipeline:
        
        GET DATA FRAME FROM CSV --> FILTER NULL ENTRIES --> SELECT COUNTRY OF INTEREST --> REMOVE STOP-WORDS IF 
        NECESSARY --> SELECT SECTOR OF INTEREST --> SELECT RANDOM SAMPLE OF COMPANIES --> TURN DATA FRAME COLUMN OF 
        NAMES INTO PYTHON LIST --> ADD DOUBLE-QUOTES AROUND NAMES --> RETURN
        
        See function getListOfCompanyNames().
        
                        - S.R.C.
"""

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

LOGGER = logging.getLogger()
REMOVE_LIST = "., -"
SPARK_CONTEXT = None
SQL_CONTEXT = None
SPARK = None


def pySparkSetup():
    """
        Sets up necessary contexts and Spark session.

        :return: None
        """
    global SPARK_CONTEXT
    global SQL_CONTEXT
    global SPARK

    SPARK = SparkSession \
        .builder \
        .appName("S.E.B. Google Places") \
        .getOrCreate()

    SPARK_CONTEXT = SparkContext.getOrCreate()
    SQL_CONTEXT = SQLContext(SPARK_CONTEXT)


def getDfFromCSVFile(fileName):
    """
        Reads comma-separated values (CSV) file of the following format and returns it as a dataframe:
        |-----------------------------------------------------------|
        | Company Name | Country | Sector | Industry | Sub Industry |
        |-----------------------------------------------------------|


        :param fileName: Address of CSV file
        :type fileName: str

        :rtype: pyspark.sql.dataframe.DataFrame
        """
    df = SQL_CONTEXT.read.csv(fileName, header=True)
    df = df.select("Company Name", 'Country', 'Sector', 'Industry', 'Sub Industry')
    return df


def filterNullEntries(dataFrame, columnName):
    """
        Filters out null entries in a specific column of a data frame and returns the modified data frame

        :param dataFrame: Spark data frame to be filtered
        :type dataFrame: pyspark.sql.dataframe.DataFrame

        :param columnName: Column name in data frame to be filtered
        :type columnName: str
        TODO - Add a check to see if column name is valid, otherwise exit program gracefully

        :return: Filtered data frame
        :rtype: pyspark.sql.dataframe.DataFrame
        """

    if columnName not in dataFrame.columns:
        LOGGER.error(columnName + " does not exist in data frame!")
        raise ValueError

    dataFrame = dataFrame.where(dataFrame[columnName] != 'Undefined')
    dataFrame = dataFrame.where(dataFrame[columnName] != 'null')

    if columnName == "Country":
        # In our specific use case, this is relevant
        # More of a sanity check, if anything
        dataFrame = dataFrame.where(length(dataFrame["Country"]) == 3)

    return dataFrame


def selectCountry(dataFrame, countryCode):
    """
        Returns data frame of company information from a specific country code.

        :param dataFrame: Spark data frame to be filtered
        :type dataFrame: pyspark.sql.dataframe.DataFrame

        :param countryCode: Code of country of interest
        :type countryCode: str
        TODO - Add a check to see if country code is valid, otherwise exit program gracefully

        :return: Filtered data frame
        :rtype: pyspark.sql.dataframe.DataFrame
        """

    dataFrame = dataFrame.where(dataFrame['Country'] == countryCode.upper())
    dataFrame.dropDuplicates(['Company Name'])

    return dataFrame


def removeMostCommonWordsFromColumn(dataFrame, columnName, numberOfWords=3):
    """
        Creates a list of the most common words in a data frame column and uses it to create a custom stop-words
        remover, which is then applied on that same column.

        :param dataFrame: Data frame to be considered
        :type dataFrame: pyspark.sql.dataframe.DataFrame

        :param columnName: Name of the column to be considered
        :type columnName: str
        TODO - Add a check to see if column name is valid, otherwise exit program gracefully

        :param numberOfWords: Number of stop words to be added to stop-words remover
        :type numberOfWords: int

        :return: Processed dataframe
        :rtype: pyspark.sql.dataframe.DataFrame
        """

    # Counts the most common words in a data frame column
    mostCommonWords = dataFrame.withColumn('word', explode(split(dataFrame[columnName], ' '))) \
        .groupBy('word') \
        .count() \
        .sort('count', ascending=False)

    # turns list into a python list
    mostCommonWordArray = [row.word for row in mostCommonWords.collect()][:numberOfWords]

    dataFrame = dataFrame.withColumn("Split Name", split(dataFrame["Company Name"], " "))
    swr = StopWordsRemover(inputCol='Split Name', outputCol='Clean Name', stopWords=mostCommonWordArray)
    dataFrame = swr.transform(dataFrame).select("Company Name", "Country", "Sector", "Industry", "Sub Industry",
                                                "Clean Name")
    dataFrame = dataFrame.withColumn('Clean Name', concat_ws(' ', 'Clean Name'))
    dataFrame = dataFrame.select("Clean Name", "Country", "Sector", "Industry", "Sub Industry")

    return dataFrame


def getCompaniesInSector(dataFrame, sector):
    """
        Simply returns a filtered data frame if its given sector matches the user-inputed sector

        :param dataFrame: Data frame to be considered
        :type dataFrame: pyspark.sql.dataframe.DataFrame

        :param sector: Desired sector to be isolated
        :type sector: str

        :return: Data frame containing only companies of the above sector
        :rtype: pyspark.sql.dataframe.DataFrame
        """
    return dataFrame.where(dataFrame["Sector"] == sector)


def getRandomSample(dataFrame, size):
    """
        Returns random sample of a desired size from a data frame.

        :param dataFrame: Original data framme
        :type dataFrame: pyspark.sql.dataframe.DataFrame

        :param size: Size of the randome sample desired
        :type size: int

        :return: Sample of data frame
        :rtype: pyspark.sql.dataframe.DataFrame
        """
    return dataFrame.select("Clean Name", "Country", "Sector", "Industry", "Sub Industry") \
        .orderBy(rand()). \
        limit(size)


def getListOfColumn(dataFrame, columnName):
    """
    Turns a specified column from a data frame into a python list

    :param dataFrame: Data frame to be considered
    :type dataFrame: pyspark.sql.dataframe.DataFrame

    :param columnName: Name of the column inside dataFrame to be considered
    :type columnName: str

    :return: A list representation of the column
    :rtype: [str]
    """
    return [str(i[columnName]) for i in dataFrame.select(columnName).collect()]


def addDoubleQuotes(columnList):
    """
    Since adding double-quotes to a search query in any Google product affects the results, this function will add
    them around any Python list. Uses regular expressions.

    :param columnList: The list to be modified and returned
    :type columnList: [str]

    :return: Modified list with double-quotes around each of its elements
    :rtype: [str]
    """
    columnList = [elem.lower().strip() for elem in columnList]
    return [("\"" + re.sub(r'[^\w' + REMOVE_LIST + ']', '', elem) + "\"") for elem in columnList]


def removeSpecificKeyword(entry, keyword = None):
    """
    In case a specific word is not included in the stop-words remover and must still be removed, one can use this
    function

    :param entry: String to be considered
    :type entry: str

    :param keyword: Keyword to be searched and removed
    :type keyword: str

    :return: Processed entry
    :rtype: str
    """

    if keyword is not None:
        return entry.replace(keyword, "").strip()
    else:
        return entry


def getListOfCompanyNames(fileName, sizeOfList, country, numberOfStopWordsToRemove, sector):
    """
    Returns a list of companies of a given size from a given country and sector taken from a CSV file. If desired, it
    will also filter a given number of stop-words based on a dictionary created from the data frame itself.

    :param fileName: CSV file address
    :type fileName: str

    :param sizeOfList: Size of list desired
    :type sizeOfList: int

    :param country: Country code where companies are listed
    :type country: str

    :param numberOfStopWordsToRemove: Number of stop words to be added to stop-words remover
    :type numberOfStopWordsToRemove: int

    :param sector: Desired sector to be isolated
    :type sector: str

    :return:
    """
    pySparkSetup()
    dataFrame = getDfFromCSVFile(fileName)
    dataFrame = filterNullEntries(dataFrame, "Country")
    dataFrame = selectCountry(dataFrame, country)
    dataFrame = removeMostCommonWordsFromColumn(dataFrame, "Company Name", numberOfStopWordsToRemove)
    dataFrame = getCompaniesInSector(dataFrame, sector)
    sample = getRandomSample(dataFrame, sizeOfList)
    return addDoubleQuotes(getListOfColumn(sample, "Clean Name"))
