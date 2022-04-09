# Exploration of Google Places API

### Usage

Run the driver file, [**example.py**](example.py)

#### _Mac / Linux:_

```commandline
python3 example.py
```

#### _Windows_

```commandline
py example.py
```

---

### Description

The following is a demonstration of the capabilities of the [**Google Places
API**](https://developers.google.com/maps/documentation/places/web-service/overview) when searching for locations of
companies.

### Pipeline

#### _Pre-Processing_

The pipeline acquires a [**sample of company names**](All_comp2019May.csv) from a given country and sector (see
[**PySparkPreProcessing.py**](PySparkPreProcessing.py) for pre-processing details), along with a list of the coordinates
of the [**1,000 largest American
cities**](1000-largest-us-cities-by-population-with-geographic-coordinates.csv), and performs queries for each company
name in each city.

#### _Filtering_

Query result names are first run through a customizable [**fuzzy string filter**](FuzzyStringFilter.py) to measure their
similarity to the actual company name. Results are further filtered to not include duplicates, permanently-closed
locations (which are also returned by the API), and places of irrelevant types (e.g. ``'hindu_temple'`, ``'rv_park'`,
etc.).

#### _Output_

Finally, the results are stored in `JSON` format with the following schema:
```
root
 |
 |-- companyName: string (nullable = true)
 |
 |-- queryResultList: array (nullable = true)
 |
 |    |-- element: struct (containsNull = true)
 |    |    |
 |    |    |-- companyKeyword: string (nullable = true)
 |    |    |
 |    |    |-- geometry: struct (nullable = true)
 |    |    |    |
 |    |    |    |-- lat: double (nullable = true)
 |    |    |    |-- lon: double (nullable = true)
 |    |    |
 |    |    |-- resultName: string (nullable = true)
 |    |    |
 |    |    |-- types: array (nullable = true)
 |    |    |    |
 |    |    |    |-- element: string (containsNull = true)
 |    |    |
 |    |    |-- vicinity: string (nullable = true)
```

I.e., every company's `JSON` object will contain an array of `JSON` objects for every result returned by the Google
Places API query.


### Notes

This program is not meant to be an exhaustive representation of the full capabilities of the Google Places API, but
rather a proof-of-concept exploration of the API's free-tier capabilities and how it could be potentially scaled up.

Completed during the summer of 2019 as part of an internship for Skandinaviska Enskilda Banken (SEB) in New York, New
York.

<sub>© Skandinaviska Enskilda Banken and Sebastián Romero Cruz, 2019</sub>