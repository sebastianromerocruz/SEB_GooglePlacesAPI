def convertObjectToDictionary(obj):
    """
    Takes in a custom object and returns a dictionary representation of the object.
    Includes meta data such as object's module and class names.

    Based on: https://medium.com/python-pandemonium/json-the-python-way-91aac95d4041

    :param obj: Object to be turned into a dictionary
    :type obj: object

    :return: Dictionary representation of obj
    """

    # obj meta-data
    objectDictionary = {
        # Uncomment the following two lines if result is to be unpickleable:
    # "__class__": obj.__class__.__name__,
    # "__module__": obj.__module__
    }

    # obj properties
    objectDictionary.update(obj.__dict__)

    return objectDictionary
