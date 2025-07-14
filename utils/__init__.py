from os import getenv
from .bookmarks import read_bookmark_file, save_bookmark_file
from .transformation import filter_keys_inplace, replace_dots_in_column_names, lowercase_keys, standardize_columns

event_columns = [
    "name",
    "type",
    "id",
    "locale",
    "sales",
    "dates",
    "info",
    "classifications",
    "promoter",
    "promoters",
    "priceRanges",
    "products",
    "accessibility",
    "location",
    "units",
    "description",
    "ageRestrictions",
    "ticketing",
    "place",
    "_embedded",
]

default_configuration = {
        "apiKey": getenv("API_KEY"),
        "events": {
            "startDateTime": "2020-01-01T00:00:00Z",
            "size": 200,
            "sort": "date,asc"
        }
    }

