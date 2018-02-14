from django.dispatch import Signal

saved_search_modified = Signal(providing_args=['request', 'saved_search', 'changes'])
"""
Sent when a SavedSearch object is modified. The following arguments will be provided:
- sender = The seeker view class that performed the updated.
- request = The http request.
- saved_search = The SavedSearch object that was updated.
- changes = A list of dictionaries in the following format:
    [{ 'field': <SavedSearch field>,
       'old_value': <value it was>,
       'new_value': <value it changed to> },]
"""

search_performed = Signal(providing_args=['request', 'context'])
"""
Sent when a search is executed. The following arguments will be provided:
- sender = The seeker view class that performed the search.
- request = The http request.
- context = The context dictionary that will be used to render this search. 
            Among many other things, this includes 'saved_search' which holds the SavedSearch object if one was used to load this search.
"""

search_saved = Signal(providing_args=['request', 'saved_search'])
"""
Sent when a search is saved. The following arguments will be provided:
- sender = The seeker view class that saved the search.
- request = The http request.
- saved_search = The SavedSearch object used to perform the search. This is None for querystring searches.
"""