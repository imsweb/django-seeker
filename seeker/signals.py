from django.dispatch import Signal

search_complete = Signal(providing_args=('context'))
"""
Sent after a search is performed within a SeekerView object
The "sender" will be the instance of the SeekerView that was involved.
The "context" will be the context dictionary passed to the template. 
"""

advanced_search_performed = Signal(providing_args=['request', 'context'])
"""
Sent when an advanced search is executed. The following arguments will be provided:
- sender = The seeker view class that performed the search.
- request = The http request.
- context = The context dictionary that will be used to render this search. 
            Among many other things, this includes 'saved_search' which holds the SavedSearch object if one was used to load this search.
"""