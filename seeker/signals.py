from django.dispatch import Signal

# TODO - let's rename this and make the sender the objects class (not instance)
search_complete = Signal()
"""
Sent after a search is performed within a SeekerView object
The "sender" will be the instance of the SeekerView that was involved.
The "context" will be the context dictionary passed to the template. 
"""

advanced_search_performed = Signal()
"""
Sent when an advanced search is executed. The following arguments will be provided:
- sender = The seeker view class that performed the search.
- request = The http request.
- context = The context dictionary that will be used to render this search.
- json_response = The json dictionary that will be returned as the response. 
                  This will include 'search_object' which defines the search that was performed.
"""
