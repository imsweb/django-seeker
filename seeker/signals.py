from django.dispatch import Signal


search_complete = Signal(providing_args=('context'))
"""
Sent after a search is performed within a SeekerView object
The "sender" will be the instance of the SeekerView that was involved.
The "context" will be the context dictionary passed to the template. 
"""
