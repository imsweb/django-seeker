from django.forms import ModelForm
from .models import AdvancedSavedSearch

class AdvancedSavedSearchForm(ModelForm):
    class Meta:
        model = AdvancedSavedSearch
        fields = ['name', 'url', 'default', 'search_object']