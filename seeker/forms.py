from django.forms import ModelForm

from .models import AdvancedSavedSearch, SavedSearch


class BaseSavedSearchForm(ModelForm):
    def __init__(self, *args, **kwargs):
        # saved_searches is a list of saved search objects that are in the same group that this form will be related to
        # By "same group" we mean are related to the same seeker instance, possibly the same user (depending on if that is enforced, etc.)
        self.saved_searches = kwargs.pop('saved_searches', [])
        self.enforce_unique_name = kwargs.pop('enforce_unique_name', True)
        super(BaseSavedSearchForm, self).__init__(*args, **kwargs)
        
    def save(self, commit=True):
        saved_search = super(BaseSavedSearchForm, self).save(commit=False)
        # We can only have one default in the group, so set the others to false
        # This queryset does not include the saved_search about to be saved
        if saved_search.default:
            self.saved_searches.update(default=False)
        
        # We enforce the naming restrictions here (depending on the 'unique_name_enforcement' setting)
        if self.enforce_unique_name:
            same_name_searches = self.saved_searches.filter(name=saved_search.name).delete()
            
        if commit:
            saved_search.save()
        return saved_search


class SavedSearchForm(BaseSavedSearchForm):
    class Meta:
        model = SavedSearch
        fields = ['name', 'default']

        
class AdvancedSavedSearchForm(BaseSavedSearchForm):
    class Meta:
        model = AdvancedSavedSearch
        fields = ['name', 'url', 'default', 'search_object']
