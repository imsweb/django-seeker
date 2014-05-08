from django.apps import apps

def get_app_mappings(app_label):
    seeker_app = apps.get_app_config('seeker')
    return seeker_app.app_mappings.get(app_label, [])

def get_model_mappings(model_class):
    seeker_app = apps.get_app_config('seeker')
    return seeker_app.model_mappings.get(model_class, [])

def get_facet_filters(request_data, facets):
    filters = {}
    facet_filters = []
    for facet in facets:
        if facet.field in request_data:
            terms = request_data.getlist(facet.field)
            filters[facet.field] = set(terms)
            facet_filters.append(facet.filter(terms))
    return filters, facet_filters
