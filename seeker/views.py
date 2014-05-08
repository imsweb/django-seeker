from django.views.generic import TemplateView
from .query import TermAggregate

class SeekerView (TemplateView):
    mapping = None
    template_name = 'seeker/seeker.html'
    page_param = 'page'
    page_size = 10

    def get_facets(self):
        for name, t in self.mapping.instance().field_map.items():
            if t.facet:
                try:
                    f = self.mapping.model._meta.get_field(name)
                    yield TermAggregate(name, label=f.verbose_name)
                except:
                    yield TermAggregate(name)

    def get_context_data(self, **kwargs):
        filters = {}
        facets = []
        facet_filters = []
        keywords = self.request.GET.get('q', '').strip()
        page = self.request.GET.get(self.page_param, '').strip()
        page = int(page) if page.isdigit() else 1
        for facet in self.get_facets():
            facets.append(facet)
            if facet.field in self.request.GET:
                terms = self.request.GET.getlist(facet.field)
                filters[facet.field] = set(terms)
                facet_filters.append(facet.filter(terms))
        offset = (page - 1) * self.page_size
        results = self.mapping.instance().query(query=keywords, filters=facet_filters, facets=facets, limit=self.page_size, offset=offset)
        params = self.request.GET.copy()
        try:
            params.pop('page')
        except:
            pass
        kwargs.update({
            'results': results,
            'filters': filters,
            'keywords': keywords,
            'request_path': self.request.path,
            'page': page,
            'page_param': self.page_param,
            'page_size': self.page_size,
            'querystring': params.urlencode(),
        })
        return kwargs
