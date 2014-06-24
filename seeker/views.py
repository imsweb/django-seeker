from django.views.generic import TemplateView
from django.shortcuts import redirect
from django.contrib import messages
from .query import TermAggregate

class SeekerView (TemplateView):
    mapping = None
    template_name = 'seeker/seeker.html'
    page_param = 'page'
    page_size = 10

    def get_facets(self):
        mapping = self.mapping.instance()
        for name, t in mapping.field_map.iteritems():
            if t.facet:
                yield TermAggregate(name, label=mapping.field_label(name))

    def get_context_data(self, **kwargs):
        filters = {}
        facets = []
        facet_filters = []
        keywords = self.request.GET.get('q', '').strip()
        page = self.request.GET.get(self.page_param, '').strip()
        page = int(page) if page.isdigit() else 1
        sort = self.request.GET.get('sort', None)
        for facet in self.get_facets():
            facets.append(facet)
            if facet.field in self.request.GET:
                terms = self.request.GET.getlist(facet.field)
                filters[facet.field] = set(terms)
                facet_filters.append(facet.filter(terms))
        offset = (page - 1) * self.page_size
        results = self.mapping.instance().query(query=keywords, filters=facet_filters, facets=facets, limit=self.page_size, offset=offset, sort=sort)
        params = self.request.GET.copy()
        querystring = params.urlencode()
        try:
            params.pop('page')
        except:
            pass
        try:
            from .models import SavedSearch
            can_save = True
            current_search = SavedSearch.objects.filter(user=self.request.user, url=self.request.path, querystring=querystring).first()
            saved_searches = SavedSearch.objects.filter(user=self.request.user, url=self.request.path)
        except ImportError:
            can_save = False
            current_search = None
            saved_searches = None
        kwargs.update({
            'results': results,
            'filters': filters,
            'keywords': keywords,
            'request_path': self.request.path,
            'page': page,
            'page_param': self.page_param,
            'page_size': self.page_size,
            'querystring': querystring,
            'can_save': can_save,
            'current_search': current_search,
            'saved_searches': saved_searches,
            'mapping': self.mapping.instance(),
        })
        return kwargs

    def get(self, request, *args, **kwargs):
        try:
            from .models import SavedSearch
            saved = SavedSearch.objects.get(user=request.user, url=request.path, default=True)
            querystring = request.GET.urlencode()
            if querystring == '' and saved.querystring != querystring:
                return redirect(saved)
        except:
            pass
        return super(SeekerView, self).get(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        qs = request.POST.get('querystring', '').strip()
        if '_save' in request.POST:
            name = request.POST.get('name', '').strip()
            if not name:
                messages.error(request, 'You did not provide a name for the saved search. Please try again.')
                return redirect(request.get_full_path())
            default = request.POST.get('default', '').strip() == '1'
            if default:
                request.user.seeker_searches.filter(url=request.path).update(default=False)
            search = request.user.seeker_searches.create(name=name, url=request.path, querystring=qs, default=default)
            messages.success(request, 'Successfully saved "%s".' % search)
            return redirect(search)
        elif '_default' in request.POST:
            request.user.seeker_searches.filter(url=request.path).update(default=False)
            request.user.seeker_searches.filter(url=request.path, querystring=qs).update(default=True)
        elif '_unset' in request.POST:
            request.user.seeker_searches.filter(url=request.path).update(default=False)
        elif '_delete' in request.POST:
            request.user.seeker_searches.filter(url=request.path, querystring=qs).delete()
        return redirect(request.get_full_path())
