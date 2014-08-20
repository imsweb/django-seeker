from django.views.generic import TemplateView
from django.shortcuts import redirect
from django.contrib import messages
from django.http import StreamingHttpResponse
from .query import TermAggregate
from .utils import get_facet_filters
from elasticsearch.helpers import scan

class SeekerView (TemplateView):
    mapping = None
    template_name = 'seeker/seeker.html'
    page_param = 'page'
    page_size = 10
    display = None
    links = None
    export_name = 'seeker'

    def get_facets(self):
        mapping = self.mapping.instance()
        for name, t in mapping.field_map.iteritems():
            if t.facet:
                yield TermAggregate(name, label=mapping.field_label(name))

    def get_default_fields(self):
        if self.display:
            return self.display
        mapping = self.mapping.instance()
        return mapping.field_map.keys()

    def get_url(self, result, field_name):
        try:
            return result.instance.get_absolute_url()
        except:
            return ''

    def get_context_data(self, **kwargs):
        keywords = self.request.GET.get('q', '').strip()
        display_fields = self.request.GET.getlist('d') or self.get_default_fields()
        page = self.request.GET.get(self.page_param, '').strip()
        page = int(page) if page.isdigit() else 1
        sort = self.request.GET.get('sort', None)

        facets = list(self.get_facets())
        filters, facet_filters = get_facet_filters(self.request.GET, facets)

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
        params = super(SeekerView, self).get_context_data(**kwargs)
        params.update({
            'results': results,
            'filters': filters,
            'display_fields': display_fields,
            'link_fields': self.links or (display_fields[0],),
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
        return params

    def export(self, request):
        mapping = self.mapping.instance()
        keywords = self.request.GET.get('q', '').strip()
        display_fields = self.request.GET.getlist('d') or self.get_default_fields()
        sort = self.request.GET.get('sort', None)
        facet_filters = get_facet_filters(self.request.GET, self.get_facets())[1]
        query = mapping.query(query=keywords, filters=facet_filters, sort=sort).to_elastic()

        def csv_escape(value):
            if isinstance(value, (list, tuple)):
                value = '; '.join(unicode(v) for v in value)
            return '"%s"' % unicode(value).replace('"', '""')

        def csv_generator():
            yield ','.join(mapping.field_label(f) for f in display_fields) + '\n'
            for result in scan(mapping.es, index=mapping.index_name, doc_type=mapping.doc_type, query=query):
                yield ','.join(csv_escape(result['_source'].get(f, '')) for f in display_fields) + '\n'

        resp = StreamingHttpResponse(csv_generator(), content_type='text/csv')
        resp['Content-Disposition'] = 'attachment; filename=%s.csv' % self.export_name
        return resp

    def get(self, request, *args, **kwargs):
        if '_export' in request.GET:
            return self.export(request)
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
