from django.conf import settings
from django.contrib import messages
from django.http import Http404, JsonResponse, QueryDict, StreamingHttpResponse
from django.shortcuts import redirect, render
from django.template import Context, RequestContext, loader, TemplateDoesNotExist
from django.utils import timezone
from django.utils.encoding import force_text
from django.utils.html import escape
from django.utils.http import urlencode
from django.utils.safestring import mark_safe
from django.views.generic import View
from elasticsearch_dsl.utils import AttrList
from elasticsearch_dsl import Q
import elasticsearch_dsl as dsl
import six

from seeker.templatetags.seeker import seeker_format

from .mapping import DEFAULT_ANALYZER
from .signals import search_complete, advanced_search_performed

import abc
import collections
import inspect
import re
import json
import warnings
from django.http.response import HttpResponseBadRequest, HttpResponseForbidden
from django.views.generic.edit import FormView, CreateView

seekerview_field_templates = {}

class Column (object):
    """
    """

    view = None
    visible = False

    def __init__(self, field, label=None, sort=None, value_format=None, template=None, header=None, export=True, highlight=None):
        self.field = field
        self.label = label if label is not None else field.replace('_', ' ').replace('.raw', '').capitalize()
        self.sort = sort
        self.template = template
        self.value_format = value_format
        self.header_html = escape(self.label) if header is None else header
        self.export = export
        self.highlight = highlight

    def __str__(self):
        return self.label

    def __repr__(self):
        return 'Column(%s)' % self.field

    def bind(self, view, visible):
        self.view = view
        self.visible = visible
        if self.visible:
            if self.template:
                self.template_obj = loader.get_template(self.template)
            else:
                self.template_obj = self.view.get_field_template(self.field)
        return self

    def header(self):
        cls = '%s_%s' % (self.view.document._doc_type.name, self.field.replace('.', '_'))
        if not self.sort:
            return mark_safe('<th class="%s">%s</th>' % (cls, self.header_html))
        q = self.view.request.GET.copy()
        field = q.get('s', '')
        sort = None
        cls += ' sort'
        if field.lstrip('-') == self.field:
            # If the current sort field is this field, give it a class a change direction.
            sort = 'Descending' if field.startswith('-') else 'Ascending'
            cls += ' desc' if field.startswith('-') else ' asc'
            d = '' if field.startswith('-') else '-'
            q['s'] = '%s%s' % (d, self.field)
        else:
            q['s'] = self.field
        next_sort = 'descending' if sort == 'Ascending' else 'ascending'
        sr_label = (' <span class="sr-only">(%s)</span>' % sort) if sort else ''
        html = '<th class="%s"><a href="?%s" title="Click to sort %s" data-sort="%s">%s%s</a></th>' % (cls, q.urlencode(), next_sort, q['s'], self.header_html, sr_label)
        return mark_safe(html)

    def context(self, result, **kwargs):
        return kwargs

    def render(self, result, **kwargs):
        value = getattr(result, self.field, None)
        if self.value_format:
            value = self.value_format(value)
        try:
            if '*' in self.highlight:
                # If highlighting was requested for multiple fields, grab any matching fields as a dictionary.
                r = self.highlight.replace('*', r'\w+').replace('.', r'\.')
                highlight = {f.replace('.', '_'): result.meta.highlight[f] for f in result.meta.highlight if re.match(r, f)}
            else:
                highlight = result.meta.highlight[self.highlight]
        except:
            highlight = []
        params = {
            'result': result,
            'field': self.field,
            'value': value,
            'highlight': highlight,
            'view': self.view,
            'user': self.view.request.user,
            'query': self.view.get_keywords(self.view.request.GET),
        }
        params.update(self.context(result, **kwargs))
        return self.template_obj.render(params)

    def export_value(self, result):
        export_field = self.field if self.export is True else self.export
        if export_field:
            value = getattr(result, export_field, '')
            export_val = ', '.join(force_text(v.to_dict() if hasattr(v, 'to_dict') else v) for v in value) if isinstance(value, AttrList) else seeker_format(value)
        else:
            export_val = ''
        return export_val
    
class SeekerView (View):
    document = None
    """
    A :class:`elasticsearch_dsl.DocType` class to present a view for.
    """

    using = None
    """
    The ES connection alias to use.
    """

    index = None
    """
    The ES index to use. Defaults to the SEEKER_INDEX setting.
    """

    template_name = 'seeker/seeker.html'
    """
    The overall seeker template to render.
    """

    header_template = 'seeker/header.html'
    """
    The template used to render the search results header.
    """

    results_template = 'seeker/results.html'
    """
    The template used to render the search results.
    """

    footer_template = 'seeker/footer.html'
    """
    The template used to render the search results footer.
    """

    columns = None
    """
    A list of Column objects, or strings representing mapping field names. If None, all mapping fields will be available.
    """

    exclude = None
    """
    A list of field names to exclude when generating columns.
    """

    display = None
    """
    A list of field/column names to display by default.
    """

    required_display = []
    """
    A list of tuples, ex. ('field name', 0), representing field/column names that will always be displayed (cannot be hidden by the user).
    The second value is the index/position of the field (used as the index in list.insert(index, 'field name')).
    """

    @property
    def required_display_fields(self):
        return [t[0] for t in self.required_display]

    sort = None
    """
    A list of field/column names to sort by default, or None for no default sort order.
    """

    search = None
    """
    A list of field names to search. By default, will included all fields defined on the document mapping.
    """

    highlight = True
    """
    A list of field names to highlight, or True/False to enable/disable highlighting for all fields.
    """

    highlight_encoder = 'html'
    """
    An 'encoder' parameter is used when highlighting to define how highlighted text will be encoded. It can be either
    'default' (no encoding) or 'html' (will escape html, if you use html highlighting tags).
    """

    facets = []
    """
    A list of :class:`seeker.Facet` objects that are available to facet the results by.
    """

    initial_facets = {}
    """
    A dictionary of initial facets, mapping fields to lists of initial values.
    """

    page_size = 10
    """
    The number of results to show per page.
    """

    page_spread = 7
    """
    The number of pages (not including first and last) to show in the paginator widget.
    """

    can_save = True
    """
    Whether searches for this view can be saved.
    """

    export_name = 'seeker'
    """
    The filename (without extension, which will be .csv) to use when exporting data from this view.
    """

    export_timestamp = False
    """
    Whether or not to append a timestamp of the current time to the export filename when exporting data from this view.
    """

    show_rank = True
    """
    Whether or not to show a Rank column when performing keyword searches.
    """

    field_columns = {}
    """
    A dictionary of field column overrides.
    """

    field_labels = {}
    """
    A dictionary of field label overrides.
    """

    sort_fields = {}
    """
    A dictionary of sort field overrides.
    """

    highlight_fields = {}
    """
    A dictionary of highlight field overrides.
    """

    query_type = getattr(settings, 'SEEKER_QUERY_TYPE', 'query_string')
    """
    The query type to use when performing keyword queries (either 'query_string' (default) or 'simple_query_string').
    """

    operator = getattr(settings, 'SEEKER_DEFAULT_OPERATOR', 'AND')
    """
    The query operator to use by default.
    """

    permission = None
    """
    If specified, a permission to check (using ``request.user.has_perm``) for this view.
    """

    extra_context = {}
    """
    This property is slated to be deprecated in the future. Please use "modify_context".
    Extra context variables to use when rendering. May be passed via as_view(), or overridden as a property.
    """
    
    field_templates = {}
    """
    A dictionary of field template overrides.
    """
    
    _field_templates = {}
    """
    A dictionary of default templates for each field
    """
        
    def modify_context(self, context):
        """
        This function allows modifications to the context that will be used to render the initial seeker page. 
        NOTE: The changes to context should be done in place. This function does not have a return (similar to 'dict.update()').
        """
        pass
    
    def modify_results_context(self, context):
        """
        This function allows modifications to the context that will be used to render the results table. 
        NOTE: The changes to context should be done in place. This function does not have a return (similar to 'dict.update()').
        """
        pass
    
    view_name = None
    """
    An optional name to call this view, used to differentiate two views using the same mapping and class.
    """

    def get_view_name(self):
        """
        Returns the view_name if set, otherwise return the class name and document name.
        """
        if self.view_name:
            return self.view_name
        else:
            return self.__class__.__name__ + self.document._doc_type.name

    def normalized_querystring(self, qs=None, ignore=None):
        """
        Returns a querystring with empty keys removed, keys in sorted order, and values (for keys whose order does not
        matter) in sorted order. Suitable for saving and comparing searches.

        :param qs: (Optional) querystring to use; defaults to request.GET
        :param ignore: (Optional) list of keys to ignore when building the querystring
        """
        data = QueryDict(qs) if qs is not None else self.request.GET
        parts = []
        for key in sorted(data):
            if ignore and key in ignore:
                continue
            if not data[key]:
                continue
            if key == 'p' and data[key] == '1':
                continue
            values = data.getlist(key)
            # Make sure display/facet/sort fields maintain their order. Everything else can be sorted alphabetically for consistency.
            if key not in ('d', 'f', 's'):
                values = sorted(values)
            parts.extend(urlencode({key: val}) for val in values)
        return '&'.join(parts)

    def get_field_label(self, field_name):
        """
        Given a field name, returns a human readable label for the field.
        """
        if field_name.endswith('.raw'):
            field_name = field_name[:-4]
        if field_name in self.field_labels:
            return self.field_labels[field_name]
        try:
            # If the document is a ModelIndex, try to get the verbose_name of the Django field.
            f = self.document.queryset().model._meta.get_field(field_name)
            return f.verbose_name[0].upper() + f.verbose_name[1:]
        except:
            # Otherwise, just make the field name more human-readable.
            return field_name.replace('_', ' ').capitalize()

    def get_field_sort(self, field_name):
        """
        Given a field name, returns the field name that should be used for sorting. If a mapping defines
        a .raw sub-field, that is used, otherwise the field name itself is used if index=not_analyzed.
        """
        if field_name.endswith('.raw'):
            return field_name
        if field_name in self.sort_fields:
            return self.sort_fields[field_name]
        if field_name in self.document._doc_type.mapping:
            dsl_field = self.document._doc_type.mapping[field_name]
            if isinstance(dsl_field, (dsl.Object, dsl.Nested)):
                return None
            if not isinstance(dsl_field, dsl.String):
                return field_name
            if 'raw' in dsl_field.fields:
                return '%s.raw' % field_name
            elif getattr(dsl_field, 'index', None) == 'not_analyzed':
                return field_name
        return None

    def get_field_template(self, field_name):
        """
        Returns the default template instance for the given field name.
        """
        if not self._field_templates:
            try:
                self._field_templates = seekerview_field_templates[self.get_view_name()]
            except KeyError:
                seekerview_field_templates.update({self.get_view_name(): {}})
                self._field_templates = seekerview_field_templates[self.get_view_name()]            
        try:
            return self._field_templates[field_name]
        except KeyError:
            return self._find_field_template(field_name)

    def _find_field_template(self, field_name):
        """
        finds and sets the default template instance for the given field name with the given template.
        """
        search_templates = []
        if field_name in self.field_templates:
            search_templates.append(self.field_templates[field_name])
        for _cls in inspect.getmro(self.document):
            if issubclass(_cls, dsl.DocType):
                search_templates.append('seeker/%s/%s.html' % (_cls._doc_type.name, field_name))
        search_templates.append('seeker/column.html')
        template = loader.select_template(search_templates)
        existing_templates = list(set(self._field_templates.values()))
        for existing_template in existing_templates:
            #If the template object already exists just re-use the existing one.
            if template.template.name == existing_template.template.name:
                template = existing_template
                break
        self._field_templates.update({field_name: template})
        return template

    def get_field_highlight(self, field_name):
        if field_name in self.highlight_fields:
            return self.highlight_fields[field_name]
        if field_name in self.document._doc_type.mapping:
            dsl_field = self.document._doc_type.mapping[field_name]
            if isinstance(dsl_field, (dsl.Object, dsl.Nested)):
                return '%s.*' % field_name
            return field_name
        return None

    def make_column(self, field_name):
        """
        Creates a :class:`seeker.Column` instance for the given field name.
        """
        if field_name in self.field_columns:
            return self.field_columns[field_name]
        label = self.get_field_label(field_name)
        sort = self.get_field_sort(field_name)
        highlight = self.get_field_highlight(field_name)
        return Column(field_name, label=label, sort=sort, highlight=highlight)

    def get_columns(self):
        """
        Returns a list of :class:`seeker.Column` objects based on self.columns, converting any strings.
        """
        columns = []
        if not self.columns:
            # If not specified, all mapping fields will be available.
            for f in self.document._doc_type.mapping:
                if self.exclude and f in self.exclude:
                    continue
                columns.append(self.make_column(f))
        else:
            # Otherwise, go through and convert any strings to Columns.
            for c in self.columns:
                if isinstance(c, six.string_types):
                    if self.exclude and c in self.exclude:
                        continue
                    columns.append(self.make_column(c))
                elif isinstance(c, Column):
                    if self.exclude and c.field in self.exclude:
                        continue
                    columns.append(c)
        # Make sure the columns are bound and ordered based on the display fields (selected or default).
        display = self.get_display()
        visible_columns = []
        non_visible_columns=[]
        for c in columns:
            c.bind(self, c.field in display)
            if c.visible:
                visible_columns.append(c)
            else:
                non_visible_columns.append(c)
        visible_columns.sort(key=lambda  c: display.index(c.field))
        non_visible_columns.sort(key=lambda c: c.label)
        
        return visible_columns + non_visible_columns

    def get_keywords(self, data_dict):
        return data_dict.get('q', '').strip()

    def get_facets(self):
        return list(self.facets) if self.facets else []

    def get_display(self):
        """
        Returns a list of display field names. If the user has selected display fields, those are used, otherwise
        the default list is returned. If no default list is specified, all fields are displayed.
        """
        default = list(self.display) if self.display else list(self.document._doc_type.mapping)
        display_fields = self.request.GET.getlist('d') or default
        display_fields = [f for f in display_fields if f not in self.required_display_fields]
        for field, i in self.required_display:
            display_fields.insert(i, field)
        return display_fields

    def get_saved_search(self):
        """
        Returns the "saved_search" GET parameter if it's in the proper format, otherwise returns None.
        """
        saved_search_vals = [val for val in self.request.GET.getlist('saved_search') if val]
        if len(saved_search_vals) == 1 and saved_search_vals[0].isdigit():
            return saved_search_vals[0]
        return None

    def get_facet_data(self, data_dict, initial=None, exclude=None):
        if initial is None:
            initial = {}
        facets = collections.OrderedDict()
        for f in self.get_facets():
            if f.field != exclude:
                facets[f] = data_dict.getlist(f.field) or initial.get(f.field, [])
        return facets
    
    def get_saved_search_model(self):
        from .models import SavedSearch
        return SavedSearch

    def get_search_fields(self, mapping=None, prefix=''):
        if self.search:
            return self.search
        elif mapping is not None:
            fields = []
            for field_name in mapping:
                if mapping[field_name].to_dict().get('analyzer') == DEFAULT_ANALYZER:
                    fields.append(prefix + field_name)
                if hasattr(mapping[field_name], 'properties'):
                    fields.extend(self.get_search_fields(mapping=mapping[field_name].properties, prefix=prefix + field_name + '.'))
            return fields
        else:
            return self.get_search_fields(mapping=self.document._doc_type.mapping)

    def get_search_query_type(self, search, keywords, analyzer=DEFAULT_ANALYZER):
        kwargs = {'query': keywords,
                  'analyzer': analyzer,
                  'fields': self.get_search_fields(),
                  'default_operator': self.operator}
        if self.query_type == 'simple_query':
            kwargs['auto_generate_phrase_queries'] = True
        return search.query(self.query_type, **kwargs)

    def get_search(self, keywords=None, facets=None, aggregate=True):
        using = self.using or self.document._doc_type.using or 'default'
        index = self.index or self.document._doc_type.index or getattr(settings, 'SEEKER_INDEX', 'seeker')
        # TODO: self.document.search(using=using, index=index) once new version is released
        s = self.document.search().index(index).using(using).extra(track_scores=True)
        if keywords:
            s = self.get_search_query_type(s, keywords)
        if facets:
            for facet, values in facets.items():
                if values:
                    s = facet.filter(s, values)
                if aggregate:
                    facet.apply(s)
        return s

    def render(self):
        SavedSearchModel = self.get_saved_search_model()

        querystring = self.normalized_querystring(ignore=['p', 'saved_search'])

        if self.request.user and self.request.user.is_authenticated and not querystring and not self.request.is_ajax():
            default = self.request.user.seeker_searches.filter(url=self.request.path, default=True).first()
            if default and default.querystring:
                return redirect(default)

        # Figure out if this is a saved search, and grab the current user's saved searches.
        saved_search = None
        if self.request.user and self.request.user.is_authenticated:
            saved_search_pk = self.get_saved_search()
            if saved_search_pk:
                try:
                    saved_search = self.request.user.seeker_searches.get(pk=saved_search_pk, url=self.request.path, querystring=querystring)
                except SavedSearchModel.DoesNotExist:
                    pass
            saved_searches = list(self.request.user.seeker_searches.filter(url=self.request.path))
        else:
            saved_searches = []

        keywords = self.get_keywords(self.request.GET)
        facets = self.get_facet_data(self.request.GET, initial=self.initial_facets if not self.request.is_ajax() else None)
        search = self.get_search(keywords, facets)
        columns = self.get_columns()

        # Make sure we sanitize the sort fields.
        sort_fields = []
        column_lookup = {c.field: c for c in columns}
        sorts = self.request.GET.getlist('s', None)
        if not sorts:
            if keywords:
                sorts = []
            else:
                sorts = self.sort or []
        for s in sorts:
            # Get the column based on the field name, and use it's "sort" field, if applicable.
            c = column_lookup.get(s.lstrip('-'))
            if c and c.sort:
                sort_fields.append('-%s' % c.sort if s.startswith('-') else c.sort)

        # Highlight fields.
        if self.highlight:
            highlight_fields = self.highlight if isinstance(self.highlight, (list, tuple)) else [c.highlight for c in columns if c.visible and c.highlight]
            search = search.highlight(*highlight_fields, number_of_fragments=0).highlight_options(encoder=self.highlight_encoder)

        # Calculate paging information.
        page = self.request.GET.get('p', '').strip()
        page = int(page) if page.isdigit() else 1
        offset = (page - 1) * self.page_size
        results_count = search[0:0].execute().hits.total
        if results_count < offset:
            page = 1
            offset = 0

        # Finally, grab the results.
        results = search.sort(*sort_fields)[offset:offset + self.page_size].execute()

        context_querystring = self.normalized_querystring(ignore=['p'])
        sort = sorts[0] if sorts else None
        context = {
            'document': self.document,
            'keywords': keywords,
            'columns': columns,
            'optional_columns': [c for c in columns if c.field not in self.required_display_fields],
            'display_columns': [c for c in columns if c.visible],
            'facets': facets,
            'selected_facets': self.request.GET.getlist('f') or self.initial_facets.keys(),
            'form_action': self.request.path,
            'results': results,
            'page': page,
            'page_size': self.page_size,
            'page_spread': self.page_spread,
            'sort': sort,
            'querystring': context_querystring,
            'reset_querystring': self.normalized_querystring(ignore=['p', 's', 'saved_search']),
            'show_rank': self.show_rank,
            'export_name': self.export_name,
            'can_save': self.can_save and self.request.user and self.request.user.is_authenticated,
            'header_template': self.header_template,
            'results_template': self.results_template,
            'footer_template': self.footer_template,
            'saved_search': saved_search,
            'saved_searches': saved_searches,
        }

        if self.extra_context:
            context.update(self.extra_context)
            
        self.modify_context(context)

        search_complete.send(sender=self, context=context)
        if self.request.is_ajax():
            return JsonResponse({
                'querystring': context_querystring,
                'page': page,
                'sort': sort,
                'saved_search_pk': saved_search.pk if saved_search else '',
                'table_html': loader.render_to_string(self.results_template, context, request=self.request),
                'facet_data': {facet.field: facet.data(results) for facet in self.get_facets()},
            })
        else:
            return self.render_to_response(context)
        
    def render_to_response(self, context):
        return render(self.request, self.template_name, context)

    def render_facet_query(self):
        keywords = self.get_keywords(self.request.GET)
        facet = {f.field: f for f in self.get_facets()}.get(self.request.GET.get('_facet'))
        if not facet:
            raise Http404()
        # We want to apply all the other facet filters besides the one we're querying.
        facets = self.get_facet_data(self.request.GET, exclude=facet)
        search = self.get_search(keywords, facets, aggregate=False)
        fq = '.*' + self.request.GET.get('_query', '').strip() + '.*'
        facet.apply(search, include={'pattern': fq, 'flags': 'CASE_INSENSITIVE'})
        return JsonResponse(facet.data(search.execute()))

    def export(self):
        """
        A helper method called when ``_export`` is present in ``request.GET``. Returns a ``StreamingHttpResponse``
        that yields CSV data for all matching results.
        """
        keywords = self.get_keywords(self.request.GET)
        facets = self.get_facet_data(self.request.GET)
        search = self.get_search(keywords, facets, aggregate=False)
        columns = self.get_columns()

        def csv_escape(value):
            if isinstance(value, (list, tuple)):
                value = '; '.join(force_text(v) for v in value)
            return '"%s"' % force_text(value).replace('"', '""')

        def csv_generator():
            yield ','.join('"%s"' % c.label for c in columns if c.visible and c.export) + '\n'
            for result in search.scan():
                yield ','.join(csv_escape(c.export_value(result)) for c in columns if c.visible and c.export) + '\n'

        export_timestamp = ('_' + timezone.now().strftime('%m-%d-%Y_%H-%M-%S')) if self.export_timestamp else ''
        export_name = '%s%s.csv' % (self.export_name, export_timestamp)
        resp = StreamingHttpResponse(csv_generator(), content_type='text/csv; charset=utf-8')
        resp['Content-Disposition'] = 'attachment; filename=%s' % export_name
        return resp

    def get(self, request, *args, **kwargs):
        if '_facet' in request.GET:
            return self.render_facet_query()
        elif '_export' in request.GET:
            return self.export()
        else:
            return self.render()

    def post(self, request, *args, **kwargs):
        if not self.can_save:
            return redirect(request.get_full_path())
        post_qs = request.POST.get('querystring', '')
        qs = self.normalized_querystring(post_qs, ignore=['p', 'saved_search'])
        saved_search_pk = request.POST.get('saved_search', '').strip()
        if not saved_search_pk.isdigit():
            saved_search_pk = None
        if '_save' in request.POST:
            name = request.POST.get('name', '').strip()
            if not name:
                messages.error(request, 'You did not provide a name for this search. Please try again.')
                return redirect('%s?%s' % (request.path, post_qs))
            default = request.POST.get('default', '').strip() == '1'
            if default:
                request.user.seeker_searches.filter(url=request.path).update(default=False)
            search_values = {'querystring': qs, 'default': default}
            search, created = request.user.seeker_searches.update_or_create(name=name, url=request.path, defaults=search_values)
            messages.success(request, 'Successfully saved "%s".' % search)
            return redirect(search)
        elif '_default' in request.POST and saved_search_pk:
            request.user.seeker_searches.filter(url=request.path).update(default=False)
            request.user.seeker_searches.filter(pk=saved_search_pk, url=request.path, querystring=qs).update(default=True)
        elif '_unset' in request.POST and saved_search_pk:
            request.user.seeker_searches.filter(url=request.path).update(default=False)
        elif '_delete' in request.POST and saved_search_pk:
            request.user.seeker_searches.filter(pk=saved_search_pk, url=request.path, querystring=qs).delete()
            return redirect(request.path)
        return redirect('%s?%s' % (request.path, post_qs))

    def check_permission(self, request):
        """
        Check to see if the user has permission for this view. This method may optionally return an ``HttpResponse``.
        """
        if self.permission and not request.user.has_perm(self.permission):
            raise Http404

    def dispatch(self, request, *args, **kwargs):
        """
        Overridden to perform permission checking by calling ``self.check_permission``.
        """
        resp = self.check_permission(request)
        if resp is not None:
            return resp
        return super(SeekerView, self).dispatch(request, *args, **kwargs)

class AdvancedColumn (Column):
    def header(self):
        cls = '%s_%s' % (self.view.document._doc_type.name, self.field.replace('.', '_'))
        if not self.sort:
            return mark_safe('<th class="%s">%s</th>' % (cls, self.header_html))
        current_sort = self.view.search_object['sort']
        sort = None
        cls += ' sort'
        if current_sort.lstrip('-') == self.field:
            # If the current sort field is this field, give it a class a change direction.
            sort = 'Descending' if current_sort.startswith('-') else 'Ascending'
            cls += ' desc' if current_sort.startswith('-') else ' asc'
            d = '' if current_sort.startswith('-') else '-'
            data_sort = '{}{}'.format(d, self.field)
        else:
            data_sort = self.field
        next_sort = 'descending' if sort == 'Ascending' else 'ascending'
        sr_label = (' <span class="sr-only">(%s)</span>' % sort) if sort else ''
        html = '<th class="{}"><a href="#" title="Click to sort {}" data-sort="{}">{}{}</a></th>'.format(cls, next_sort, data_sort, self.header_html, sr_label)
        return mark_safe(html)

class AdvancedSeekerView (SeekerView):
    boolean_translations = {
        'AND': 'must',
        'OR': 'should'
    }
    """
    This dictionary translates the boolean operators passed from the frontend into their elasticsearch equivalents.
    """
    
    footer_template = 'seeker/footer.html'
    """
    The template used to render the search results footer.
    """

    header_template = 'seeker/header.html'
    """
    The template used to render the search results header.
    """
    
    results_template = 'seeker/results.html'
    """
    The template used to render the search results.
    """
    
    template_name = 'seeker/seeker.html'
    """
    The overall seeker template to render.
    """
    
    @abc.abstractproperty
    def save_search_url(self):
        pass
    """
    This property should return the url of the associated AdvancedSavedSearchView.
    It is set to abstract because it needs to be defined on a site by site basis. None is a valid value if saved searches are not being used.
    """
    
    @abc.abstractproperty
    def search_url(self):
        pass
    """
    This property should return the url of this seeker view.
    It is set to abstract because it needs to be defined on a site by site basis.
    Generally, this will be a 'reverse' call to the URL associated with this view.
    """
    
    sort = ''
    """
    Default field to sort by. Prepend '-' to reverse sort.
    """
    
    add_facets_to_display = True
    """
    Facet fields with selected values will automatically be added to the 'display' list (shown on the results table).
    """
    
    def __init__(self):
        if getattr(SeekerView, 'get_search_query_type').__func__ != getattr(self, 'get_search_query_type').__func__:
            warnings.warn(
                "'get_search_query_type' function is deprecated, please use 'get_keyword_query' instead.",
                DeprecationWarning
            )
    
    def modify_json_response(self, json_response, context):
        """
        This function allows modifications to the json data that will be returned when rendering results.
        The context used to render the results template is passed in as a convenience.
        NOTE: The changes to context should be done in place. This function does not have a return (similar to 'dict.update()').
        """
        pass
        
    def get_columns(self, display):
        """
        Returns a list of :class:`seeker.Column` objects based on self.columns, converting any strings.
        """
        columns = []
        if not self.columns:
            # If not specified, all mapping fields will be available.
            for f in self.document._doc_type.mapping:
                if self.exclude and f in self.exclude:
                    continue
                columns.append(self.make_column(f))
        else:
            # Otherwise, go through and convert any strings to Columns.
            for c in self.columns:
                if isinstance(c, six.string_types):
                    if self.exclude and c in self.exclude:
                        continue
                    columns.append(self.make_column(c))
                elif isinstance(c, Column):
                    if self.exclude and c.field in self.exclude:
                        continue
                    columns.append(c)
        # Make sure the columns are bound and ordered based on the display fields (selected or default).
        visible_columns = []
        non_visible_columns=[]
        for c in columns:
            c.bind(self, c.field in display)
            if c.visible:
                visible_columns.append(c)
            else:
                non_visible_columns.append(c)
        visible_columns.sort(key=lambda  c: display.index(c.field))
        non_visible_columns.sort(key=lambda c: c.label)
        
        return visible_columns + non_visible_columns

    def get_display(self, display_list, facets_searched):
        """
        Returns a list of display field names. If the user has selected display fields and display_list is not empty those are used otherwise
        the default list is returned. If no default list is specified, all fields are displayed.
        """
        default = list(self.display) if self.display else list(self.document._doc_type.mapping)
        display_list = display_list or default
        
        if self.add_facets_to_display:
            for field in facets_searched:
                if field not in display_list + self.required_display:
                    display_list.append(field)
                    
        display_fields = [f for f in display_list if f not in self.required_display_fields]
        for field, i in self.required_display:
            display_fields.insert(i, field)
        return display_fields
    
#     def get_search_query_type(self, search, keywords, analyzer=DEFAULT_ANALYZER):
#         """
#         This function is deprecated. Please use 'get_keyword_query' directly.
#         """
#         return search.query(self.get_keyword_query(keywords, analyzer))
#     
#     def get_keyword_query(self, keywords, analyzer=DEFAULT_ANALYZER):
#         kwargs = {'query': keywords,
#                   'analyzer': analyzer,
#                   'fields': self.get_search_fields(),
#                   'default_operator': self.operator}
#         if self.query_type == 'simple_query':
#             kwargs['auto_generate_phrase_queries'] = True
#         return Q(self.query_type, **kwargs)

    def get_search(self, keywords=None, facets=None, aggregate=True):
        s = self.get_dsl_search()
        if keywords:
            # TODO - Once 'get_search_query_type' is removed this can be cleaned up to:
            # s.query(self.get_keyword_query(keywords))
            s = self.get_search_query_type(s, keywords)
        if facets:
            for facet, values in facets.items():
                if values:
                    s = facet.filter(s, values)
                if aggregate:
                    facet.apply(s)
        return s
    
    def get_dsl_search(self):
        using = self.using or self.document._doc_type.using or 'default'
        index = self.index or self.document._doc_type.index or getattr(settings, 'SEEKER_INDEX', 'seeker')
        # TODO: self.document.search(using=using, index=index) once new version is released
        return self.document.search().index(index).using(using).extra(track_scores=True)
    
    def make_column(self, field_name):
        """
        Creates a :class:`seeker.Column` instance for the given field name.
        """
        if field_name in self.field_columns:
            return self.field_columns[field_name]
        label = self.get_field_label(field_name)
        sort = self.get_field_sort(field_name)
        highlight = self.get_field_highlight(field_name)
        return AdvancedColumn(field_name, label=label, sort=sort, highlight=highlight)
    
    def get_sort_field(self, columns, sort, display):
        """
        Returns the appropriate sort field for a given sort value.
        """
        # Make sure we sanitize the sort fields.
        column_lookup = { c.field: c for c in columns }
        # Order of precedence for sort is: parameter, the default from the view, and then the first displayed column (if any are displayed)
        sort = sort or self.sort or display[0] if len(display) else ''
        # Get the column based on the field name, and use it's "sort" field, if applicable.
        c = column_lookup.get(sort.lstrip('-'))
        if c and c.sort:
            return '-{}'.format(c.sort) if sort.startswith('-') else c.sort
        return sort
        
    def get(self, request, *args, **kwargs):
        facets = self.get_facets()
        context = {
            'can_save': self.can_save and self.request.user and self.request.user.is_authenticated(),
            'facets': facets,
#             'selected_facets': self.initial_facets.keys(),
            'search_url': self.search_url,
            'save_search_url': self.save_search_url
        }
         
        if self.extra_context:
            context.update(self.extra_context)
             
        self.modify_context(context)
        return self.render_to_response(context)
    
    def post(self, request, *args, **kwargs):
        """
        Parameters:
            'search_object' - A json string representing a dictionary in the following format:
                {
                    'query': (dictionary) The boolean query to be performed (see 'advanced_search' for format),
                    'keywords': (string) Used to search the specified fields (see 'search' attr). NOTE: This is SUBTRACTIVE ONLY!
                    'page': (integer) The page of the search,
                    'sort': (string) The field name to be used when sorting (prepend '-' for reverse),
                    'display': (list) The string field names to be displayed in the result table. NOTE: This is ordered!
                }
            '_export' - If present, the return value will be a CSV file of the results.
        NOTE: The 'search_object' parameter key values listed here are the only fields that are required.
              Since it will be passed back in the response extra values can be added to give the site context as to what search is being loaded.
        """
        if request.is_ajax():
            try:
                string_search_object = request.POST.get('search_object')
                # We attach this to self so AdvancedColumn can have access to it
                self.search_object = json.loads(string_search_object)
            except KeyError:
                return HttpResponseBadRequest("No 'search_object' found in the request data.")
            except ValueError:
                return HttpResponseBadRequest("Improperly formatted 'search_object', json.loads failed.")
            export = request.POST.get('_export', False)
            
            # Sanity check that the search object has all of it's required components
            if not all(k in self.search_object for k in ('query', 'keywords', 'page', 'sort', 'display')):
                return HttpResponseBadRequest("The 'search_object' is not in the proper format.")
            return self.render_results(export)
        else:
            return HttpResponseBadRequest("This endpoint only accepts AJAX requests.")
    
    def render_results(self, export):
        facets = self.get_facets()
        facet_lookup = { facet.field: facet for facet in facets }
        search = self.get_dsl_search()
        query = self.search_object.get('query')
        
        # Hook to allow the search to be filtered before seeker begins it's work
        self.additional_query_filters(search)
        
        # Hook to allow the search to be aggregated
        self.apply_aggregations(search, query, facet_lookup)
        
        # Build the actual query that will be applied via post_filter
        advanced_query, facets_searched = self.build_query(query, facet_lookup)
        
        # If there are any keywords passed in, we combine the advanced query with the keyword query
        keywords = self.search_object['keywords'].strip()
        if keywords:
            search = search.query(self.get_search_query_type(keywords))
            
        # We use post_filter to allow the aggregations to be run before applying the filter
        search = search.post_filter(advanced_query)
         
        page, offset = self.calculate_page_and_offset(self.search_object['page'], search)
        
        display = self.get_display(self.search_object['display'], facets_searched)
        columns = self.get_columns(display)
        if export:
            return self.export(search, columns)
        
        # Highlight fields.
        if self.highlight:
            highlight_fields = self.highlight if isinstance(self.highlight, (list, tuple)) else [c.highlight for c in columns if c.visible and c.highlight]
            search = search.highlight(*highlight_fields, number_of_fragments=0).highlight_options(encoder=self.highlight_encoder)
        
        # Finally, grab the results.
        sort = self.get_sort_field(columns, self.search_object['sort'], display)
        if sort:
            results = search.sort(sort)[offset:offset + self.page_size].execute()
        else:
            results = search[offset:offset + self.page_size].execute()
            
        # TODO clean this up (may not need everything)
        context = {
            'columns': columns,
            'display_columns': [c for c in columns if c.visible],
            'facet_lookup': facet_lookup,
            'facets_searched': facets_searched,
            'footer_template': self.footer_template,
            'header_template': self.header_template,
            'optional_columns': sorted([c for c in columns if c.field not in self.required_display_fields], key=lambda col: col.label),
            'page': page,
            'page_spread': self.page_spread,
            'page_size': self.page_size,
            'query': query,
            'results': results,
            'show_rank': self.show_rank,
            'sort': sort,
        }
        self.modify_results_context(context)
            
        advanced_search_performed.send(sender=self.__class__, request=self.request, context=context)
        json_response = {
            'filters': [facet.build_filter_dict(results) for facet in facets], # Relies on the default 'apply_aggregations' being applied.
            'table_html': loader.render_to_string(self.results_template, context, request=self.request),
            'search_object': self.search_object
        }
        self.modify_json_response(json_response, context)
        return JsonResponse(json_response)
        
    def calculate_page_and_offset(self, page, search):
        offset = (page - 1) * self.page_size
        results_count = search[0:0].execute().hits.total
        if results_count < offset:
            page = 1
            offset = 0
        return page, offset
    
    def apply_aggregations(self, search, query, facet_lookup):
        """
        Applies the desired aggregations to the search.
        By default this function applies each facet individually.
        NOTE: This function makes the modification of the search object in place, there is no return value.
        NOTE: It is recommended that any function overwriting this should call super (or replicate the aggregations done here).
              If that doesn't happen then the 'filters' dictionary may not be build appropriately.
        """
        for facet in facet_lookup.values():
            facet.apply(search)
    
    def additional_query_filters(self, search):
        """
        Allows additional search filters (Q objects) to be applied to the search.
        Ideally, filters applied by this function will be applied for every search.
        For that reason nothing is passed to this function except the search.
        NOTE: This function makes the modification of the search object in place, there is no return value.
        """
        pass
        
    def build_query(self, advanced_query, facet_lookup, excluded_facets=[]):
        """
        Returns two values:
        1) The ES DSL Q object representing the 'advanced_query' dictionary passed in
        2) A list of the selected fields for this query
        
        The advanced_query is a dictionary representation of the advanced query. The following is an example of the accepted format:
        {
            "condition": "<boolean operator>",
            "rules": [
                {
                    "id": "<elasticsearch field id>",
                    "operator": "<comparison operator>",
                    "value": "<search value>"
                },
                {
                    "condition": "<boolean operator>",
                    "rules": [
                        {
                            "id": "<elasticsearch field id>",
                            "operator": "<comparison operator>",
                            "value": "<search value>"
                        }, ...
                    ],
                    "not": <flag to negate sibling rules>
                }, ...
            ],
            "not": <flag to negate sibling rules>
        }
         
        NOTES:
        Each 'rule' is a dictionary containing single rules and groups of rules. The value for each rule field are as follows:
            - id:     The name of the field in the elasticsearch document being searched.
            - operator:  A key in COMPARISON_CONVERSION dictionary. It is up to you to ensure the operator will work with the given field.
            - value:     The value to be used in the comparison for this rule
        Each group of rules will have:
            - condition: The boolean operator to apply to all rules in this group.
            - rules: A list of dictionaries containing either groups or rules.
            - not: A boolean (true/false) to indicate if this group should be additive or subtractive to the search.
        """
        # Check if all required keys are present for an individual rule
        if all(k in advanced_query for k in ('id', 'operator', 'value')):
            if advanced_query['id'] not in excluded_facets:
                facet = facet_lookup.get(advanced_query['id'])
                return facet.es_query(advanced_query['operator'], advanced_query['value']), [facet.field]
            return None, None
        
        # Check if all required keys are present for a group   
        elif all(k in advanced_query for k in ('condition', 'rules')):
            group_operator = self.boolean_translations.get(advanced_query.get('condition'), None)
            if not group_operator:
                raise ValueError(u"'{}' is not a valid boolean operator.".format(v))
            
            queries = []
            selected_facets = []
            # The central portion of the recursion, we iterate over all rules inside this group
            for dict in advanced_query.get('rules'):
                query, facet_field = self.build_query(dict, facet_lookup, excluded_facets)
                if query:
                    queries.append(query)
                    selected_facets += facet_field
                
            if advanced_query.get('not', False):
                return ~Q('bool', **{group_operator: queries}), list(set(selected_facets))
            else:
                return Q('bool', **{group_operator: queries}), list(set(selected_facets))
            
        # The advanced_query must have been missing something, so we cannot create this query
        else:
            raise ValueError(u"The dictionary passed in did not have the proper structure. Dictionary: {}".format(str(advanced_query)))

    def export(self, search, columns):
        """
        A helper method called when ``_export`` is present in the http request. Returns a ``StreamingHttpResponse``
        that yields CSV data for all matching results.
        """
        def csv_escape(value):
            if isinstance(value, (list, tuple)):
                value = '; '.join(force_text(v) for v in value)
            return '"%s"' % force_text(value).replace('"', '""')

        def csv_generator():
            yield ','.join('"%s"' % c.label for c in columns if c.visible and c.export) + '\n'
            for result in search.scan():
                yield ','.join(csv_escape(c.export_value(result)) for c in columns if c.visible and c.export) + '\n'

        export_timestamp = ('_' + timezone.now().strftime('%m-%d-%Y_%H-%M-%S')) if self.export_timestamp else ''
        export_name = '%s%s.csv' % (self.export_name, export_timestamp)
        resp = StreamingHttpResponse(csv_generator(), content_type='text/csv; charset=utf-8')
        resp['Content-Disposition'] = 'attachment; filename=%s' % export_name
        return resp

class SearchFailed(Exception):
    """ Thrown when a search fails """
    pass

class AdvancedSavedSearchView(View):
    pk_parameter = 'saved_search_pk'
    """
    The parameter to check for to get the saved search id (either via URL or request GET/POST)
    """
    
    url_parameter = 'url'
    """
    The parameter to check for to get the url associated with the desired saved searches.
    """
    
    restrict_to_user = True
    """
    If users should only be able to view their own saved searches.
    """
    
    form_template = 'advanced_seeker/save_form.html'
    """
    The form template used to display the save search form.
    """
    
    unique_name_enforcement = 'delete'
    """
    The system will enforce the unique name requirement. How it is enforced depends on the value of this field.
    The three options are:
        - 'delete': All previously existing saved searches with the same name as the new one will be deleted.
        - 'error': If a previously existing saved search shares the same name as the new one an error will be thrown.
        - None: This enforcement is ignored, no restrictions on saved search names.
    """
        
    def get(self, request, *args, **kwargs):
        if self.request.is_ajax():
            try:
                url = request.GET.get(self.url_parameter)
            except KeyError:
                return JsonResponse({ 'error': 'No URL provided.' }, 400)
            
            SavedSearchModel = self.get_saved_search_model()
            filter_kwargs = { 'url': url }
            if self.restrict_to_user:
                filter_kwargs['user'] = request.user
            saved_searches = SavedSearchModel.objects.filter(**filter_kwargs).all()
            
            search_pk = kwargs.get(self.pk_parameter, request.GET.get(self.pk_parameter, None))
            if search_pk:
                try:
                    saved_search = saved_searches.get(pk=search_pk)
                except SavedSearchModel.DoesNotExist:
                    return JsonResponse({ 'error': 'Saved search not found.' }, 400)
            else:
                # By design this will return None if there are no default searches found
                saved_search = saved_searches.filter(default=True).first()
                
            SavedSearchForm = self.get_saved_search_form()
                
            data = { 
                'current_search': None,
            }
            if saved_search:
                data.update({ 'current_search': saved_search.get_details_dict() })
            if saved_searches:
                # We don't need to manually include the current search (like we do in post)
                # because nothing is being modified (it should already be in there)
                data.update({ 'all_searches': self.sort_searches([saved_search.get_details_dict() for saved_search in saved_searches]) })
                
            self.update_GET_response_data(data, saved_search)  
            
            # Binding the saved search to a form causes issues with the saved_search object (removes string data)
            # So we do this at the very end
            # TODO - figure out why the data is removed from the object itself when binding it to a form
            form_kwargs = {}
            if saved_search:
                form_kwargs['instance'] = saved_search
            form = SavedSearchForm(request.GET, **form_kwargs)
            data['form_html'] = loader.render_to_string(self.form_template, { 'form': form }, request=self.request)
            return JsonResponse(data)
        else:
            return HttpResponseBadRequest("This endpoint only accepts AJAX requests.")
        
    def post(self, request, *args, **kwargs):
        if self.request.is_ajax():
            try:
                url = request.POST.get(self.url_parameter)
            except KeyError:
                return JsonResponse({'error': 'No URL provided.'}, 400)
            
            form_kwargs = {}
            search_pk = kwargs.get(self.pk_parameter, request.POST.get(self.pk_parameter, None))
            SavedSearchModel = self.get_saved_search_model()
            filter_kwargs = { 'url': url }
            if self.restrict_to_user:
                filter_kwargs['user'] = request.user
            saved_searches = SavedSearchModel.objects.filter(**filter_kwargs)
            
            # These are used to determine alternate paths
            delete = request.POST.get('_delete', False)
            modify_default = request.POST.get('modify_default', '')
            form_kwargs = {}
            
            if search_pk:
                try:
                    instance = saved_searches.get(pk=search_pk)
                    if delete:
                        instance.delete()
                    else: 
                        # This will be used to build the form =
                        # We put it in KWARGS so it doesn't have to be passed in for a new object (no search_pk)
                        form_kwargs['instance'] = instance
                except SavedSearchModel.DoesNotExist:
                    # If we are deleting the search anyway, we can pass if it wasn't found.
                    # Otherwise (not deleting), throw the exception
                    if not delete:
                        return JsonResponse({'error': 'Saved search not found.'}, 400)
            
            # Collect all the saved searches information here
            all_searches = [search.get_details_dict() for search in saved_searches]
            data = {}

            # We have three paths: delete, modify_default, or save
            status = 200
            if delete:
                saved_search = None
            elif modify_default:
                if modify_default == 'set':
                    instance.default = True
                    saved_searches.update(default = False)
                    instance.save()
                    saved_search = instance
                elif modify_default == 'unset':
                    instance.default = False
                    instance.save()
                    saved_search = instance
                else:
                    data["error"] = 'Invalid value for "modify_default" field'
                    saved_search = None
                    status = 400
            else:
                SavedSearchForm = self.get_saved_search_form()
                form = SavedSearchForm(request.POST, **form_kwargs)
                if form.is_valid():
                    saved_search = form.save(commit=False)
                    saved_search.user = request.user
                    
                    # We can only have one default in the group, so set the others to false
                    # This queryset does not include the saved_search about to be saved
                    if saved_search.default:
                        saved_searches.update(default=False)
                    
                    # We enforce the naming restrictions here (depending on the setting)
                    same_name_searches = saved_searches.filter(name=saved_search.name)
                    if self.unique_name_enforcement == 'delete' and same_name_searches.count():
                        same_name_searches.delete()
                    if self.unique_name_enforcement == 'error' and same_name_searches.count():
                        return JsonResponse({'error': 'A search already exists with the name provided.'}, 400)
                    
                    saved_search.save()
                    current_search = saved_search.get_details_dict()
                    
                    # Pass the current search details along to be returned
                    # This happens both directly and in the list of all searches
                    all_searches.append(current_search)
                    data['current_search'] = current_search
                else:
                    # This error message is likely redundant
                    data["error"] = 'Invalid form submitted'
                    saved_search = None
                    status = 400
                # We add the form here because we want to return it rendered even if the form was not valid
                data['form_html'] = loader.render_to_string(self.form_template, { 'form': form }, request=self.request)
            
            data['all_searches'] = self.sort_searches(all_searches)
            
            self.update_POST_response_data(data, saved_search)
            return JsonResponse(data, status=status)
        else:
            return HttpResponseBadRequest("This endpoint only accepts AJAX requests.")
    
    def update_GET_response_data(self, data, saved_search=None):
        """
        This function allows modifications to the json data that will be returned with a GET request.
        The 'saved_search' being loaded may be passed in for convenience (it may be None).
        NOTE: The changes to data should be done in place. This function does not have a return (similar to 'dict.update()').
        """
        pass
    
    def update_POST_response_data(self, data, saved_search=None):
        """
        This function allows modifications to the json data that will be returned with a POST request.
        The 'saved_search' being loaded may be passed in for convenience (it may be None).
        NOTE: The changes to data should be done in place. This function does not have a return (similar to 'dict.update()').
        """
        pass
    
    def sort_searches(self, all_searches):
        """
        This function sorts the list of searches that will be returned.
        """
        return sorted(all_searches, key=lambda search: search.get('name'))
                
    def get_saved_search_model(self):
        from .models import SavedSearch
        return SavedSearch
    
    def get_saved_search_form(self):
        from .forms import AdvancedSavedSearchForm
        return AdvancedSavedSearchForm