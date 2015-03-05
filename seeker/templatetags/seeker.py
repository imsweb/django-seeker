from django import template
from django.utils.html import escape
from django.http import QueryDict
from django.conf import settings
from django.utils import dateformat
from django.core.paginator import Paginator
from django.contrib.humanize.templatetags.humanize import intcomma
import datetime
import string

register = template.Library()

@register.inclusion_tag('seeker/facet.html')
def render_facet(facet, results, facet_fields=None):
    values = []
    for data in facet.values(results):
        key = facet.get_key(data)
        count = data['doc_count']
        values.append((key, count))
    return {
        'facet': facet,
        'values': values,
        'checked': facet_fields and facet.field in facet_fields,
    }

@register.simple_tag
def facet_values(facet, filters, missing='MISSING', remove='&times;'):
    html = '<ul class="list-unstyled facet-values">'
    for term in filters.get(facet.field, []):
        if not term:
            term = missing
        html += '<li><a class="remove" data-term="%(term)s" title="Remove this term">%(remove)s</a> %(term)s</li>' % {'term': escape(term), 'remove': remove}
    html += '</ul>'
    return html

@register.filter
def list_display(values, sep=', '):
    return sep.join(unicode(v) for v in values)

@register.simple_tag
def sort_link(sort_by, label=None, querystring='', name='sort', document=None):
    q = QueryDict(querystring).copy()
    field = q.get(name, '')
    direction = 'asc'
    if field.startswith('-'):
        field = field[1:]
        direction = 'desc'
    d = '' if direction == 'desc' or field != sort_by else '-'
    q[name] = '%s%s' % (d, sort_by)
    if label is None:
        if document:
            label = document.label_for_field(sort_by)
        else:
            label = string.capwords(sort_by.replace('.raw', '').replace('_', ' '))
    return '<a href="?%s" class="sort %s">%s</a>' % (q.urlencode(), direction, escape(label))

@register.simple_tag
def field_label(mapping, field_name):
    return mapping.field_label(field_name)

@register.simple_tag
def result_value(result, field_name):
    value = getattr(result, field_name, None)
    if value is None:
        return ''
    if isinstance(value, (list, tuple)):
        return list_display(value)
    if isinstance(value, datetime.datetime):
        return dateformat.format(value, settings.DATETIME_FORMAT)
    if isinstance(value, datetime.date):
        return dateformat.format(value, settings.DATE_FORMAT)
    return value

@register.simple_tag
def result_link(result, field_name, view=None):
    if view is not None:
        return view.get_url(result, field_name)
    else:
        try:
            return result.instance.get_absolute_url()
        except:
            pass
    return ''

@register.simple_tag
def result_score(result, max_score):
    pct = result.meta.score / max_score if max_score else 0.0
    return """
        <div class="progress" style="margin-bottom:0;">
            <div class="progress-bar" style="width:%.3f%%;"></div>
        </div>
    """ % (pct * 100.0)

@register.simple_tag
def suggest_link(suggestions, querystring='', name='q'):
    q = QueryDict(querystring).copy()
    keywords = q.get(name, '').strip()
    for term, replacement in suggestions.iteritems():
        keywords = keywords.replace(term, replacement)
    q[name] = keywords
    return '<a href="?%s" class="suggest">%s</a>' % (q.urlencode(), escape(keywords))

@register.inclusion_tag('seeker/pager.html')
def pager(total, page_size=10, page=1, param='page', querystring='', spread=7):
    paginator = Paginator(range(total), page_size)
    page = paginator.page(page)
    if paginator.num_pages > spread:
        start = max(1, page.number - (spread // 2))
        page_range = range(start, start + spread)
    else:
        page_range = paginator.page_range
    return {
        'page_range': page_range,
        'page': page,
        'param': param,
        'querystring': querystring,
    }
