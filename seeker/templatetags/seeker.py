from django import template
from django.template import loader
from django.template.base import Context
from django.utils.html import escape
from django.http import QueryDict
from django.conf import settings
from django.utils import dateformat
from django.core.paginator import Paginator
from django.contrib.humanize.templatetags.humanize import intcomma
import datetime
import string

register = template.Library()

@register.simple_tag
def facet_checkbox(facet, value, filters=None, missing='MISSING', count_prefix=''):
    if filters is None:
        filters = {}
    key = facet.get_key(value)
    return '<label><input type="checkbox" name="%(name)s" value="%(key)s"%(checked)s data-count="%(count)s" /> %(key_fmt)s (%(count_prefix)s%(count_fmt)s)</label>' % {
        'name': facet.field,
        'key': key or '',
        'key_fmt': key or missing,
        'count': value['doc_count'],
        'count_fmt': intcomma(value['doc_count']),
        'count_prefix': count_prefix,
        'checked': ' checked="checked"' if facet.field in filters and key in filters[facet.field] else '',
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
def string_format(value):
    if value is None:
        return ''
    if isinstance(value, (list, tuple)):
        return list_display(value)
    if isinstance(value, dict):
        return dict_display(value)
    if isinstance(value, datetime.datetime):
        return dateformat.format(value, settings.DATETIME_FORMAT)
    if isinstance(value, datetime.date):
        return dateformat.format(value, settings.DATE_FORMAT)
    return unicode(value)

@register.filter
def list_display(values, sep=', '):
    return sep.join(string_format(v) for v in values)

@register.filter
def dict_display(d, sep=', '):
    parts = []
    for key, value in d.items():
        if key and value:
            parts.append('%s: %s' % (key, string_format(value)))
    return sep.join(parts)

@register.simple_tag
def sort_link(sort_by, label=None, querystring='', name='sort', mapping=None):
    q = QueryDict(querystring).copy()
    parts = q.get(name, '').split(':')
    if parts[0] and parts[0] == sort_by:
        if len(parts) > 1:
            d = '' if parts[1] == 'desc' else 'desc'
            cur = parts[1] or 'asc'
        else:
            d = 'desc'
            cur = 'asc'
    else:
        d = cur = ''
    q[name] = '%s:%s' % (sort_by, d) if d else sort_by
    if label is None:
        if mapping:
            label = mapping.field_label(sort_by)
        else:
            label = string.capwords(sort_by.replace('_', ' '))
    return '<a href="?%s" class="sort %s">%s</a>' % (q.urlencode(), cur, escape(label))

@register.simple_tag
def field_label(mapping, field_name):
    return mapping.field_label(field_name)

@register.simple_tag
def result_value(result, field_name, highlight=True, template=None):
    if highlight:
        try:
            value = result.hit['highlight'][field_name][0]
        except:
            value = result.data.get(field_name, '')
        # For nested objects, see if any of the sub-fields were highlighted.
        if isinstance(value, dict):
            for key in value:
                try:
                    value[key] = result.hit['highlight']['%s.%s' % (field_name, key)][0]
                except:
                    pass
    else:
        value = result.data.get(field_name, '')
    # First, try to render the field using a template.
    search_templates = [
        'seeker/%s/%s.html' % (result.mapping.doc_type, field_name),
    ]
    if template:
        search_templates.insert(0, template)
    try:
        t = loader.select_template(search_templates)
        return t.render(Context({
            'result': result,
            'value': value,
        }))
    except:
        pass
    # Otherwise, do our best to render the value as a string.
    return string_format(value)

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
