from django import template
from django.template import loader
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
def render_facet(facet, results, selected=None, template='seeker/facet.html'):
    values = []
    for data in facet.values(results):
        key = facet.get_key(data)
        count = data['doc_count']
        sel = selected and key in selected
        values.append((key, count, sel))
    template_name = facet.template or template
    return loader.render_to_string(template_name, {
        'facet': facet,
        'values': values,
        'selected': selected,
    })

@register.simple_tag
def seeker_header(column, querystring):
    return column.header(querystring)

@register.simple_tag
def seeker_column(column, result, **kwargs):
    return column.render(result, **kwargs)

@register.simple_tag
def seeker_score(result, max_score=None, template='seeker/score.html'):
    pct = result.meta.score / max_score if max_score else 0.0
    return loader.render_to_string(template, {
        'score': result.meta.score,
        'max_score': max_score,
        'percentile': pct * 100.0,
    })

@register.simple_tag
def suggest_link(suggestions, querystring='', name='q'):
    q = QueryDict(querystring).copy()
    keywords = q.get(name, '').strip()
    for term, replacement in suggestions.iteritems():
        keywords = keywords.replace(term, replacement)
    q[name] = keywords
    return '<a href="?%s" class="suggest">%s</a>' % (q.urlencode(), escape(keywords))

@register.simple_tag
def pager(total, page_size=10, page=1, param='p', querystring='', spread=7, template='seeker/pager.html'):
    paginator = Paginator(range(total), page_size)
    page = paginator.page(page)
    if paginator.num_pages > spread:
        start = max(1, min(paginator.num_pages + 1 - spread, page.number - (spread // 2)))
        end = min(start + spread, paginator.num_pages + 1)
        page_range = range(start, end)
    else:
        page_range = paginator.page_range
    return loader.render_to_string(template, {
        'page_range': page_range,
        'paginator': paginator,
        'page': page,
        'param': param,
        'querystring': querystring,
    })
