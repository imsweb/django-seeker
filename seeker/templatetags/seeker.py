from django import template
from django.template import loader
from django.core.paginator import Paginator
from django.contrib.humanize.templatetags.humanize import intcomma

register = template.Library()

# Convenience so people don't need to install django.contrib.humanize
register.filter(intcomma)

@register.simple_tag
def seeker_facet(facet, results, selected=None, **params):
    params.update({
        'facet': facet,
        'selected': selected,
        'data': facet.data(results),
    })
    return loader.render_to_string(facet.template, params)

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
def seeker_pager(total, page_size=10, page=1, param='p', querystring='', spread=7, template='seeker/pager.html'):
    paginator = Paginator(range(total), page_size)
    if paginator.num_pages < 2:
        return ''
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
