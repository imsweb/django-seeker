import elasticsearch_dsl as dsl

import seeker


class BaseDocument (seeker.Indexable):
    base_field = dsl.String()
