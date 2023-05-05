from seeker.dsl import dsl

import seeker


class BaseDocument(seeker.Indexable):
    base_field = dsl.Text()
