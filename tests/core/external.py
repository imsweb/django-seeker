import seeker
from seeker.dsl import dsl


class BaseDocument(seeker.Indexable):
    base_field = dsl.Text()
