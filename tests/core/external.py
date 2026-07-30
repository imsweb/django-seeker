import seeker


class BaseDocument(seeker.Indexable):
    base_field = seeker.Text()
