
class Deserializer:
    """
    Base class for all key and value deserializers.
    This default implementation returns the value intact.
    """
    def __init__(self, config, **kwargs):
        pass

    def deserialize(self, value, **kwargs):
        return value
