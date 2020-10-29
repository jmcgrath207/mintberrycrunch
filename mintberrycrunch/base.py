from addict import Dict
from deepmerge import always_merger


class Base:
    _attrs = Dict()
    
    @property
    def attrs(self):
        """I'm the 'x' property."""
        return self._attrs

    @attrs.setter
    def attrs(self, dictionary):
        _attrs = always_merger.merge(dictionary, self._attrs)
        self._attrs = Dict(_attrs)
