from autoba.entities.issue import Issue


class InfoPopulatedIssue(Issue):
    """
    This class models an information Populated Issue.
    """

    def __init__(self, data):
        # Call parent's __init__ first
        super().__init__(data)
        self.files = self.split_string(data[8])
        # resolvers are the pople who helped to fix the issue/bug
        self.resolvers = self.split_string(data[9])

    @property
    def files(self):
        return self.__files

    @property
    def resolvers(self):
        return self.__resolvers

    @files.setter
    def files(self, val):
        self.__files = val

    @resolvers.setter
    def resolvers(self, val):
        self.__resolvers = val
