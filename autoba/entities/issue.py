class Issue:
    """
    This class models a bug/issue.
    """

    @staticmethod
    def split_string(commenters_string):
        return commenters_string.split("; ")

    def __init__(self, data):
        self.issue_id = data[0]
        self.creator_login_id = data[1]
        self.created_date = data[2]
        self.closed_date = data[3]
        self.closed_by = data[4]
        self.commenters = self.split_string(data[5])
        self.title = data[6]
        self.description = data[7]

    @property
    def issue_id(self):
        return self.__issue_id

    @property
    def creator_login_id(self):
        return self.__creator_login_id

    @property
    def created_date(self):
        return self.__created_date

    @property
    def closed_date(self):
        return self.__closed_date

    @property
    def closed_by(self):
        return self.__closed_by

    @property
    def commenters(self):
        return self.__commenters

    @property
    def title(self):
        return self.__title

    @property
    def description(self):
        return self.__description

    @issue_id.setter
    def issue_id(self, val):
        self.__issue_id = val

    @creator_login_id.setter
    def creator_login_id(self, val):
        self.__creator_login_id = val

    @created_date.setter
    def created_date(self, val):
        self.__created_date = val

    @closed_date.setter
    def closed_date(self, val):
        self.__closed_date = val

    @closed_by.setter
    def closed_by(self, val):
        self.__closed_by = val

    @commenters.setter
    def commenters(self, val):
        self.__commenters = val

    @title.setter
    def title(self, val):
        self.__title = val

    @description.setter
    def description(self, val):
        self.__description = val
