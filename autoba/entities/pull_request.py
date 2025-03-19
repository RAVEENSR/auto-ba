class PullRequest:
    """
    This class models a related pull-request for a given bug/issue.
    """

    @staticmethod
    def __initialize_files(files_string):
        return files_string.split("; ")

    def __init__(self, data):
        self.issue_id = data[0]
        self.pr_id = data[1]
        self.title = data[2]
        self.description = data[3]
        self.status = data[4]
        self.files = self.__initialize_files(data[5])
        self.creator_login_id = data[6]
        self.created_date = data[7]
        self.merged_date = data[8]

    @property
    def issue_id(self):
        return self.__issue_id

    @property
    def pr_id(self):
        return self.__pr_id

    @property
    def title(self):
        return self.__title

    @property
    def description(self):
        return self.__description

    @property
    def status(self):
        return self.__status

    @property
    def files(self):
        return self.__files

    @property
    def creator_login_id(self):
        return self.__creator_login_id

    @property
    def created_date(self):
        return self.__created_date

    @property
    def merged_date(self):
        return self.__merged_date

    @issue_id.setter
    def issue_id(self, val):
        self.__issue_id = val

    @pr_id.setter
    def pr_id(self, val):
        self.__pr_id = val

    @title.setter
    def title(self, val):
        self.__title = val

    @description.setter
    def description(self, val):
        self.__description = val

    @status.setter
    def status(self, val):
        self.__status = val

    @files.setter
    def files(self, val):
        self.__files = val

    @creator_login_id.setter
    def creator_login_id(self, val):
        self.__creator_login_id = val

    @created_date.setter
    def created_date(self, val):
        self.__created_date = val

    @merged_date.setter
    def merged_date(self, val):
        self.__merged_date = val




