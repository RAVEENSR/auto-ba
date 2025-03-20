class Developer:
    """
    This class models a Developer.
    """
    def __init__(self, login_name):
        self.developer_login = login_name
        self.longest_common_prefix_score = 0
        self.longest_common_suffix_score = 0
        self.longest_common_sub_string_score = 0
        self.longest_common_sub_sequence_score = 0
        self.issue_title_similarity = 0
        self.issue_description_similarity = 0
        self.activeness = 0

    @property
    def developer_login(self):
        return self.__developer_login

    @property
    def longest_common_prefix_score(self):
        return self.__longest_common_prefix_score

    @property
    def longest_common_suffix_score(self):
        return self.__longest_common_suffix_score

    @property
    def longest_common_sub_string_score(self):
        return self.__longest_common_sub_string_score

    @property
    def longest_common_sub_sequence_score(self):
        return self.__longest_common_sub_sequence_score

    @property
    def issue_title_similarity(self):
        return self.__issue_title_similarity

    @property
    def issue_description_similarity(self):
        return self.__issue_description_similarity

    @property
    def activeness(self):
        return self.__activeness

    @developer_login.setter
    def developer_login(self, val):
        self.__developer_login = val

    @longest_common_prefix_score.setter
    def longest_common_prefix_score(self, val):
        self.__longest_common_prefix_score = val

    @longest_common_suffix_score.setter
    def longest_common_suffix_score(self, val):
        self.__longest_common_suffix_score = val

    @longest_common_sub_string_score.setter
    def longest_common_sub_string_score(self, val):
        self.__longest_common_sub_string_score = val

    @longest_common_sub_sequence_score.setter
    def longest_common_sub_sequence_score(self, val):
        self.__longest_common_sub_sequence_score = val

    @issue_title_similarity.setter
    def issue_title_similarity(self, val):
        self.__issue_title_similarity = val

    @issue_description_similarity.setter
    def issue_description_similarity(self, val):
        self.__issue_description_similarity = val

    @activeness.setter
    def activeness(self, val):
        self.__activeness = val
