class FilePathSimilarityCalculator:
    """
    This class handles file path similarity score calculating operations.
    """

    @staticmethod
    def path_separator(file_string):
        return file_string.split("/")

    def longest_common_prefix_similarity(self, file1, file2):
        file1 = self.path_separator(file1)
        file2 = self.path_separator(file2)
        common_file_path = 0
        min_length = min(len(file1), len(file2))
        for i in range(min_length):
            if file1[i] == file2[i]:
                common_file_path += 1
            else:
                break
        return common_file_path

    def longest_common_suffix_similarity(self, file1, file2):
        file1 = self.path_separator(file1)
        file2 = self.path_separator(file2)
        common_file_path = 0
        rng = range(min(len(file1), len(file2)))
        rng = reversed(rng)
        for i in rng:
            if file1[i] == file2[i]:
                common_file_path += 1
            else:
                break
        return common_file_path

    def longest_common_sub_string_similarity(self, file1, file2):
        file1 = self.path_separator(file1)
        file2 = self.path_separator(file2)
        common_file_path = 0
        if len(set(file1) & set(file2)) > 0:
            matrix = [[0 for x in range(len(file2) + 1)] for x in range(len(file1) + 1)]
            for i in range(len(file1) + 1):
                for j in range(len(file2) + 1):
                    if i == 0 or j == 0:
                        matrix[i][j] = 0
                    elif file1[i - 1] == file2[j - 1]:
                        matrix[i][j] = matrix[i - 1][j - 1] + 1
                        common_file_path = max(common_file_path, matrix[i][j])
                    else:
                        matrix[i][j] = 0
        return common_file_path

    def longest_common_sub_sequence_similarity(self, file1, file2):
        file1 = self.path_separator(file1)
        file2 = self.path_separator(file2)
        common_file_path = 0
        if len(set(file1) & set(file2)) > 0:
            lst = [[0 for x in range(len(file2) + 1)] for x in range(len(file1) + 1)]
            for i in range(len(file1) + 1):
                for j in range(len(file2) + 1):
                    if i == 0 or j == 0:
                        lst[i][j] = 0
                    elif file1[i - 1] == file2[j - 1]:
                        lst[i][j] = lst[i - 1][j - 1] + 1
                    else:
                        lst[i][j] = max(lst[i - 1][j], lst[i][j - 1])
            common_file_path = lst[len(file1)][len(file2)]
        return common_file_path

    @staticmethod
    def add_file_path_similarity_ranking(data_frame):
        data_frame["file_path_rank"] = data_frame["file_similarity"].rank(method='min', ascending=False)
