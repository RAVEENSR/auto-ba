import logging

import pandas as pd

from autoba.entities.info_populated_issue import InfoPopulatedIssue


class AccuracyCalculator:
    """
    This class calculates the accuracy for factor weight combinations.

    :param spark: spark session variable
    :type spark: SparkSession
    """
    def __init__(self, spark):
        logging.basicConfig(level=logging.INFO, filename='app.log', format='%(asctime)s-%(name)s-%(levelname)s - '
                                                                           '%(message)s')
        self.spark = spark

    @staticmethod
    def __is_in_top_k(ranked_df, actual_issue_resolvers):
        is_included = False
        for row in ranked_df.itertuples(index=False):
            for actual_resolver in actual_issue_resolvers:
                if actual_resolver == row.developer:
                    is_included = True
                    break
        return is_included

    def __test_accuracy_by_field(self, ranked_data_frame, new_issue, column_name='final_rank', top1=True, top3=False,
                                 top5=False):
        included_in_top1 = False
        included_in_top3 = False
        included_in_top5 = False
        actual_bug_resolvers = new_issue.resolvers

        sorted_ranked_data_frame = ranked_data_frame.sort_values(column_name, ascending=True)
        accuracy = lambda: None

        if top1:
            ranked_one_df = sorted_ranked_data_frame[sorted_ranked_data_frame[column_name] == 1]
            included_in_top1 = self.__is_in_top_k(ranked_one_df, actual_bug_resolvers)
            setattr(accuracy, "top1", included_in_top1)

        if top3:
            ranked_three_df = sorted_ranked_data_frame[sorted_ranked_data_frame[column_name] <= 3]
            included_in_top3 = self.__is_in_top_k(ranked_three_df, actual_bug_resolvers)
            setattr(accuracy, "top3", included_in_top3)

        if top5:
            ranked_five_df = sorted_ranked_data_frame[sorted_ranked_data_frame[column_name] <= 5]
            included_in_top5 = self.__is_in_top_k(ranked_five_df, actual_bug_resolvers)
            setattr(accuracy, "top5", included_in_top5)

        return accuracy

    def __test_combined_accuracy(self, ranked_data_frame, new_issue, top1=True, top3=False, top5=False):
        return self.__test_accuracy_by_field(ranked_data_frame, new_issue, 'final_rank', top1, top3, top5)

    @staticmethod
    def __get_actual_rank_place_of_actual_resolvers(ranked_data_frame, new_issue, column_name='final_rank',
                                                    identifier=1):
        actual_issue_resolvers = new_issue.resolvers
        sorted_ranked_data_frame = ranked_data_frame.sort_values(column_name, ascending=True)
        for row in sorted_ranked_data_frame.itertuples(index=False):
            if row.developer in actual_issue_resolvers:
                if identifier == 1:
                    return int(row.final_rank)
                if identifier == 2:
                    return int(row.file_path_rank)
                if identifier == 3:
                    return int(row.text_rank)
                if identifier == 4:
                    return int(row.activeness_rank)
        return 0

    def test_weight_combination_accuracy_for_all_issues(self, autoba_processor, limit, main_data_frame):
        query1 = "SELECT issue_id, creator_login_id, created_date, closed_date, closed_by, commenters, title, " \
                 "description, files, resolvers " \
                 "FROM issue " \
                 "ORDER BY issue_id " \
                 "LIMIT %d" % limit
        all_issues = self.spark.sql(query1)

        results = []
        for i in range(1, 9):
            for j in range(1, 9):
                for k in range(1, 9):
                    if i != 0 and j != 0 and k != 0 and i + j + k == 10:
                        total_issues = 0
                        cmb_accuracy_array = [0, 0, 0]
                        combined_mrr = 0

                        print("")
                        print("---------------------------------------------------------------------------")
                        print("alpha= " + str(i / 10) + " beta= " + str(j / 10) + " gamma= " + str(k / 10))
                        logging.info("")
                        logging.info("---------------------------------------------------------------------------")
                        logging.info("alpha= " + str(i / 10) + " beta= " + str(j / 10) + " gamma= " + str(k / 10))

                        for new_issues in all_issues.collect():
                            total_issues += 1
                            new_issues = InfoPopulatedIssue(new_issues)

                            scores_df = main_data_frame.loc[main_data_frame['new_issue_id']
                                                            == new_issues.issue_id].copy()

                            ranked_data_frame \
                                = autoba_processor.generate_ranked_list(scores_df, i / 10, j / 10, k / 10)

                            combined_rank = self.__get_actual_rank_place_of_actual_resolvers(
                                ranked_data_frame, new_issues, 'final_rank', 1)

                            if combined_rank != 0:
                                combined_mrr = combined_mrr + (1.0 / combined_rank)

                            combined_accuracy \
                                = self.__test_combined_accuracy(ranked_data_frame, new_issues, True, True, True)

                            if hasattr(combined_accuracy, 'top1') and combined_accuracy.top1:
                                cmb_accuracy_array[0] += 1
                            if hasattr(combined_accuracy, 'top3') and combined_accuracy.top3:
                                cmb_accuracy_array[1] += 1
                            if hasattr(combined_accuracy, 'top5') and combined_accuracy.top5:
                                cmb_accuracy_array[2] += 1

                        combined_mrr = combined_mrr / total_issues

                        avg_combined_top1_accuracy = cmb_accuracy_array[0] / total_issues
                        avg_combined_top3_accuracy = cmb_accuracy_array[1] / total_issues
                        avg_combined_top5_accuracy = cmb_accuracy_array[2] / total_issues

                        combination_result = {
                            'alpha': (i / 10),
                            'beta': (j / 10),
                            'gamma': (k / 10),
                            'top1': avg_combined_top1_accuracy,
                            'top3': avg_combined_top3_accuracy,
                            'top5': avg_combined_top5_accuracy,
                            'mrr': combined_mrr
                        }

                        results.append(combination_result)

                        print("---------------------------------------------------------------------------")
                        print("                         Top1          Top3            Top5")
                        print("Combined Accuracy         " + str(avg_combined_top1_accuracy) + "          " +
                              str(avg_combined_top3_accuracy) + "         " + str(avg_combined_top5_accuracy))
                        print("AutoBA MRR: " + str(combined_mrr))
                        logging.info("---------------------------------------------------------------------------")
                        logging.info("                         Top1          Top3            Top5")
                        logging.info("Combined Accuracy         " + str(avg_combined_top1_accuracy) + "          " +
                                     str(avg_combined_top3_accuracy) + "         " + str(avg_combined_top5_accuracy))
                        logging.info("AutoBA MRR: " + str(combined_mrr))
        return results

    def __test_file_path_similarity_accuracy(self, ranked_data_frame, new_issue, top1=True, top3=False, top5=False):
        return self.__test_accuracy_by_field(ranked_data_frame, new_issue, 'file_path_rank', top1, top3, top5)

    def __test_text_similarity_accuracy(self, ranked_data_frame, new_issue, top1=True, top3=False, top5=False):
        return self.__test_accuracy_by_field(ranked_data_frame, new_issue, 'text_rank', top1, top3, top5)

    def __test_activeness_accuracy(self, ranked_data_frame, new_issue, top1=True, top3=False, top5=False):
        return self.__test_accuracy_by_field(ranked_data_frame, new_issue, 'activeness_rank', top1, top3, top5)

    def test_weight_combination_accuracy_for_all_issues_with_individual_factor_accuracy(self, autoba_processor, offset,
                                                                                     limit, main_data_frame):
        query1 = "SELECT issue_id, creator_login_id, created_date, closed_date, closed_by, commenters, title, " \
                 "description, files, resolvers " \
                 "FROM issue " \
                 "WHERE issue_id > '%s' and issue_id <= '%s' " \
                 "ORDER BY issue_id " \
                 "LIMIT %d" % (offset, offset + limit, limit)
        all_issues = self.spark.sql(query1)

        file_path__similarity_mrr = 0
        text_similarity_mrr = 0
        activeness_mrr = 0

        file_accuracy_array = [0, 0, 0]
        txt_accuracy_array = [0, 0, 0]
        act_accuracy_array = [0, 0, 0]
        df = pd.DataFrame()
        flag = True
        for i in range(1, 9):
            for j in range(1, 9):
                for k in range(1, 9):
                    if i != 0 and j != 0 and k != 0 and i + j + k == 10:
                        total_issues = 0
                        cmb_accuracy_array = [0, 0, 0]
                        combined_mrr = 0

                        for new_issue in all_issues.collect():
                            total_issues += 1
                            new_issue = InfoPopulatedIssue(new_issue)

                            scores_df = main_data_frame.loc[main_data_frame['new_issue_id']
                                                            == new_issue.issue_id].copy()

                            ranked_data_frame \
                                = autoba_processor.generate_ranked_list(scores_df, i / 10, j / 10, k / 10)

                            file_similarity_rank = self.__get_actual_rank_place_of_actual_resolvers(ranked_data_frame,
                                                                                                    new_issue,
                                                                                                     'file_path_rank',
                                                                                                    2)
                            if file_similarity_rank != 0:
                                file_path__similarity_mrr = file_path__similarity_mrr + (1.0/file_similarity_rank)

                            text_similarity_rank = self.__get_actual_rank_place_of_actual_resolvers(ranked_data_frame,
                                                                                                    new_issue,
                                                                                                     'text_rank', 3)
                            if text_similarity_rank != 0:
                                text_similarity_mrr = text_similarity_mrr + (1.0/text_similarity_rank)

                            activeness_rank = self.__get_actual_rank_place_of_actual_resolvers(ranked_data_frame,
                                                                                               new_issue,
                                                                                                'activeness_rank', 4)
                            if activeness_rank != 0:
                                activeness_mrr = activeness_mrr + (1.0/activeness_rank)

                            combined_rank = self.__get_actual_rank_place_of_actual_resolvers(ranked_data_frame,
                                                                                             new_issue, 'final_rank', 1)
                            if combined_rank != 0:
                                combined_mrr = combined_mrr + (1.0 / combined_rank)

                            combined_accuracy \
                                = self.__test_combined_accuracy(ranked_data_frame, new_issue, True, True, True)

                            if hasattr(combined_accuracy, 'top1') and combined_accuracy.top1:
                                cmb_accuracy_array[0] += 1
                            if hasattr(combined_accuracy, 'top3') and combined_accuracy.top3:
                                cmb_accuracy_array[1] += 1
                            if hasattr(combined_accuracy, 'top5') and combined_accuracy.top5:
                                cmb_accuracy_array[2] += 1

                            if flag:
                                file_path_accuracy \
                                    = self.__test_file_path_similarity_accuracy(ranked_data_frame, new_issue, True, True, True)
                                text_accuracy \
                                    = self.__test_text_similarity_accuracy(ranked_data_frame, new_issue, True, True, True)
                                activeness_accuracy \
                                    = self.__test_activeness_accuracy(ranked_data_frame, new_issue, True, True, True)

                                if hasattr(file_path_accuracy, 'top1') and file_path_accuracy.top1:
                                    file_accuracy_array[0] += 1
                                if hasattr(file_path_accuracy, 'top3') and file_path_accuracy.top3:
                                    file_accuracy_array[1] += 1
                                if hasattr(file_path_accuracy, 'top5') and file_path_accuracy.top5:
                                    file_accuracy_array[2] += 1

                                if hasattr(text_accuracy, 'top1') and text_accuracy.top1:
                                    txt_accuracy_array[0] += 1
                                if hasattr(text_accuracy, 'top3') and text_accuracy.top3:
                                    txt_accuracy_array[1] += 1
                                if hasattr(text_accuracy, 'top5') and text_accuracy.top5:
                                    txt_accuracy_array[2] += 1

                                if hasattr(activeness_accuracy, 'top1') and activeness_accuracy.top1:
                                    act_accuracy_array[0] += 1
                                if hasattr(activeness_accuracy, 'top3') and activeness_accuracy.top3:
                                    act_accuracy_array[1] += 1
                                if hasattr(activeness_accuracy, 'top5') and activeness_accuracy.top5:
                                    act_accuracy_array[2] += 1

                        combined_mrr = combined_mrr/total_issues
                        file_path__similarity_mrr = file_path__similarity_mrr/total_issues
                        text_similarity_mrr = text_similarity_mrr/total_issues
                        activeness_mrr = activeness_mrr/total_issues

                        avg_combined_top1_accuracy = cmb_accuracy_array[0] / total_issues
                        avg_combined_top3_accuracy = cmb_accuracy_array[1] / total_issues
                        avg_combined_top5_accuracy = cmb_accuracy_array[2] / total_issues

                        avg_file_path_top1_accuracy = file_accuracy_array[0] / total_issues
                        avg_file_path_top3_accuracy = file_accuracy_array[1] / total_issues
                        avg_file_path_top5_accuracy = file_accuracy_array[2] / total_issues

                        avg_text_top1_accuracy = txt_accuracy_array[0] / total_issues
                        avg_text_top3_accuracy = txt_accuracy_array[1] / total_issues
                        avg_text_top5_accuracy = txt_accuracy_array[2] / total_issues

                        avg_act_top1_accuracy = act_accuracy_array[0] / total_issues
                        avg_act_top3_accuracy = act_accuracy_array[1] / total_issues
                        avg_act_top5_accuracy = act_accuracy_array[2] / total_issues

                        if flag:
                            print("---------------------------------------------------------------------------")
                            print("                         Top1          Top3            Top5")
                            logging.info("---------------------------------------------------------------------------")
                            logging.info("                         Top1          Top3            Top5")
                            print("File Path Accuracy        " + str(avg_file_path_top1_accuracy) + "          " +
                                  str(avg_file_path_top3_accuracy) + "         " + str(avg_file_path_top5_accuracy))
                            print("Text Accuracy             " + str(avg_text_top1_accuracy) + "          " +
                                  str(avg_text_top3_accuracy) + "         " + str(avg_text_top5_accuracy))
                            print("Activeness Accuracy       " + str(avg_act_top1_accuracy) + "          " +
                                  str(avg_act_top3_accuracy) + "         " + str(avg_act_top5_accuracy))
                            print("File Path Similarity MRR: " + str(file_path__similarity_mrr))
                            print("Text Similarity MRR: " + str(text_similarity_mrr))
                            print("Activeness MRR: " + str(activeness_mrr))
                            logging.info("File Path Accuracy        " + str(avg_file_path_top1_accuracy) + "          "
                                         + str(avg_file_path_top3_accuracy) + "         "
                                         + str(avg_file_path_top5_accuracy))
                            logging.info("Text Accuracy             " + str(avg_text_top1_accuracy) + "          " +
                                         str(avg_text_top3_accuracy) + "         " + str(avg_text_top5_accuracy))
                            logging.info("Activeness Accuracy       " + str(avg_act_top1_accuracy) + "          " +
                                         str(avg_act_top3_accuracy) + "         " + str(avg_act_top5_accuracy))
                            logging.info("File Path Similarity MRR: " + str(file_path__similarity_mrr))
                            logging.info("Text Similarity MRR: " + str(text_similarity_mrr))
                            logging.info("Activeness MRR: " + str(activeness_mrr))
                        flag = False

                        print("")
                        print("---------------------------------------------------------------------------")
                        print("alpha= " + str(i / 10) + " beta= " + str(j / 10) + " gamma= " + str(k / 10))
                        print("---------------------------------------------------------------------------")
                        print("                         Top1          Top3            Top5")
                        print("Combined Accuracy         " + str(avg_combined_top1_accuracy) + "          " +
                              str(avg_combined_top3_accuracy) + "         " + str(avg_combined_top5_accuracy))
                        print("AutoBA MRR: " + str(combined_mrr))
                        logging.info("---------------------------------------------------------------------------")
                        logging.info("                         Top1          Top3            Top5")
                        logging.info("Combined Accuracy         " + str(avg_combined_top1_accuracy) + "          " +
                                     str(avg_combined_top3_accuracy) + "         " + str(avg_combined_top5_accuracy))
                        logging.info("Combined MRR: " + str(combined_mrr))

                        row = {'alpha': str(i / 10),
                               'beta': str(j / 10),
                               'gamma': str(k / 10),
                               'Top1': str(avg_combined_top1_accuracy),
                               'Top3': str(avg_combined_top3_accuracy),
                               'Top5': str(avg_combined_top5_accuracy),
                               'MRR': str(combined_mrr)}
                        df = df.append(row, ignore_index=True)

        df.to_csv(str(autoba_processor.database) + ".csv", index=False)
