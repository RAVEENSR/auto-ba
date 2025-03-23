"""
autoba_processor.py
====================================
The core of the AutoBA system
"""
import os
import logging
from datetime import timedelta, datetime

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, trim, col, array_contains, lit, broadcast

import autoba_config as acfg
from autoba.accuracy_calculation.accuracy_calculation import AccuracyCalculator
from autoba.activeness.developer_activeness import ActivenessCalculator
from autoba.entities.developer import Developer
from autoba.entities.info_populated_issue import InfoPopulatedIssue
from autoba.string_compare.file_path_similarity import FilePathSimilarityCalculator
from autoba.text_similarity.text_similarity import TextSimilarityCalculator


class AutoBAProcessor:
    """
    This is the main class for the AutoBA system. This class handles all the major operations in the system.
    """
    def __init__(self):
        self.repo_name = acfg.repo['name']
        self.spark = None
        self.all_issues_df = None
        self.all_developers_df = None
        self.all_developers = None
        self.issue_count = 0
        self.developer_count = 0
        self.file_path_similarity_calculator = FilePathSimilarityCalculator()
        self.activeness_calculator = ActivenessCalculator(const_lambda=-1)
        self.text_similarity_calculator = TextSimilarityCalculator()
        self.__initialise_app()
        self.accuracy_calculator = AccuracyCalculator(spark=self.spark)
        self.alpha = acfg.system_defaults['alpha']
        self.beta = acfg.system_defaults['beta']
        self.gamma = acfg.system_defaults['gamma']
        self.date_window = acfg.system_constants['date_window']
        logging.basicConfig(level=logging.INFO, filename='app.log',
                            format='%(asctime)s-%(name)s-%(levelname)s - %(message)s')
        logging.info("AutoBA Processor created")

    def __initialise_app(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_dir, "dataset", f"{self.repo_name}_updated_issue_details.csv")

        # Create a Spark session with configuration settings set at build time
        self.spark = SparkSession.builder \
            .master('local') \
            .appName("AutoBA") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.dynamicAllocation.enabled", "true") \
            .getOrCreate()

        # Load issue data from CSV and filter out rows with null or empty 'resolvers'
        raw_issues_df = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiLine", "true") \
            .load(file_path)

        # Count total issues before filtering
        print("Total issues (before filtering):", raw_issues_df.count())

        # Filter out rows with null or empty 'resolvers'
        self.all_issues_df = raw_issues_df.filter((col("resolvers").isNotNull()) & (col("resolvers") != ""))

        # Add a precomputed resolver_array column (and cache the DataFrame)
        self.all_issues_df = self.all_issues_df.withColumn("resolver_array", split(col("resolvers"), r";\s*"))
        self.all_issues_df.cache()  # Persist the DataFrame in memory

        # Extract unique developer names from the resolver_array column
        dev_df = self.all_issues_df.select(explode("resolver_array").alias("developer")) \
            .withColumn("developer", trim(col("developer"))) \
            .dropDuplicates()
        # Broadcast the small developer DataFrame to optimize joins and filters
        self.all_developers_df = broadcast(dev_df)
        self.all_developers_df.createOrReplaceTempView("developer")

        # Register the issues DataFrame as a temp view (if needed)
        self.all_issues_df.createOrReplaceTempView("issue")

        # Collect all developers as a list of Row objects from the broadcasted DataFrame
        self.all_developers = self.all_developers_df.collect()

        # Count stats
        self.issue_count = self.all_issues_df.count()
        self.developer_count = self.all_developers_df.count()

        logging.info("Issue count (after filtering): %s", self.issue_count)
        logging.info("Developer count: %s", self.developer_count)

    def __calculate_scores(self, new_issue, date_window=0):
        rows = []  # Collect rows as dicts
        num_new_files = len(new_issue.files)

        # Pre-convert new_issue.created_date for Spark filtering
        new_issue_ts = lit(new_issue.created_date)

        # Calculate scores for each developer
        for developer in self.all_developers:
            issue_resolver = Developer(developer[0])

            # Use DataFrame filtering instead of SQL query
            base_df = self.all_issues_df.filter(col("closed_date") < new_issue_ts)
            if date_window:
                start_date = new_issue.created_date - timedelta(days=date_window)
                base_df = base_df.filter(col("closed_date") > lit(start_date))
            base_df = base_df.filter(array_contains(col("resolver_array"), issue_resolver.developer_login))
            developer_resolved_issues = base_df.collect()

            for developer_resolved_issue in developer_resolved_issues:
                old_issue = InfoPopulatedIssue(developer_resolved_issue)
                old_issue_file_paths = old_issue.files
                num_old_files = len(old_issue_file_paths)
                if num_new_files == 0 or num_old_files == 0:
                    continue
                number_of_file_combinations = num_new_files * num_old_files

                # Calculate file path similarity for each file combination
                for new_issue_file_path in new_issue.files:
                    for file_path in old_issue_file_paths:
                        max_file_path_length = max(
                            len(new_issue_file_path.split("/")), len(file_path.split("/"))
                        )
                        divider = max_file_path_length * number_of_file_combinations
                        issue_resolver.longest_common_prefix_score += (
                            self.file_path_similarity_calculator.longest_common_prefix_similarity(
                                new_issue_file_path, file_path
                            ) / divider
                        )
                        issue_resolver.longest_common_suffix_score += (
                            self.file_path_similarity_calculator.longest_common_suffix_similarity(
                                new_issue_file_path, file_path
                            ) / divider
                        )
                        issue_resolver.longest_common_sub_string_score += (
                            self.file_path_similarity_calculator.longest_common_sub_string_similarity(
                                new_issue_file_path, file_path
                            ) / divider
                        )
                        issue_resolver.longest_common_sub_sequence_score += (
                            self.file_path_similarity_calculator.longest_common_sub_sequence_similarity(
                                new_issue_file_path, file_path
                            ) / divider
                        )

                # Calculate cosine similarity for title and description
                issue_resolver.issue_title_similarity += self.text_similarity_calculator.cos_similarity(
                    new_issue.title, old_issue.title
                )
                if new_issue.description != "" and old_issue.description != "":
                    issue_resolver.issue_description_similarity += self.text_similarity_calculator.cos_similarity(
                        new_issue.description, old_issue.description
                    )
                # Calculate activeness
                issue_resolver.activeness += (self.activeness_calculator
                                              .calculate_developer_activeness(new_issue, old_issue))

            row = {
                'new_issue_id': new_issue.issue_id,
                'developer': issue_resolver.developer_login,
                'lcp': issue_resolver.longest_common_prefix_score,
                'lcs': issue_resolver.longest_common_suffix_score,
                'lc_substr': issue_resolver.longest_common_sub_string_score,
                'ls_subseq': issue_resolver.longest_common_sub_sequence_score,
                'cos_title': issue_resolver.issue_title_similarity,
                'cos_description': issue_resolver.issue_description_similarity,
                'activeness': issue_resolver.activeness,
                'file_similarity': (
                    issue_resolver.longest_common_prefix_score +
                    issue_resolver.longest_common_suffix_score +
                    issue_resolver.longest_common_sub_string_score +
                    issue_resolver.longest_common_sub_sequence_score
                ),
                'text_similarity': (
                    issue_resolver.issue_title_similarity +
                    issue_resolver.issue_description_similarity
                )
            }
            rows.append(row)
        return rows

    def __calculate_scores_for_all_issues(self, limit, date_window=0):
        # Use the cached DataFrame with limit() and collect all issues
        all_issues = self.all_issues_df.limit(limit).collect()
        total_issues = 0
        all_rows = []

        for issue_row in all_issues:
            total_issues += 1
            new_issue = InfoPopulatedIssue(issue_row)
            issue_rows = self.__calculate_scores(new_issue, date_window)
            all_rows.extend(issue_rows)
            print(f"Scores calculated for: {date_window}_{new_issue.issue_id}")
            logging.info(f"Scores calculated for: {date_window}_{new_issue.issue_id}")

        df = pd.DataFrame(all_rows)
        df.to_csv(f"{date_window}_{self.repo_name}_all_resolvers_scores_for_each_test_pr.csv", index=False)
        return df

    @staticmethod
    def __standardize_score(score, min_val, max_val):
        if (max_val - min_val) == 0:
            new_value = 0
        else:
            new_value = ((score - min_val) * 100) / (max_val - min_val)
        return new_value

    def __add_standard_scores_to_data_frame(self, main_df):
        act_min = main_df['activeness'].min()
        act_max = main_df['activeness'].max()
        file_sim_min = main_df['file_similarity'].min()
        file_sim_max = main_df['file_similarity'].max()
        txt_sim_min = main_df['text_similarity'].min()
        txt_sim_max = main_df['text_similarity'].max()

        main_df['std_activeness'] = main_df['activeness'].apply(self.__standardize_score, args=(act_min, act_max))
        main_df['std_file_similarity'] = main_df['file_similarity'].apply(self.__standardize_score,
                                                                          args=(file_sim_min, file_sim_max))
        main_df['std_text_similarity'] = main_df['text_similarity'].apply(self.__standardize_score,
                                                                          args=(txt_sim_min, txt_sim_max))
        return main_df

    def generate_ranked_list(self, data_frame, alpha, beta, gamma):
        logging.info("Generating ranked list started")
        self.file_path_similarity_calculator.add_file_path_similarity_ranking(data_frame)
        self.text_similarity_calculator.add_text_similarity_ranking(data_frame)
        self.activeness_calculator.add_activeness_ranking(data_frame)

        data_frame = self.__add_standard_scores_to_data_frame(data_frame)
        data_frame['combined_score'] = (data_frame['std_file_similarity'] * alpha) + \
                                       (data_frame['std_text_similarity'] * beta) + \
                                       (data_frame['std_activeness'] * gamma)
        data_frame["final_rank"] = data_frame["combined_score"].rank(method='min', ascending=False)
        logging.info("Generating ranked list finished")
        return data_frame

    def get_weight_combinations_for_factors(self, limit, main_data_frame=None, main_data_csv_file_name=None,
                                            use_csv_file=False):
        limit = int(limit)
        if use_csv_file:
            if main_data_csv_file_name is None:
                logging.error("main_data_csv_file_name parameter is none!")
            logging.info("Getting weight combinations for factors for csv file:" + str(main_data_csv_file_name))
            main_df = pd.read_csv(main_data_csv_file_name)
        else:
            if main_data_frame is None:
                logging.error("main_data_frame parameter is none!")
            main_df = main_data_frame

        return self.accuracy_calculator.test_weight_combination_accuracy_for_all_issues(
            autoba_processor=self, limit=limit, main_data_frame=main_df
        )

    def calculate_scores_and_get_weight_combinations_for_factors(self, limit):
        """
        This function calculates scores for every issue and provides accuracy for each factor weight combination.

        :EXAMPLE:

        >>> autoba.calculate_scores_and_get_weight_combinations_for_factors(300)

        :param limit: Limit of the issues needed to be considered when calculating scores
        :type limit: int
        :return: Accuracy for each factor weight combination in terms of top1, top3, top5 accuracy and MRR
        :rtype: object
        """
        logging.info("Calculating scores and getting weight combinations for factors started")
        limit = int(limit)
        df = self.__calculate_scores_for_all_issues(limit)
        logging.info("Calculating scores and getting weight combinations for factors finished")
        return self.get_weight_combinations_for_factors(limit, df, use_csv_file=False)

    def set_weight_combination_for_factors(self, alpha, beta, gamma, date_window=0):
        """
        This function sets the weights for each factor(file path similarity, text similarity, activeness) of the system.
        These weights are used to determine the final score for the developer. If date_window is not set default value
        will be considered.

        :EXAMPLE:

        >>> autoba.set_weight_combination_for_factors(0.1, 0.2, 0.7)

        :param alpha: Weight for file path similarity score
        :type alpha: float
        :param beta: Weight for text similarity score
        :type beta: float
        :param gamma: Weight for activeness score
        :type gamma: float
        :param date_window: (optional) Dates needed to be considered back from issue created date to calculate scores
        :return: Whether the operation is successful or not
        :rtype: bool
        """
        self.alpha = float(alpha)
        self.beta = float(beta)
        self.gamma = float(gamma)
        self.date_window = date_window
        logging.info("Setting weights for factors finished. alpha: " + str(alpha) +
                     " beta: " + str(beta) + " gamma: " + str(gamma))


    def get_issue_details(self, issue_id):
        """
        This function provides details of an issue.

        :EXAMPLE:

        >>> autoba.get_issue_details(10)

        :param issue_id: Issue id number
        :type issue_id: int
        :return: Details of the issue
        :rtype: list
        """
        logging.info("Getting details for issue " + str(issue_id) + " started")
        query = f"SELECT issue_id, creator_login_id, created_date, closed_date, closed_by, commenters, title, " \
                f"description, files, resolvers FROM issue WHERE issue_id='{issue_id}'"
        result = self.spark.sql(query)
        details = result.collect()[0]
        logging.info("Details for issue " + str(issue_id) + " presented")
        return details

    def get_related_developers_for_issue(self, issue_id, creator_login_id, created_date_time, closed_date_time,
                                         closed_by, commenters, title, description, files, resolvers):
        """
        This function calculates scores for each factor for each developer and provides a ranked data frame which
        includes top five developers.

        :EXAMPLE:

        >>> autoba.get_related_developers_for_issue(10, 'John', '2024-03-19T18:03:48Z', '2025-03-19T18:03:48Z', 'Max',
        >>> 'Max,David', 'Issue Title', 'Issue Description', 'abc.js,def.js,ghi.js', 'Max,David')

        :param issue_id: Issue id
        :type issue_id: int
        :param creator_login_id: Issue creator username
        :type creator_login_id: String
        :param created_date_time: Creation date of issue
        :type created_date_time: String
        :param closed_date_time: Closed date of issue
        :type closed_date_time: String
        :param closed_by: Issue closed by username
        :type closed_by: String
        :param commenters: Commenters of the issue
        :type commenters: String
        :param title: Title of the issue
        :type title: String
        :param description: Description of the issue
        :type description: String
        :param files: File paths of associated to the issue
        :type files: String
        :param resolvers: Resolvers of the issue
        :type resolvers: String
        :return: Top five developers data frame
        :rtype: DataFrame
        """
        logging.info("Getting related developers by issue details for issue " + str(issue_id) + " started")
        created_date_time = datetime.strptime(created_date_time, '%Y-%m-%dT%H:%M:%SZ')
        closed_date_time = datetime.strptime(closed_date_time, '%Y-%m-%dT%H:%M:%SZ')
        issue_data = [issue_id, creator_login_id, created_date_time, closed_date_time, closed_by, commenters,
                      title, description, files, resolvers]
        new_issue = InfoPopulatedIssue(issue_data)
        df = self.__calculate_scores(new_issue, self.date_window)
        ranked_df = self.generate_ranked_list(df, self.alpha, self.beta, self.gamma)
        sorted_ranked_data_frame = ranked_df.sort_values('final_rank', ascending=True)
        ranked_five_df = sorted_ranked_data_frame[sorted_ranked_data_frame['final_rank'] <= 5]
        logging.info("Top five developers for issue " + str(issue_id) + " presented")
        return ranked_five_df

    def get_related_developers_for_issue_by_issue_id(self, issue_id):
        """
        This function calculates scores for each factor for each developer and provides a ranked data frame which
        includes top five developers.

        :EXAMPLE:

        >>> autoba.get_related_developers_for_issue_by_issue_id(10)

        :param issue_id: Issue id
        :type issue_id: int
        :return: Top five developers data frame
        :rtype: DataFrame
        """
        logging.info("Getting related developers by issue id for Issue " + str(issue_id) + " started")
        issue_details = self.get_issue_details(issue_id)
        new_issue = InfoPopulatedIssue(issue_details)
        df = self.__calculate_scores(new_issue, self.date_window)
        ranked_df = self.generate_ranked_list(df, self.alpha, self.beta, self.gamma)
        sorted_ranked_data_frame = ranked_df.sort_values('final_rank', ascending=True)
        ranked_five_df = sorted_ranked_data_frame[sorted_ranked_data_frame['final_rank'] <= 5]
        logging.info("Top five developers for issue " + str(issue_id) + " presented")
        return ranked_five_df
