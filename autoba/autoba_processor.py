"""
autoba_processor.py
====================================
The optimized core of the AutoBA system
"""

import os
import logging
from datetime import timedelta, datetime
import pandas as pd
import numpy as np
from functools import lru_cache

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, trim, col, array_contains, collect_list, udf, pandas_udf
from pyspark.sql.types import FloatType, ArrayType, StringType, StructType, StructField

from autoba.accuracy_calculation.accuracy_calculation import AccuracyCalculator
from autoba.activeness.developer_activeness import ActivenessCalculator
from autoba.entities.developer import Developer
from autoba.entities.info_populated_issue import InfoPopulatedIssue
from autoba.string_compare.file_path_similarity import FilePathSimilarityCalculator
from autoba.text_similarity.text_similarity import TextSimilarityCalculator
import autoba_config as acfg


class AutoBAProcessor:
    """
    This is the main class for the AutoBA system. This class handles all the major operations in the system
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
        logging.basicConfig(level=logging.INFO, filename='app.log', format='%(asctime)s-%(name)s-%(levelname)s '
                                                                          '- %(message)s')
        logging.info("AutoBA Processor created")

    def __initialise_app(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_dir, "dataset", "updated-issue-details.csv")

        # Create a spark session with optimized configuration
        self.spark = SparkSession \
            .builder \
            .master('local[*]') \
            .appName("AutoBA") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.default.parallelism", "8") \
            .getOrCreate()

        # Load issue data from CSV
        raw_issues_df = self.spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(file_path)

        # Filter out rows where 'resolvers' is null or empty - do this once
        self.all_issues_df = raw_issues_df.filter(
            (col("resolvers").isNotNull()) & (col("resolvers") != "")
        ).cache()  # Cache for reuse

        # Convert files column to array for easier processing
        self.all_issues_df = self.all_issues_df.withColumn(
            "files_array",
            split(col("files"), ";")
        )

        # Extract unique developer names from 'resolvers' column more efficiently
        self.all_developers_df = self.all_issues_df.select(
            explode(split("resolvers", ";")).alias("developer")
        ).withColumn("developer", trim("developer")).dropDuplicates().cache()  # Cache for reuse

        # Register Temp Views for Spark SQL
        self.all_issues_df.createOrReplaceTempView("issue")
        self.all_developers_df.createOrReplaceTempView("developer")

        # Collect all developers as list efficiently
        self.all_developers = self.all_developers_df.collect()

        # Count stats
        self.issue_count = self.all_issues_df.count()
        self.developer_count = self.all_developers_df.count()

    @lru_cache(maxsize=256)
    def _calculate_file_similarity(self, new_path, old_path):
        """Calculate file path similarity with caching for repeated calculations"""
        max_file_path_length = max(len(new_path.split("/")), len(old_path.split("/")))

        lcp = self.file_path_similarity_calculator.longest_common_prefix_similarity(new_path, old_path)
        lcs = self.file_path_similarity_calculator.longest_common_suffix_similarity(new_path, old_path)
        lc_substr = self.file_path_similarity_calculator.longest_common_sub_string_similarity(new_path, old_path)
        lc_subseq = self.file_path_similarity_calculator.longest_common_sub_sequence_similarity(new_path, old_path)

        return lcp, lcs, lc_substr, lc_subseq, max_file_path_length

    @lru_cache(maxsize=256)
    def _calculate_text_similarity(self, text1, text2):
        """Calculate text similarity with caching for repeated calculations"""
        if not text1 or not text2:
            return 0
        return self.text_similarity_calculator.cos_similarity(text1, text2)

    def _process_developer_resolved_issues(self, new_issue, dev_issues_df):
        """Process all resolved issues for a developer in a vectorized way"""
        rows = []
        dev_issues = dev_issues_df.collect()

        for issue_row in dev_issues:
            old_issue = InfoPopulatedIssue(issue_row)

            # File path similarity calculations
            file_sim_scores = {"lcp": 0, "lcs": 0, "lc_substr": 0, "lc_subseq": 0}
            new_files = new_issue.files.split(";") if isinstance(new_issue.files, str) else new_issue.files
            old_files = old_issue.files.split(";") if isinstance(old_issue.files, str) else old_issue.files

            if not new_files or not old_files:
                continue

            num_combinations = len(new_files) * len(old_files)
            if num_combinations == 0:
                continue

            for new_file in new_files:
                for old_file in old_files:
                    if not new_file or not old_file:
                        continue
                    lcp, lcs, lc_substr, lc_subseq, max_length = self._calculate_file_similarity(new_file, old_file)
                    divider = max_length * num_combinations

                    if divider > 0:
                        file_sim_scores["lcp"] += lcp / divider
                        file_sim_scores["lcs"] += lcs / divider
                        file_sim_scores["lc_substr"] += lc_substr / divider
                        file_sim_scores["lc_subseq"] += lc_subseq / divider

            # Text similarity calculations
            title_sim = self._calculate_text_similarity(new_issue.title, old_issue.title)
            desc_sim = 0
            if new_issue.description and old_issue.description:
                desc_sim = self._calculate_text_similarity(new_issue.description, old_issue.description)

            # Activeness calculation
            activeness = self.activeness_calculator.calculate_developer_activeness(new_issue, old_issue)

            # Return values so they can be aggregated later
            results = {
                "lcp": file_sim_scores["lcp"],
                "lcs": file_sim_scores["lcs"],
                "lc_substr": file_sim_scores["lc_substr"],
                "lc_subseq": file_sim_scores["lc_subseq"],
                "title_sim": title_sim,
                "desc_sim": desc_sim,
                "activeness": activeness
            }

            rows.append(results)

        return rows

    def __calculate_scores(self, new_issue, date_window=0):
        all_scores = []

        # Create a DataFrame with developer names for results
        developers_df = self.all_developers_df.toPandas()
        results_data = []

        # Get developers and their resolved issues more efficiently
        for developer_row in self.all_developers:
            developer_login = developer_row[0]

            # Create date window filter efficiently
            if date_window == 0:
                date_filter = (col("closed_date") < new_issue.created_date)
            else:
                start_date = new_issue.created_date - timedelta(days=date_window)
                date_filter = (col("closed_date") < new_issue.created_date) & (col("closed_date") > start_date)

            # Get all issues resolved by this developer within date window
            dev_issues_df = self.all_issues_df.filter(
                date_filter &
                array_contains(split(col("resolvers"), ";"), developer_login)
            )

            # Skip if no resolved issues
            if dev_issues_df.count() == 0:
                continue

            issue_scores = self._process_developer_resolved_issues(new_issue, dev_issues_df)

            # Aggregate all scores for this developer
            if issue_scores:
                lcp_sum = sum(score["lcp"] for score in issue_scores)
                lcs_sum = sum(score["lcs"] for score in issue_scores)
                lc_substr_sum = sum(score["lc_substr"] for score in issue_scores)
                lc_subseq_sum = sum(score["lc_subseq"] for score in issue_scores)
                title_sim_sum = sum(score["title_sim"] for score in issue_scores)
                desc_sim_sum = sum(score["desc_sim"] for score in issue_scores)
                activeness_sum = sum(score["activeness"] for score in issue_scores)

                # Add to results
                results_data.append({
                    'new_issue_id': new_issue.issue_id,
                    'developer': developer_login,
                    'lcp': lcp_sum,
                    'lcs': lcs_sum,
                    'lc_substr': lc_substr_sum,
                    'ls_subseq': lc_subseq_sum,
                    'cos_title': title_sim_sum,
                    'cos_description': desc_sim_sum,
                    'activeness': activeness_sum,
                    'file_similarity': lcp_sum + lcs_sum + lc_substr_sum + lc_subseq_sum,
                    'text_similarity': title_sim_sum + desc_sim_sum
                })

        # Convert results to DataFrame
        if results_data:
            return pd.DataFrame(results_data)
        else:
            return pd.DataFrame(columns=[
                'new_issue_id', 'developer', 'lcp', 'lcs', 'lc_substr', 'ls_subseq',
                'cos_title', 'cos_description', 'activeness', 'file_similarity', 'text_similarity'
            ])

    def __calculate_scores_for_all_issues(self, limit, date_window=0):
        # Get issues to process
        query = f"""
            SELECT issue_id, creator_login_id, created_date, closed_date, closed_by, commenters, title,
                   description, files, resolvers
            FROM issue
            LIMIT {limit}
        """
        all_issues = self.spark.sql(query)
        issue_count = all_issues.count()

        # Process issues in batches for better memory management
        batch_size = min(50, issue_count)  # Adjust batch size as needed
        all_rows = []

        for i in range(0, issue_count, batch_size):
            batch_issues = all_issues.limit(batch_size).offset(i)
            batch_rows = []

            for issue in batch_issues.collect():
                new_issue = InfoPopulatedIssue(issue)
                issue_rows = self.__calculate_scores(new_issue, date_window)
                batch_rows.append(issue_rows)
                logging.info(f"Scores calculated for: {date_window}_{new_issue.issue_id}")

            # Combine batch results
            if batch_rows:
                batch_df = pd.concat(batch_rows, ignore_index=True)
                all_rows.append(batch_df)

        # Combine all batches
        if all_rows:
            final_df = pd.concat(all_rows, ignore_index=True)
            final_df.to_csv(f"{date_window}_{self.repo_name}_all_resolvers_scores_for_each_test_pr.csv", index=False)
            return final_df
        else:
            return pd.DataFrame()

    @staticmethod
    def __standardize_score(score, min_val, max_val):
        if pd.isna(score) or (max_val - min_val) == 0:
            return 0
        return ((score - min_val) * 100) / (max_val - min_val)

    def __add_standard_scores_to_data_frame(self, main_df):
        # Handle empty dataframe case
        if main_df.empty:
            return main_df

        # Fill NaN values with 0 to avoid issues
        main_df = main_df.fillna(0)

        # Use vectorized operations instead of apply
        act_min = main_df['activeness'].min()
        act_max = main_df['activeness'].max()
        file_sim_min = main_df['file_similarity'].min()
        file_sim_max = main_df['file_similarity'].max()
        txt_sim_min = main_df['text_similarity'].min()
        txt_sim_max = main_df['text_similarity'].max()

        # Use vectorized operations for standardization
        if act_max != act_min:
            main_df['std_activeness'] = (main_df['activeness'] - act_min) * 100 / (act_max - act_min)
        else:
            main_df['std_activeness'] = 0

        if file_sim_max != file_sim_min:
            main_df['std_file_similarity'] = (main_df['file_similarity'] - file_sim_min) * 100 / (file_sim_max - file_sim_min)
        else:
            main_df['std_file_similarity'] = 0

        if txt_sim_max != txt_sim_min:
            main_df['std_text_similarity'] = (main_df['text_similarity'] - txt_sim_min) * 100 / (txt_sim_max - txt_sim_min)
        else:
            main_df['std_text_similarity'] = 0

        return main_df

    def generate_ranked_list(self, data_frame, alpha, beta, gamma):
        logging.info("Generating ranked list started")

        # Skip ranking if dataframe is empty
        if data_frame.empty:
            return data_frame

        # Add rankings efficiently (replacing the individual ranking calls)
        data_frame = self.__add_standard_scores_to_data_frame(data_frame)

        # Calculate combined score using vectorized operations
        data_frame['combined_score'] = (
            data_frame['std_file_similarity'] * alpha +
            data_frame['std_text_similarity'] * beta +
            data_frame['std_activeness'] * gamma
        )

        # Rank efficiently
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
            autoba_processor=self,
            limit=limit,
            main_data_frame=main_df
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
        logging.info(f"Setting weights for factors finished. alpha: {alpha} beta: {beta} gamma: {gamma}")
        return True

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
        logging.info(f"Getting details for issue {issue_id} started")

        # Use DataFrame operations instead of SQL for better performance
        result = self.all_issues_df.filter(col("issue_id") == issue_id).limit(1)

        if result.count() == 0:
            logging.error(f"Issue {issue_id} not found")
            return None

        details = result.collect()[0]
        logging.info(f"Details for issue {issue_id} presented")
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
        logging.info(f"Getting related developers by issue details for issue {issue_id} started")

        try:
            created_date_time = datetime.strptime(created_date_time, '%Y-%m-%dT%H:%M:%SZ')
            closed_date_time = datetime.strptime(closed_date_time, '%Y-%m-%dT%H:%M:%SZ')
        except ValueError as e:
            logging.error(f"Date parsing error: {e}")
            return pd.DataFrame()

        issue_data = [issue_id, creator_login_id, created_date_time, closed_date_time, closed_by, commenters,
                     title, description, files, resolvers]
        new_issue = InfoPopulatedIssue(issue_data)
        df = self.__calculate_scores(new_issue, self.date_window)

        if df.empty:
            logging.warning(f"No scores calculated for issue {issue_id}")
            return df

        ranked_df = self.generate_ranked_list(df, self.alpha, self.beta, self.gamma)
        sorted_ranked_data_frame = ranked_df.sort_values('final_rank', ascending=True)
        ranked_five_df = sorted_ranked_data_frame[sorted_ranked_data_frame['final_rank'] <= 5]

        logging.info(f"Top five developers for issue {issue_id} presented")
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
        logging.info(f"Getting related developers by issued id for Issue {issue_id} started")
        issue_details = self.get_issue_details(issue_id)

        if issue_details is None:
            logging.error(f"Issue {issue_id} not found")
            return pd.DataFrame()

        new_issue = InfoPopulatedIssue(issue_details)
        df = self.__calculate_scores(new_issue, self.date_window)

        if df.empty:
            logging.warning(f"No scores calculated for issue {issue_id}")
            return df

        ranked_df = self.generate_ranked_list(df, self.alpha, self.beta, self.gamma)
        sorted_ranked_data_frame = ranked_df.sort_values('final_rank', ascending=True)
        ranked_five_df = sorted_ranked_data_frame[sorted_ranked_data_frame['final_rank'] <= 5]

        logging.info(f"Top five developers for issue {issue_id} presented")
        return ranked_five_df
