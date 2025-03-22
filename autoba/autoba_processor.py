"""
autoba_processor.py (Optimized)
=====================================
The core of the AutoBA system - Optimized for execution time and performance
"""
import os
import logging
from datetime import timedelta, datetime

import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, trim, col
from pyspark import SparkContext

import autoba_config as acfg
from autoba.accuracy_calculation.accuracy_calculation import AccuracyCalculator
from autoba.activeness.developer_activeness import ActivenessCalculator
from autoba.entities.developer import Developer
from autoba.entities.info_populated_issue import InfoPopulatedIssue
from autoba.string_compare.file_path_similarity import FilePathSimilarityCalculator
from autoba.text_similarity.text_similarity import TextSimilarityCalculator


class AutoBAProcessor:
    def __init__(self):
        self.repo_name = acfg.repo['name']
        self.spark = None
        self.all_issues_df = None
        self.all_developers_df = None
        self.all_developers = None
        self.broadcast_developers = None
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

        logging.basicConfig(level=logging.INFO, filename='app.log', format='%(asctime)s-%(name)s-%(levelname)s - %(message)s')
        logging.info("AutoBA Processor created")

    def __initialise_app(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_dir, "dataset", "updated-issue-details.csv")

        self.spark = SparkSession.builder.master('local').appName("AutoBA").getOrCreate()
        raw_issues_df = self.spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)

        self.all_issues_df = raw_issues_df.filter((col("resolvers").isNotNull()) & (col("resolvers") != "")).cache()
        self.all_developers_df = self.all_issues_df.select(explode(split("resolvers", ";")).alias("developer")).withColumn("developer", trim("developer")).dropDuplicates().cache()

        self.all_issues_df.createOrReplaceTempView("issue")
        self.all_developers_df.createOrReplaceTempView("developer")

        self.all_developers = self.all_developers_df.toPandas()['developer'].tolist()
        self.broadcast_developers = SparkContext.getOrCreate().broadcast(self.all_developers)

        self.issue_count = self.all_issues_df.count()
        self.developer_count = self.all_developers_df.count()

    def __calculate_scores(self, new_issue, date_window=0):
        rows = []
        new_file_parts = [fp.split("/") for fp in new_issue.files]

        for developer_login in self.broadcast_developers.value:
            issue_resolver = Developer(developer_login)

            start_date_clause = "" if date_window == 0 else f"AND closed_date > timestamp('{new_issue.created_date - timedelta(days=date_window)}')"
            query = f"""
                SELECT * FROM issue
                WHERE closed_date < timestamp('{new_issue.created_date}')
                  {start_date_clause}
                  AND array_contains(split(resolvers, '; '), '{developer_login}')
            """

            resolved_issues = self.spark.sql(query).collect()

            for resolved in resolved_issues:
                old_issue = InfoPopulatedIssue(resolved)

                for old_fp in old_issue.files:
                    old_parts = old_fp.split("/")
                    combinations = len(old_issue.files) * len(new_issue.files)
                    if not new_file_parts:
                        continue  # Skip similarity calc if new issue has no files

                    try:
                        max_len = max([len(old_parts)] + [len(fp) for fp in new_file_parts])
                    except ValueError:
                        continue  # If somehow both are empty, skip
                    divider = max_len * combinations

                    for new_parts in new_file_parts:
                        issue_resolver.longest_common_prefix_score += self.file_path_similarity_calculator.longest_common_prefix_similarity("/".join(new_parts), old_fp) / divider
                        issue_resolver.longest_common_suffix_score += self.file_path_similarity_calculator.longest_common_suffix_similarity("/".join(new_parts), old_fp) / divider
                        issue_resolver.longest_common_sub_string_score += self.file_path_similarity_calculator.longest_common_sub_string_similarity("/".join(new_parts), old_fp) / divider
                        issue_resolver.longest_common_sub_sequence_score += self.file_path_similarity_calculator.longest_common_sub_sequence_similarity("/".join(new_parts), old_fp) / divider

                issue_resolver.issue_description_similarity += self.text_similarity_calculator.cos_similarity(new_issue.title, old_issue.title)
                if new_issue.description and old_issue.description:
                    issue_resolver.issue_description_similarity += self.text_similarity_calculator.cos_similarity(new_issue.description, old_issue.description)

                issue_resolver.activeness += self.activeness_calculator.calculate_developer_activeness(new_issue, old_issue)

            file_sim = issue_resolver.longest_common_prefix_score + issue_resolver.longest_common_suffix_score + \
                       issue_resolver.longest_common_sub_string_score + issue_resolver.longest_common_sub_sequence_score
            text_sim = issue_resolver.issue_title_similarity + issue_resolver.issue_description_similarity

            rows.append({
                'new_issue_id': new_issue.issue_id,
                'developer': developer_login,
                'lcp': issue_resolver.longest_common_prefix_score,
                'lcs': issue_resolver.longest_common_suffix_score,
                'lc_substr': issue_resolver.longest_common_sub_string_score,
                'ls_subseq': issue_resolver.longest_common_sub_sequence_score,
                'cos_title': issue_resolver.issue_title_similarity,
                'cos_description': issue_resolver.issue_description_similarity,
                'activeness': issue_resolver.activeness,
                'file_similarity': file_sim,
                'text_similarity': text_sim
            })

        return rows

    def __calculate_scores_for_all_issues(self, limit, date_window=0):
        issues_query = f"SELECT * FROM issue LIMIT {limit}"
        issues = self.spark.sql(issues_query).toPandas().itertuples(index=False)

        all_rows = []
        for issue_row in issues:
            new_issue = InfoPopulatedIssue(issue_row)
            scores = self.__calculate_scores(new_issue, date_window)
            all_rows.extend(scores)
            logging.info(f"Scores calculated for issue {new_issue.issue_id}")

        df = pd.DataFrame(all_rows)
        df.to_csv(f"{date_window}_{self.repo_name}_all_resolvers_scores_for_each_test_pr.csv", index=False)
        return df

    @staticmethod
    def __standardize_score(score, min_val, max_val):
        return 0 if max_val == min_val else ((score - min_val) * 100) / (max_val - min_val)

    def __add_standard_scores_to_data_frame(self, df):
        for col_name in ['activeness', 'file_similarity', 'text_similarity']:
            min_val, max_val = df[col_name].min(), df[col_name].max()
            df[f'std_{col_name}'] = df[col_name].apply(self.__standardize_score, args=(min_val, max_val))
        return df

    def generate_ranked_list(self, df, alpha, beta, gamma):
        logging.info("Generating ranked list")
        self.file_path_similarity_calculator.add_file_path_similarity_ranking(df)
        self.text_similarity_calculator.add_text_similarity_ranking(df)
        self.activeness_calculator.add_activeness_ranking(df)

        df = self.__add_standard_scores_to_data_frame(df)
        df['combined_score'] = alpha * df['std_file_similarity'] + beta * df['std_text_similarity'] + gamma * df['std_activeness']
        df.sort_values('combined_score', ascending=False, inplace=True)
        df['final_rank'] = range(1, len(df) + 1)
        return df

    def calculate_scores_and_get_weight_combinations_for_factors(self, limit):
        logging.info("Starting score calculation and weight optimization")
        df = self.__calculate_scores_for_all_issues(limit)
        return self.get_weight_combinations_for_factors(limit, df, use_csv_file=False)

    def get_weight_combinations_for_factors(self, limit, main_data_frame=None, main_data_csv_file_name=None, use_csv_file=False):
        if use_csv_file:
            df = pd.read_csv(main_data_csv_file_name) if main_data_csv_file_name else None
        else:
            df = main_data_frame
        return self.accuracy_calculator.test_weight_combination_accuracy_for_all_issues(self, limit, main_data_frame=df)
