class ActivenessCalculator:
    """
    This class handles developer activeness score calculations related tasks.

    :param const_lambda: constant used to calculate the activeness of a developer.
    :type const_lambda: int
    """
    def __init__(self, const_lambda=-1):
        # time_decaying_parameter
        self.const_lambda = const_lambda

    def calculate_developer_activeness(self, new_issue, old_issue):
        # calculate activeness of the integrator
        activeness = new_issue.created_date - old_issue.closed_date
        if hasattr(activeness, 'days'):
            activeness = activeness.days
        else:
            activeness = 0
        if activeness > 0:
            activeness = activeness ** self.const_lambda

        return activeness

    @staticmethod
    def add_activeness_ranking(data_frame):
        data_frame["activeness_rank"] = data_frame["activeness"].rank(method='min', ascending=False)
