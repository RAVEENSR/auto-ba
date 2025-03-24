"""
main.py
====================================
The interface module of AutoBA
"""

import logging

from flask import Flask
from flask import request
from flask.json import jsonify
from flask_cors import CORS
from gevent.pywsgi import WSGIServer

from autoba.autoba_processor import AutoBAProcessor

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO, filename='app.log', format='%(asctime)s-%(name)s-%(levelname)s - %(message)s')
logging.Formatter("%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S")
autoba = AutoBAProcessor()
navbar_info = {'repository': autoba.repo_name,
               'pr_count': autoba.issue_count,
               'integrator_count': autoba.developer_count}

@app.route('/get_issue_count', methods=['POST', 'GET'])
def get_pr_count():
    result_object = autoba.issue_count
    logging.info("Issue count checked")
    return jsonify(count=result_object), 200


@app.route('/integrators')
def api_get_integrators():
    developer_list = []
    for row in autoba.all_developers:
        developer_list.append({'name': row['developer_login']})
    logging.info("Integrator Results Served")
    return jsonify(integrators=developer_list), 200


@app.route('/set_weight_factors', methods=['POST'])
def api_set_weights():
    content = request.json
    alpha = content['alpha']
    beta = content['beta']
    gamma = content['gamma']
    autoba.set_weight_combination_for_factors(alpha=float(alpha), beta=float(beta), gamma=float(gamma))
    logging.info("Weights have been set: alpha:" + alpha + " beta: " + beta + " gamma: " + gamma)
    response = app.response_class(status=200)
    return response


@app.route('/get_weight_combination_accuracy', methods=['POST'])
def api_get_weight_accuracy():
    content = request.json
    limit = content['limit']
    result_object = autoba.calculate_scores_and_get_weight_combinations_for_factors(limit=int(limit))
    logging.info("Weight Combination Accuracy Results Served")
    return jsonify(result=result_object), 200


@app.route('/get_weight_accuracy_by_file', methods=['POST'])
def api_get_weight_accuracy_by_file():
    content = request.json
    limit = content['limit']
    file_name = request.form['file_name']
    result_object = autoba.get_weight_combinations_for_factors(limit=int(limit),
                                                                main_data_csv_file_name=file_name, use_csv_file=True)
    logging.info("Weight Combination Accuracy by File Results Served")
    return jsonify(result=result_object), 200


@app.route('/get_weight_accuracy_by_file_with_individual_factor_accuracy', methods=['POST'])
def api_get_weight_accuracy_by_file_with_individual_factor_accuracy():
    content = request.json
    limit = content['limit']
    file_name = request.form['file_name']
    result_object = autoba.get_weight_combinations_for_factors_with_individual_factors(limit=int(limit),
                                                                main_data_csv_file_name=file_name, use_csv_file=True)
    logging.info("Weight Combination Accuracy by File Results Served")
    return jsonify(result=result_object), 200


@app.route('/find_issue_developers', methods=['POST', 'GET'])
def api_find_pr_integrators():
    if request.method == 'POST':
        content = request.json
        issue_id = content['issue_id']
        creator_login_id = content['creator_login_id']
        created_date_time = content['created_date_time']
        title = content['title']
        description = content['description']

        ranked_five_df = autoba.get_related_developers_for_issue(issue_id=issue_id, creator_login_id=creator_login_id,
                                                                 created_date_time=created_date_time, title=title,
                                                                 description=description)
    else:
        issue_id = request.args['prId']
        ranked_five_df = autoba.get_related_developers_for_issue_by_issue_id(issue_id)

    rec_developers = []
    for row in ranked_five_df.itertuples(index=False):
        developer_object = {'rank': int(row.final_rank),
                             'username': row.developer,
                             'f_score': "{0:.2f}".format(row.combined_score),
                             'fp_score': "{0:.2f}".format(row.std_file_similarity),
                             't_score': "{0:.2f}".format(row.std_text_similarity),
                             'a_score': "{0:.2f}".format(row.std_activeness)}
        rec_developers.append(developer_object)
    logging.info("Recommended Developers for PR served")
    return jsonify(developers=rec_developers), 200


if __name__ == '__main__':
    # creating the server
    # http_server = WSGIServer(('', 5000), app)
    # logging.info("Server started")
    # http_server.serve_forever()
    result = autoba.issue_count
    logging.info(result)
    result2 = autoba.calculate_scores_and_get_weight_combinations_for_factors(100000)
    logging.info("Weight Combination Accuracy Results Served")
    print(result2)
