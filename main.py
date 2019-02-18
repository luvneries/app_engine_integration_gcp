from flask import Flask, request
import os
import logging
import launch_dataflow_template as launch_dataflow_template							#importing the launch dataflow script

project_id = os.getenv('GOOGLE_CLOUD_PROJECT')


app = Flask(__name__)

@app.route('/launch_dataflow_template/skyhook_visits')                                                # maps directly maps to url that handles request in cron.yaml file
def start_skyhook_visits_dataflow_template():

	dataflow_template_location = "gs://dataprep-workflows/austin_crimes/temp"
	dataflow_job_name = "austin_crimes_dataflow"
	input_location = "{\"location1\":\"mlongcp:staging.crimes\"}"
	output_location = "{\"location1\":\"target.crimes_categories\"}"
	gcs_path = "gs://dataprep-workflows/austin_crimes/temp/cloud-dataprep-austin-crimes-1658414-by-mlongcp_template"
	
	is_cron = request.headers.get('X-Appengine-Cron', False)
	if not is_cron:
		return 'Bad Request', 400

	try:
		#executing main method within launch_datafow_template
		launch_dataflow_template.run(project_id, dataflow_job_name, gcs_path, dataflow_template_location, input_location, output_location)
		return "Pipeline started", 200
	except Exception as e:
		logging.exception(e)
		return "Error: <pre>{}</pre>".format(e), 500


@app.errorhandler(500)                                                              #error handling script for troubleshooting
def server_error(e):
	logging.exception('An error occurred during a request.')
	return """
	An internal error occurred: <pre>{}</pre>
	See logs for full stacktrace.
	""".format(e), 500

if __name__ == '__main__':                                                          #hosting administration syntax
	app.run(host='127.0.0.1', port=8080, debug=True)