import googleapiclient.discovery
from google.oauth2 import service_account

#function to execute a static dataflow template residing on GCS bucket
def run(project_id, job_name, gcs_path, template_location, input_locations, output_locations):

	SERVICE_ACCOUNT_FILE = 'dataflow_launch_private-key.json'

	credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)

	dataflow_service = googleapiclient.discovery.build('dataflow', 'v1b3', credentials=credentials)

	body = {
		"jobName": "{jobname}".format(jobname=job_name),
		"parameters": {
			"customGcsTempLocation": "{template_loc}".format(template_loc=template_location),
			"inputLocations" : "{input_loc}".format(input_loc=input_locations),
			"outputLocations": "{output_loc}".format(output_loc=output_locations)
		}				
	}

	request = dataflow_service.projects().templates().launch(projectId=project_id, gcsPath=gcs_path, body=body).execute()