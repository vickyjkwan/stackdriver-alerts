import base64
import requests
import json
from datetime import timedelta
from datetime import datetime
from jinja2 import Template
from google.cloud import storage
import re


# this CF takes fivetran logs pub/sub and transform to messages for Slack webhook ingestion
def inject_to_slack(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # define header for slack incoming webhook
    headers = {
        'Content-type': 'application/json',
    }  

    # TO DO: get your webhook url from Slack
    slack_url = f'https://hooks.slack.com/services/{slack_credentials}'
    
    # decode event paylod
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    pubsub_message = json.loads(pubsub_message)
    
    # convert timezone to PST
    ts = datetime.strptime(pubsub_message['receiveTimestamp'][:-4], "%Y-%m-%dT%H:%M:%S.%f")
    new_ts = datetime.strftime(ts + timedelta(hours=-7), "%Y-%m-%dT%H:%M:%S.%f")

    # set up alert message format; this is where you would like to customize your alert message:
    alert_dict = {
        'log_id':  pubsub_message['insertId'],
        'connector_id': pubsub_message['jsonPayload']['connector_id'],
        'connector_type': pubsub_message['jsonPayload']['connector_type'],
        'severity': pubsub_message['severity'],
        'logName': pubsub_message['logName'],
        'receiveTimestamp': new_ts
    }

    if 'data' in pubsub_message['jsonPayload'].keys():
        alert_dict['jsonPayload_data'] = pubsub_message['jsonPayload']['data']

    # only push ERROR or WARNING message to Slack
    if alert_dict['severity'] == "ERROR": # or alert_dict['severity'] == "WARNING"
        # WARNING: payload changes for every severity type. Need to change the keys of alert_dict['jsonPayload_data]

        # set up jinja template
        id_array = alert_dict['logName'].split("/")[-1].split("-")

        pretty_msg = """
            :red_circle: Connector Failed.  
            *Project*: {proj} 
            *Connector Type*: {connType}
            *Connector Schema*: {connSchema}
            *Alert Reason*: {reason}
            *Alert Status*: {status}
            *StackDriver Log ID*: {logId}
            *Received Timestamp*: {receivedAtTimestamp}
            *Severity*: {severity}
            *fivetran Dash URL*: {dash_url}
            """.format(
            proj=id_array[1],
            connType=alert_dict['connector_type'],
            connSchema=alert_dict['connector_id'],
            reason=alert_dict['jsonPayload_data']['reason'],
            status=alert_dict['jsonPayload_data']['status'],
            logId=alert_dict['log_id'],
            receivedAtTimestamp=alert_dict['receiveTimestamp'],
            severity=alert_dict['severity'],
            dash_url=f"https://fivetran.com/dashboard/connectors/{id_array[2]}/{alert_dict['connector_id']}"
            )

        t = Template('{"text": "{{pretty_msg}}"}')
        pretty_payload = t.render(pretty_msg=pretty_msg)

        response = requests.post(slack_url, headers=headers, data=pretty_payload)
        print(response.text)

    else:
        return None


# this CF takes airflow logs that are being stored in Cloud Storage and send to StackDriver only the traceback errors 
def airflow_handler(data, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # define header for slack incoming webhook:
    headers = {
        'Content-type': 'application/json',
    }  
    slack_url = f'https://hooks.slack.com/services/{slack_credentials}'

    # getting blob metadata:
    client = storage.Client()
    bucket = client.get_bucket(format(data['bucket']))
    blob = bucket.get_blob(format(data['name']))

    blob_str = blob.download_as_string()
    new_blob_str = blob_str.decode('utf-8')
    new_blob_list = new_blob_str.split('\n')

    blob_name = blob.name
    log_array = blob_name.split('/')


    # a helper that catches the beginning line number of Traceback and the ending line number of the same Traceback. Catches all.
    def begin_end_trace(msg_list):
        counter = 0
        counter_tuple = []

        for c, l in enumerate(msg_list):
            if 'Traceback' in l and 'INFO' in l:
                b_counter = c
                counter = c

                for sub_c, sub_l in enumerate(msg_list[counter:]):
                    if 'ERROR' in sub_l:
                        e_counter = sub_c + counter
                        counter_tuple.append((b_counter, e_counter))

        return (counter_tuple)  


    # a helper that extracts trace errors
    def get_trace(msg_list):
        trace_dict = {}

        for err_line in msg_list:

            # first get the attempt number:
            if 'Starting attempt' in err_line:
                trace_dict['Attempts'] = err_line.split('-')[-1]
                # only send final failed attempt
                if err_line.split('-')[-1].split(' ')[-3] == err_line.split('-')[-1].split(' ')[-1]:
                    for begin_end_pos in begin_end_trace(msg_list):
                        begin_pos = begin_end_pos[0]
                        end_pos = begin_end_pos[1]
                        trace_dict['Traceback'] = msg_list[begin_pos: end_pos - 2]

        return trace_dict


    # a helper that trims down traceback:
    def trimmer(long_list):
        slim_list = []
        for line in long_list:
            if 'INFO -' in line:
                slim_list.append(line.split("INFO -")[1])
        return slim_list
        

    # checking the logging_mixin output (last line of log before the empty line)
    if 'Task exited with return code 1' in new_blob_list[-2]:

        trace_dict = get_trace(msg_list = new_blob_list)

        ts = datetime.strptime(new_blob_list[0][1:20], "%Y-%m-%d %H:%M:%S")
        new_ts = datetime.strftime(ts + timedelta(hours=-7), "%Y-%m-%dT%H:%M:%S")

        if 'Traceback' in trace_dict:
            # set up alert message format; this is where you would like to customize your alert message:
            alert_dict = {
                'dag_id': log_array[0],
                'task_id': log_array[1],
                'attempts': trace_dict['Attempts'],
                'traceback': trimmer(trace_dict['Traceback']),
                'exec_ts': new_ts
            }

            new_alert_dict = '\n '.join(alert_dict['traceback'])
            newer_alert_dict = new_alert_dict.replace("\"", "'")
            conv_alert_dict = newer_alert_dict.replace('{}', '{{}}')

        pretty_msg = """
            :red_circle: Airflow DAG Failed.  
            *DAG ID*: {dagId} 
            *Task ID*: {taskId}
            *Attempts of Retries*: {attempts}
            *Execution Timestamp*: {execTimestamp}
            *Traceback Details*: 
            {traceback}
            *Severity*: {severity}
            *Airflow DAG Status URL*: {url}
            """.format(
            dagId=alert_dict['dag_id'],
            taskId=alert_dict['task_id'],
            attempts=alert_dict['attempts'],
            execTimestamp=alert_dict['exec_ts'],
            traceback=conv_alert_dict,
            severity='ERROR',
            # pass your airflow domain plus port
            url = f"http://{airflow_domain_port}/admin/airflow/log?task_id={alert_dict['task_id']}&dag_id={alert_dict['dag_id']}&execution_date={log_array[2].replace(':', '%3A').replace('+', '%2B')}&format=json")

        t = Template('{"text": "{{pretty_msg}}"}')
        pretty_payload = t.render(pretty_msg=pretty_msg)
        response = requests.post(slack_url, headers=headers, data=pretty_payload)
        print(response.text)


# this CF takes airflow server error logs around gs handler 404, and transform to messages for Slack 
def sys_error(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
        # define header for slack incoming webhook
    headers = {
        'Content-type': 'application/json',
    }  
    slack_url = f'https://hooks.slack.com/services/{slack_credentials}'
    
    # decode event paylod
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    pubsub_message = json.loads(pubsub_message)
    
    # convert timezone to PST
    ts = datetime.strptime(pubsub_message['receiveTimestamp'][:-4], "%Y-%m-%dT%H:%M:%S.%f")
    new_ts = datetime.strftime(ts + timedelta(hours=-7), "%Y-%m-%dT%H:%M:%S.%f")

    # set up alert message format; this is where you would like to customize your alert message:
    alert_dict = {
        'log_id': pubsub_message['insertId'],
        'labels': pubsub_message['labels']['compute.googleapis.com/resource_name'],
        'logName': pubsub_message['logName'],
        'receiveTimestamp': new_ts,
        'textPayload': pubsub_message['textPayload'].replace('{', '{{').replace('}', '}}').replace('"', "'").replace('\\', ' ')
    }

    pretty_msg = """
        :red_circle: Airflow GCS Handler experienced HTTP 404 Error.  
        *Log ID*: {logId} 
        *Labels*: {labels}
        *Log Name*: {logName}
        *Received Timestamp*: {receivedAtTimestamp}
        *Error Message*: {logPayload}
        """.format(
        logId=alert_dict['log_id'],
        labels=alert_dict['labels'],
        logName=alert_dict['logName'],
        receivedAtTimestamp=alert_dict['receiveTimestamp'],
        logPayload=alert_dict['textPayload']
    )

    t = Template('{"text": "{{pretty_msg}}"}')
    pretty_payload = t.render(pretty_msg=pretty_msg)

    response = requests.post(slack_url, headers=headers, data=pretty_payload)
    print(response.text)
    