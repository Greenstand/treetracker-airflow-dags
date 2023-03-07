import datetime
import requests
from airflow.models import Variable


def print_time(message):
  """Prints a message with the current time"""
  current_time = datetime.datetime.now()
  print(f"{message} at {current_time}!!!!")

def slack_message(message=''):
  """Sends a message to Slack. Please see: https://api.slack.com/messaging/webhooks
      The SLACK_WEBHOOK Airflow variable must be configured in order to use this.
      You can see Airflow variables in the Airflow UI under Admin -> Variables 
  """
  url = Variable.get('SLACK_WEBHOOK')
  myobj = {'text': message}
  x = requests.post(url, json = myobj)

def on_failure_callback(context):
    """Sends a message to Slack if this DAG fails.
        Please see: https://stackoverflow.com/questions/44586356/airflow-failed-slack-message
                    https://codingshower.com/airflow-slack-notifications/"""
    slack_msg = f"""
    :red_circle: Airflow DAG Failed.
    *Task*: {context.get('task_instance').task_id}
    *Dag*: {context.get('task_instance').dag_id}
    *Execution Time*: {context.get('execution_date')}
    *Log Url*: {context.get('task_instance').log_url}
    """
    slack_message(slack_msg)

# export the function
__all__ = [
    'print_time',
    'slack_message',
    'on_failure_callback',
]

# given a date, return the first day and last day of the month
def get_first_last_day_of_month(date):
  """Given a date, return the first day and last day of the month"""
  # get the first day of the month
  start_date = date + "-01"
  # calculate end_date of the month
  import datetime
  # get the last day of the month
  end_date = (datetime.datetime.strptime(start_date, "%Y-%m-%d") + datetime.timedelta(days=31)).strftime("%Y-%m-%d")
  print ("to export data from:", start_date, "to:", end_date)
  return start_date, end_date

def get_start_and_end_date_of_previous_month(date):
  """Given a date, return the first day and last day of the previous month"""
  # check date with regex (yyyy-mm-dd) format
  print ("date:", date)
  import re
  if not re.match(r'^\d{4}-\d{2}-\d{2}$', date):
    raise ValueError(f'date format error: {date}')
  # use regex to get the year and month
  import re
  year_month = re.search(r'^(\d{4})-(\d{2})-\d{2}$', date)
  year = year_month.group(1)
  month = year_month.group(2)
  print ('year_month:', year, month)
  month = int(month) - 1
  if(month < 1):
    month = 12
    year = int(year) - 1

  import calendar
  print ('month:', month)
  print ('year:', year)
  start_end = calendar.monthrange(int(year), int(month))
  print ('start_end:', start_end)

  # get the first day of the month
  start_date = f'{year}-{month}-01'
  end_date = f'{year}-{month}-{start_end[1]}'
  print ("to export data from:", start_date, "to:", end_date)
  return start_date, end_date