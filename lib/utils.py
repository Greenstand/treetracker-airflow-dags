import datetime


def print_time(message):
  """Prints a message with the current time"""
  current_time = datetime.datetime.now()
  print(f"{message} at {current_time}!!!!")

# export the function
__all__ = [
    'print_time',
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