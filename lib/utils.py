import datetime


def print_time(message):
  """Prints a message with the current time"""
  current_time = datetime.datetime.now()
  print(f"{message} at {current_time}!!!!")

# export the function
__all__ = [
    'print_time',
]