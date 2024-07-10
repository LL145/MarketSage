from datetime import datetime, time, timedelta
import pytz

def get_ny_date_without_timezone():
  new_york_timezone = pytz.timezone('America/New_York')
  now_ny = datetime.now(new_york_timezone)
  target_date = now_ny if now_ny.time() >= time(16, 0) else now_ny - timedelta(days=1)
  target_date_without_tz = datetime.combine(target_date.date(), time(23, 0))
  while target_date_without_tz.weekday() >= 5:
      target_date_without_tz -= timedelta(days=1)
  return target_date_without_tz