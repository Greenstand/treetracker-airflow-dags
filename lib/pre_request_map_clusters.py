

# def function
from datetime import datetime, timedelta
from lib.pre_request import pre_request


def pre_request_map_clusters(url_prefix):
  print("do pre request job:")
  def request(begin_zoom_level, end_zoom_level, query_string):
      for zoom_level in range(begin_zoom_level, end_zoom_level + 1):
          # url=http://treetracker-tile-server.tile-server.svc.cluster.local/${i}/1/1.png
          url = f"{url_prefix}/{zoom_level}/1/1.png?{query_string}"
          print(f"request: {url}")
          begin_time = datetime.now()
          pre_request(url)
          end_time = datetime.now()
          print(f"request: took {end_time - begin_time}")
  request(2,15, "")
  request(2,15, "map_name=freetown")
  request(2,15, "map_name=TheHaitiTreeProject")
  request(2,15, "wallet=FinorX")