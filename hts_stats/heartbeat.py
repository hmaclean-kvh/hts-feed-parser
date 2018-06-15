import requests

def send_hb():
  hb_config = {}
  hb_config['origin'] = "HTS Terminal Stats Processor"
  hb_config['timeout'] = 300
  hb_config['tags'] = ["Mclean,Docker,dev_hts_bot"]
  r = requests.post("http://192.168.220.35:8080/api/heartbeat",json=hb_config,timeout=10)
