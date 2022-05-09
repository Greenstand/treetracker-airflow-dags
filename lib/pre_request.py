
import io
import requests
import os

def pre_request(url):
    print("pre_request:", url)
    
    # request the url and print the response, catch any errors
    try:
        # set timeout to 60 seconds
        response = requests.get(url, timeout=600)
        # print("response:", response)
        print("response.status_code:", response.status_code)
        # check the response status code, if not 200, raise an error
        if response.status_code != 200:
            print("response:", response)
            print("response.status_code:", response.status_code)
            raise Exception("response status code is not 200")
    # catch any errors
    except Exception as e:
        print("get error when exec request:", e)
        raise e
        
        

    