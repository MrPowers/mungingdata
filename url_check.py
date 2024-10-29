import requests

with open("data.txt") as file:
    for line in file:
        # url = line.replace("https://mungingdata.com", "http://127.0.0.1:8000").rstrip('\r\n')
        url = line.rstrip('\r\n')
        x = requests.get(url)
        if x.status_code != 200:
            print(x.status_code)
            print(url)
