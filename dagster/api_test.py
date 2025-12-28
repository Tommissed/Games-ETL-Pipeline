import requests
import pandas as pd

api_key = "ffaf1b523fdb49ee99baef1a38cad440"
page_size = 40
page = 1
tags = []

while True:
    url = "https://api.rawg.io/api/tags"
    params = {"key": api_key, "ordering": "added", "page": page, "page_size": page_size}
    r = requests.get(url, params=params)
    r.raise_for_status()
    r = r.json()
    results = r.get("results", [])
    tags.extend(results)
    print(len(results))
    print(page)
    page += 1
    if not r.get("next") or page == 2:
        break

print(tags)
df = pd.json_normalize(results)
# print(df)

# print (json.dumps(r.json(), indent=4))
# df = pd.json_normalize(r.json())
# print(df['results'][0])
