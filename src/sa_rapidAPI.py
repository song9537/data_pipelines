import requests


def news_trending():
    url = "https://seeking-alpha.p.rapidapi.com/news/v2/list-trending"

    querystring = {"size":"20"}

    headers = {
        "X-RapidAPI-Key": "c06a309b0emsha916df6ffec78f2p10f86ejsn882ce429c9ae",
        "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    print(response.json())


def list_by_symbol():
    url = "https://seeking-alpha.p.rapidapi.com/news/v2/list-by-symbol"

    querystring = {"id":"tsla","size":"20","number":"1"}

    headers = {
        "X-RapidAPI-Key": "c06a309b0emsha916df6ffec78f2p10f86ejsn882ce429c9ae",
        "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    print(response.json())


def main():
    news_trending()
    list_by_symbol()


if __name__ == '__main__':
    main()
