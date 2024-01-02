import requests
headers = {
"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36",
}

params = {
"to":"2021-11-29T09:15:00.000Z",
"limit":"321",
"lpAddress":"0xd8b6A853095c334aD26621A301379Cc3614f9663",
"interval":"1m",
"baseLp":"0x58F876857a02D6762E0101bb5C46A8c1ED44Dc16"
}

response = requests.get("https://api2.poocoin.app/candles-bsc", params=params, headers=headers)

# whole response from API call for a particular token (i believe)
# some data needs to be adjusted (open/close price, etc.)
for result in response:
    count = result["count"]
    time = result["time"]
    open_price = result["open"]
    close_price = result["close"]
    high = result["high"]
    low = result["low"]
    volume = result["volume"]
    base_open = result["baseOpen"]
    base_close = result["baseClose"]
    base_high = result["baseHigh"]
    base_low = result["baseLow"]

    print(f"{count}\n"
    F"{_time}\n"
    F"{open_price}\n"
    F"{close_price}\n"
    F"{high}\n"
    f"{low}\n" 
    F"{volume}\n"
    F"{base_open}\n"
    f"{base_close}\n" 
    F"{base_high}\n"
    f"{base_low}\n")
