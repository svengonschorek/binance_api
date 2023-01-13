from binance.client import Client, BinanceAPIException

import os
binance_api_key = os.getenv('BINANCE_API_KEY')
binance_api_secret = os.getenv('BINANACE_API_SECRET')

def main():
    # get binance data from binance API
    binance_client = Client(binance_api_key, binance_api_secret)

    # get wallet data
    try:
        # get binance server time
        time_res = binance_client.get_server_time()

        # get all prices
        prices = binance_client.get_symbol_ticker()

        # get price eur to busd
        eur_busd = binance_client.get_symbol_ticker(symbol="EURBUSD")

        # get account data
        info = binance_client.get_account()
        assets = info['balances']

        wallet = []
        for asset in assets:
            quantity = float(asset["free"]) + float(asset["locked"])
            if quantity > 0:
                if asset["asset"] == "BUSD":
                    price_in_busd = 1
                else:
                    for price in prices:
                        if price["symbol"] == asset["asset"] + "BUSD":
                            price_in_busd = float(price["price"])

                amount_in_busd = (float(asset["free"]) + float(asset["locked"])) * price_in_busd
                amount_in_eur = amount_in_busd / float(eur_busd["price"])

                wallet_asset = {
                    "ts": time_res["serverTime"],
                    "asset": asset["asset"],
                    "quantity": float(asset["free"]) + float(asset["locked"]),
                    "amount_busd": amount_in_busd,
                    "amount_eur": amount_in_eur
                }

                wallet.append(wallet_asset)

    except BinanceAPIException as e:
        print(e.status_code)
        print(e.message)

    print(wallet)

if __name__ == "__main__":
    main()