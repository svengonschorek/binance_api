import os
import time

from binance.client import Client, BinanceAPIException

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

binance_api_key = os.getenv('BINANCE_API_KEY')
binance_api_secret = os.getenv('BINANACE_API_SECRET')

binance_client = Client(binance_api_key, binance_api_secret)
bigquery_client = bigquery.Client()

def check_table(project_id, dataset_id, table_id):
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = bigquery.TableReference(dataset_ref, table_id)
    try:
        bigquery_client.get_table(table_ref)
        return True
    except NotFound:
        pass

def check_dataset(project_id, dataset_id):
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    try:
        bigquery_client.get_dataset(dataset_ref)
        return True
    except NotFound:
        return False

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

    # write data to google bigquery
    # -----------------------------------------------------------
    dataset_name = "raw_data"
    table_name = "daily_wallet_export"

    if not check_dataset(bigquery_client.project, dataset_name):
        dataset_id = "{}.{}".format(bigquery_client.project, dataset_name)
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "europe-west3"
        dataset = bigquery_client.create_dataset(dataset, timeout=30)
        print("Created dataset {}.{}".format(bigquery_client.project, dataset.dataset_id))
    else:
        dataset = bigquery_client.dataset(dataset_name, project=bigquery_client.project)

    if not check_table(bigquery_client.project, dataset_name, table_name):
        table_id = "{}.{}.{}".format(bigquery_client.project, dataset_name, table_name)
        schema = [
            bigquery.SchemaField("ts", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("asset", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("quantity", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("amount_busd", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("amount_eur", "FLOAT", mode="REQUIRED")
        ]
    
        table = bigquery.Table(table_id, schema=schema)
        table = bigquery_client.create_table(table)  # Make an API request.
        print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
    else:
        table = bigquery_client.get_table(dataset.table(table_name))

    max_retries = 5
    tries = 0
    errors = True
    while errors:
        tries += 1
        try:
            result = bigquery_client.insert_rows_json(table, wallet)
            if result == []:
                errors = False
                print("New rows have been added.")
        except Exception as result:
            err_str = result
            if tries <= max_retries:
                print("Try no. {} failed to insert data.".format(tries))
            else:
                print(err_str)
                print("Max number of retries ({}) reached. Execution will be stopped.".format(max_retries))
                break
            time.sleep(1)
            pass

if __name__ == "__main__":
    main()