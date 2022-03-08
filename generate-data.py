#!/usr/bin/python3
import random

class WeightedChoice(object):
    def __init__(self, weights):
        """Pick items with weighted probabilities.
             https://stackoverflow.com/a/1556403/4504053

            weights
                a sequence of tuples of item and it's weight.
        """
        self._total_weight = 0.
        self._item_levels = []
        for item, weight in weights:
            self._total_weight += weight
            self._item_levels.append((self._total_weight, item))
        assert self._total_weight == 1, f"total weighted chance across all item must equal to 1.0"

    def pick(self):
        pick = self._total_weight * random.random()
        for level, item in self._item_levels:
            if level >= pick:
                return item

# Initialize distribution pool

merchantProbabilities = [
    ("merchant-1", 0.8), 
    ("merchant-2", 0.1), 
    ("merchant-3", 0.05),
    ("merchant-4", 0.03),
    ("merchant-5", 0.001),
    ("merchant-6", 0.001),
    ("merchant-7", 0.001),
    ("merchant-8", 0.001),
    ("merchant-9", 0.001),
    ("merchant-10", 0.001),
    ("merchant-11", 0.001),
    ("merchant-12", 0.001),
    ("merchant-13", 0.001),
    ("merchant-14", 0.001),
]
merchantProbabilities.extend([("merchant-" + str(15 + i), 0.000125) for i in range(80)])
merchantProbabilities[len(merchantProbabilities) - 1] = (
    merchantProbabilities[len(merchantProbabilities)-1][0],
    merchantProbabilities[len(merchantProbabilities)-1][1] - (sum([y for (_,y) in merchantProbabilities]) - 1)
)

merchantPool = WeightedChoice(merchantProbabilities)

storePool = WeightedChoice([
    ("store-1", 0.2), 
    ("store-2", 0.2), 
    ("store-3", 0.2), 
    ("store-4", 0.2), 
    ("store-5", 0.2), 
])

category1Pool = WeightedChoice([
    (1, 0.4),
    (2, 0.3),
    (3, 0.3),
])
category2Pool = WeightedChoice([
    (1, 0.5),
    (2, 0.5),
])
category3Pool = WeightedChoice([
    (1, 0.5),
    (2, 0.5),
])
category4Pool = WeightedChoice([
    (1, 0.5),
    (2, 0.5),
])
category5Pool = WeightedChoice([
    (1, 0.5),
    (2, 0.3),
    (3, 0.2),
])

# Generate data

header = [
    "pay_merchant_grab_id",
    "pay_store_grab_id",
    "txn_updated_at",
    "tx_status",
    "type",
    "tx_type",
    "tx_sub_type",
    "transaction_id",
    "food_order_id",
    "id",
    "channel",
    "created_at",
    "currency",
    "day",
    "deleted_at",
    "external_settlement_id",
    "fee_breakup",
    "food_adjustment",
    "food_booking_code",
    "food_cancel_code",
    "food_cancel_msg",
    "food_cashier_metadata",
    "food_description",
    "food_final_state",
    "food_merchant_id",
    "food_order_metadata",
    "food_order_mode",
    "food_order_state",
    "food_order_type",
    "food_pax_id",
    "food_payment_method",
    "food_payment_token_id",
    "food_short_order_number",
    "food_tx_status",
    "food_tx_type",
    "food_user_id",
    "food_user_type",
    "gross_amount",
    "group_txn_id",
    "gst",
    "hour",
    "internal_settlement_id",
    "mdr",
    "mex_funded",
    "mex_promo_amount",
    "month",
    "net_amount",
    "net_sales",
    "partition_key",
    "partner_group_txn_id",
    "partner_txn_id",
    "pay_description",
    "pay_from_wallet_id",
    "pay_gateway_merchant_grab_id",
    "pay_merchant_grab_pay_id",
    "pay_merchant_name",
    "pay_partner_id",
    "pay_pax_name",
    "pay_payment_method",
    "pay_postscript",
    "pay_reward_meta",
    "pay_reward_type",
    "pay_source",
    "pay_store_city",
    "pay_store_grab_pay_id",
    "pay_store_name",
    "pay_terminal_id",
    "pay_to_wallet_id",
    "pay_tx_status",
    "pay_tx_type",
    "reference_id",
    "refunded_amount",
    "st_amount",
    "st_status",
    "total_fees",
    "txn_cashout_at",
    "txn_cashout_at_local",
    "txn_created_at",
    "txn_created_at_local",
    "txn_updated_at_local",
    "updated_at",
    "wallet_transaction_id",
    "year",
]

from functools import reduce
import csv
import uuid
import datetime
from faker import Faker
import tqdm
import sys

if len(sys.argv) != 4:
    sys.exit("Usage: python3 generate-data.py txCount startDate endDate")

NUM_OF_TRANSACTIONS = int(sys.argv[1])
START_DATE = sys.argv[2]
END_DATE = sys.argv[3]

with open('transactions.csv', 'w', encoding='UTF8') as f:
    writer = csv.writer(f)
    writer.writerow(header)
    fake = Faker()

    end_time = datetime.datetime.strptime(START_DATE, '%Y-%m-%d')
    start_time = datetime.datetime.strptime(END_DATE, '%Y-%m-%d')
    delta_between_transactions = (end_time - start_time).total_seconds() * 1000 / NUM_OF_TRANSACTIONS
    delta_between_transactions = datetime.timedelta(milliseconds=delta_between_transactions)
    current_time = start_time 

    for i in tqdm.tqdm(range(NUM_OF_TRANSACTIONS)):
        current_time += delta_between_transactions
        current_epoch = round((current_time - datetime.datetime.utcfromtimestamp(0)).total_seconds())
        fees = [random.randint(1, 100), random.randint(1, 100), random.randint(1, 100), random.randint(1, 100), random.randint(1, 100), random.randint(1, 100), random.randint(1, 100), random.randint(1, 100), random.randint(1, 100), random.randint(1, 100)]
        total_fees = reduce(lambda a,  b: a + b, fees)
        data = [
    merchantPool.pick(), # "pay_merchant_grab_id",
    storePool.pick(), # "pay_store_grab_id",
    current_epoch, # "txn_updated_at",
    category1Pool.pick(), # "tx_status",
    category2Pool.pick(), # "type",
    category1Pool.pick(), # "tx_type",
    category3Pool.pick(), # "tx_sub_type",
    uuid.uuid4(), # "transaction_id",
    uuid.uuid4(), # "food_order_id",
    random.randint(0, 10000000), # "id",
    "", # "channel",
    current_epoch, # "created_at",
    "FOO", # "currency",
    1, # "day",
    "", # "deleted_at",
    uuid.uuid1(), # "external_settlement_id",
    fees[0], # "fee_breakup",
    fees[1], # "food_adjustment",
    uuid.uuid1(), # "food_booking_code",
    uuid.uuid1() if random.randint(0,1) > 0 else "", # "food_cancel_code",
    uuid.uuid1() if random.randint(0,1) > 0 else "", # "food_cancel_msg",
    "", # "food_cashier_metadata",
    "", # "food_description",
    "", # "food_final_state",
    "", # "food_merchant_id",
    "", # "food_order_metadata",
    "", # "food_order_mode",
    "", # "food_order_state",
    "", # "food_order_type",
    500, # "food_pax_id",
    "", # "food_payment_method",
    uuid.uuid1(), # "food_payment_token_id",
    "BAR", # "food_short_order_number",
    category1Pool.pick(), # "food_tx_status",
    category1Pool.pick(), # "food_tx_type",
    category1Pool.pick(), # "food_user_id",
    category1Pool.pick(), # "food_user_type",
    fees[2], # "gross_amount",
    uuid.uuid1(), # "group_txn_id",
    fees[3], # "gst",
    1, # "hour",
    uuid.uuid1(), # "internal_settlement_id",
    "", # "mdr",
    fees[4], # "mex_funded",
    fees[5], # "mex_promo_amount",
    1, # "month",
    fees[6], # "net_amount",
    fees[7], # "net_sales",
    "", # "partition_key",
    uuid.uuid1(), # "partner_group_txn_id",
    uuid.uuid1(), # "partner_txn_id",
    "", # "pay_description",
    uuid.uuid1(), # "pay_from_wallet_id",
    merchantPool.pick(), # "pay_gateway_merchant_grab_id",
    600, # "pay_merchant_grab_pay_id",
    merchantPool.pick(), # "pay_merchant_name",
    uuid.uuid1(), # "pay_partner_id",
    "", # "pay_pax_name",
    "", # "pay_payment_method",
    "", # "pay_postscript",
    "", # "pay_reward_meta",
    "", # "pay_reward_type",
    "", # "pay_source",
    "", # "pay_store_city",
    700, # "pay_store_grab_pay_id",
    storePool.pick(), # "pay_store_name",
    uuid.uuid1(), # "pay_terminal_id",
    uuid.uuid1(), # "pay_to_wallet_id",
    category1Pool.pick(), # "pay_tx_status",
    category1Pool.pick(), # "pay_tx_type",
    uuid.uuid1(), # "reference_id",
    fees[8], # "refunded_amount",
    fees[9], # "st_amount",
    "", # "st_status",
    sum(fees), # "total_fees",
    0, # "txn_cashout_at",
    0, # "txn_cashout_at_local",
    0, # "txn_created_at",
    0, # "txn_created_at_local",
    0, # "txn_updated_at_local",
    0, # "updated_at",
    uuid.uuid1(), # "wallet_transaction_id",
    1, # "year",
        ]

        writer.writerow(data)
        current_time += delta_between_transactions
