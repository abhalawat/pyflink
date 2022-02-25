import argparse
import atexit
import json
import logging
import time
import sys
from web3 import Web3
from web3.middleware import geth_poa_middleware

import asyncio

from confluent_kafka import Producer


logging.basicConfig(
  format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
  datefmt='%Y-%m-%d %H:%M:%S',
  level=logging.INFO,
  handlers=[
      logging.FileHandler("address_producer.log"),
      logging.StreamHandler(sys.stdout)
  ]
)

logger = logging.getLogger()

#SELLERS = ['LNK', 'OMA', 'KC', 'DEN']


class ProducerCallback:
    def __init__(self, record, log_success=False):
        self.record = record
        self.log_success = log_success

    def __call__(self, err, msg):
        if err:
            logger.error('Error producing record {}'.format(self.record))
        elif self.log_success:
            logger.info('Produced {} to topic {} partition {} offset {}'.format(
                self.record,
                msg.topic(),
                msg.partition(),
                msg.offset()
            ))

def web3function(block):
    # web3 = Web3(Web3.WebsocketProvider("wss://eth-mainnet.alchemyapi.io/v2/NMMzTK9vae0CA0DrxtR_TiqCHkf3qkqD"))
    # web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
    #block = web3.eth.get_block('latest').number 
    #block = self.value()       
    for tx_hash in web3.eth.get_block(block).transactions:
        try:
            contractAddress = web3.eth.getTransactionReceipt(tx_hash).to
            print(contractAddress, block)
            # if contractAddress != None:
            #     #print(contractAddress, block)
            #     return contractAddress,block
            return contractAddress, block
        except asyncio.TimeoutError:
            pass

def main(args):
    logger.info('Starting sales producer')
    conf = {
        'bootstrap.servers': args.bootstrap_server,
        'linger.ms': 200,
        'client.id': 'sales-1',
        'partitioner': 'murmur2_random'
    }

    producer = Producer(conf)

    atexit.register(lambda p: p.flush(), producer)

    #i = 1
    while True:
        # is_tenth = i % 10 == 0
        # try:
        #     address,block = web3function()
        # except TypeError:
        #     pass
        # if address != None:    
        #     sales = {
        #         'address': address,
        #         'block': block,
        #         #'sale_ts': int(time.time() * 1000)
        #     }
        #     producer.produce(topic=args.topic,
        #                     value=json.dumps(sales),
        #                     on_delivery=ProducerCallback(sales, log_success=is_tenth))

        #     if is_tenth:
        #         producer.poll(1)
        #         time.sleep(5)
        #         i = 0 # no need to let i grow unnecessarily large

        #     i += 1
        block=web3.eth.get_block('latest').number
        while block>=0:
            block-=1
            address,blockno = web3function(block)
            if address != None:
                data = {
                    'address':address,
                    'block': blockno
                }
                producer.produce(topic=args.topic,
                                value=json.dumps(data),
                                on_delivery=ProducerCallback(data, log_success=1))


if __name__ == '__main__':
    web3 = Web3(Web3.WebsocketProvider("wss://eth-mainnet.alchemyapi.io/v2/NMMzTK9vae0CA0DrxtR_TiqCHkf3qkqD"))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap-server', default='localhost:9092')
    parser.add_argument('--topic', default='address-block')
    args = parser.parse_args()
    main(args)