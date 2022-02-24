import ray
from web3 import Web3
from web3.middleware import geth_poa_middleware
from kafka import KafkaProducer
import json
#from config.kafka_config import *

# producer = KafkaProducer(
#  bootstrap_servers="kafka-deqode-0c18.aivencloud.com"+":"+str(21780),
#  security_protocol="SSL",
#  ssl_cafile="ca.pem",
#  ssl_certfile="service.cert",
#  ssl_keyfile="service.key",
#  value_serializer=lambda v: json.dumps(v).encode('ascii'),
#  key_serializer=lambda k: json.dumps(k).encode('ascii')
# )
# topic_name = 'First_Topic'

@ray.remote
def web3Functions(block):
    #block = 5134541 
    #data = []
    web3 = Web3(Web3.WebsocketProvider("wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
    for tx_hash in web3.eth.get_block(block).transactions:
        contractAddress = web3.eth.getTransactionReceipt(tx_hash).contractAddress
        #print(contractAddress)
        print(contractAddress, block)
        if contractAddress != None:
            #data.append((contractAddress, block))
            #kafkaData.remote(contractAddress,block)
            kafkaData(contractAddress,block)
    del web3
    del block

#@ray.remote
def kafkaData(contractAddress,block):
    producer = KafkaProducer(
    bootstrap_servers="kafka-deqode-0c18.aivencloud.com"+":"+str(21780),
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
    value_serializer=lambda v: json.dumps(v).encode('ascii'),
    key_serializer=lambda k: json.dumps(k).encode('ascii')
    )
    topic_name = 'Address'
    producer.send(
        topic_name,
        #key={"id": count},
        value={"address":contractAddress, "block":block}
    )
    
if __name__=="__main__":
    #ray.init(address='auto')    
    web3 = Web3(Web3.WebsocketProvider("wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
    block= web3.eth.get_block('latest').number + 1
    del web3
    while block>=0:
        block-=1
        web3Functions.remote(block)
