from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction
from web3 import Web3
from web3.middleware import geth_poa_middleware

class MyMapFunction(MapFunction):
    
    def map(self, value):
        web3 = Web3(Web3.WebsocketProvider("wss://eth-mainnet.alchemyapi.io/v2/NMMzTK9vae0CA0DrxtR_TiqCHkf3qkqD"))
        web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
        for tx_hash in web3.eth.get_block(value).transactions:
            contractAddress = web3.eth.getTransactionReceipt(tx_hash).to
            if contractAddress != None:
                print(contractAddress, value)
                return contractAddress
        # print(value+1)
        # return value + 1

def demo():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    # env.set_python_requirements(requirements_file_path="requirements.txt", requirements_cache_dir="cached_dir")
    env.set_python_requirements("/opt/examples/datastream/batch/requirements.txt")
    #data_stream = env.from_collection([1, 2, 3, 4, 5], type_info=Types.INT())
    data_stream = env.from_collection([14300930, 14300929], type_info=Types.INT())
    mapped_stream = data_stream.map(MyMapFunction(), output_type=Types.STRING())
    env.execute('demo')


if __name__ == '__main__':
    demo()
