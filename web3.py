import asyncio
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction
from web3 import Web3
from web3.middleware import geth_poa_middleware
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.table import StreamTableEnvironment
# file1 = open("myfile.txt", "a")  # append mode


class MyMapFunction(MapFunction):
    
    def map(self, value):
        web3 = Web3(Web3.WebsocketProvider("wss://eth-mainnet.alchemyapi.io/v2/NMMzTK9vae0CA0DrxtR_TiqCHkf3qkqD"))
        web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
        try:
            for tx_hash in web3.eth.get_block(value).transactions:
                contractAddress = web3.eth.getTransactionReceipt(tx_hash).to
                if contractAddress != None:
                    #file1.write(contractAddress," ",value)
                    print(contractAddress, value)
                    #data.append((contractAddress,value))
                    #source = table_env.from_elements(data, ['address', 'block'])
                    #print(source.to_pandas())
                    return contractAddress
                else:
                    return "Noneaddress"
        except asyncio.TimeoutError:
            pass
        # print(value+1)
        # return value + 1

def demo(latest):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # env.set_python_requirements(requirements_file_path="requirements.txt", requirements_cache_dir="cached_dir")
    env.set_python_requirements("/opt/examples/datastream/batch/requirements.txt")
    data = list(reversed(range(latest-100,latest-1)))
    #data_stream = env.from_collection([1, 2, 3, 4, 5], type_info=Types.INT())
    #data_stream = env.from_collection([14300930, 14300929], type_info=Types.INT())
    data_stream = env.from_collection(data, type_info=Types.INT())
    mapped_stream = data_stream.map(MyMapFunction(), output_type=Types.STRING())
    env.execute('demo')


if __name__ == '__main__':
    data=[]
    web3 = Web3(Web3.WebsocketProvider("wss://eth-mainnet.alchemyapi.io/v2/NMMzTK9vae0CA0DrxtR_TiqCHkf3qkqD"))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    block= web3.eth.get_block('latest').number
    demo(block)

    
