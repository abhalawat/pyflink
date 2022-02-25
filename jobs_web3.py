
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.common.typeinfo import Types
from web3 import Web3
from web3.middleware import geth_poa_middleware
from pyflink.datastream.connectors import FileSink, OutputFileConfig, NumberSequenceSource
from pyflink.common.serialization import Encoder
from pyflink.common import WatermarkStrategy, Row
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, MapFunction



class MyMapFunction(MapFunction):
    def map(self, value):
        web3 = Web3(Web3.WebsocketProvider("wss://eth-mainnet.alchemyapi.io/v2/NMMzTK9vae0CA0DrxtR_TiqCHkf3qkqD"))
        web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
        #block = web3.eth.get_block('latest').number 
        block = self.value()
        for tx_hash in web3.eth.get_block(block).transactions:
            contractAddress = web3.eth.getTransactionReceipt(tx_hash).to
            print(contractAddress, block)
            return contractAddress

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    seq_num_source = NumberSequenceSource(1, 10)

    output_path = '/opt/examples/datastream/output/web3_output'
    file_sink = FileSink \
        .for_row_format(output_path, Encoder.simple_string_encoder()) \
        .with_output_file_config(OutputFileConfig.builder().with_part_prefix('pre').with_part_suffix('suf').build()) \
        .build()
    
    ds = env.from_source(
        source=seq_num_source,
        #watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name='seq_num_source',
        type_info=Types.INT())

    ds.map(MyMapFunction(), output_type=Types.STRING())
    
    env.execute('new')



if __name__ == '__main__':
    main()

if __name__ == '__main__':
    # schedule.every(10).seconds.do(web3Functions)

    # while 1:
    #     schedule.run_pending()
    #     time.sleep(1)
    main()
