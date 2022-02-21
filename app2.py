from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.common.typeinfo import Types
from web3 import Web3
from web3.middleware import geth_poa_middleware

def web3Functions():
    block = 5134541 
    data = []
    web3 = Web3(Web3.WebsocketProvider("wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
    for tx_hash in web3.eth.get_block(block).transactions:
        contractAddress = web3.eth.getTransactionReceipt(tx_hash).to
        #print(contractAddress)
        print(contractAddress, block)
        data.append((contractAddress, block))
    del web3
    del block
    return data
        


# create environments of both APIs
env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

data = web3Functions()

# create a DataStream
# ds = env.from_collection([("Alice", 12), ("Bob", 10), ("Alice", 100)],
#                           type_info=Types.ROW_NAMED(
#                           ["a", "b"],
#                           [Types.STRING(), Types.INT()]))
ds = env.from_collection(data,
                          type_info=Types.ROW_NAMED(
                          ["a", "b"],
                          [Types.STRING(), Types.INT()]))

input_table = t_env.from_data_stream(ds).alias("address", "block")

# register the Table object as a view and query it
# the query contains an aggregation that produces updates
t_env.create_temporary_view("InputTable", input_table)
#res_table = t_env.sql_query("SELECT address, SUM(block) FROM InputTable GROUP BY address")
res_table = t_env.sql_query("SELECT * FROM InputTable")

# interpret the updating Table as a changelog DataStream
res_stream = t_env.to_changelog_stream(res_table)

# add a printing sink and execute in DataStream API
res_stream.print()
env.execute()

# prints:
# +I[Alice, 12]
# +I[Bob, 10]
# -U[Alice, 12]
# +U[Alice, 112]
