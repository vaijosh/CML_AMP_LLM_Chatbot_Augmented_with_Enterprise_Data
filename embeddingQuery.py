from hbase.rest_client import HBaseRESTClient
from hbase.scan_filter_helper import *
from hbase.scan import Scan
import utils.model_embedding_utils as model_embedding_utils
query="document for cml architecture?"
import pandas as pd
from IPython.display import display

def extractEmbeddingsAndQueryDb(query):
  emb =  model_embedding_utils.get_embeddings(query)
  client = HBaseRESTClient(['http://172.27.213.141:20550'])
  scan_filter = build_single_column_value_filter(operation="EQUAL",
                                                 family="embedding",
                                                 qualifier="thumbprint",
                                                 value=emb,
                                                 comparator="BinaryPrefixComparator")
  scan = Scan(client)
  return scan.scan("onlinehelp", scan_filter)


if __name__ == '__main__':
  query="document for cml architecture?"
  _, data = extractEmbeddingsAndQueryDb(query)
  rows = data["row"]
  rows = eval(str(data["row"]).replace("b'","'"))
  print(rows)
  #pd_object = pd.read_json('astronomy_simple.json', typ='series')
  df = pd.DataFrame(data)
  display(df)