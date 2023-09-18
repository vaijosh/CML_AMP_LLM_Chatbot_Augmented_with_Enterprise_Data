import os
import gradio
import json
from hbase.rest_client import HBaseRESTClient
from hbase.scan_filter_helper import *
from hbase.scan import Scan
import utils.model_embedding_utils as model_embedding_utils
inputBox = gradio.Textbox(label="Question", placeholder="what is iceberg ?")
def main():
    # Configure gradio QA app 
    print("Configuring gradio app")
    demo = gradio.Interface(fn=get_responses,
                            inputs=inputBox,
                            #theme=gradio.themes.Default(primary_hue="red", secondary_hue="pink"),
                            examples=["Cloudera Machine Learning benefits This section details the Cloudera Machine Learning benefits for each type of user",
                                      "Distributed object store optimized for big data workloads Apache Ozone",
                                      "What kinds of users use CML?",
                                      "How do data scientists use CML?"],
                            outputs=gradio.outputs.HTML(),
                            title="Vector Search in HBase",
                            flagging_options=["Poor","Unsatisfactory", "Satisfactory","Very Satisfactory", "Outstanding"],
                            flagging_dir="./userFeedback",
                            analytics_enabled=True,
                            thumbnail="/Users/vjoshi/DevNotes/HACKATHON-2023/CML_AMP_LLM_Chatbot_Augmented_with_Enterprise_Data/4_app/icon.png",
                            )

    # Launch gradio app
    print("Launching gradio app")
    demo.launch(share=True,
                enable_queue=True,
                show_error=True,
                server_name='127.0.0.1',
                server_port=int(os.getenv('CDSW_APP_PORT')))
    print("Gradio app ready")
# Helper function for generating responses for the QA app
def get_responses(question):
  _, data = extractEmbeddingsAndQueryDb(question)
  #rows=eval(str(data["row"]).replace("b'","'"))
  #print(rows)
  data = embedded_score_sorted(eval(str(data).replace("b'","'")))
  print("Query Response:" + json.dumps(data))
  return convert_dict_to_html(data)

def convert_dict_to_html(dict_data):
  itr = 1
  table_html = """<table><tr><th>No</th><th>KEY</th><th> Cell info </th>"""

  for item_key in dict_data:
    print(item_key)
    table_html = table_html + "<tr><td>" + str(itr) + "</td><td>" + str(item_key) + "</td>"
    table_html = table_html + "<td><table>"

    for cell_info, val in dict_data[item_key].items():
      if not val:
        val = "NA"
      if cell_info == "f:url":
        val = "<a href="+ val +">" + str(val).split("/")[-1] +"</a>"

      table_html = table_html +str(cell_info).replace("f:", "") + ":&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp" +str(val) + "<br/>"
    itr = itr + 1
    table_html = table_html + "</table></td></tr>"
  return table_html

def convert_json_to_html(raw_data):
  itr = 1
  table_html = """<table><tr><th>Sr No</th><th>Key</th><th>Cell</th>"""
  for data in json.loads(raw_data)["row"]:
    table_html = table_html + "<tr><td>" + str(itr) + "</td><td>" + str(data["key"]) +"</td>"
    itr += 1

    table_html = table_html + "<td><table>"
    for column in data["cell"]:
      if column["$"]:
        value = str(column["$"])
        if column["column"] == "f:url" :

          value = "<a href="+ column["$"] +">" + str(column["$"]).split("/")[-1] +"</a>"
        table_html = table_html + "<tr><td>" + str(column["column"]).replace("f:","") + "</td><td>" + value + "</td></tr>"
    table_html = table_html + "</table></td></tr>"
  return table_html


def table_to_dict(json_data):
  data_dict = {}
  for row in json_data["row"]:
    column_dict = {}
    for cell_info in row["cell"]:
      column_dict[cell_info["column"]] = cell_info["$"]
    data_dict[row["key"]] = column_dict
  print(data_dict)
  return data_dict
def embedded_score_sorted(json_data):
  dicts = table_to_dict(json_data)

  # Sort the outer dictionary by 'embedding:score' within each nested dictionary
  sorted_data = dict(sorted(dicts.items(), key=lambda item: float(item[1]['embedding:score']), reverse=True))

  # Print the sorted dictionary
  for key, value in sorted_data.items():
    print(f"Key: {key}, Score: {value['embedding:score']}")
  print(sorted_data)

  return sorted_data

def extractEmbeddingsAndQueryDb(query):
  emb =  model_embedding_utils.get_embeddings(query)
  client = HBaseRESTClient(['http://172.27.213.141:20550'])
  scan_filter = build_single_column_value_filter(operation="EQUAL",
                                                 family="embedding",
                                                 qualifier="thumbprint",
                                                 value=emb,
                                                 comparator="BinaryPrefixComparator")

  print("Performing Hbase scan with filter : " + scan_filter)
  scan = Scan(client)
  return scan.scan("onlinehelp", scan_filter)
#
# # Get embeddings for a user question and query Milvus vector DB for nearest knowledge base chunk
# def get_nearest_chunk_from_vectordb(vector_db_collection, question):
#     # Generate embedding for user question
#     question_embedding =  model_embedding.get_embeddings(question)
#
#     # Define search attributes for Milvus vector DB
#     vector_db_search_params = {"metric_type": "IP", "params": {"nprobe": 10}}
#
#     # Execute search and get nearest vector, outputting the relativefilepath
#     nearest_vectors = vector_db_collection.search(
#         data=[question_embedding], # The data you are querying on
#         anns_field="embedding", # Column in collection to search on
#         param=vector_db_search_params,
#         limit=1, # limit results to 1
#         expr=None,
#         output_fields=['relativefilepath'], # The fields you want to retrieve from the search result.
#         consistency_level="Strong"
#     )
#
#     # Print the file path of the kb chunk
#     print(nearest_vectors[0].ids[0])
#
#     # Return text of the nearest knowledgebase chunk
#     return load_context_chunk_from_data(nearest_vectors[0].ids[0])
#
# # Return the Knowledge Base doc based on Knowledge Base ID (relative file path)
# def load_context_chunk_from_data(id_path):
#     with open(id_path, "r") as f: # Open file in read mode
#         return f.read()
#
# def create_enhanced_prompt(context, question):
#     prompt_template = """<human>:%s. Answer this question based on given context %s
# <bot>:"""
#     prompt = prompt_template % (context, question)
#     return prompt
#
# # Pass through user input to LLM model with enhanced prompt and stop tokens
# def get_llm_response(prompt):
#     stop_words = ['<human>:', '\n<bot>:']
#
#     generated_text = model_llm.get_llm_generation(prompt,
#                                                   stop_words,
#                                                   max_new_tokens=256,
#                                                   do_sample=False,
#                                                   temperature=0.7,
#                                                   top_p=0.85,
#                                                   top_k=70,
#                                                   repetition_penalty=1.07)
#     return generated_text

if __name__ == "__main__":
    main()
