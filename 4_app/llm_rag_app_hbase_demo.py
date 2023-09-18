import os
import gradio as gr
import json
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup
from hbase.rest_client import HBaseRESTClient
from hbase.scan_filter_helper import *
from hbase.scan import Scan
from requests.exceptions import HTTPError
import utils.model_embedding_utils as model_embedding_utils

required_columns = {"f:category": "category", "f:filename": "filename", "f:product": "product", "f:topic": "product",
                    "f:url": "url", "embedding:score": "score"}

cache = dict()
def main():
    # Configure gradio QA app 
    print("Configuring gradio app")
    with gr.Blocks() as demo:
        with gr.Row():
            gr.HTML("<h3 style=\"text-align:center\">Vector Search in HBase</h3>")
        with gr.Row():
            with gr.Column(scale=1, min_width=600):
                question = gr.Textbox(label="Question", placeholder="What are iceberg tables?")
                with gr.Row():
                    clear_btn = gr.ClearButton(value="Clear")
                    submit_btn = gr.Button(value="Submit", variant="primary")
                with gr.Row():
                    gr.Examples(["Cloudera Machine Learning benefits This section details the Cloudera Machine Learning benefits for each type of user",
                                 "Distributed object store optimized for big data workloads Apache Ozone",
                                 "What kinds of users use CML?",
                                 "How do data scientists use CML?"], inputs=[question])
            with gr.Column(scale=2, min_width=600):
                llm_without_ctxt = gr.Textbox(label="Asking LLM with No Context", placeholder="", visible=True)
                llm_with_ctxt = gr.Textbox(label="Asking LLM with Context (RAG) using HBase", placeholder="",
                                           visible=True)
        with gr.Row(variant="panel"):
            gr.HTML(get_table_header())
        with gr.Row():
            output_html = gr.outputs.HTML(label="Solution")
        with gr.Accordion("Scan Request", open=False):
            scan_req = gr.Textbox(label="", placeholder="Scan Request", visible=True, lines=25)

        clear_btn.click(lambda: (None, None, None, None, None), inputs=[],
                        outputs=[scan_req, question, llm_without_ctxt, llm_with_ctxt, output_html])
        submit_btn.click(get_responses, inputs=[question],
                         outputs=[scan_req, output_html, llm_without_ctxt, llm_with_ctxt])
        submit_btn.click(lambda: gr.update(value=""), None, outputs=[output_html], queue=False)

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
    client, scan_filter = extractEmbeddings(question)
    _, data = query_db(client, scan_filter)
    json_data = eval(str(data).replace("b'", "'"))
    df = json_to_dataframe(json_data)
    context_chunk = load_context_chunk_from_data(df.iloc[0].url)
    prompt_with_context = create_enhanced_prompt(context_chunk, question)
    prompt_without_context = create_enhanced_prompt("none", question)

    start_time = time.time()
    contextResponse = get_llm_response(prompt_with_context)
    plainResponse = get_llm_response(prompt_without_context)
    end_time = time.time()
    print("Total time taken in LLM Response: ", end_time - start_time)
    return scan_filter, dataframe_to_html_table(df), plainResponse, contextResponse

def dataframe_to_html_table(df):
    return df.to_html(classes=["table", "table-stripped"], index=False, escape=False, render_links=True)

def json_to_dataframe(json_data):
    rows = []
    for row_data in json_data["row"]:
        key = row_data["key"]
        values = {value: 'None' for value in required_columns.values()}
        for cell in row_data["cell"]:
            column = cell["column"]
            value = cell["$"]
            if column in required_columns:
                readable_column = column.split(":")[-1] if ":" in column else column
                if column == "f:url":
                    values[readable_column] = "<a href=" + value + ">" + str(value).split("/")[-1] + "</a>"
                else:
                    values[readable_column] = value + ""
        row = {"key": key, **values}
        rows.append(row)
    df = pd.DataFrame(rows)
    df = df.reset_index(drop=True)
    return df.sort_values(by='score', ascending=False)

def get_table_header():
    return "<h3>HBase Response</h3>"


def query_db(client, scan_filter):
    scan = Scan(client)
    return scan.scan("onlinehelp", scan_filter)


def extractEmbeddings(query):
    emb = model_embedding_utils.get_embeddings(query)
    client = HBaseRESTClient(['http://172.27.213.141:20550'])
    scan_filter = build_single_column_value_filter(operation="EQUAL",
                                                   family="embedding",
                                                   qualifier="thumbprint",
                                                   value=emb,
                                                   comparator="BinaryPrefixComparator")
    print("Performing Hbase scan with filter : " + scan_filter)
    return client, scan_filter


def load_context_chunk_from_data(html_text):
    HREF = 'href='
    start_index = html_text.find(HREF)
    end_index = html_text.find('>')
    url_link = html_text[start_index + len(HREF): end_index]
    extracted_text = "none"
    if url_link.find("github.com") != -1:
        response = requests.get(url_link.replace("github.com", "raw.githubusercontent.com").replace("/blob", ""))
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            for script in soup(["script", "style"]):
                script.extract()
            extracted_text = soup.get_text()
    return extracted_text

def create_enhanced_prompt(context, question):
    prompt_template = """<human>:%s. Answer this question based on given context %s
<bot>:"""
    prompt = prompt_template % (context, question)
    return prompt

def get_llm_response(prompt, key=None, dict=None):

    try:
        headers = {"Content-Type": "application/json"}
        data = {"request":{"prompt":"","temperature":"100","max_new_tokens":"100",
                           "top_p":"1.0","top_k":"0",
                           "repetition_penalty":"1.0","num_beams":"1"}}

        data["request"]["prompt"]=prompt
        url = 'https://modelservice.%s/model?accessKey=%s' %(os.getenv('ML_DOMAIN'),os.getenv('ML_ACCESS_KEY'))

        strCacheKey = url+data.__str__();

        bot_resp = ""
        if strCacheKey not in cache:
            response = requests.post(url, data=json.dumps(data), headers=headers)
            response.raise_for_status()
            jsonResponse = response.json()['response']
            print(jsonResponse)
            BOT_STR = '<bot>:'
            bot_resp = jsonResponse[jsonResponse.find(BOT_STR) + len(BOT_STR) + 1:]
            cache[strCacheKey] = bot_resp
        else:
            bot_resp = cache.get(strCacheKey)

        if dict is not None and key is not None:
            dict[key] = bot_resp
        return bot_resp

    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')

if __name__ == "__main__":
    main()
