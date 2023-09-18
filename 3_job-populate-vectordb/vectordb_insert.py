import os
import subprocess
from pathlib import Path

import utils.model_embedding_utils as model_embedding_utils
import csv
import hashlib
import sys

f = open("../data/clouderaonlinehelp.text", "a")
ids = set()
urlPrefix = "https://github.com/vaijosh/CML_AMP_LLM_Chatbot_Augmented_with_Enterprise_Data/blob/main/data/"

# Create an embedding for given text/doc and insert it into Milvus Vector DB
def insert_embedding(filePath, content, filename):
    filePath = filePath.replace('../data/', '')
    fileComponents = filePath.split("/")
    category = ""
    product = ""
    topic = fileComponents.__getitem__(len(fileComponents) - 1)
    if(len(fileComponents) == 2):
      product = fileComponents.__getitem__(0)
    if(len(fileComponents) == 4):
      product = fileComponents.__getitem__(0)
      category = fileComponents.__getitem__(1)
      topic = fileComponents.__getitem__(3)

    embeddings =  model_embedding_utils.get_embeddings(content)
    contentHash = hashlib.md5(content.encode('utf-8')).hexdigest().__str__()
    if(ids.__contains__(contentHash)) :
      print("skipping duplicate resume:" + filename)
    else :
      ids.add(contentHash)
      data = "put 'onlinehelp','" + contentHash.__str__() + "','f:url','" + urlPrefix + filePath + "'"
      f.write(data)
      f.write("\n")
      data = "put 'onlinehelp','" + contentHash.__str__() + "','f:filename','" + filename + "'"
      f.write(data)
      f.write("\n")
      data = "put 'onlinehelp','" + contentHash.__str__() + "','f:product','" +  product + "'"
      f.write(data)
      f.write("\n")
      if(not category == ""):
        data = "put 'onlinehelp','" + contentHash.__str__() + "','f:category','" + category + "'"
        f.write(data)
        f.write("\n")
      if( not topic == ""):
        data = "put 'onlinehelp','" + contentHash.__str__() + "','f:topic','" +  topic + "'"
        f.write(data)
        f.write("\n")

      data = "put 'onlinehelp','" + contentHash.__str__() + "','embedding:thumbprint','" + embeddings.__str__() + "'"
      f.write(data)
      f.write("\n")

def main():
  # Reset the vector database files

  try:

    # Read KB documents in ./data directory and insert embeddings into Vector DB for each doc
    # The default embeddings generation model specified in this AMP only generates embeddings for the first 256 tokens of text.
    doc_dir = '../data'
    for file in Path(doc_dir).glob(f'**/*.txt'):
        with open(file, "r") as f: # Open file in read mode
            print("Generating embeddings for: %s" % os.path.abspath(file))
            text = f.read()
            insert_embedding(os.path.relpath(file), text, file.name)
    # sys.path.append("../utils")
    # with open('../data/UpdatedResumeDataSet.csv', newline='') as csvfile:
    #   spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    #   count = 0;
    #   for row in spamreader:
    #     count += 1
    #     if(count % 25 == 0):
    #       print("processing row:" + count.__str__())
    #
    #     resumefilename = "../data/resumes/resume" + count.__str__() + ".txt"
    #     f1 = open(resumefilename, "a")
    #     # f1.write(row[1])
    #     # f1.close()
    #     insert_embedding(row[0],row[1],f1.name.split("/")[-1])
    # f.close()

  except Exception as e:
    raise (e)

if __name__ == "__main__":
    main()