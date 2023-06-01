CREATE OR REPLACE FUNCTION stateful_service_medigy_rdv_miniflux.sample_array()
	RETURNS text
	LANGUAGE plpython3u
AS $function$
from langchain import PromptTemplate, LLMChain
import os
import psycopg2
import pandas as pd
from bs4 import BeautifulSoup
import openai
import re
import json

api_key = plpy.execute("SELECT model_key FROM model_token WHERE model_name='OpenAI'")
plpy.info(api_key)
openai.api_key = api_key[0]['model_key']

def extract_text(content):
    soup = BeautifulSoup(content, 'html.parser')
    content = soup.get_text().strip()
    return content

# Retrieve data using plpy.execute
feeds = plpy.execute("SELECT title FROM fdw_stateful_service_miniflux.entries ORDER BY created_at DESC LIMIT 50;")
pred_dct = {}

def create_topic(context):
    context = extract_text(context)
    response = openai.Completion.create(
        engine="text-davinci-003",
        prompt="Please provide a list of related topics for the following content in descending order of relevance. No need for newlines. Separate the topics with commas." + context,
        max_tokens=1000,
        n=1,
        temperature=0.8
    )

    topics = response.choices
    topic_lst = [topic['text'] for topic in topics]
    return topic_lst

data1 = []
data2 = []
for feed in feeds:
    data1.append(extract_text(feed['title']))
    temp_data = create_topic(feed['title'])
    fdata = str()
    for data in temp_data:
        fdata = fdata + data.replace('\\n', '') + ','
    data2.append(fdata)

pred_dct = {
    data1[index]: data2[index]
    for index, feed in enumerate(feeds)
}

# Convert pred_dct to JSON array
json_array = json.dumps(pred_dct, ensure_ascii=False)

# Insert JSON array into the table
insert_query = plpy.prepare("INSERT INTO general_topic_predictions_result(feed_title, model_name, related_topic_with_title, created_at) VALUES ($1, $2, $3, CURRENT_DATE);", ["text", "text", "text"])
model_name = "OpenAI"

for title, related_topic_with_title in pred_dct.items():
    plpy.execute(insert_query, [title, model_name, related_topic_with_title])

# Return JSON array as output
json_array

$function$
;