import * as cli from "https://deno.land/x/cliffy@v0.25.7/command/mod.ts";
import * as ws from "https://raw.githubusercontent.com/netspective-labs/sql-aide/v0.0.11/lib/universal/whitespace.ts";
import * as SQLa from "https://raw.githubusercontent.com/netspective-labs/sql-aide/v0.0.11/render/mod.ts";
import * as dvp from "https://raw.githubusercontent.com/netspective-labs/sql-aide/v0.0.11/pattern/data-vault.ts";
import * as sqlsp from "https://raw.githubusercontent.com/netspective-labs/sql-aide/v0.0.11/render/dialect/pg/mod.ts";
import { z } from "https://deno.land/x/zod@v3.21.4/mod.ts";

// deno-lint-ignore no-explicit-any
type Any = any;

//const ctx = SQLa.typicalSqlEmitContext();
const ctx = SQLa.typicalSqlEmitContext({sqlDialect: SQLa.postgreSqlDialect()})
//const ctx = SQLa.typicalSqlEmitContext({sqlDialect: SQLa.msSqlServerDialect()})


type EmitContext = typeof ctx;

const stso = SQLa.typicalSqlTextSupplierOptions<EmitContext>();
const dvts = dvp.dataVaultTemplateState<EmitContext>();
const { text, textNullable, integer, integerNullable, date } = dvts.domains;
const { ulidPrimaryKey: primaryKey } = dvts.keys;
const pkcFactory = SQLa.primaryKeyColumnFactory<EmitContext>();


const nf_dv_schema = SQLa.sqlSchemaDefn("stateful_service_medigy_rdv_miniflux", {
  isIdempotent: true,
});



//const pythonExtn = sqlsp.pgExtensionDefn(SQLa.sqlSchemaDefn("stateful_service_medigy_rdv_miniflux"),"plpython3u",);
function sqlDDL(options: {
  destroyFirst?: boolean;
  schemaName?: string;
} = {}) {
  const { destroyFirst, schemaName } = options;

  // NOTE: every time the template is "executed" it will fill out tables, views
  //       in dvts.tablesDeclared, etc.
  // deno-fmt-ignore
  return SQLa.SQL<EmitContext>(dvts.ddlOptions)`

    ${ schemaName
       ? `create schema if not exists ${schemaName};`
       : "-- no schemaName provided" }
        SET search_path TO ${nf_dv_schema.sqlNamespace};
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        CREATE EXTENSION IF NOT EXISTS "plpython3u";
        CREATE OR REPLACE FUNCTION instruct_embedding_prediction()
  RETURNS text
  LANGUAGE plpython3u
  AS $function$
  
import numpy as np
import pandas as pd
import psycopg2
import langchain.embeddings
from openai.embeddings_utils import cosine_similarity as cs
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from langchain.embeddings import HuggingFaceInstructEmbeddings
embeddings = HuggingFaceInstructEmbeddings(query_instruction="Represent the query for retrieval: ")
model_name = "Instruct Embeddings"
# Fetch topics
topics_query = "SELECT topic FROM stateless_service_medigy_topics.offering_topic_url_status;"
topics_cursor = plpy.execute(topics_query)
df_t = pd.DataFrame((row['topic'] for row in topics_cursor), columns=["topic"])

# Fetch entries
entries_query = "SELECT title, content FROM fdw_stateful_service_miniflux.entries ORDER BY created_at DESC LIMIT 20;"
entries_cursor = plpy.execute(entries_query)
df_n_data = [(row['title'], row['content']) for row in entries_cursor]
df_n = pd.DataFrame(df_n_data, columns=["title", "content"])

def extract_text(content):
    soup = BeautifulSoup(content, 'html.parser')
    content = soup.get_text().strip()
    return content

df_n['content'] = df_n['content'].apply(extract_text)
df_n['title'] = df_n['title'].apply(extract_text)

df_t["topic_emb"] = [embeddings.embed_query(row["topic"]) for _, row in df_t.iterrows()]

feed_title_topic = []
feed_content_topic = []
feed_title_content = []

def Sort_Tuple(tup):
    tup.sort(key=lambda x: x[1], reverse=True)
    return tup

def topic_matcher(df_t, news_embedding, percent):
    related_topics = [
        (
            topic.split(":")[1] if ":" in topic else topic,
            cs(topic_embedding, news_embedding)
        )
        for _, topic_embedding, topic in zip(df_t.index, df_t['topic_emb'], df_t['topic'])
    ]
    related_topics = [
        (topic, float(similarity_score[0]) if isinstance(similarity_score, list) else float(similarity_score))
        for topic, similarity_score in related_topics
    ]
    scores = [score for _, score in related_topics]
    Sorted_topic = Sort_Tuple(related_topics)
    out_put = []
    min_score = min(scores)
    max_score = max(scores)
    score_list = []
    threshold = max_score - (max_score - min_score) * percent / 100
    for x, y in Sorted_topic:
        if y > threshold:
            if x not in out_put:
                out_put.append(x)
                score_list.append(y)
        else:
            break

    return out_put, score_list

pred_score_title = []
pred_score_content = []
pred_score_title_content = []

for _, row in df_n.iterrows():
    feed_title = row['title']
    feed_content = row['content']
    feed_title_emb = embeddings.embed_query(feed_title)
    result = topic_matcher(df_t, feed_title_emb, 10)
    feed_title_topic.append(result[0])
    pred_score_title.append(result[1])

    if len(feed_content) > 0:
        feed_title_emb = embeddings.embed_query(feed_content)
        result = topic_matcher(df_t, feed_title_emb, 10)
        feed_content_topic.append(result[0])
        pred_score_content.append(result[1])
    else:
        result = "", ""
        feed_content_topic.append(result)
        pred_score_content.append(result)

    if len(feed_content) > 0:
        title_contentfeed = feed_title + feed_content
        feed_title_emb = embeddings.embed_query(title_contentfeed)
        result = topic_matcher(df_t, feed_title_emb, 10)
        feed_title_content.append(result[0])
        pred_score_title_content.append(result[1])
    else:
        result = "", ""
        feed_title_content.append(result)
        pred_score_title_content.append(result)

for index, row in df_n.iterrows():
    feed_title = row['title']
    feed_content = row['content']
    feed_title_and_content = feed_title + "." + feed_content

    # Remove unwanted characters from feed_title_topic
    feed_title_topic_cleaned = str(feed_title_topic[index]).replace('{', '').replace('}', '').replace('"', '').replace('[', '').replace(']', '').replace("'", '')
    pred_score_title_cleaned = str(pred_score_title[index]).replace('{', '').replace('}', '').replace('"', '').replace('[', '').replace(']', '').replace("'", '')
    feed_content_topic_cleaned = str(feed_content_topic[index]).replace('{', '').replace('}', '').replace('"', '').replace('[', '').replace(']', '').replace("'", '')
    pred_score_content_cleaned = str(pred_score_content[index]).replace('{', '').replace('}', '').replace('"', '').replace('[', '').replace(']', '').replace("'", '')
    feed_title_content_cleaned = str(feed_title_content[index]).replace('{', '').replace('}', '').replace('"', '').replace('[', '').replace(']', '').replace("'", '')
    pred_score_title_cleaned = str(pred_score_title[index]).replace('{', '').replace('}', '').replace('"', '').replace('[', '').replace(']', '').replace("'", '')

    insert_query = plpy.prepare("INSERT INTO stateful_service_medigy_rdv_miniflux.langchain_predictions_result (feed_title, feed_content, feed_title_and_content, model_name, related_topic_with_title, prediction_score_title, related_topic_with_content, prediction_score_content, related_topic_with_title_and_content, prediction_score_title_content, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CURRENT_DATE);", ["text"] * 10)
    insert_values = (feed_title,feed_content,feed_title_and_content, model_name,feed_title_topic_cleaned,pred_score_title_cleaned,feed_content_topic_cleaned,pred_score_content_cleaned,feed_title_content_cleaned,pred_score_title_cleaned) 

    
    plpy.execute(insert_query, insert_values)





  








$function$
;
;
;
;
;


        `;
}

function handleSqlCmd(options: {
  dest?: string | undefined;
  destroyFirst?: boolean;
  schemaName?: string;
} = {}) {
  const output = ws.unindentWhitespace(sqlDDL(options).SQL(ctx));
  if(options.dest) {
    Deno.writeTextFileSync(options.dest, output)
  } else {
    console.log(output)
  }
}

// deno-fmt-ignore (so that command indents don't get reformatted)
await new cli.Command()
  .name("er-dv-sqla")
  .version("0.0.2")
  .description("Entity Resolution Data Vault SQL Aide")
  .action(() => handleSqlCmd())
  .command("help", new cli.HelpCommand().global())
  .command("completions", new cli.CompletionsCommand())
  .command("sql", "Emit SQL")
    .option("-d, --dest <file:string>", "Output destination, STDOUT if not supplied")
    .option("--destroy-first", "Include SQL to destroy existing objects first (dangerous but useful for development)")
    .option("--schema-name <schemaName:string>", "If destroying or creating a schema, this is the name of the schema")
    .action((options) => handleSqlCmd(options))
  .command("diagram", "Emit Diagram")
    .option("-d, --dest <file:string>", "Output destination, STDOUT if not supplied")
    .action((options) => {
      // "executing" the following will fill dvts.tablesDeclared but we don't
      // care about the SQL output, just the state management (tablesDeclared)
      sqlDDL().SQL(ctx);
      const pumlERD = dvts.pumlERD(ctx).content;
      if(options.dest) {
        Deno.writeTextFileSync(options.dest, pumlERD)
      } else {
        console.log(pumlERD)
      }
    })
    .parse(Deno.args);

