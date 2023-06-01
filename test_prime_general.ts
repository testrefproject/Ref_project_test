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
        CREATE OR REPLACE FUNCTION general_prediction()
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
  
  api_key=plpy.execute("select model_key from model_token where model_name='OpenAI'")
  plpy.info(api_key)
  openai.api_key=api_key[0]['model_key']
  
  def extract_text(content):
      soup = BeautifulSoup(content, 'html.parser')
      content = soup.get_text().strip()
      return content
  # Retrieve data using plpy.execute
  feeds = plpy.execute("select title from fdw_stateful_service_miniflux.entries ORDER BY created_at DESC LIMIT 50;")
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
  data1=[]  
  data2=[]
  for feed in feeds:
    data1.append(extract_text(feed['title']))
    temp_data=create_topic(feed['title'])
    fdata=str()
    for data in temp_data:
        fdata=fdata+data.replace('\\n','')+','
    data2.append(fdata)
  pred_dct = {
    data1[index]:data2[index]
    for index,feed in enumerate(feeds)
  } 
  
  
  
  insert_query = plpy.prepare("INSERT INTO general_topic_predictions_result(feed_title, model_name, related_topic_with_title,created_at) VALUES ($1, $2, $3,CURRENT_DATE) ;",["text","text","text"])
  model_name = "OpenAI"
  
  insert_values = [(title, model_name, related_topic_with_title) for title, related_topic_with_title in pred_dct.items()]
  
  for values in insert_values:
    plpy.execute(insert_query, values)

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

