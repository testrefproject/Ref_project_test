import * as cli from "https://deno.land/x/cliffy@v0.25.7/command/mod.ts";
import * as ws from "https://raw.githubusercontent.com/netspective-labs/sql-aide/v0.0.25/lib/universal/whitespace.ts";
import * as SQLa from "https://raw.githubusercontent.com/netspective-labs/sql-aide/v0.0.25/render/mod.ts";
import * as dvp from "https://raw.githubusercontent.com/netspective-labs/sql-aide/v0.0.25/pattern/data-vault/mod.ts";
import * as sqlsp from "https://raw.githubusercontent.com/netspective-labs/sql-aide/v0.0.25/render/dialect/pg/mod.ts";
import { z } from "https://deno.land/x/zod@v3.21.4/mod.ts";
import * as tp from "https://raw.githubusercontent.com/netspective-labs/sql-aide/v0.0.14/pattern/typical/mod.ts";



// deno-lint-ignore no-explicit-any
type Any = any;

//const ctx = SQLa.typicalSqlEmitContext();
const ctx = SQLa.typicalSqlEmitContext({sqlDialect: SQLa.postgreSqlDialect()})
//const ctx = SQLa.typicalSqlEmitContext({sqlDialect: SQLa.msSqlServerDialect()})


type EmitContext = typeof ctx;

const gts = tp.governedTemplateState<tp.GovernedDomain,EmitContext>();
const gm = tp.governedModel<tp.GovernedDomain, EmitContext>(gts.ddlOptions);
const stso = SQLa.typicalSqlTextSupplierOptions<EmitContext>();
const dvts = dvp.dataVaultTemplateState<EmitContext>();
const { text, textNullable,jsonNullable, integer, integerNullable, date ,serialPrimaryKey} = dvts.domains;
const pkcFactory = SQLa.primaryKeyColumnFactory<EmitContext>();
const { ulidPrimaryKey: primaryKey, autoIncPrimaryKey:autoIncPK ,serialPrimaryKey:serialPK} = gm.keys;


const nf_dv_schema = SQLa.sqlSchemaDefn("stateful_service_medigy_rdv_miniflux", {
  isIdempotent: true,
});


const MedigyPredictionResult = SQLa.tableDefinition("langchain_predictions_result", {
  feed_title: textNullable(),
  model_name: textNullable(),
  feed_content: textNullable(),
  feed_title_and_content: textNullable(),
  related_topic_with_title: jsonNullable(),
  prediction_score_title: jsonNullable(),
  related_topic_with_content: jsonbNullable(),
  prediction_score_content: jsonbNullable(),
  related_topic_with_title_and_content: jsonbNullable(),
  prediction_score_title_content: jsonbNullable(),
  created_at: z.date().default(new Date())
});



const GeneralPredictionResult = SQLa.tableDefinition("general_topic_predictions_result", {  
  feed_title: textNullable(),
  model_name: textNullable(),
  related_topic_with_title: textNullable(),
  created_at: z.date().default(new Date())
  
});



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

        ${MedigyPredictionResult}
        ${GeneralPredictionResult}
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

