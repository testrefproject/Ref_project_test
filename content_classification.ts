import * as cli from "https://deno.land/x/cliffy@v0.25.7/command/mod.ts";
import * as ws from "https://raw.githubusercontent.com/netspective-labs/sql-aide/v0.0.25/lib/universal/whitespace.ts";
import * as SQLa from "https://raw.githubusercontent.com/netspective-labs/sql-aide/v0.0.25/render/mod.ts";
import * as dvp from "https://raw.githubusercontent.com/netspective-labs/sql-aide/v0.0.25/pattern/data-vault/mod.ts";
import * as sqlsp from "https://raw.githubusercontent.com/netspective-labs/sql-aide/v0.0.25/render/dialect/pg/mod.ts";
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

const dvContentHub = dvts.hubTable("content", {
  hub_content_id: primaryKey(),
  ...dvts.housekeeping.columns,
});

const dvClassificationHub = dvts.hubTable("classification", {
  hub_classification_id: primaryKey(),
  ...dvts.housekeeping.columns,
});

const tblAlgorithmMetaTable = SQLa.tableDefinition("algorithm", {
  algorithm_name: text(),
  algorithm_id: primaryKey(),
});

const dvJobHub = dvts.hubTable("job", {
  hub_job_id: primaryKey(),
  hub_job_name: text(),
  ...dvts.housekeeping.columns,
});

const dvContentHubSat = dvContentHub.satelliteTable("content_attribute", {
  hub_content_id: dvContentHub.references.hub_content_id(),
  content: text(),
  content_type: text(),
  ...dvts.housekeeping.columns,
  sat_content_id: primaryKey(),
});

const dvClassificationHubSat = dvClassificationHub.satelliteTable("classification_attribute", {
  hub_classification_id: dvClassificationHub.references.hub_classification_id(),
  classification: text(),
  ...dvts.housekeeping.columns,
  sat_classification_id: primaryKey(),
});

const dvJobHubSat = dvJobHub.satelliteTable("job_detail", {
  run_date_time: date(),
  status: text(),
  ...dvts.housekeeping.columns,
  hub_job_id: dvJobHub.references.hub_job_id(),
  sat_job_id: primaryKey(),
});

const dvContentClassificationLink = dvts.linkTable("classified_content",{
  link_content_classification_id: primaryKey(),
  hub_content_id: dvContentHub.references.hub_content_id(),
  hub_classification_id: dvClassificationHub.references.hub_classification_id(),
  ...dvts.housekeeping.columns,
});

const dvContentClassificationJobLink = dvts.linkTable("classified_content_job", {
  link_content_classification_job_id: primaryKey(),
  hub_content_id: dvContentHub.references.hub_content_id(),
  hub_classification_id: dvClassificationHub.references.hub_classification_id(),
  hub_job_id: dvJobHub.references.hub_job_id(),
  algorithm: tblAlgorithmMetaTable.references.algorithm_id(),
});

const dvContentClassificationLinkSat = dvContentClassificationLink.satelliteTable("classified_content_algorithm",{
  content_classification_link_sat_id: primaryKey(),
  link_content_classification_id: dvContentClassificationLink.references.link_content_classification_id(),
  algorithm: tblAlgorithmMetaTable.references.algorithm_id(),
  scores: text(),
  ...dvts.housekeeping.columns,
}
);

const dvGeneralContentClassificationLinkSat = dvContentClassificationLink.satelliteTable("classified_general_topics",{
  general_content_classification_link_sat_id: primaryKey(),
  link_content_classification_id: dvContentClassificationLink.references.link_content_classification_id(),
  algorithm: tblAlgorithmMetaTable.references.algorithm_id(),
  ...dvts.housekeeping.columns,
}
);

// create view - complete results medigy topics
const dvResultsView = SQLa.viewDefinition("results_view_medigy_topics", {
 embeddedStsOptions: SQLa.typicalSqlTextSupplierOptions(),
isIdempotent: false,
before: (viewName) => SQLa.dropView(viewName),
})
`SELECT sc.content,
    sc.content_type,
    a.algorithm_id,
    string_agg(scc_classification.classification, ', '::text) AS classification
   FROM stateful_service_medigy_rdv_miniflux.link_classified_content l
     JOIN stateful_service_medigy_rdv_miniflux.hub_content hc ON l.hub_content_id = hc.hub_content_id
     JOIN stateful_service_medigy_rdv_miniflux.hub_classification hcl ON l.hub_classification_id = hcl.hub_classification_id
     JOIN stateful_service_medigy_rdv_miniflux.sat_classified_content_classified_content_algorithm sacca ON l.link_content_classification_id = sacca.link_content_classification_id
     JOIN stateful_service_medigy_rdv_miniflux.sat_content_content_attribute sc ON hc.hub_content_id = sc.hub_content_id
     JOIN stateful_service_medigy_rdv_miniflux.sat_classification_classification_attribute scc ON hcl.hub_classification_id = scc.hub_classification_id
     JOIN stateful_service_medigy_rdv_miniflux.algorithm a ON sacca.algorithm = a.algorithm_id
     JOIN LATERAL ( SELECT json_array_elements_text(scc.classification::json) AS classification) scc_classification ON true
  GROUP BY sc.content, sc.content_type, a.algorithm_id;`;

// create view - complete results general topics
const dvResultsViewGeneral = SQLa.viewDefinition("results_view_general_topics", {
 embeddedStsOptions: SQLa.typicalSqlTextSupplierOptions(),
isIdempotent: false,
before: (viewName) => SQLa.dropView(viewName),
})
`SELECT sc.content,a.algorithm_id,scc.classification  FROM stateful_service_medigy_rdv_miniflux.link_classified_content l
     JOIN stateful_service_medigy_rdv_miniflux.hub_content hc ON l.hub_content_id = hc.hub_content_id
     JOIN stateful_service_medigy_rdv_miniflux.hub_classification hcl ON l.hub_classification_id = hcl.hub_classification_id
     JOIN stateful_service_medigy_rdv_miniflux.sat_classified_content_classified_general_topics sccg ON l.link_content_classification_id = sccg.link_content_classification_id
     JOIN stateful_service_medigy_rdv_miniflux.sat_content_content_attribute sc ON hc.hub_content_id = sc.hub_content_id
     JOIN stateful_service_medigy_rdv_miniflux.sat_classification_classification_attribute scc ON hcl.hub_classification_id = scc.hub_classification_id
     JOIN stateful_service_medigy_rdv_miniflux.algorithm a ON sccg.algorithm = a.algorithm_id;`;


//create stored procedure to add algorithm
const srb_algorithm = sqlsp.storedRoutineBuilder("upsert_algorithm", {
  algorithm_id: z.string(),
  algorithm_name: z.string(),
});
const { argsSD: { sdSchema: spa }, argsIndex: spi } = srb_algorithm;
const sp_algorithm_insert = sqlsp.storedProcedure(
  srb_algorithm.routineName,
  srb_algorithm.argsDefn,
  (name, args, _, bo) => sqlsp.typedPlPgSqlBody(name, args, ctx, bo),
  {
    embeddedStsOptions: SQLa.typicalSqlTextSupplierOptions(),
    autoBeginEnd: false,
    isIdempotent: false,
  },
)
`BEGIN
  UPDATE stateful_service_medigy_rdv_miniflux.algorithm SET ${tblAlgorithmMetaTable.tableName}.${spa.algorithm_id} = ${spi.algorithm_id},${tblAlgorithmMetaTable.tableName}.${spa.algorithm_name} = ${spi.algorithm_name} WHERE ${tblAlgorithmMetaTable.tableName}.${spa.algorithm_id} = ${spi.algorithm_id};
  IF NOT FOUND THEN
      INSERT INTO stateful_service_medigy_rdv_miniflux.algorithm (${tblAlgorithmMetaTable.tableName}.${spa.algorithm_id},${tblAlgorithmMetaTable.tableName}.${spa.algorithm_name}) VALUES (${spi.algorithm_id},${spi.algorithm_name});
  END IF;
END;`;


//create stored function to load the medigy topics prediction results to dv
const srb_prediction = sqlsp.storedRoutineBuilder("sf-load_output",{});
//const { argsSD: { sdSchema: spp }, argsIndex: sppi } = srb_prediction;
const returns = "void";
const sf_result_load = sqlsp.storedFunction(
  "predictions_result_sf_insert",
  srb_prediction.argsDefn,
  returns,
  (name, args, _, bo) => sqlsp.typedPlPgSqlBody(name, args, ctx, bo),
  {
    embeddedStsOptions: SQLa.typicalSqlTextSupplierOptions(),
    autoBeginEnd: false,
    isIdempotent: false,
  },
)`
BEGIN
  CREATE TEMPORARY TABLE temp_table as
  SELECT feed_title AS content, related_topic_with_title AS classification, model_name as model_name ,prediction_score_title as scores, stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text as hub_content_id, stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text as sat_content_id,
  stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text as hub_classification_id, stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text sat_classification_id,stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text as link_content_classification_id,  'feed_title' as content_type
  FROM stateful_service_medigy_rdv_miniflux.langchain_predictions_result
  UNION all SELECT  feed_content AS content, related_topic_with_content AS classification,model_name as model_name,prediction_score_content as scores,  stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text as hub_content_id, stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text as sat_content_id,
  stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text as hub_classification_id, stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text sat_classification_id,stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text as link_content_classification_id, 'feed_content' as content_type
  FROM stateful_service_medigy_rdv_miniflux.langchain_predictions_result UNION ALL
  SELECT feed_title_and_content AS content, related_topic_with_title_and_content AS classification, model_name as model_name,prediction_score_title_content as scores, stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text as hub_content_id, stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text as sat_content_id,
  stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text as hub_classification_id, stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text sat_classification_id,stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text as link_content_classification_id, 'feed_title_and_content' as content_type
  FROM stateful_service_medigy_rdv_miniflux.langchain_predictions_result;

 INSERT INTO stateful_service_medigy_rdv_miniflux.hub_content (hub_content_id,created_at, created_by, provenance)
 SELECT hub_content_id,current_timestamp, current_user, 'medigy' FROM temp_table;

 INSERT INTO sat_content_content_attribute (hub_content_id, sat_content_id, content,content_type, created_at, created_by, provenance)
 SELECT hub_content_id,sat_content_id,content,content_type,current_timestamp, current_user, 'medigy' FROM temp_table;

 INSERT INTO hub_classification (hub_classification_id, created_at, created_by, provenance)
 SELECT hub_classification_id ,current_timestamp, current_user, 'medigy' FROM temp_table;

 INSERT INTO sat_classification_classification_attribute (hub_classification_id, sat_classification_id, classification, created_at, created_by, provenance)
 SELECT hub_classification_id ,sat_classification_id,classification,current_timestamp, current_user, 'medigy' FROM temp_table;

 INSERT INTO link_classified_content (link_content_classification_id, hub_content_id, hub_classification_id, created_at, created_by, provenance)
 SELECT link_content_classification_id, hub_content_id ,hub_classification_id,current_timestamp, current_user, 'medigy' FROM temp_table;

 INSERT INTO sat_classified_content_classified_content_algorithm (content_classification_link_sat_id, link_content_classification_id, algorithm,scores, created_at, created_by, provenance)
 select stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text,link_content_classification_id,model_name,scores,current_timestamp, current_user, 'medigy' FROM temp_table;
 DROP TABLE IF EXISTS temp_table;
END;
`;

//create stored function to load the general-prediction results to dv
const srb_gprediction = sqlsp.storedRoutineBuilder("sf-load_output_general",{});
//const { argsSD: { sdSchema: spp }, argsIndex: sppi } = srb_prediction;
const greturns = "void";
const sf_gen_result_load = sqlsp.storedFunction(
  "general_predictions_result_sf_insert",
  srb_gprediction.argsDefn,
  greturns,
  (name, args, _, bo) => sqlsp.typedPlPgSqlBody(name, args, ctx, bo),
  {
    embeddedStsOptions: SQLa.typicalSqlTextSupplierOptions(),
    autoBeginEnd: false,
    isIdempotent: false,
  },
)`
 BEGIN
  CREATE TEMPORARY TABLE temp_table as
  SELECT feed_title AS content, related_topic_with_title AS classification, model_name as model_name , stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text as hub_content_id, stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text as sat_content_id,
  stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text as hub_classification_id, stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text sat_classification_id,stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text as link_content_classification_id,  'feed_title' as content_type
  FROM stateful_service_medigy_rdv_miniflux.general_topic_predictions_result;


 INSERT INTO stateful_service_medigy_rdv_miniflux.hub_content (hub_content_id,created_at, created_by, provenance)
 SELECT hub_content_id,current_timestamp, current_user, 'general' FROM temp_table;

 INSERT INTO sat_content_content_attribute (hub_content_id, sat_content_id, content,content_type, created_at, created_by, provenance)
 SELECT hub_content_id,sat_content_id,content,content_type,current_timestamp, current_user, 'general' FROM temp_table;

 INSERT INTO hub_classification (hub_classification_id, created_at, created_by, provenance)
 SELECT hub_classification_id ,current_timestamp, current_user, 'general' FROM temp_table;

 INSERT INTO sat_classification_classification_attribute (hub_classification_id, sat_classification_id, classification, created_at, created_by, provenance)
 SELECT hub_classification_id ,sat_classification_id,classification,current_timestamp, current_user, 'general' FROM temp_table;

 INSERT INTO link_classified_content (link_content_classification_id, hub_content_id, hub_classification_id, created_at, created_by, provenance)
 SELECT link_content_classification_id, hub_content_id ,hub_classification_id,current_timestamp, current_user, 'general' FROM temp_table;

 INSERT INTO sat_classified_content_classified_general_topics (general_content_classification_link_sat_id, link_content_classification_id, algorithm,created_at, created_by, provenance)
 select stateful_service_medigy_rdv_miniflux.uuid_generate_v4()::text,link_content_classification_id,model_name,current_timestamp, current_user, 'general' FROM temp_table;
 DROP TABLE IF EXISTS temp_table;
END;
`;


function sqlDDL(options: {
  destroyFirst?: boolean;
  schemaName?: string;
} = {}) {
  const { destroyFirst, schemaName } = options;

  // NOTE: every time the template is "executed" it will fill out tables, views
  //       in dvts.tablesDeclared, etc.
  // deno-fmt-ignore
  return SQLa.SQL<EmitContext>(dvts.ddlOptions)`
    ${
      destroyFirst && schemaName
        ? `drop schema if exists ${schemaName} cascade;`
        : "-- not destroying first (for development)"
    }
    ${ schemaName
       ? `create schema if not exists ${schemaName};`
       : "-- no schemaName provided" }
        SET search_path TO ${nf_dv_schema.sqlNamespace};
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

        ${tblAlgorithmMetaTable}
        ${dvContentHub}
        ${dvClassificationHub}
        ${dvJobHub}
        ${dvContentHubSat}
        ${dvClassificationHubSat}
        ${dvJobHubSat}
        ${dvContentClassificationLink}
        ${dvContentClassificationJobLink}
        ${dvContentClassificationLinkSat}
        ${dvGeneralContentClassificationLinkSat}
        ${dvResultsView}
        ${dvResultsViewGeneral}
        ${sp_algorithm_insert}
        ${sf_result_load}
        ${sf_gen_result_load}
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

