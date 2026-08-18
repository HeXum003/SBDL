from pyspark.sql.functions import col, filter, collect_list, array, when, isnull, filter, current_timestamp, date_format, expr, struct, lit
import uuid
from lib import DataLoader, Configloader, Utils
from lib.logger import Log4j2

def get_insert_operation(column, alias):
    return struct(lit("INSERT").alias("operation"),
                  column.alias("newValue"),
                  lit(None).alias("oldValue")).alias(alias)

def get_contract(df):
    contract_df = array(when(~isnull("legal_title_1"),
                             struct(lit("lgl_ttl_ln_1").alias("contractTitleLineType"),
                         col("legal_title_1").alias("contractTitleLine")).alias("contractTitle")),
                        when(~isnull("legal_title_2"),
                             struct(lit("lgl_ttl_ln_2").alias("contractTitleLineType"),
                                    col("legal_title_2").alias("contractTitleLine")).alias("contractTitle"))
                            )
    contract_title_nl = filter(contract_df, lambda x: ~isnull(x))

    tax_identifier = struct(col("tax_id_type").alias("taxIdType"),
                            col("tax_id").alias("taxId")).alias("taxIdentifier")

    return df.select("account_id", get_insert_operation(col("account_id"), alias="contractIdentifier"),
                     get_insert_operation(col("source_sys"), alias="sourceSystemIdentifier"),
                     get_insert_operation(col("account_start_date"), alias="contactStartDateTime"),
                     get_insert_operation(contract_title_nl, alias="contractTitle"),
                     get_insert_operation(tax_identifier, alias="taxIdentifier"),
                     get_insert_operation(col("branch_code"), alias="contractBranchCode"),
                     get_insert_operation(col("country"), alias="contractCountry")
                     )

def get_relations(df):
    return df.select("account_id", "party_id",
                     get_insert_operation(col("party_id"), "partyIdentifier"),
                     get_insert_operation(col("relation_type"), "partyRelationshipType"),
                     get_insert_operation(col("relation_start_date"), "partyRelationStartDateTime")
                     )

def get_address(df):
    address_df = struct(col("address_line_1").alias("addressLine1"),
                        col("address_line_2").alias("addressLine2"),
                        col("city").alias("addressCity"),
                        col("postal_code").alias("addressPostalCode"),
                        col("country_of_address").alias("addressCountry"),
                        col("address_start_date").alias("addressStartDate")
                        )

    return df.select("party_id", get_insert_operation(address_df, "partyAddress"))

def join_party_address(party_df, address_df):
    return (party_df.join(address_df, "party_id", "left")
            .groupBy("account_id")
            .agg(collect_list(struct("partyIdentifier", "partyRelationshipType",
                                     "partyRelationStartDateTime", "partyAddress").alias("partyDetails")
                              ).alias("partyRelations"))
            )

def join_contract_party(contract_df, party_df):
    return contract_df.join(party_df, "account_id", "left")

def apply_event_header(spark, df):
    header_info = [("SBDL-Contract", 1, 0), ]

    header_df = (spark.createDataFrame(header_info)
                 .toDF("eventType", "majorSchemaVersion", "minorSchemaVersion")
                 )

    event_df = (header_df.hint("broadcast").crossJoin(df)
                .select(struct(expr("uuid()").alias("eventIdentifier"),
                      col("eventType"), col("majorSchemaVersion"), col("minorSchemaVersion"),
                      lit(date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssZ")).alias("eventDateTime")
                      ).alias("eventHeader"),
                      array(struct(lit("contractIdentifier").alias("keyField"),
                            col("account_id").alias("keyValue"))).alias("keys"),
                      struct(col("contractIdentifier"),
                             col("sourceSystemIdentifier"),
                             col("contactStartDateTime"),
                             col("contractTitle"),
                             col("taxIdentifier"),
                             col("contractBranchCode"),
                             col("contractCountry"),
                             col("partyRelations")).alias("payload")
                      )
                )
    return event_df

def transformations(spark, job_run_env):
    job_run_id = "SBDL-" + str(uuid.uuid4())

    print("Initializing SBDL Job in " + job_run_env + " Job ID: " + job_run_id)
    print("Creating Spark Session")

    log = Log4j2(spark)

    log.info("Reading SBDL Account Data")
    accounts_df = DataLoader.read_accounts(spark, job_run_env)
    accounts_df.show()
    log.info("Success Starting with Next..............")

    log.info("Applying Transformation to Account Data")
    contract_df = get_contract(accounts_df)
    contract_df.show()
    log.info("Success Starting with Next..............")

    log.info("Reading SBDL Party Data")
    parties_df = DataLoader.read_parties(spark, job_run_env)
    parties_df.show()
    log.info("Success Starting with Next..............")

    log.info("Applying Transformation to Parties Data")
    relations_df = get_relations(parties_df)
    relations_df.show()
    log.info("Success Starting with Next..............")

    log.info("Reading SBDL Address Data")
    address_df = DataLoader.read_address(spark, job_run_env)
    address_df.show()
    log.info("Success Starting with Next..............")

    log.info("Applying Transformation to Address Data")
    relation_address_df = get_address(address_df)
    relation_address_df.show()
    log.info("Success Starting with Next..............")

    log.info("Applying Join to Party Relations and Address")
    party_address_df = join_party_address(relations_df, relation_address_df)
    party_address_df.show()
    log.info("Success Starting with Next..............")

    log.info("Applying Join to Account and Parties")
    data_df = join_contract_party(contract_df, party_address_df)
    data_df.show()
    log.info("Success Starting with Next..............")

    log.info("Applying Event Header and create Event")
    final_df = apply_event_header(spark, data_df)
    final_df.show()
    log.info("Success - All Executed Successfully..............")
    return final_df

def contract_json(spark, env):
    account_df = DataLoader.read_accounts(spark, env)
    contract_df = get_contract(account_df)

    Utils.save_tojson(contract_df, "contract_df")

def final_json(spark, env):
    final_df = transformations(spark, env)

    Utils.save_tojson(final_df, "final_df")
