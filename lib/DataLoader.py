from lib import Configloader

def get_accounts_data_schema():
    schema = """load_date date,active_ind int,account_id string,source_sys string,account_start_date timestamp
              ,legal_title_1 string,legal_title_2 string,tax_id_type string,tax_id string,branch_code string,country string"""

    return schema

def get_parties_data_schema():
    schema = """load_date date,account_id string,party_id string,
                relation_type string,relation_start_date timestamp"""

    return schema

def get_address_data_schema():
    schema = """load_date date,party_id string,address_line_1 string,address_line_2 string,
                city string,postal_code string,country_of_address string,address_start_date date"""

    return schema

def read_accounts(spark, env):
    runtime_filter = Configloader.get_data_filter(env, "account.filter")

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(get_accounts_data_schema())
        .load("test_data/accounts/account_samples.csv")
    )
    if isinstance(runtime_filter, str) and runtime_filter.strip() and runtime_filter.strip().lower() != "true":
        df = df.where(runtime_filter)
    return df

def read_parties(spark, env):
    runtime_filter = Configloader.get_data_filter(env, "party.filter")

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(get_parties_data_schema())
        .load("test_data/parties/party_samples.csv")
    )
    if isinstance(runtime_filter, str) and runtime_filter.strip() and runtime_filter.strip().lower() != "true":
        df = df.where(runtime_filter)
    return df

def read_address(spark, env):
    runtime_filter = Configloader.get_data_filter(env, "address.filter")

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(get_address_data_schema())
        .load("test_data/party_address/address_samples.csv")
    )
    if isinstance(runtime_filter, str) and runtime_filter.strip() and runtime_filter.strip().lower() != "true":
        df = df.where(runtime_filter)
    return df