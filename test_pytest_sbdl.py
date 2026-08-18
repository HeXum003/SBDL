import pytest
from lib import DataLoader, Transformation, Configloader
from lib.Utils import get_spark_session

from pyspark.testing.utils import assertDataFrameEqual

from pyspark.sql.types import StructType, StructField, StringType, NullType, TimestampType, ArrayType, DateType, Row
from datetime import datetime, date


@pytest.fixture(scope='session')
def spark():
    return get_spark_session("LOCAL")

@pytest.fixture(scope='session')
def expected_rows_parties():
    return [Row(load_date=date(2022, 8, 2), account_id='6982391060',
                party_id='9823462810', relation_type='F-N', relation_start_date=datetime(2019, 7, 29, 6, 21, 32)),
            Row(load_date=date(2022, 8, 2), account_id='6982391061', party_id='9823462811', relation_type='F-N',
                relation_start_date=datetime(2018, 8, 31, 5, 27, 22)),
            Row(load_date=date(2022, 8, 2), account_id='6982391062', party_id='9823462812', relation_type='F-N',
                relation_start_date=datetime(2018, 8, 25, 15, 50, 29)),
            Row(load_date=date(2022, 8, 2), account_id='6982391063', party_id='9823462813', relation_type='F-N',
                relation_start_date=datetime(2018, 5, 11, 7, 23, 28)),
            Row(load_date=date(2022, 8, 2), account_id='6982391064', party_id='9823462814', relation_type='F-N',
                relation_start_date=datetime(2019, 6, 6, 14, 18, 12)),
            Row(load_date=date(2022, 8, 2), account_id='6982391065', party_id='9823462815', relation_type='F-N',
                relation_start_date=datetime(2019, 5, 4, 5, 12, 37)),
            Row(load_date=date(2022, 8, 2), account_id='6982391066', party_id='9823462816', relation_type='F-N',
                relation_start_date=datetime(2019, 5, 15, 10, 39, 29)),
            Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462817', relation_type='F-N',
                relation_start_date=datetime(2018, 5, 16, 9, 53, 4)),
            Row(load_date=date(2022, 8, 2), account_id='6982391068', party_id='9823462818', relation_type='F-N',
                relation_start_date=datetime(2017, 11, 27, 1, 20, 12)),
            Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462820', relation_type='F-S',
                relation_start_date=datetime(2017, 11, 20, 14, 18, 5)),
            Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462821', relation_type='F-S',
                relation_start_date=datetime(2018, 7, 19, 18, 56, 57))]

@pytest.fixture(scope="session")
def expected_parties_list():
    return [
        (date(2022, 8, 2), '6982391060', '9823462810', 'F-N', datetime.fromisoformat('2019-07-29 06:21:32.000+05:30')),
        (date(2022, 8, 2), '6982391061', '9823462811', 'F-N', datetime.fromisoformat('2018-08-31 05:27:22.000+05:30')),
        (date(2022, 8, 2), '6982391062', '9823462812', 'F-N', datetime.fromisoformat('2018-08-25 15:50:29.000+05:30')),
        (date(2022, 8, 2), '6982391063', '9823462813', 'F-N', datetime.fromisoformat('2018-05-11 07:23:28.000+05:30')),
        (date(2022, 8, 2), '6982391064', '9823462814', 'F-N', datetime.fromisoformat('2019-06-06 14:18:12.000+05:30')),
        (date(2022, 8, 2), '6982391065', '9823462815', 'F-N', datetime.fromisoformat('2019-05-04 05:12:37.000+05:30')),
        (date(2022, 8, 2), '6982391066', '9823462816', 'F-N', datetime.fromisoformat('2019-05-15 10:39:29.000+05:30')),
        (date(2022, 8, 2), '6982391067', '9823462817', 'F-N', datetime.fromisoformat('2018-05-16 09:53:04.000+05:30')),
        (date(2022, 8, 2), '6982391068', '9823462818', 'F-N', datetime.fromisoformat('2017-11-27 01:20:12.000+05:30')),
        (date(2022, 8, 2), '6982391067', '9823462820', 'F-S', datetime.fromisoformat('2017-11-20 14:18:05.000+05:30')),
        (date(2022, 8, 2), '6982391067', '9823462821', 'F-S', datetime.fromisoformat('2018-07-19 18:56:57.000+05:30'))]

@pytest.fixture(scope="session")
def expected_contract_df(spark):
    schema = StructType([StructField("account_id", StringType()),
                         StructField("contractIdentifier",
                                     StructType([StructField("operation", StringType()),
                                                StructField("newValue", StringType()),
                                                 StructField("oldValue", NullType())])),
                         StructField("sourceSystemIdentifier",
                                     StructType([StructField("operation", StringType()),
                                                 StructField("newValue", StringType()),
                                                 StructField("oldValue", NullType())])),
                         StructField("contactStartDateTime",
                                     StructType([StructField("operation", StringType()),
                                                 StructField("newValue", TimestampType()),
                                                 StructField("oldValue", NullType())])),
                         StructField("contractTitle",
                                     StructType([StructField("operation", StringType()),
                                                 StructField("newValue" ,
                                                             ArrayType(StructType([
                                                                 StructField("contractTitleLineType", StringType()),
                                                                 StructField("contractTitleLine", StringType())
                                                             ]))),
                                                 StructField("oldValue", NullType())])),
                         StructField("taxIdentifier",
                                     StructType([StructField("operation", StringType()),
                                                 StructField("newValue" ,
                                                             StructType([StructField("taxIdType", StringType()),
                                                                         StructField("taxId", StringType())])),
                                                 StructField("oldValue", NullType())])),
                         StructField("contractBranchCode",
                                     StructType([StructField("operation", StringType()),
                                                 StructField("newValue", StringType()),
                                                 StructField("oldValue", NullType())])),
                         StructField('contractCountry',
                                     StructType([StructField("operation", StringType()),
                                                 StructField("newValue", StringType()),
                                                 StructField("oldValue", NullType())])),

                         ])
    return spark.read.format("json").schema(schema).load("test_data/results/contract_df.json")

@pytest.fixture(scope="session")
def expected_final_df(spark):
    schema = StructType(
        [StructField('keys',
                     ArrayType(StructType([StructField('keyField', StringType()),
                                           StructField('keyValue', StringType())]))),
         StructField('payload',
                     StructType([
                         StructField('contractIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('sourceSystemIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contactStartDateTime',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', TimestampType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractTitle',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', ArrayType(
                                                     StructType([StructField('contractTitleLineType', StringType()),
                                                                 StructField('contractTitleLine', StringType())]))),
                                                 StructField('oldValue', NullType())])),
                         StructField('taxIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             StructType([StructField('taxIdType', StringType()),
                                                                         StructField('taxId', StringType())])),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractBranchCode',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractCountry',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('partyRelations',
                                     ArrayType(StructType([
                                         StructField('partyIdentifier',
                                                     StructType([
                                                         StructField('operation', StringType()),
                                                         StructField('newValue', StringType()),
                                                         StructField('oldValue', NullType())])),
                                         StructField('partyRelationshipType',
                                                     StructType([
                                                         StructField('operation', StringType()),
                                                         StructField('newValue', StringType()),
                                                         StructField('oldValue', NullType())])),
                                         StructField('partyRelationStartDateTime',
                                                     StructType([
                                                         StructField('operation', StringType()),
                                                         StructField('newValue', TimestampType()),
                                                         StructField('oldValue', NullType())])),
                                         StructField('partyAddress',
                                                     StructType([StructField('operation', StringType()),
                                                                 StructField(
                                                                     'newValue',
                                                                     StructType(
                                                                         [StructField('addressLine1', StringType()),
                                                                          StructField('addressLine2', StringType()),
                                                                          StructField('addressCity', StringType()),
                                                                          StructField('addressPostalCode',
                                                                                      StringType()),
                                                                          StructField('addressCountry', StringType()),
                                                                          StructField('addressStartDate', DateType())
                                                                          ])),
                                                                 StructField('oldValue', NullType())]))])))]))])
    return spark.read.format("json").schema(schema).load("test_data/results/final_df.json").select("keys", "payload")

def test_blank_test(spark):
    print(spark.version)
    assert spark.version == "4.2.0"

def test_get_config():
    conf_local = Configloader.get_config("LOCAL")
    conf_qa = Configloader.get_config("QA")
    assert conf_qa["hive.database"] == "sbdl_db_qa"

def test_read_account(spark, env="LOCAL"):
    accounts_df = DataLoader.read_accounts(spark, env)
    assert accounts_df.count() == 8

def test_read_parties_row(spark, expected_rows_parties):
    actual_row_parties = DataLoader.read_parties(spark, "LOCAL").collect()
    assert expected_rows_parties == actual_row_parties

def test_read_parties(spark, expected_parties_list):
    expected_parties_df = spark.createDataFrame(expected_parties_list)
    actual_parties_list = DataLoader.read_parties(spark, "LOCAL")
    assertDataFrameEqual(expected_parties_df, actual_parties_list, ignoreColumnName=True)

def test_read_party_schema(spark, expected_parties_list):
    expected_df = spark.createDataFrame(expected_parties_list, DataLoader.get_parties_data_schema())
    actual_df = DataLoader.read_parties(spark, "LOCAL")
    assertDataFrameEqual(expected_df, actual_df)

def test_get_contract(spark, expected_contract_df):
    account_df = DataLoader.read_accounts(spark, "LOCAL")
    actual_contract_df = Transformation.get_contract(account_df)
    assert expected_contract_df.collect() == actual_contract_df.collect()
    assertDataFrameEqual(expected_contract_df, actual_contract_df, ignoreColumnName=True)

def test_kafka_kv_df(spark, expected_final_df):
    account_df = DataLoader.read_accounts(spark, "LOCAL")
    parties_df = DataLoader.read_parties(spark, "LOCAL")
    address_df = DataLoader.read_address(spark, "LOCAL")
    #--------- Transformations ---------#
    contract_df = Transformation.get_contract(account_df)
    relations_df = Transformation.get_relations(parties_df)
    party_address_df = Transformation.get_address(address_df)
    join_pa_df = Transformation.join_party_address(relations_df, party_address_df)
    final_join_df = Transformation.join_contract_party(contract_df, join_pa_df)
    #--------- Final -------------------#
    actual_final_df = Transformation.apply_event_header(spark, final_join_df).select("keys", "payload")
    assertDataFrameEqual(expected_final_df, actual_final_df)