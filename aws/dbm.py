import boto3
import graphene
import time
import yaml

client = boto3.client('dynamodb', endpoint_url='http://localhost:8000')
print("Tables:\n", client.list_tables())

with open("config.yml") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

TableName = "TableName"
AttrDefs = "AttributeDefinitions"
AttrName = "AttributeName"
AttrType = "AttributeType"
KeySchema = "KeySchema"
KeyType = "KeyType"
IndName = "IndexName"
HASH = "HASH"
RANGE = "RANGE"
Projection = "Projection"
ProjType = "ProjectionType"
LocalSecondaryIndexes = "LocalSecondaryIndexes"
ProvisionedThroughput = "ProvisionedThroughput"

class DynamoConfig:
    """Wrapper class for creating boto3 compliant dynamodb configuration
    to simplify the process of customizing a dynamo table"""
    def __init__(self, config: dict, name_override: str = None):
        self.extract_key = lambda dictionary: list(dictionary.keys())[0]
        self.extract_val = lambda dictionary: list(dictionary.values())[0]
        
        self.pk = list(config[AttrDefs][0].keys())[0]
        self.build_config = dict()
        self.build_config[TableName] = config[TableName] if name_override is None else name_override
        self.build_config[ProvisionedThroughput] = config[ProvisionedThroughput]
        
        self.extract_attrs()
        self.extract_schemas()
        self.extract_secondary_indices()
    
    @property
    def config(self):
        return self.build_config

    def extract_attrs(self):
        self.build_config[AttrDefs] = [
            {AttrName: self.extract_key(attr), AttrType: self.extract_val(attr)} for attr in config[AttrDefs]
        ]

    def extract_schemas(self):
        self.build_config[KeySchema] = [
            {AttrName: self.extract_key(schema), KeyType: self.extract_val(schema)} for schema in config[KeySchema]
        ]

    def extract_secondary_indices(self):
        index_name = lambda sort_key: f"{sort_key}_index"
        self.build_config[LocalSecondaryIndexes] = [
            {
                IndName: index_name(self.extract_key(index)),
                KeySchema: [
                    {AttrName: self.pk, KeyType: HASH}, 
                    {AttrName: self.extract_key(index), KeyType: RANGE}
                ],
                Projection: {ProjType: self.extract_val(index)}
            }
            for index in config[LocalSecondaryIndexes]
        ]


# dynamo_config = TableConfig(config).config
# response = client.create_table(**dynamo_config)


#################################################################################################

class MetadataRecord:
    def __init__(self, task_id, application, iid, metadata):
        self.task_id = task_id
        self.application = application
        self.iid = iid
        self.metadata = metadata

    @property
    def id(self):
        return "__".join([self.task_id, self.application, self.iid])

class DummyDBM:
    def __init__(self, table):
        self.table = table

    def post(self, record: MetadataRecord):
        id = record.id
        print(id)

dbm = DummyDBM("welte-table")

class Metadata(graphene.ObjectType):
    task_id = graphene.String()
    application = graphene.String()
    iid = graphene.String()
    product_type = graphene.String()
    metadata = graphene.Field(lambda: Metadata)

    def mutate(self, info):
        record = MetadataRecord(self.task_id, self.application, self.iid, self.metadata)
        dbm.post(record)
