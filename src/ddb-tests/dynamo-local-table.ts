import {
  DynamoDB,
  CreateTableCommandInput,
  DeleteTableCommandInput,
  DynamoDBClientConfig,
  GlobalSecondaryIndex,
  KeySchemaElement,
  AttributeDefinition,
} from '@aws-sdk/client-dynamodb';
import { TableIndexInfo } from '../ddb-raw';

export async function createLocalTable(
  tableName: string,
  pkName: string,
  skName: string,
  indexes: TableIndexInfo[]
) {
  const dynamodb = new DynamoDB(getDynamoLocalConfig());

  const createParam: CreateTableCommandInput = {
    TableName: tableName,
    AttributeDefinitions: getAttributes(pkName, skName, indexes),
    KeySchema: [
      {
        AttributeName: pkName,
        KeyType: 'HASH',
      },
      {
        AttributeName: skName,
        KeyType: 'RANGE',
      },
    ],
    BillingMode: 'PAY_PER_REQUEST',
    GlobalSecondaryIndexes: getGSIs(indexes),
  };
  try {
    await dynamodb.createTable(createParam);
  } catch (err) {
    // Local dynamoDB takes a little to delete table. If it fails to create, it
    // could be previous test files deleting this table yet
    const waitTimeOut = (secs: number) => {
      return new Promise((resolve) => setTimeout(resolve, secs * 1000));
    };
    await waitTimeOut(0.5);
    try {
      console.log('Creating table after 0.5 secs: ', tableName);
      await dynamodb.createTable(createParam);
    } catch (error) {
      console.log(
        'Fail to create table after 1 second wait: ',
        tableName,
        err.message
      );
    }
  }
}

export async function deleteLocalTable(tableName: string) {
  const dynamodb = new DynamoDB(getDynamoLocalConfig());
  const deleteTableParam: DeleteTableCommandInput = {
    TableName: tableName,
  };
  try {
    await dynamodb.deleteTable(deleteTableParam);
  } catch (err) {
    console.log('Fail to delete ', tableName);
    console.error(err);
  }
}

function getGSIs(
  indexes: TableIndexInfo[]
): GlobalSecondaryIndex[] | undefined {
  const gsis = indexes.map((index) => {
    const pkSchema: KeySchemaElement = {
      AttributeName: index.pkName,
      KeyType: 'HASH',
    };
    const skSchema: KeySchemaElement = {
      AttributeName: index.skName,
      KeyType: 'RANGE',
    };
    const gsi: GlobalSecondaryIndex = {
      IndexName: index.name,
      KeySchema: [pkSchema, skSchema],
      Projection: { ProjectionType: 'ALL' },
    };
    return gsi;
  });
  return gsis.length ? gsis : undefined;
}

function getAttributes(
  pkName: string,
  skName: string,
  indexes: TableIndexInfo[]
): AttributeDefinition[] {
  const makeSAttribute = (name: string): AttributeDefinition => ({
    AttributeName: name,
    AttributeType: 'S',
  });

  const attributes = [makeSAttribute(pkName), makeSAttribute(skName)];
  indexes.forEach((index) => {
    if (!attributes.find((attr) => attr.AttributeName === index.pkName))
      attributes.push(makeSAttribute(index.pkName));
    if (!attributes.find((attr) => attr.AttributeName === index.skName))
      attributes.push(makeSAttribute(index.skName));
  });
  return attributes;
}

export function getDynamoLocalConfig() {
  const dynamoOptionsLocal: DynamoDBClientConfig = {
    region: 'localhost',
    endpoint: 'http://localhost:8000',
  };
  return dynamoOptionsLocal;
}
