import { DynamoDBClient, AttributeValue } from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';
import { putDDBRawItem } from '../ddb-raw/put-ddb-raw-item';
import { buildSETUpdateExpression, objToPrefixedDDB } from '../ddb-utils';
import { updateDDBRawItem } from '../ddb-raw/update-ddb-raw-item';

export class DDBWriteRepository {
  private tableName: string;
  private ddbClient: DynamoDBClient;
  constructor(tableName: string, ddbClient: DynamoDBClient) {
    this.tableName = tableName;
    this.ddbClient = ddbClient;
  }

  public async putDDBItem(pk: string, sk: string, item: Object) {
    const rawItem = buildRawItem(pk, sk, item);
    await putDDBRawItem(this.ddbClient, this.tableName, rawItem);
  }

  public async upsertDDBItem(pk: string, sk: string, item: Object) {
    const key: Record<string, AttributeValue> = {
      pk: { S: pk },
      sk: { S: sk },
    };
    let updateExp = buildSETUpdateExpression(item);
    const updateExpValues = objToPrefixedDDB(item, ':');
    await updateDDBRawItem(
      this.ddbClient,
      this.tableName,
      key,
      updateExp,
      updateExpValues
    );
  }
}

function buildRawItem(
  pk: string,
  sk: string,
  item: Object
): Record<string, AttributeValue> {
  const rawItem: Record<string, AttributeValue> = {
    PK: { S: pk },
    SK: { S: sk },
    ...marshall(item),
  };
  return rawItem;
}
