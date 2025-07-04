import { DynamoDBClient, AttributeValue } from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import { putDDBRawItem } from '../ddb-raw/put-ddb-raw-item';
import { buildSETUpdateExpression, objToPrefixedDDB } from '../ddb-utils';
import { updateDDBRawItem } from '../ddb-raw/update-ddb-raw-item';
import { updatedNewDDBRawItem } from '../ddb-raw/updatednew-ddb-raw-item';

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

  /**
   * Increment a number field in atomic way.
   * @param pk Partition key
   * @param sk Sort key
   * @param fieldName Field name to increment
   * @returns Incremented item of type T
   */
  public async incrementedNumber<T>(pk: string, sk: string, fieldName: string) {
    const key: Record<string, AttributeValue> = {
      pk: { S: pk },
      sk: { S: sk },
    };
    const updateExp = `SET ${fieldName} = if_not_exists(${fieldName}, :zero) + :inc`;
    const expAttValues: Record<string, AttributeValue> = {
      ':zero': { N: '0' },
      ':inc': { N: '1' },
    };
    const rawItem = await updatedNewDDBRawItem(
      this.ddbClient,
      this.tableName,
      key,
      updateExp,
      expAttValues
    );
    return rawItem ? (unmarshall(rawItem) as T) : null;
  }
}

function buildRawItem(
  pk: string,
  sk: string,
  item: Object
): Record<string, AttributeValue> {
  const rawItem: Record<string, AttributeValue> = {
    pk: { S: pk },
    sk: { S: sk },
    ...marshall(item),
  };
  return rawItem;
}
