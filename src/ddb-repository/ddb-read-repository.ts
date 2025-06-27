import { DynamoDBClient, AttributeValue } from '@aws-sdk/client-dynamodb';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import { getDDBRawItem } from '../ddb-raw/get-ddb-raw-item';
import { KeysInfo, QueryOptions } from '../ddb-raw';
import { queryDDBRawItems } from '../ddb-raw/query-ddb-raw-items';

export class DDBReadRepository {
  private tableName: string;
  private ddbClient: DynamoDBClient;
  constructor(tableName: string, ddbClient: DynamoDBClient) {
    this.tableName = tableName;
    this.ddbClient = ddbClient;
  }
  public async getDDBItem<T>(pk: string, sk: string) {
    const key: Record<string, AttributeValue> = {
      pk: { S: pk },
      sk: { S: sk },
    };
    const rawItem = await getDDBRawItem(this.ddbClient, this.tableName, key);
    return rawItem ? (unmarshall(rawItem) as T) : null;
  }

  public async queryDDBItems<T>(pk: string, queryOptions?: QueryOptions) {
    const keysInfo: KeysInfo = {
      pkName: 'pk',
      skName: 'sk',
      pkValue: pk,
    };
    const rawItems = await queryDDBRawItems(
      this.ddbClient,
      this.tableName,
      keysInfo,
      queryOptions
    );
    const retObjs = rawItems.map((rawItem) => unmarshall(rawItem)) as T[];
    return retObjs;
  }
}
