import { DynamoDBClient, AttributeValue } from '@aws-sdk/client-dynamodb';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import { getDDBRawItem } from '../ddb-raw/get-ddb-raw-item';
import { KeysInfo, QueryOptions, ScanOptions } from '../ddb-raw';
import { queryDDBRawItems } from '../ddb-raw/query-ddb-raw-items';
import {
  scanDDBRawChunk,
  scanDDBRawItems,
} from '../ddb-raw/scan-ddb-raw-items';
import { DDBItemsChunk } from './ddb-read-repository.type';
import {
  lastEvaluatedKeyObjToStr,
  lastEvaluatedKeyStrToObj,
} from 'src/ddb-utils';

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

  public async scanDDBItems<T>(scanOptions?: ScanOptions) {
    const rawItems = await scanDDBRawItems(
      this.ddbClient,
      this.tableName,
      scanOptions
    );
    const retObjs = rawItems.map((rawItem) => unmarshall(rawItem)) as T[];
    return retObjs;
  }

  public async scanDDBChunk<T>(
    nextToken: string | undefined,
    scanOptions?: ScanOptions
  ): Promise<DDBItemsChunk<T>> {
    const rawItemsChunk = await scanDDBRawChunk(
      this.ddbClient,
      this.tableName,
      lastEvaluatedKeyStrToObj(nextToken),
      scanOptions
    );
    const retObjs = rawItemsChunk.items.map((rawItem) =>
      unmarshall(rawItem)
    ) as T[];
    return {
      items: retObjs,
      nextToken: lastEvaluatedKeyObjToStr(rawItemsChunk.lastEvaluatedKey),
    };
  }
}
