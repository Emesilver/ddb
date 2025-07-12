import {
  DynamoDBClient,
  AttributeValue,
  ScanCommandInput,
  ScanCommand,
  ScanCommandOutput,
} from '@aws-sdk/client-dynamodb';
import { DDBItemsRawChunk, ScanOptions } from './ddb-raw.type';

export async function scanDDBRawItems(
  ddbClient: DynamoDBClient,
  tableName: string,
  scanOptions?: ScanOptions
) {
  const params = buildScanParams(tableName, scanOptions);
  try {
    let scanResult: ScanCommandOutput;
    let allItems: Record<string, AttributeValue>[] = [];
    do {
      params.ExclusiveStartKey = scanResult?.LastEvaluatedKey;
      scanResult = await ddbClient.send(new ScanCommand(params));
      allItems = allItems.concat(scanResult.Items);
    } while (
      scanResult.LastEvaluatedKey ||
      (scanOptions?.limit && allItems.length < scanOptions.limit)
    );
    return scanOptions?.limit ? allItems.slice(0, scanOptions.limit) : allItems;
  } catch (error) {
    throw new Error('scanDDBRawItems failed:' + error.message);
  }
}

export async function scanDDBRawChunk(
  ddbClient: DynamoDBClient,
  tableName: string,
  exclusiveStartKey: Record<string, AttributeValue> | undefined,
  scanOptions?: ScanOptions
): Promise<DDBItemsRawChunk> {
  const params = buildScanParams(tableName, scanOptions);
  params.ExclusiveStartKey = exclusiveStartKey;
  try {
    const scanResult = await ddbClient.send(new ScanCommand(params));
    return {
      lastEvaluatedKey: scanResult.LastEvaluatedKey,
      items: scanResult.Items,
    };
  } catch (error) {
    console.error(error);
    throw new Error('scanDDBRawItems failed');
  }
}

function buildScanParams(tableName: string, scanOptions: ScanOptions) {
  const params: ScanCommandInput = {
    TableName: tableName,
  };
  if (scanOptions?.indexName) params.IndexName = scanOptions.indexName;
  if (scanOptions?.limit) params.Limit = scanOptions.limit;
  if (scanOptions?.exclusiveStartKey)
    params.ExclusiveStartKey = scanOptions.exclusiveStartKey;
  if (scanOptions?.scanFilter) {
    params.FilterExpression = scanOptions.scanFilter.filterExpression;
    params.ExpressionAttributeValues =
      scanOptions.scanFilter.expressionAttributeValues;
  }
  return params;
}
