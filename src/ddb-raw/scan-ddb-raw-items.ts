import {
  DynamoDBClient,
  AttributeValue,
  ScanCommandInput,
  ScanCommand,
  ScanCommandOutput,
} from '@aws-sdk/client-dynamodb';
import { ScanOptions } from './ddb-raw.type';

export async function scanDDBRawItems(
  ddbClient: DynamoDBClient,
  tableName: string,
  scanOptions?: ScanOptions
) {
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
  try {
    let scanResult: ScanCommandOutput;
    let allItems: Record<string, AttributeValue>[] = [];
    do {
      params.ExclusiveStartKey = scanResult?.LastEvaluatedKey;
      scanResult = await ddbClient.send(new ScanCommand(params));
      allItems = allItems.concat(scanResult.Items);
    } while (scanResult.LastEvaluatedKey);
    return allItems;
  } catch (error) {
    throw new Error('scanDDBRawItems failed:' + error.message);
  }
}
