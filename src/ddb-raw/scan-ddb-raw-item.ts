import {
  DynamoDBClient,
  AttributeValue,
  ScanCommandInput,
  ScanCommand,
  ScanCommandOutput,
} from '@aws-sdk/client-dynamodb';

export type ScanFilter = {
  filterExpression: string;
  expressionAttributeValues: Record<string, AttributeValue>;
};
export type ScanOptions = {
  indexName?: string;
  scanFilter?: ScanFilter;
};
export async function scanDDBRawItems(
  ddbClient: DynamoDBClient,
  tableName: string,
  scanOptions?: ScanOptions
) {
  const params: ScanCommandInput = {
    TableName: tableName,
  };
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
