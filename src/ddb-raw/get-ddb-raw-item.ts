import {
  DynamoDBClient,
  AttributeValue,
  GetItemCommandInput,
  GetItemCommand,
} from '@aws-sdk/client-dynamodb';

/**
 * Read a DynamoDB item
 * @param ddbClient Client dynamoDB
 * @param tableName Table name to add data
 * @param key Partition and sort keys to find
 * @returns a record in DynamoDB format
 */
export async function getDDBRawItem(
  ddbClient: DynamoDBClient,
  tableName: string,
  key: Record<string, AttributeValue>
): Promise<Record<string, AttributeValue>> {
  const params: GetItemCommandInput = {
    TableName: tableName,
    Key: key,
  };
  try {
    const getResult = await ddbClient.send(new GetItemCommand(params));
    return getResult.Item;
  } catch (error) {
    throw new Error('getDDBRawItem failed:' + error.message);
  }
}
