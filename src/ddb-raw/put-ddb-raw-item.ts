import {
  DynamoDBClient,
  AttributeValue,
  PutItemCommandInput,
  PutItemCommand,
} from '@aws-sdk/client-dynamodb';

/**
 * Add a new record or override an existing one
 * @param ddbClient Client dynamoDB
 * @param tableName Table name to add data
 * @param rawItem Record to add
 */
export async function putDDBRawItem(
  ddbClient: DynamoDBClient,
  tableName: string,
  rawItem: Record<string, AttributeValue>
) {
  const params: PutItemCommandInput = {
    TableName: tableName,
    Item: rawItem,
  };
  try {
    await ddbClient.send(new PutItemCommand(params));
  } catch (error) {
    console.log('putDDBRawItem failed:', error.message);
  }
}
