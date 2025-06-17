import {
  DynamoDBClient,
  AttributeValue,
  UpdateItemCommandInput,
  UpdateItemCommand,
} from '@aws-sdk/client-dynamodb';

/**
 * Update an existing record or add a new one if the key doesn't exist
 * @param ddbClient Client dynamoDB
 * @param tableName Table name to add data
 * @param key Partition and sort keys to update
 * @param updateExp Update instruction
 * @param expAttValues New values
 */
export async function updateDDBRawItem(
  ddbClient: DynamoDBClient,
  tableName: string,
  key: Record<string, AttributeValue>,
  updateExp: string,
  expAttValues: Record<string, AttributeValue>
) {
  const params: UpdateItemCommandInput = {
    TableName: tableName,
    Key: key,
    UpdateExpression: updateExp,
    ExpressionAttributeValues: expAttValues,
  };
  try {
    await ddbClient.send(new UpdateItemCommand(params));
  } catch (error) {
    console.log('updateDDBRawItem failed:', error.message);
  }
}
