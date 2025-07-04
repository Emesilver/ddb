import {
  DynamoDBClient,
  AttributeValue,
  UpdateItemCommandInput,
  UpdateItemCommand,
} from '@aws-sdk/client-dynamodb';

/**
 * Update an item in DynamoDB and return the updated item.
 * @param ddbClient Client dynamoDB
 * @param tableName Table name to update/add data
 * @param key Partition and sort keys to update
 * @param updateExp Update instruction
 * @param expAttValues New values
 * @returns
 */
export async function updatedNewDDBRawItem(
  ddbClient: DynamoDBClient,
  tableName: string,
  key: Record<string, AttributeValue>,
  updateExp: string,
  expAttValues: Record<string, AttributeValue>
): Promise<Record<string, AttributeValue>> {
  const params: UpdateItemCommandInput = {
    TableName: tableName,
    Key: key,
    UpdateExpression: updateExp,
    ExpressionAttributeValues: expAttValues,
    ReturnValues: 'UPDATED_NEW',
  };
  try {
    const result = await ddbClient.send(new UpdateItemCommand(params));
    return result.Attributes;
  } catch (error) {
    console.log('updatednewDDBRawItem failed:', error.message);
  }
}
