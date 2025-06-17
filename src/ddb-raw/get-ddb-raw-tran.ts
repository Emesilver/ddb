import {
  DynamoDBClient,
  AttributeValue,
  TransactGetItemsInput,
  TransactGetItem,
  Get,
  TransactGetItemsCommand,
} from '@aws-sdk/client-dynamodb';

/**
 * Read multiple items in a transaction
 */
export async function getDDBRawTran(
  ddbClient: DynamoDBClient,
  tableName: string,
  rawKeys: Record<string, AttributeValue>[]
) {
  const params: TransactGetItemsInput = {
    TransactItems: [],
  };
  for (const rawKey of rawKeys) {
    const getKey: Get = {
      Key: rawKey,
      TableName: tableName,
    };
    const transactGetItem: TransactGetItem = {
      Get: getKey,
    };
    params.TransactItems.push(transactGetItem);
  }
  try {
    const transactResult = await ddbClient.send(
      new TransactGetItemsCommand(params)
    );
    return transactResult.Responses;
  } catch (error) {
    console.log('getDDBRawTransaction failed:', error.message);
  }
}
