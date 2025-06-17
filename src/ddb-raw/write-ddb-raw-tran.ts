import {
  DynamoDBClient,
  AttributeValue,
  TransactWriteItemsInput,
  TransactWriteItem,
  Put,
  TransactWriteItemsCommand,
  Update,
} from '@aws-sdk/client-dynamodb';

export enum WriteDDBRawTranType {
  PUT = 'put',
  UPDATE = 'update',
  DELETE = 'delete',
  CONDITION = 'condition',
}
export type WriteDDBRawTranCommand = {
  commandType: WriteDDBRawTranType;
  rawItem?: Record<string, AttributeValue>;
  updateItem?: {
    key: Record<string, AttributeValue>;
    updateExp: string;
    conditionExp?: string;
    expAttValues: Record<string, AttributeValue>;
  };
};
export async function writeDDBRawTran(
  ddbClient: DynamoDBClient,
  tableName: string,
  rawWriteItems: WriteDDBRawTranCommand[]
) {
  const params: TransactWriteItemsInput = {
    TransactItems: [],
  };
  for (const rawWriteItem of rawWriteItems) {
    if (rawWriteItem.commandType === WriteDDBRawTranType.PUT) {
      const putParam: Put = {
        TableName: tableName,
        Item: rawWriteItem.rawItem,
      };
      const transactionItem: TransactWriteItem = { Put: putParam };
      params.TransactItems.push(transactionItem);
    }
    if (rawWriteItem.commandType === WriteDDBRawTranType.UPDATE) {
      const updateParam: Update = {
        TableName: tableName,
        Key: rawWriteItem.updateItem.key,
        UpdateExpression: rawWriteItem.updateItem.updateExp,
        ExpressionAttributeValues: rawWriteItem.updateItem.expAttValues,
      };
      if (rawWriteItem.updateItem.conditionExp)
        updateParam.ConditionExpression = rawWriteItem.updateItem.conditionExp;
      const transactionItem: TransactWriteItem = { Update: updateParam };
      params.TransactItems.push(transactionItem);
    }
    if (rawWriteItem.commandType === WriteDDBRawTranType.DELETE) {
      // TODO
    }
    if (rawWriteItem.commandType === WriteDDBRawTranType.CONDITION) {
      // TODO
    }
  }
  try {
    console.log('Transaction params:', JSON.stringify(params));
    await ddbClient.send(new TransactWriteItemsCommand(params));
  } catch (error) {
    console.log('writeDDBRawTransaction failed:', error.message);
  }
}
