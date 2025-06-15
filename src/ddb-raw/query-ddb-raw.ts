import {
  AttributeValue,
  DynamoDBClient,
  QueryCommand,
  QueryCommandInput,
  QueryCommandOutput,
} from '@aws-sdk/client-dynamodb';
import { DDBItemsChunk, QueryOptions } from './ddb-raw.type';
//import { DDBItemsChunk, showMessageError } from '../../utils';

/**
 * Returns all items of a queryOptions (not only the DynamoDB first page)
 */
export async function queryDDBRaw(
  ddbClient: DynamoDBClient,
  tableName: string,
  pkName: string,
  skName: string,
  pkValue: string,
  queryOptions?: QueryOptions
): Promise<Record<string, AttributeValue>[]> {
  const params = buildQueryParams(
    tableName,
    pkName,
    skName,
    pkValue,
    queryOptions
  );
  try {
    let queryResult: QueryCommandOutput | undefined = undefined;
    let allItems: Record<string, AttributeValue>[] = [];
    let limitMet = false;
    do {
      params.ExclusiveStartKey = queryResult?.LastEvaluatedKey;
      queryResult = await ddbClient.send(new QueryCommand(params));
      if (queryResult?.Items) allItems = allItems.concat(queryResult?.Items);

      if (queryOptions?.limit) {
        if (allItems.length > queryOptions.limit)
          allItems = allItems.slice(0, queryOptions.limit);
        limitMet = allItems.length === queryOptions.limit;
      }
    } while (queryResult.LastEvaluatedKey && !limitMet);
    return allItems;
  } catch (error) {
    console.error(error);
    throw new Error('queryDDBRawItems failed');
  }
}

export async function queryDDBRawChunk(
  ddbClient: DynamoDBClient,
  tableName: string,
  pkName: string,
  skName: string,
  pkValue: string,
  exclusiveStartKey: Record<string, AttributeValue> | undefined,
  queryOptions?: QueryOptions
): Promise<DDBItemsChunk<Record<string, AttributeValue>>> {
  const params = buildQueryParams(
    tableName,
    pkName,
    skName,
    pkValue,
    queryOptions
  );
  params.ExclusiveStartKey = exclusiveStartKey;
  try {
    const queryResult = await ddbClient.send(new QueryCommand(params));
    return {
      lastEvaluatedKey: queryResult.LastEvaluatedKey,
      items: queryResult.Items,
    };
  } catch (error) {
    console.error(error);
    throw new Error('queryDDBRawItems failed');
  }
}

export function buildQueryParams(
  tableName: string,
  pkName: string,
  skName: string,
  pkValue: string,
  queryOptions?: QueryOptions
) {
  const pkFieldName = queryOptions?.indexInfo
    ? queryOptions.indexInfo.pkName
    : pkName;
  const skFieldName = queryOptions?.indexInfo
    ? queryOptions.indexInfo.skName
    : skName;
  const {
    keysCondition,
    expAttrValues,
  }: { keysCondition: string; expAttrValues: Record<string, AttributeValue> } =
    buildKeyCondition(pkFieldName, pkValue, skFieldName, queryOptions);
  const params: QueryCommandInput = {
    TableName: tableName,
    KeyConditionExpression: keysCondition,
    ExpressionAttributeValues: expAttrValues,
  };
  if (queryOptions?.indexInfo) params.IndexName = queryOptions.indexInfo.name;
  if (queryOptions?.limit) params.Limit = queryOptions.limit;
  if (queryOptions?.scanForward !== undefined)
    params.ScanIndexForward = queryOptions.scanForward;
  if (queryOptions?.consistentRead !== undefined)
    params.ConsistentRead = queryOptions.consistentRead;
  if (queryOptions?.fieldList)
    params.ProjectionExpression = queryOptions.fieldList.join(',');
  if (queryOptions?.fieldsFilter) {
    params.FilterExpression = queryOptions.fieldsFilter.filterExpression;
    if (queryOptions.fieldsFilter.filterValues) {
      const convFilterValues = Object.keys(
        queryOptions.fieldsFilter.filterValues
      )
        .map((key) => {
          if (!queryOptions?.fieldsFilter?.filterValues) return {};
          return {
            [key]: { S: queryOptions.fieldsFilter.filterValues[key] },
          };
        })
        .reduce((acc, e) => {
          return { ...acc, ...e };
        }, {});
      params.ExpressionAttributeValues = {
        ...params.ExpressionAttributeValues,
        ...convFilterValues,
      };
    }
  }
  return params;
}

function buildKeyCondition(
  pkFieldName: string,
  pkValue: string,
  skFieldName: string,
  queryOptions?: QueryOptions
) {
  let keysCondition = `${pkFieldName} = :pk`;
  let expAttrValues: Record<string, AttributeValue> = { ':pk': { S: pkValue } };
  if (queryOptions?.skFilter?.sk) {
    keysCondition += ` AND ${skFieldName} = :sk`;
    expAttrValues[':sk'] = { S: queryOptions.skFilter.sk };
  }
  if (queryOptions?.skFilter?.skBeginsWith) {
    keysCondition += ` AND begins_with(${skFieldName}, :skBW)`;
    expAttrValues[':skBW'] = { S: queryOptions.skFilter.skBeginsWith };
  }
  if (queryOptions?.skFilter?.skBetween) {
    keysCondition += ` AND ${skFieldName} BETWEEN :skStart AND :skEnd`;
    expAttrValues[':skStart'] = { S: queryOptions.skFilter.skBetween.start };
    expAttrValues[':skEnd'] = { S: queryOptions.skFilter.skBetween.end };
  }
  if (queryOptions?.skFilter?.skGreaterThan) {
    keysCondition += ` AND ${skFieldName} > :skGT`;
    expAttrValues[':skGT'] = { S: queryOptions.skFilter.skGreaterThan };
  }
  if (queryOptions?.skFilter?.skGreaterThanOrEqual) {
    keysCondition += ` AND ${skFieldName} >= :skGTE`;
    expAttrValues[':skGTE'] = { S: queryOptions.skFilter.skGreaterThanOrEqual };
  }
  if (queryOptions?.skFilter?.skLessThan) {
    keysCondition += ` AND ${skFieldName} < :skLT`;
    expAttrValues[':skLT'] = { S: queryOptions.skFilter.skLessThan };
  }
  if (queryOptions?.skFilter?.skLessThanOrEqual) {
    keysCondition += ` AND ${skFieldName} <= :skLTE`;
    expAttrValues[':skLTE'] = { S: queryOptions.skFilter.skLessThanOrEqual };
  }
  return { keysCondition, expAttrValues };
}
