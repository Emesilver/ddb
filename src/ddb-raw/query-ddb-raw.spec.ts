import { QueryCommandInput } from '@aws-sdk/client-dynamodb';
//import {  TableInfo } from '../../utils';
import {
  KeysInfo,
  QueryOptions,
  SKFilter,
  TableIndexInfo,
} from './ddb-raw.type';
import { buildQueryParams } from './query-ddb-raw';

describe('query-ddb-raw', () => {
  const TABLE_TEST_INDEX: TableIndexInfo = {
    name: 'indexName',
    pkName: 'indexPk',
    skName: 'indexSk',
  };
  // const TABLE_TEST: TableInfo = {
  //   tableName: 'TEST-TABLE',
  //   partitionKeyName: 'pk',
  //   encryptTable: true,
  //   numberAsStrFields: [],
  //   noEncryptFields: [],
  //   sortKeyName: 'sk',
  // };
  const tableName = 'TEST-TABLE';
  const basicExpectedResult: QueryCommandInput = {
    TableName: tableName,
    KeyConditionExpression: 'pk = :pk',
    ExpressionAttributeValues: { ':pk': { S: 'pkValue' } },
  };
  const keysInfo: KeysInfo = {
    pkName: 'pk',
    skName: 'sk',
    pkValue: 'pkValue',
  };
  describe('buildQueryParams', () => {
    it('should build simple query pk only', () => {
      const ret = buildQueryParams(tableName, keysInfo);
      expect(ret).toStrictEqual(basicExpectedResult);
    });

    it('should build simple query with sk', () => {
      const skFilter: SKFilter = {
        sk: 'skValue',
      };
      const expectedResult: QueryCommandInput = {
        ...basicExpectedResult,
        KeyConditionExpression: 'pk = :pk AND sk = :sk',
        ExpressionAttributeValues: {
          ...basicExpectedResult.ExpressionAttributeValues,
          ':sk': { S: skFilter.sk! },
        },
      };
      const ret = buildQueryParams(tableName, keysInfo, { skFilter });
      expect(ret).toStrictEqual(expectedResult);
    });

    it('should build simple query with sk BEGINS_WITH', () => {
      const skFilter: SKFilter = {
        skBeginsWith: 'sk begins with ...',
      };
      const expectedResult: QueryCommandInput = {
        ...basicExpectedResult,
        KeyConditionExpression: 'pk = :pk AND begins_with(sk, :skBW)',
        ExpressionAttributeValues: {
          ...basicExpectedResult.ExpressionAttributeValues,
          ':skBW': { S: skFilter.skBeginsWith! },
        },
      };
      const ret = buildQueryParams(tableName, keysInfo, { skFilter });
      expect(ret).toStrictEqual(expectedResult);
    });

    it('should build simple query with sk BETWEEN', () => {
      const skFilter: SKFilter = {
        skBetween: { start: 'skStart', end: 'skEnd' },
      };
      const expectedResult: QueryCommandInput = {
        ...basicExpectedResult,
        KeyConditionExpression: 'pk = :pk AND sk BETWEEN :skStart AND :skEnd',
        ExpressionAttributeValues: {
          ...basicExpectedResult.ExpressionAttributeValues,
          ':skStart': { S: skFilter.skBetween!.start },
          ':skEnd': { S: skFilter.skBetween!.end },
        },
      };
      const ret = buildQueryParams(tableName, keysInfo, { skFilter });
      expect(ret).toStrictEqual(expectedResult);
    });

    it('should build simple query in a index', () => {
      const skFilter: SKFilter = {
        sk: 'skIndexValue',
      };
      const expectedResult: QueryCommandInput = {
        ...basicExpectedResult,
        IndexName: TABLE_TEST_INDEX.name,
        KeyConditionExpression: 'indexPk = :pk AND indexSk = :sk',
        ExpressionAttributeValues: {
          ...basicExpectedResult.ExpressionAttributeValues,
          ':pk': { S: 'pkIndexValue' },
          ':sk': { S: 'skIndexValue' },
        },
      };
      const queryOptions: QueryOptions = {
        skFilter,
        indexInfo: TABLE_TEST_INDEX,
      };
      const ret = buildQueryParams(tableName, keysInfo, queryOptions);
      expect(ret).toStrictEqual(expectedResult);
    });

    it('should build simple query with scan backward', () => {
      const skFilter: SKFilter = {
        sk: 'skValue',
      };
      const queryOptions: QueryOptions = {
        skFilter,
        scanForward: false,
      };
      const expectedResult: QueryCommandInput = {
        ...basicExpectedResult,
        KeyConditionExpression: 'pk = :pk AND sk = :sk',
        ExpressionAttributeValues: {
          ...basicExpectedResult.ExpressionAttributeValues,
          ':sk': { S: skFilter.sk! },
        },
        ScanIndexForward: false,
      };
      const ret = buildQueryParams(tableName, keysInfo, queryOptions);
      expect(ret).toStrictEqual(expectedResult);
    });

    it('should build simple query with limit', () => {
      const skFilter: SKFilter = {
        sk: 'skValue',
      };
      const queryOptions: QueryOptions = {
        skFilter,
        limit: 12,
      };
      const expectedResult: QueryCommandInput = {
        ...basicExpectedResult,
        KeyConditionExpression: 'pk = :pk AND sk = :sk',
        ExpressionAttributeValues: {
          ...basicExpectedResult.ExpressionAttributeValues,
          ':sk': { S: skFilter.sk! },
        },
        Limit: 12,
      };
      const ret = buildQueryParams(tableName, keysInfo, queryOptions);
      expect(ret).toStrictEqual(expectedResult);
    });

    it('should build simple query with field projection', () => {
      const skFilter: SKFilter = {
        sk: 'skValue',
      };
      const queryOptions: QueryOptions = {
        skFilter,
        fieldList: ['field1', 'field2'],
      };
      const expectedResult: QueryCommandInput = {
        ...basicExpectedResult,
        KeyConditionExpression: 'pk = :pk AND sk = :sk',
        ExpressionAttributeValues: {
          ...basicExpectedResult.ExpressionAttributeValues,
          ':sk': { S: skFilter.sk! },
        },
        ProjectionExpression: queryOptions.fieldList!.join(','),
      };
      const ret = buildQueryParams(tableName, keysInfo, queryOptions);
      expect(ret).toStrictEqual(expectedResult);
    });

    it('should build simple query with filter in a field other than keys', () => {
      const skFilter: SKFilter = {
        sk: 'skValue',
      };
      const queryOptions: QueryOptions = {
        skFilter,
        fieldsFilter: {
          filterExpression: 'field1 = :field1',
          filterValues: { ':field1': 'content 1 to filter' },
        },
      };
      const expectedResult: QueryCommandInput = {
        ...basicExpectedResult,
        KeyConditionExpression: 'pk = :pk AND sk = :sk',
        FilterExpression: queryOptions.fieldsFilter!.filterExpression,
        ExpressionAttributeValues: {
          ...basicExpectedResult.ExpressionAttributeValues,
          ':sk': { S: skFilter.sk! },
          ':field1': { S: 'content 1 to filter' },
        },
      };
      const ret = buildQueryParams(tableName, keysInfo, queryOptions);
      expect(ret).toStrictEqual(expectedResult);
    });

    it('should build simple query with filter in fields other than keys', () => {
      const skFilter: SKFilter = {
        sk: 'skValue',
      };
      const queryOptions: QueryOptions = {
        skFilter,
        fieldsFilter: {
          filterExpression: 'field1 = :field1 AND field2 = :field2',
          filterValues: {
            ':field1': 'content 1 to filter',
            ':field2': 'content 2 to filter',
          },
        },
      };
      const expectedResult: QueryCommandInput = {
        ...basicExpectedResult,
        KeyConditionExpression: 'pk = :pk AND sk = :sk',
        FilterExpression: queryOptions.fieldsFilter!.filterExpression,
        ExpressionAttributeValues: {
          ...basicExpectedResult.ExpressionAttributeValues,
          ':sk': { S: skFilter.sk! },
          ':field1': { S: 'content 1 to filter' },
          ':field2': { S: 'content 2 to filter' },
        },
      };
      const ret = buildQueryParams(tableName, keysInfo, queryOptions);
      expect(ret).toStrictEqual(expectedResult);
    });

    it('should build simple query with filterFields', () => {
      const queryOptions: QueryOptions = {
        skFilter: { skBeginsWith: 'BEGIN-' },
        fieldsFilter: {
          filterExpression: 'attribute_exists(master_schedule_id)',
        },
      };
      const expectedResult: QueryCommandInput = {
        ...basicExpectedResult,
        KeyConditionExpression: 'pk = :pk AND begins_with(sk, :skBW)',
        FilterExpression: 'attribute_exists(master_schedule_id)',
        ExpressionAttributeValues: {
          ...basicExpectedResult.ExpressionAttributeValues,
          ':skBW': { S: 'BEGIN-' },
        },
      };
      const ret = buildQueryParams(tableName, keysInfo, queryOptions);
      expect(ret).toStrictEqual(expectedResult);
    });
  });
});
