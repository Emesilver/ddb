import {
  DynamoDBClient,
  ScanCommand,
  ScanCommandOutput,
} from '@aws-sdk/client-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { scanDDBRawItems, scanDDBRawChunk } from './scan-ddb-raw-items';
import { ScanOptions } from './ddb-raw.type';

const ddbMock = mockClient(DynamoDBClient);

describe('scan-ddb-raw-items', () => {
  const tableName = 'TEST-TABLE';
  let ddbClient: DynamoDBClient;

  beforeEach(() => {
    ddbMock.reset();
    ddbClient = new DynamoDBClient({});
  });

  describe('scanDDBRawItems', () => {
    it('should scan all items without options', async () => {
      const mockItems = [
        { pk: { S: 'item1' }, sk: { S: 'value1' } },
        { pk: { S: 'item2' }, sk: { S: 'value2' } },
      ];

      ddbMock.on(ScanCommand).resolves({
        Items: mockItems,
        LastEvaluatedKey: undefined,
      });

      const result = await scanDDBRawItems(ddbClient, tableName);

      expect(result).toEqual(mockItems);
      expect(ddbMock.commandCalls(ScanCommand)).toHaveLength(1);
      expect(ddbMock.commandCalls(ScanCommand)[0].args[0].input).toEqual({
        TableName: tableName,
      });
    });

    it('should scan items with limit', async () => {
      const mockItems = [
        { pk: { S: 'item1' }, sk: { S: 'value1' } },
        { pk: { S: 'item2' }, sk: { S: 'value2' } },
        { pk: { S: 'item3' }, sk: { S: 'value3' } },
      ];

      ddbMock.on(ScanCommand).resolves({
        Items: mockItems,
        LastEvaluatedKey: undefined,
      });

      const scanOptions: ScanOptions = { limit: 2 };
      const result = await scanDDBRawItems(ddbClient, tableName, scanOptions);

      expect(result).toEqual(mockItems.slice(0, 2));
      expect(ddbMock.commandCalls(ScanCommand)).toHaveLength(1);
    });

    it('should scan items with pagination', async () => {
      const firstBatch = [
        { pk: { S: 'item1' }, sk: { S: 'value1' } },
        { pk: { S: 'item2' }, sk: { S: 'value2' } },
      ];
      const secondBatch = [
        { pk: { S: 'item3' }, sk: { S: 'value3' } },
        { pk: { S: 'item4' }, sk: { S: 'value4' } },
      ];
      const lastEvaluatedKey = { pk: { S: 'item2' }, sk: { S: 'value2' } };

      ddbMock
        .on(ScanCommand)
        .resolvesOnce({
          Items: firstBatch,
          LastEvaluatedKey: lastEvaluatedKey,
        })
        .resolvesOnce({
          Items: secondBatch,
          LastEvaluatedKey: undefined,
        });

      const result = await scanDDBRawItems(ddbClient, tableName);

      expect(result).toEqual([...firstBatch, ...secondBatch]);
      expect(ddbMock.commandCalls(ScanCommand)).toHaveLength(2);
      expect(
        ddbMock.commandCalls(ScanCommand)[1].args[0].input.ExclusiveStartKey
      ).toEqual(lastEvaluatedKey);
    });

    it('should scan items with index name', async () => {
      const mockItems = [{ pk: { S: 'item1' }, sk: { S: 'value1' } }];
      const scanOptions: ScanOptions = { indexName: 'test-index' };

      ddbMock.on(ScanCommand).resolves({
        Items: mockItems,
        LastEvaluatedKey: undefined,
      });

      const result = await scanDDBRawItems(ddbClient, tableName, scanOptions);

      expect(result).toEqual(mockItems);
      expect(ddbMock.commandCalls(ScanCommand)[0].args[0].input).toEqual({
        TableName: tableName,
        IndexName: 'test-index',
      });
    });

    it('should scan items with filter', async () => {
      const mockItems = [{ pk: { S: 'item1' }, field1: { S: 'value1' } }];
      const scanOptions: ScanOptions = {
        scanFilter: {
          filterExpression: 'field1 = :val1',
          expressionAttributeValues: { ':val1': { S: 'value1' } },
        },
      };

      ddbMock.on(ScanCommand).resolves({
        Items: mockItems,
        LastEvaluatedKey: undefined,
      });

      const result = await scanDDBRawItems(ddbClient, tableName, scanOptions);

      expect(result).toEqual(mockItems);
      expect(ddbMock.commandCalls(ScanCommand)[0].args[0].input).toEqual({
        TableName: tableName,
        FilterExpression: 'field1 = :val1',
        ExpressionAttributeValues: { ':val1': { S: 'value1' } },
      });
    });

    it('should throw error when scan fails', async () => {
      ddbMock.on(ScanCommand).rejects(new Error('DynamoDB error'));

      await expect(scanDDBRawItems(ddbClient, tableName)).rejects.toThrow(
        'scanDDBRawItems failed:DynamoDB error'
      );
    });
  });

  describe('scanDDBRawChunk', () => {
    it('should scan chunk without exclusive start key', async () => {
      const mockItems = [
        { pk: { S: 'item1' }, sk: { S: 'value1' } },
        { pk: { S: 'item2' }, sk: { S: 'value2' } },
      ];
      const lastEvaluatedKey = { pk: { S: 'item2' }, sk: { S: 'value2' } };

      ddbMock.on(ScanCommand).resolves({
        Items: mockItems,
        LastEvaluatedKey: lastEvaluatedKey,
      });

      const result = await scanDDBRawChunk(ddbClient, tableName, undefined);

      expect(result).toEqual({
        lastEvaluatedKey,
        items: mockItems,
      });
      expect(ddbMock.commandCalls(ScanCommand)).toHaveLength(1);
      expect(ddbMock.commandCalls(ScanCommand)[0].args[0].input).toEqual({
        TableName: tableName,
        ExclusiveStartKey: undefined,
      });
    });

    it('should scan chunk with exclusive start key', async () => {
      const exclusiveStartKey = { pk: { S: 'item1' }, sk: { S: 'value1' } };
      const mockItems = [{ pk: { S: 'item2' }, sk: { S: 'value2' } }];

      ddbMock.on(ScanCommand).resolves({
        Items: mockItems,
        LastEvaluatedKey: undefined,
      });

      const result = await scanDDBRawChunk(
        ddbClient,
        tableName,
        exclusiveStartKey
      );

      expect(result).toEqual({
        lastEvaluatedKey: undefined,
        items: mockItems,
      });
      expect(
        ddbMock.commandCalls(ScanCommand)[0].args[0].input.ExclusiveStartKey
      ).toEqual(exclusiveStartKey);
    });

    it('should scan chunk with scan options', async () => {
      const mockItems = [{ pk: { S: 'item1' }, sk: { S: 'value1' } }];
      const scanOptions: ScanOptions = {
        limit: 5,
        indexName: 'test-index',
      };

      ddbMock.on(ScanCommand).resolves({
        Items: mockItems,
        LastEvaluatedKey: undefined,
      });

      const result = await scanDDBRawChunk(
        ddbClient,
        tableName,
        undefined,
        scanOptions
      );

      expect(result).toEqual({
        lastEvaluatedKey: undefined,
        items: mockItems,
      });
      expect(ddbMock.commandCalls(ScanCommand)[0].args[0].input).toEqual({
        TableName: tableName,
        ExclusiveStartKey: undefined,
        Limit: 5,
        IndexName: 'test-index',
      });
    });
  });
});
