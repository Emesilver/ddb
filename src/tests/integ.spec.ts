import {
  createLocalTable,
  deleteLocalTable,
  getDynamoLocalConfig,
} from '../ddb-tests/dynamo-local-table';
import { DDBWriteRepository } from '../ddb-repository/ddb-write-repository';
import { DynamoDB } from '@aws-sdk/client-dynamodb';

describe('Integration Tests', () => {
  beforeAll(async () => {
    await createLocalTable('TestTable1', 'pk', 'sk', []);
  });
  afterAll(async () => {
    await deleteLocalTable('TestTable1');
  });
  it('should run integration tests', async () => {
    const ddbClient = new DynamoDB(getDynamoLocalConfig());
    const repo = new DDBWriteRepository('TestTable1', ddbClient);
    const ret1 = await repo.incrementedNumber<{ counter_fld: number }>(
      'pk1',
      'sk1',
      'counter_fld'
    );
    expect(ret1).toBeDefined();
    expect(ret1?.counter_fld).toBe(1);
    const ret2 = await repo.incrementedNumber<{ counter_fld: number }>(
      'pk1',
      'sk1',
      'counter_fld'
    );
    expect(ret2).toBeDefined();
    expect(ret2?.counter_fld).toBe(2);
    expect(true).toBe(true);
  });
});
