import { DDBClient } from './ddb-factory';

describe('ddb-factory', () => {
  it('Should create a singleton DDBClient object', async () => {
    const ddbClient1 = DDBClient.getInstance();
    const ddbClient2 = DDBClient.getInstance();
    expect(await ddbClient1.config.region()).toBe('localhost');
    expect(ddbClient1).toBe(ddbClient2);
  });
});
