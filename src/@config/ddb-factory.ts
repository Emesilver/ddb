import { DynamoDBClient } from '@aws-sdk/client-dynamodb';

export class DDBClient {
  static ddbClientInstance: DynamoDBClient;
  private constructor() {}

  static getInstance() {
    if (!this.ddbClientInstance) {
      if (!process.env.ENV) {
        const optionsUnitTest = {
          region: 'localhost',
          endpoint: 'http://localhost:8000', // Dynamo local (docker)
        };
        this.ddbClientInstance = new DynamoDBClient(optionsUnitTest);
      } else {
        this.ddbClientInstance = new DynamoDBClient({});
      }
    }
    return this.ddbClientInstance;
  }
}
