import { AttributeValue } from '@aws-sdk/client-dynamodb';

export type SKFilter = {
  sk?: string;
  skBeginsWith?: string;
  skBetween?: {
    start: string;
    end: string;
  };
  skGreaterThan?: string;
  skGreaterThanOrEqual?: string;
  skLessThan?: string;
  skLessThanOrEqual?: string;
};

export type FieldsFilter = {
  filterExpression: string;
  filterValues?: Record<string, string>;
};

export type KeysInfo = {
  pkName: string;
  skName: string;
  pkValue: string;
};
export type QueryOptions = {
  skFilter?: SKFilter;
  fieldsFilter?: FieldsFilter;
  indexInfo?: TableIndexInfo;
  scanForward?: boolean;
  limit?: number;
  fieldList?: string[];
  consistentRead?: boolean;
};

export type GetOptions = {
  fieldList?: string[];
  consistentRead?: boolean;
};

export type ScanFilter = {
  filterExpression: string;
  expressionAttributeValues: Record<string, AttributeValue>;
};
export type ScanOptions = {
  indexName?: string;
  scanFilter?: ScanFilter;
};

export type DDBItemsChunk<T> = {
  lastEvaluatedKey?: Record<string, AttributeValue>;
  items: T[] | undefined;
};

export type TableIndexInfo = {
  name: string;
  pkName: string;
  skName: string;
};
