import { AttributeValue } from '@aws-sdk/client-dynamodb';

export type SKFilter2 = {
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

export type QueryOptions = {
  skFilter?: SKFilter2;
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

export type DDBItemsChunk<T> = {
  lastEvaluatedKey?: Record<string, AttributeValue>;
  items: T[] | undefined;
};

export type TableIndexInfo = {
  name: string;
  pkName: string;
  skName: string;
};
