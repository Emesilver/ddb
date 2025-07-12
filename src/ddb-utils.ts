import { marshall } from '@aws-sdk/util-dynamodb';

/**
 * Creates a list of props to be used in a SET UpdateExpression
 */
export function buildSETUpdateExpression(obj: object) {
  return (
    'SET ' +
    Object.keys(obj)
      .map((key) => key + '=:' + key)
      .join(', ')
  );
}

/**
 * Creates a list of fields to be used at REMOVE UpdateExpression
 */
export function buildREMOVEUpdateExpression(fields: string[]) {
  return 'REMOVE ' + fields.join(', ');
}

/**
 * Inserts a prefix on every property name of an object:
 * @returns Record<:string, AttributeValue>
 */
export function objToPrefixedDDB(obj: object, propertyPrefix?: ':') {
  if (!obj) return;

  const convMap = (objMap: object) => {
    const retObjMap: Record<string, Object> = {};
    for (const key of Object.keys(objMap)) {
      if (typeof objMap[key] === 'object')
        retObjMap[key] = convMap(objMap[key]);
      else {
        const propValue = objMap[key];
        if (propValue) retObjMap[key] = propValue;
      }
    }
    return retObjMap;
  };

  const retObject: Record<string, Object> = {};
  for (const key of Object.keys(obj)) {
    const newKey = propertyPrefix ? propertyPrefix + key : key;
    if (typeof obj[key] === 'object') retObject[newKey] = convMap(obj[key]);
    else {
      const propValue = obj[key];
      if (propValue) retObject[newKey] = propValue;
    }
  }
  return marshall(retObject);
}

import { AttributeValue } from '@aws-sdk/client-dynamodb';

export function lastEvaluatedKeyStrToObj(
  lastEvaluatedKey: string | undefined
): Record<string, AttributeValue> {
  if (!lastEvaluatedKey) return undefined;
  return JSON.parse(lastEvaluatedKey);
}

export function lastEvaluatedKeyObjToStr(
  lastEvaluatedKey: Record<string, AttributeValue>
): string {
  if (!lastEvaluatedKey) return '';
  return JSON.stringify(lastEvaluatedKey);
}
