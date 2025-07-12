export type DDBItemsChunk<T> = {
  items: T[] | undefined;
  nextToken?: String;
};
