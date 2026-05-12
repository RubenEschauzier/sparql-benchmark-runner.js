export interface IQueryLoader {
  loadQueries: () => Promise<Record<string, string[]>>;
  loadQueriesMetadata: () => Promise<Record<string, IQuerySetMetadata>>;
}

export type IQuerySetMetadata = Record<string, any>[];
