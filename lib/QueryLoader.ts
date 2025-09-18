export interface IQueryLoader {
  loadQueries: () => Promise<Record<string, string[]>>;
  loadQueriesMetadata: () => Promise<Record<string, Record<string, any>>>;
}
