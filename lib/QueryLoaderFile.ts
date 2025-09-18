import { readdir, readFile } from 'node:fs/promises';
import { join, extname, resolve } from 'node:path';
import type { IQueryLoader } from './QueryLoader';

export class QueryLoaderFile implements IQueryLoader {
  protected readonly path: string;
  protected readonly extensions: Set<string>;
  protected readonly extensionsMetadata: Set<string>;
  protected readonly indicatorsMetadata: Set<string>;

  public constructor(options: IQueryLoaderFileOptions) {
    this.path = resolve(options.path);
    this.extensions = new Set<string>(options.extensions ?? [ '.txt', '.sparql', '.rq' ]);
    this.extensionsMetadata = new Set<string>(
      options.extensionsMetadata ?? [ '.json' ],
    );
    this.indicatorsMetadata = new Set<string>(
      options.indicatorsMetadata ?? [ '.metadata' ],
    );
  }

  public async loadQueries(): Promise<Record<string, string[]>> {
    const querySets: Record<string, string[]> = {};
    await QueryLoaderFile.loadQueriesStatic(querySets, this.path, this.extensions);
    return querySets;
  }

  public async loadQueriesMetadata(): Promise<Record<string, Record<string, any>>> {
    const queryMetadata: Record<string, Record<string, any>> = {};
    await QueryLoaderFile.loadQueriesMetadataStatic(
      queryMetadata,
      this.path,
      this.extensionsMetadata,
      this.indicatorsMetadata,
    );
    return queryMetadata;
  }

  protected static async loadQueriesStatic(
    querySets: Record<string, string[]>,
    path: string,
    extensions: Set<string>,
    prefix = '',
  ): Promise<void> {
    const querySeparator = '\n\n';
    for (const dirent of await readdir(path, { encoding: 'utf-8', withFileTypes: true })) {
      if (dirent.isFile()) {
        const extension = extname(dirent.name);
        if (extensions.has(extension)) {
          const fileContents = await readFile(join(path, dirent.name), { encoding: 'utf-8' });
          const queries = fileContents.split(querySeparator)
            .map(query => query.trim())
            .filter(query => query.length > 0);
          querySets[prefix + dirent.name.replace(extension, '')] = queries;
        }
      } else if (dirent.isDirectory()) {
        await QueryLoaderFile.loadQueriesStatic(querySets, join(path, dirent.name), extensions, `${prefix}${dirent.name}/`);
      }
    }
  }

  /**
   * Load metadata describing the queries in a query set. Each metadata file
   * should be associated with a query file (with the same name excluding extensions)
   *
   * The metadata files are expected to have the following structure:
   * - An arbitrary number of top-level fields with single values, representing
   *   global metadata for the whole query set (e.g., template, provenance,
   *   bottleneck type).
   * - Exactly one top-level field whose value is an array. Each element of
   *   this array describes the metadata of an individual query in the set.
   *
   * During loading:
   * - All global (single-value) fields are repeated and attached to each
   *   individual query metadata object.
   * - Each element of the array field becomes one entry in the final list,
   *   preserving the original order of the array.
   *
   * Example input:
   * {
   *   "template": "interactive-discover-3",
   *   "provenance": "benchmarkX",
   *   "sequenceElements": [
   *     { "session": { "task": "Messages Person" } },
   *     { "session": { "task": "Another Task" } }
   *   ]
   * }
   *
   * Example output:
   * [
   *   {
   *     "template": "interactive-discover-3",
   *     "provenance": "benchmarkX",
   *     "sequenceElement": { "session": { "task": "Messages Person" } }
   *   },
   *   {
   *     "template": "interactive-discover-3",
   *     "provenance": "benchmarkX",
   *     "sequenceElement": { "session": { "task": "Another Task" } }
   *   }
   * ]
   */
  protected static async loadQueriesMetadataStatic(
    queryMetadata: Record<string, Record<string, any>>,
    path: string,
    extensions: Set<string>,
    indicators: Set<string>,
    prefix = '',
  ): Promise<void> {
    for (const dirent of await readdir(path, { encoding: 'utf-8', withFileTypes: true })) {
      if (dirent.isFile()) {
        const extension = extname(dirent.name);
        if (extensions.has(extension)) {
          const filePath = join(path, dirent.name);
          const raw = await readFile(filePath, 'utf-8');
          const parsed = <Record<string, any>> JSON.parse(raw);

          // Assume there's exactly one array field (like "sequenceElements")
          let arrayKey: string | undefined;
          let arrayValue: any[] | undefined;
          for (const key in parsed) {
            if (Array.isArray(parsed[key])) {
              arrayKey = key;
              arrayValue = <any[]> parsed[key];
            }
          }
          if (!arrayValue || !arrayKey) {
            throw new Error('queries metadata has no array entry');
          }

          const baseFields = Object.fromEntries(
            Object.entries(parsed).filter(([ , v ]) => !Array.isArray(v)),
          );

          for (const element of arrayValue) {
            const obj = { ...baseFields, [arrayKey.slice(0, -1)]: <Record<string, any>> element };
            const cleanedDirent = this.removeMatches(dirent.name.replace(extension, ''), indicators);
            // Store as array of objects per file
            (queryMetadata[prefix + cleanedDirent] ??= []).push(obj);
          }
        }
      } else if (dirent.isDirectory()) {
        await QueryLoaderFile.loadQueriesMetadataStatic(queryMetadata, join(path, dirent.name), extensions, indicators, `${prefix}${dirent.name}/`);
      }
    }
  }

  protected static removeMatches(filename: string, metadataIndicators: Set<string>): string {
    let stripped = filename;
    for (const indicator of metadataIndicators) {
      if (stripped.includes(indicator)) {
        stripped = stripped.replace(indicator, '');
      }
    }
    return stripped;
  }
}

export interface IQueryLoaderFileOptions {
  /**
   * The path to load the queries from on disk.
   */
  path: string;
  /**
   * File extensions to detect as SPARQL queries.
   */
  extensions?: string[];
  /**
   * File extensions to detect as SPARQL queries metadata
   */
  extensionsMetadata?: string[];
  /**
   * File name additions to indicate that this represents metadata of
   * the file with the indicator removed. For example:
   * testQueriesOne.sparql has metadata file testQueriesOne.metadata.json.
   * Here .metadata is the indicator and .json is the extension
   */
  indicatorsMetadata?: string[];
}
