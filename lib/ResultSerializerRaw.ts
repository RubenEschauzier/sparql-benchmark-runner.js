import { writeFile } from 'node:fs/promises';
import type { IResult } from './Result';
import { ResultSerializer } from './ResultSerializer';
import type { IResultSerializerOptions } from './ResultSerializer';

export class ResultSerializerRaw extends ResultSerializer {
  protected readonly columnSeparator: string;
  protected readonly arraySeparator: string;

  public constructor(options?: IResultSerializerCsvOptions) {
    super(options);
    this.columnSeparator = options?.columnSeparator ?? ';';
    this.arraySeparator = options?.arraySeparator ?? ' ';
  }

  /**
   * Write raw results to JSON file.
   * @param path The destination file path.
   * @param results The benchmark results to serialize.
   */
  public async serialize<T extends IResult>(path: string, results: T[]): Promise<void> {
    await writeFile(path, JSON.stringify(results, null, 2), 'utf-8');
  }
}

export interface IResultSerializerCsvOptions extends IResultSerializerOptions {
  columnSeparator: string;
  arraySeparator: string;
}
