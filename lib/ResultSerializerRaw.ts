import { writeFile } from 'node:fs/promises';
import type { IResult } from './Result';
import { ResultSerializer } from './ResultSerializer';
import type { IResultSerializerOptions } from './ResultSerializer';

export class ResultSerializerRaw extends ResultSerializer {
  protected readonly columnSeparator: string;
  protected readonly arraySeparator: string;

  public constructor(options?: IResultSerializerRawOptions) {
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
    const resultsErrorMessages = results.map(result => ({
      ...result,
      error: result.error?.message ?? result.error,
    }));
    await writeFile(path, JSON.stringify(resultsErrorMessages, null, 2), 'utf-8');
  }
}

export interface IResultSerializerRawOptions extends IResultSerializerOptions {
  columnSeparator: string;
  arraySeparator: string;
}
