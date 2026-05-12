import type { Dirent } from 'node:fs';
import type * as fsPromises from 'node:fs/promises';
import { join } from 'node:path';
import type { IQueryLoader } from '../lib/QueryLoader';
import { QueryLoaderFile } from '../lib/QueryLoaderFile';

const queryFilesPath = '/tmp/queries';

let queryFiles: Record<string, string>;
let queryFilesSub: Record<string, string>;

jest.mock<typeof fsPromises>('node:fs/promises', () => <typeof fsPromises> <unknown> ({
  async readdir(path: string): Promise<Dirent[]> {
    if (path === queryFilesPath) {
      return Object.keys(queryFiles).map(file => (<Dirent> {
        name: file.at(-1) === '/' ? file.slice(0, -1) : file,
        isFile: () => file.at(-1) !== '/',
        isDirectory: () => file.at(-1) === '/',
      }));
    }
    if (path === join(queryFilesPath, 'dir')) {
      return Object.keys(queryFilesSub).map(file => (<Dirent> {
        name: file.at(-1) === '/' ? file.slice(0, -1) : file,
        isFile: () => file.at(-1) !== '/',
        isDirectory: () => file.at(-1) === '/',
      }));
    }
    throw new Error(`Requested readdir outside mocked one: ${path}`);
  },
  async readFile(path: string): Promise<string> {
    for (const [ file, contents ] of Object.entries(queryFiles)) {
      const filePath = join(queryFilesPath, file);
      if (filePath === path) {
        return contents;
      }
    }
    for (const [ file, contents ] of Object.entries(queryFilesSub)) {
      const filePath = join(join(queryFilesPath, 'dir'), file);
      if (filePath === path) {
        return contents;
      }
    }
    throw new Error(`Requested readFile outside the mocked ones: ${path}`);
  },
}));

describe('QueryLoader', () => {
  let loader: IQueryLoader;

  beforeEach(() => {
    queryFiles = {
      'a.rq': 'A',
      'b.sparql': 'B1\n\nB2\n\n\n\n',
      'c.txt': 'C',
      'd.json': 'D',
      'dir/': 'true',
    };
    queryFilesSub = {
      'e.sparql': 'E',
    };
    loader = new QueryLoaderFile({ path: queryFilesPath });
  });

  it('should load all queries', async() => {
    const queries = await loader.loadQueries();
    const queriesExpected: Record<string, string[]> = {
      a: [ 'A' ],
      b: [ 'B1', 'B2' ],
      c: [ 'C' ],
      'dir/e': [ 'E' ],
    };
    expect(queries).toEqual(queriesExpected);
  });

  it('should ignore non-metadata json files', async() => {
    await expect(loader.loadQueriesMetadata()).resolves.toEqual({});
  });

  it('should load query metadata from metadata files', async() => {
    queryFiles['a.metadata.json'] = JSON.stringify({
      template: 'template-a',
      provenance: 'dataset-a',
      sequenceElements: [
        { position: 0 },
        { position: 1 },
      ],
    });
    queryFilesSub['e.metadata.json'] = JSON.stringify({
      template: 'template-e',
      sequenceElements: [
        { position: 2 },
      ],
    });

    await expect(loader.loadQueriesMetadata()).resolves.toEqual({
      a: [
        {
          template: 'template-a',
          provenance: 'dataset-a',
          sequenceElement: { position: 0 },
        },
        {
          template: 'template-a',
          provenance: 'dataset-a',
          sequenceElement: { position: 1 },
        },
      ],
      'dir/e': [
        {
          template: 'template-e',
          sequenceElement: { position: 2 },
        },
      ],
    });
  });

  it('should reject invalid metadata files', async() => {
    queryFiles['a.metadata.json'] = JSON.stringify({
      template: 'template-a',
    });

    await expect(loader.loadQueriesMetadata()).rejects.toThrow('queries metadata has no array entry');
  });
});
