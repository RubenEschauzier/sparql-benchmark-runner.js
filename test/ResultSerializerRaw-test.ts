import { writeFile } from 'node:fs/promises';
import type { IResult } from '../lib/Result';
import { ResultSerializerRaw } from '../lib/ResultSerializerRaw';

jest.mock('node:fs/promises');
const mockedWriteFile = jest.mocked(writeFile);

describe('ResultSerializerRaw', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('constructor', () => {
    it('assigns default separators when options are omitted', () => {
      const serializer = new ResultSerializerRaw();

      // Accessing protected properties via bracket notation for testing
      expect((<any>serializer).columnSeparator).toBe(';');
      expect((<any>serializer).arraySeparator).toBe(' ');
    });

    it('assigns provided separators from options', () => {
      const serializer = new ResultSerializerRaw({
        columnSeparator: ',',
        arraySeparator: '|',
      });

      expect((<any>serializer).columnSeparator).toBe(',');
      expect((<any>serializer).arraySeparator).toBe('|');
    });
  });

  describe('serialize', () => {
    it('serializes standard results to a JSON file', async() => {
      const serializer = new ResultSerializerRaw();
      const mockPath = 'output.json';

      // Using type assertion to bypass full IResult interface implementation
      const mockResults = <IResult[]><unknown> [
        { name: 'benchmark1', ops: 100 },
      ];

      await serializer.serialize(mockPath, mockResults);

      const expectedJson = JSON.stringify(
        [{ name: 'benchmark1', ops: 100, error: undefined }],
        null,
        2,
      );

      expect(mockedWriteFile).toHaveBeenCalledTimes(1);
      expect(mockedWriteFile).toHaveBeenCalledWith(mockPath, expectedJson, 'utf-8');
    });

    it('extracts error messages from Error objects', async() => {
      const serializer = new ResultSerializerRaw();
      const mockPath = 'error-output.json';

      const errorObject = new Error('Memory limit exceeded');
      const mockResults = <IResult[]><unknown> [
        { name: 'benchmark1', error: errorObject },
        { name: 'benchmark2', error: 'String error fallback' },
      ];

      await serializer.serialize(mockPath, mockResults);

      const expectedJson = JSON.stringify(
        [
          { name: 'benchmark1', error: 'Memory limit exceeded' },
          { name: 'benchmark2', error: 'String error fallback' },
        ],
        null,
        2,
      );

      expect(mockedWriteFile).toHaveBeenCalledTimes(1);
      expect(mockedWriteFile).toHaveBeenCalledWith(mockPath, expectedJson, 'utf-8');
    });
  });
});
