import type { IAggregateResult, IResult } from '../lib/Result';
import { ResultAggregatorComunicaQuerySequence } from '../lib/ResultAggregatorComunicaQuerySequence';

describe('ResultAggregatorComunicaQuerySequence', () => {
  it('handles results without sequence metadata', () => {
    const aggregator = new ResultAggregatorComunicaQuerySequence();
    const results: IResult[] = [
      {
        name: 'a',
        id: '0',
        results: 1,
        hash: 'hash-a',
        time: 10,
        timestamps: [ 1 ],
        httpRequests: 3,
        sequenceElement: { template: 'template-a' },
      },
      {
        name: 'b',
        id: '0',
        results: 2,
        hash: 'hash-b',
        time: 20,
        timestamps: [ 2, 3 ],
        httpRequests: 5,
      },
    ];

    const aggregateResults: IAggregateResult[] = aggregator.aggregateResults(results);

    expect(aggregateResults).toHaveLength(2);
    expect(aggregateResults).toEqual(expect.arrayContaining([
      expect.objectContaining({
        name: 'template-a',
        httpRequests: 3,
      }),
      expect.objectContaining({
        name: 'b',
        httpRequests: 5,
      }),
    ]));
  });

  it('groups aggregate results by name', () => {
    const aggregator = new ResultAggregatorComunicaQuerySequence();

    expect(aggregator.groupAggregateResults([
      {
        name: 'template-a',
        id: '0',
        results: 1,
        hash: 'hash-a',
        time: 10,
        timestamps: [ 1 ],
      },
      {
        name: 'template-a',
        id: '1',
        results: 1,
        hash: 'hash-b',
        time: 12,
        timestamps: [ 2 ],
      },
    ])).toEqual({
      'template-a': [
        {
          name: 'template-a',
          id: '0',
          results: 1,
          hash: 'hash-a',
          time: 10,
          timestamps: [ 1 ],
        },
        {
          name: 'template-a',
          id: '1',
          results: 1,
          hash: 'hash-b',
          time: 12,
          timestamps: [ 2 ],
        },
      ],
    });
  });

  it('aggregates http requests across repeated sequence results', () => {
    const aggregator = new ResultAggregatorComunicaQuerySequence();

    expect(aggregator.aggregateResults([
      {
        name: 'a',
        id: '0',
        results: 1,
        hash: 'hash-a',
        time: 10,
        timestamps: [ 1 ],
        httpRequests: 3,
        sequenceElement: { template: 'template-a' },
      },
      {
        name: 'a',
        id: '0',
        results: 1,
        hash: 'hash-a',
        time: 12,
        timestamps: [ 2 ],
        httpRequests: 5,
        sequenceElement: { template: 'template-a' },
      },
    ])).toEqual([
      expect.objectContaining({
        name: 'template-a',
        httpRequests: 4,
        httpRequestsMax: 5,
        httpRequestsMin: 3,
        httpRequestsStd: 1,
      }),
    ]);
  });
});
