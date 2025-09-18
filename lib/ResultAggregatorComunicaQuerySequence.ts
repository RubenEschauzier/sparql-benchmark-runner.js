import type { IResult, IAggregateResult } from './Result';
import { ResultAggregatorComunica } from './ResultAggregatorComunica';

/**
 * Query result aggregator that handles Comunica-specific metadata.
 */
export class ResultAggregatorComunicaQuerySequence extends ResultAggregatorComunica {
  /**
   * Groups query execution results by name and id.
   * @param results Ungrouped results.
   * @returns Grouped results.
   */
  public override groupResults(results: IResult[]): Record<string, IResult[]> {
    const templates: Record<string, IResult[]> = {};
    for (const result of results) {
      const template = <string> result.template;
      result.sequence = result.name;
      result.name = template;
      if (!(template in templates)) {
        templates[template] = [];
      }
      templates[template].push(result);
    }
    return templates;
  }

  public groupAggregateResults(results: IResult[]): Record<string, IResult[]> {
    const groups: Record<string, IResult[]> = {};
    for (const result of results) {
      const key = `${result.name}`;
      if (key in groups) {
        groups[key].push(result);
      } else {
        groups[key] = [ result ];
      }
    }
    return groups;
  }

  public aggregateResults(results: IResult[]): IAggregateResult[] {
    for (const result of results) {
      result.template = <string> result.sequenceElement.template;
    }

    const groupedResults = this.groupResults(results);
    const aggregateResults = this.aggregateGroupedResults(groupedResults);
    const groupedAggregates = this.groupAggregateResults(aggregateResults);

    for (const [ key, resultGroup ] of Object.entries(groupedResults)) {
      let requestsSum = 0;
      let requestsMax = 0;
      let requestsMin = 0;
      let successfulExecutions = 0;
      for (const result of resultGroup.filter(res => !res.error && typeof res.httpRequests === 'number')) {
        const resultHttpRequests = <number>result.httpRequests;
        requestsSum = successfulExecutions > 0 ? requestsSum + resultHttpRequests : resultHttpRequests;
        requestsMax = successfulExecutions > 0 ? Math.max(requestsMax, resultHttpRequests) : resultHttpRequests;
        requestsMin = successfulExecutions > 0 ? Math.min(requestsMin, resultHttpRequests) : resultHttpRequests;
        successfulExecutions++;
      }
      if (successfulExecutions > 0) {
        groupedAggregates[key][0].httpRequests = requestsSum / successfulExecutions;
        groupedAggregates[key][0].httpRequestsMax = requestsMax;
        groupedAggregates[key][0].httpRequestsMin = requestsMin;
        groupedAggregates[key][0].httpRequestsStd = 0;

        for (const { httpRequests, error } of resultGroup) {
          if (!error) {
            groupedAggregates[key][0].httpRequestsStd += (httpRequests - groupedAggregates[key][0].httpRequests) ** 2;
          }
        }
        groupedAggregates[key][0].httpRequestsStd =
         Math.sqrt(groupedAggregates[key][0].httpRequestsStd / successfulExecutions);
      }
    }
    return aggregateResults;
  }
}
