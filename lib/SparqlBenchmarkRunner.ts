import { SparqlEndpointFetcher } from 'fetch-sparql-endpoint';
import type { IBenchmarkResults } from './IBenchmarkResults';
import { SparqlJsonParser } from 'sparqljson-parse';
import { CalculateOptimalTraversalMetric, ITraversalTopology } from './CalculateOptimalTraversalMetric';
/**
 * Executes query sets against a SPARQL endpoint.
 */
export class SparqlBenchmarkRunner {
  private readonly endpoint: string;
  private readonly querySets: Record<string, string[]>;
  private readonly replication: number;
  private readonly warmup: number;
  private readonly timestampsRecording: boolean;
  private readonly logger?: (message: string) => void;
  private readonly upQuery: string;
  private readonly additionalUrlParamsInit?: URLSearchParams;
  private readonly additionalUrlParamsRun?: URLSearchParams;
  private readonly timeout?: number;
  private readonly optimalTraversalMetric: CalculateOptimalTraversalMetric;

  public constructor(options: ISparqlBenchmarkRunnerArgs) {
    this.endpoint = options.endpoint;
    this.querySets = options.querySets;
    this.replication = options.replication;
    this.warmup = options.warmup;
    this.timestampsRecording = options.timestampsRecording;
    this.logger = options.logger;
    this.upQuery = options.upQuery || 'SELECT * WHERE { ?s ?p ?o } LIMIT 1';
    this.additionalUrlParamsInit = options.additionalUrlParamsInit;
    this.additionalUrlParamsRun = options.additionalUrlParamsRun;
    this.timeout = options.timeout;
    this.optimalTraversalMetric = new CalculateOptimalTraversalMetric();
  }

  /**
   * Once the endpoint is live,
   * execute all query sets against the SPARQL endpoint.
   * Afterwards, all results are collected and averaged.
   */
  public async run(options: IRunOptions = {}): Promise<IBenchmarkResults> {
    // Await query execution until the endpoint is live
    await this.waitUntilUp();

    // Execute queries in warmup
    this.log(`Warming up for ${this.warmup} rounds\n`);
    await this.executeQueries({}, this.warmup);

    // Execute queries
    const results: IBenchmarkResults = {};
    this.log(`Executing ${Object.keys(this.querySets).length} queries with replication ${this.replication}\n`);
    if (options.onStart) {
      await options.onStart();
    }
    await this.executeQueries(results, this.replication);
    if (options.onStop) {
      await options.onStop();
    }

    // Average results
    for (const key in results) {
      results[key].time = Math.floor(results[key].time / this.replication);
      results[key].timestamps = results[key].timestamps.map(t => Math.floor(t / this.replication));
    }

    return results;
  }

  /**
   * Execute all queries against the endpoint.
   * @param data The results to append to.
   * @param iterations The number of times to iterate.
   */
  public async executeQueries(data: IBenchmarkResults, iterations: number): Promise<void> {
    this.log('Executing query ');
    for (let iteration = 0; iteration < iterations; iteration++) {
      for (const name in this.querySets) {
        const test = this.querySets[name];
        // eslint-disable-next-line @typescript-eslint/no-for-in-array
        for (const id in test) {
          this.log(`\rExecuting query ${name}:${id} for iteration ${iteration + 1}/${iterations}`);
          const query = test[id];
          let count: number;
          let time: number;
          let timestamps: number[];
          let trackedTopology: ITraversalTopology;
          let contributingDocuments: string[][];
          let metadata: Record<string, any>;
          let errorObject: Error | undefined;

          // Execute query, and catch errors
          try {
            ({ count, time, timestamps, trackedTopology, contributingDocuments, metadata } = await this.executeQuery(query));
          } catch (error: unknown) {
            errorObject = <Error> error;
            if ('partialOutput' in <any> errorObject) {
              ({ count, time, timestamps, trackedTopology, contributingDocuments, metadata } = (<any>errorObject).partialOutput);
            } else {
              count = 0;
              time = 0;
              timestamps = [];
              trackedTopology =  {
                nodeToIndex: {},
                edgeListUnWeighted: [], edgeListDocumentSize: [], edgeListRequestTime: [],
                edgesInGraph: {},
                metadataNode: [],
                traversalOrder: [],
                traversalOrderEdges: []
              };
              contributingDocuments = [];
              metadata = {};
            }
          }

          // Calculate the metric if we obtain the required data
          const metricsCalculated = [];
          if (count > 0 && contributingDocuments.length > 0){
            metricsCalculated.push(await this.optimalTraversalMetric.calculateMetricAllResults(trackedTopology, contributingDocuments, "unweighted"));
            const metricsFirstK = await this.optimalTraversalMetric.calculateMetricFirstKResults(
              [1,2,4], 
              trackedTopology, 
              contributingDocuments,
              "unweighted", "full", 
              undefined, 
              5_000, 
              true, 
              1_000_000
            );
            metricsCalculated.push(...metricsFirstK);
          }
          else{
            // We can't calculate metric when no results are found, so we just put -1 (in the ugliest way possible)
            metricsCalculated.push(...[-1,-1,-1,-1]);
          }
          if (!data[name + id]) {
            data[name + id] = { name, id, count, time, timestamps, error: Boolean(errorObject), metricsCalculated, metadata };
          } else {
            const dataEntry = data[name + id];

            if (errorObject) {
              dataEntry.error = true;
            }

            dataEntry.time += time;

            // Combine timestamps
            const length = Math.min(dataEntry.timestamps.length, timestamps.length);
            for (let i = 0; i < length; ++i) {
              dataEntry.timestamps[i] += timestamps[i];
            }
          }

          // Delay if error
          if (errorObject) {
            this.log(`\rError occurred at query ${name}:${id} for iteration ${iteration + 1}/${iterations}: ${errorObject.message}\n`);

            // Wait until the endpoint is properly live again
            await this.sleep(3_000);
            await this.waitUntilUp();
          }
          await this.sleep(5_000);
        }
      }
    }
    this.log(`"\rExecuted all queries\n`);
  }

  /**
   * Execute a single query
   * @param query A SPARQL query string
   */
  public async executeQuery(query: string): Promise<{
    count: number; time: number; timestamps: number[]; 
    trackedTopology: ITraversalTopology; contributingDocuments: string[][]; metadata: Record<string, any>;
  }> {
    const fetcher = new SparqlEndpointFetcher({
      additionalUrlParams: this.additionalUrlParamsRun,
    });
    let promiseTimeout: Promise<any> | undefined;
    let timeoutHandle: NodeJS.Timeout | undefined;
    if (this.timeout) {
      promiseTimeout = new Promise((resolve, reject) => {
        timeoutHandle = <any> setTimeout(() => reject(new Error('Timeout for running query')), this.timeout);
      });
    }
    // TODO Change the output of the sparql-benchmark-runner
    // TODO Add metric to npm
    // TODO Use the metric calculation in sparql-benchmark-runner
    // Publish version of runner that uses the metric to alternative JBR package to npm
    // Run the benchmark runner testing code: from /temp-run-sparql-benchmark
    // node ../sparql-benchmark-runner-topology/bin/sparql-benchmark-runner.js -e http://localhost:3001/sparql -q queries/ --output output.csv --replication 1 --warmup 0
    // Run the sparql endpoint: from root comunica:
    // node engines/query-sparql-link-traversal-solid/bin/http.js --lenient --idp void --context '{"sources":[]}' --port 3001 --returnTopology true
    // sparql-benchmark-runner: "username/repistory#commitid" to match github version of forked sparql-benchmark-runner
    // start endpoint: node engines/query-sparql-link-traversal-solid/bin/http.js --lenient --idp void --context '{"sources":[], "unionDefaultGraph": true, "@comunica/bus-rdf-resolve-hypermedia-links:annotateSources": "graph"}' --port 3001 --returnTopology true -t 60 -i true --freshWorker true
    // END OWN CODE
    const promiseFetch = fetcher.fetchBindings(this.endpoint, query)
      .then(results => new Promise<{
        count: number; time: number; timestamps: number[]; trackedTopology: ITraversalTopology, contributingDocuments: string[][], metadata: Record<string, any>;
      }>((resolve, reject) => {
        let trackedTopology: ITraversalTopology = {
          nodeToIndex: {},
          edgeListUnWeighted: [], edgeListDocumentSize: [], edgeListRequestTime: [],
          edgesInGraph: {},
          metadataNode: [],
          traversalOrder: [],
          traversalOrderEdges: []
        };
        const contributingDocuments: string[][] = [];
        let count = 0;
        const hrstart = process.hrtime();
        const timestamps: number[] = [];
        let metadata: Record<string, any> = {};
        results.on('metadata', (readMetadata: any) => {
          metadata = readMetadata;
        });
        results.on('data', (data) => {
          // We retrieve the binding for the topology object here
          if (data._trackedTopology){
            trackedTopology = JSON.parse(data._trackedTopology.value);
            // Retrieve binding for contributing documents here
            contributingDocuments.push(JSON.parse(data._sourceAttribution.value));  
          }
          count++;
          if (this.timestampsRecording) {
            timestamps.push(this.countTime(hrstart));
          }
        });
        results.on('error', (error: any) => {
          error.partialOutput = {
            count,
            time: this.countTime(hrstart),
            timestamps,
            metadata,
          };
          reject(error);
        });
        results.on('end', () => {
          if (timeoutHandle) {
            clearTimeout(timeoutHandle);
          }
          resolve({ count, time: this.countTime(hrstart), timestamps, 
            trackedTopology, contributingDocuments, metadata });
        });
      }));
    return promiseTimeout ? Promise.race([ promiseTimeout, promiseFetch ]) : promiseFetch;
  }

  /**
   * Based on a hrtime start, obtain the duration.
   * @param hrstart process.hrtime
   */
  public countTime(hrstart: [number, number]): number {
    const hrend = process.hrtime(hrstart);
    return hrend[0] * 1_000 + hrend[1] / 1_000_000;
  }

  /**
   * Check if the SPARQL endpoint is available.
   */
  public isUp(): Promise<boolean> {
    const fetcher = new SparqlEndpointFetcher({
      additionalUrlParams: this.additionalUrlParamsInit,
    });
    let timeoutHandle: NodeJS.Timeout | undefined;
    const promiseTimeout = new Promise<boolean>(resolve => {
      timeoutHandle = <any> setTimeout(() => resolve(false), 10_000);
    });
    // Change promisefetch to raw stream to reflect changes in type of output of query

    const promiseFetch = fetcher.fetchBindings(this.endpoint, this.upQuery)
      .then(results => new Promise<boolean>(resolve => {
        results.on('error', () => {
          clearTimeout(timeoutHandle);
          resolve(false);
        });
        results.on('data', () => {
          // Do nothing
        });
        results.on('end', () => {
          clearTimeout(timeoutHandle);
          resolve(true);
        });
    }));

    // const promiseFetch = fetcher.fetchBindings(this.endpoint, this.upQuery)
    //   .then(results => new Promise<boolean>(resolve => {
    //     results.on('error', () => {
    //       clearTimeout(timeoutHandle);
    //       resolve(false);
    //     });
    //     results.on('data', () => {
    //       // Do nothing
    //     });
    //     results.on('end', () => {
    //       clearTimeout(timeoutHandle);
    //       resolve(true);
    //     });
    //   }));
    return Promise.race([ promiseTimeout, promiseFetch ])
      .catch(() => false);
  }

  /**
   * Wait until the SPARQL endpoint is available.
   */
  public async waitUntilUp(): Promise<void> {
    let counter = 0;
    while (!await this.isUp()) {
      await this.sleep(1_000);
      this.log(`\rEndpoint not available yet, waited for ${++counter} seconds...`);
    }
    this.log(`\rEndpoint available after ${counter} seconds.\n`);
    await this.sleep(5_000);
  }

  /**
   * Sleep for a given amount of time.
   * @param durationMs A duration in milliseconds.
   */
  public async sleep(durationMs: number): Promise<void> {
    return new Promise<void>(resolve => setTimeout(resolve, durationMs));
  }

  /**
   * Log a message.
   * @param message Message to log.
   */
  public log(message: string): void {
    return this.logger?.call(this.logger, message);
  }
}

export interface ISparqlBenchmarkRunnerArgs {
  /**
   * URL of the SPARQL endpoint to send queries to.
   */
  endpoint: string;
  /**
   * Mapping of query set name to an array of SPARQL query strings in this set.
   */
  querySets: Record<string, string[]>;
  /**
   * Number of replication runs.
   */
  replication: number;
  /**
   * Number of warmup runs.
   */
  warmup: number;
  /**
   * If a timestamps column should be added with result arrival times.
   */
  timestampsRecording: boolean;
  /**
   * Destination for log messages.
   * @param message Message to log.
   */
  logger?: (message: string) => void;
  /**
   * SPARQL SELECT query that will be sent to the endpoint to check if it is up.
   */
  upQuery?: string;
  /**
   * Additional URL parameters that must be sent to the endpoint when checking if the endpoint is up.
   */
  additionalUrlParamsInit?: URLSearchParams;
  /**
   * Additional URL parameters that must be sent to the endpoint during actual query execution.
   */
  additionalUrlParamsRun?: URLSearchParams;
  /**
   * A timeout for query execution in milliseconds.
   *
   * If the timeout is reached, the query request will NOT be aborted.
   * Instead, the query is assumed to have silently failed.
   *
   * This timeout is only supposed to be used as a fallback to an endpoint-driven timeout.
   */
  timeout?: number;
}

export interface IRunOptions {
  /**
   * A listener for when the actual query executions have started.
   */
  onStart?: () => Promise<void>;
  /**
   * A listener for when the actual query executions have stopped.
   */
  onStop?: () => Promise<void>;
}
