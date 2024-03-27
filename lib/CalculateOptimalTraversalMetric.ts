import * as path from 'path';
import * as fs from "fs";
import { IMetricInput, RunLinkTraversalPerformanceMetrics } from 'optimal-traversal-metric'


export class CalculateOptimalTraversalMetric{
    public engine: any;
    public queryEngineFactory: any;
    public metric: RunLinkTraversalPerformanceMetrics;

    public constructor(){
        this.metric = new RunLinkTraversalPerformanceMetrics();
    }


    public convertZeroIndexedToOneIndexed(edgeList: number[][]){
      return edgeList.map(x=>[x[0]+1, x[1]+1, x[2] ? x[2] : 1]);
    }

    public prepareMetricInput(
        trackedTopology: ITraversalTopology, 
        contributingDocuments: string[][],
        metricType: topologyType
    ): IMetricInput{
      let edgeList = []
      switch (metricType) {
        case 'unweighted':
          edgeList = trackedTopology.edgeListUnWeighted;
          break;
        case 'httpRequestTime':
          edgeList = trackedTopology.edgeListRequestTime;
          break;
        case 'documentSize':
          edgeList = trackedTopology.edgeListDocumentSize;
          break;
      }
      edgeList = this.convertZeroIndexedToOneIndexed(edgeList);

      // Convert string representations of relevant documents to one indexed list

      const relevantDocsOneIndexed = contributingDocuments.map(x=>x.map(y=>trackedTopology.nodeToIndex[y]+1));

      const traversalPath = trackedTopology.traversalOrderEdges;
      const traversalPathOneIndexed = this.convertZeroIndexedToOneIndexed(traversalPath);
    
      const nodeMetaData = trackedTopology.metadataNode;

      const roots = [];
      // Iterate over zero indexed metadata to find nodes with no parent node to find root nodes
      for (let k = 0; k < nodeMetaData.length; k++){
        if (!nodeMetaData[k].hasParent){
          // Convert to one indexed
          roots.push(k+1);
        }
      }

      return {
        edgeList: edgeList, 
        contributingNodes: relevantDocsOneIndexed, 
        traversedPath: traversalPathOneIndexed, 
        numNodes: nodeMetaData.length,
        roots: roots
      };
    }

  public async calculateMetricAllResults(topology: ITraversalTopology, contributingDocuments: string[][], metricType: topologyType){
      const metricInput: IMetricInput = this.prepareMetricInput(
        topology, 
        contributingDocuments, 
        metricType
      ); 
      return await this.calculateMetricAll(metricInput);
  }

  public async calculateMetricFirstKResults(
    kToCheck: number[], 
    topology: ITraversalTopology, 
    contributingDocuments: string[][], 
    metricType: topologyType, 
    searchType: searchType,
    solverInputFileLocation?: string,
    batchSize?: number,
    allowRandomSampling?: boolean,
    numberSamples?: number,
  ): Promise<number[]>
  {
    const metricInput: IMetricInput = this.prepareMetricInput(
        topology, 
        contributingDocuments, 
        metricType
      ); 
    const metricsFirstK: number[] = [];
    for (const k of kToCheck){
        if (metricInput.contributingNodes.length > k){
          metricsFirstK.push(await this.calculateMetricFirstK(
            metricInput, 
            k, 
            searchType, 
            solverInputFileLocation, 
            batchSize, 
            allowRandomSampling, 
            numberSamples
          ));
        }
        else{
          metricsFirstK.push(-1);
        }
    }

    return metricsFirstK
  }
  private async calculateMetricAll(metricInput: IMetricInput){
    const metricAll = await this.metric.runMetricAll(
      metricInput.edgeList, 
      metricInput.contributingNodes, 
      metricInput.traversedPath, 
      metricInput.roots,
      metricInput.numNodes,
    );
    return metricAll;
  }

  private async calculateMetricFirstK(
    metricInput: IMetricInput, 
    k: number, 
    searchType: searchType, 
    solverInputFileLocation?: string,
    batchSize?: number, 
    allowRandomSampling?: boolean,
    numberSamples?: number
    ){
    const metricFirstK = await this.metric.runMetricFirstK(
      k,
      metricInput.edgeList, 
      metricInput.contributingNodes, 
      metricInput.traversedPath, 
      metricInput.roots,
      metricInput.numNodes,
      searchType,
      solverInputFileLocation,
      batchSize,
      allowRandomSampling,
      numberSamples
    );
    return metricFirstK
  }

}

export interface IQueriesMetricResults{
  query: string
  /**
   * Num results in query
   */
  nResults: number;
  /**
   * Metric for all results
   */
  metricAll: number
  /**
   * All results of first k result metric, key: k , value: metric
   */
  metricsFirstK: Record<number, number>
}

/**
 * Interface that represents the fields of a Comunica topology. This is implementation specific
 */
export interface ITraversalTopology{
    nodeToIndex: Record<string, number>;
    // Edges denoted by [start, end, weight] with all weights equal
    edgeListUnWeighted: number[][];
    // Edges denoted by [start, end, weight] with weight equal to http request time
    edgeListRequestTime: number[][];
    // Edges denoted by [start, end, weight] with weight equal to #quads in end node
    edgeListDocumentSize: number[][];
    // Dictionary with string representations of edges, to check for duplicates
    edgesInGraph: Record<string, number>;
    // 0 indexed list of all metadata associated with node, same order as nodeToIndex
    metadataNode: Record<string, any>[];
    // Order in which the engine _dereferences_ nodes
    traversalOrder: string[];
    // What edges are traversed in what order
    traversalOrderEdges: number[][];
}

export type topologyType = "unweighted" | "httpRequestTime" | "documentSize";

export type searchType = "full" | "reduced";