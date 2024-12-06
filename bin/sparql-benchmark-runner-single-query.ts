import { ISparqlBenchmarkRunnerArgs, SparqlBenchmarkRunner } from "../lib"

const options: ISparqlBenchmarkRunnerArgs = {
    endpoint: "http://localhost:3000/sparql",
    querySets: {},
    replication: 1,
    warmup: 0,
    logger: (message: string): boolean => process.stdout.write(`[${new Date().toISOString()}] ${message}\n`)
}
  
const benchmarkRunner = new SparqlBenchmarkRunner(options)
const query = `
PREFIX snvoc: <https://solidbench.linkeddatafragments.org/www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>
SELECT DISTINCT ?creator ?messageContent WHERE {
  <https://solidbench.linkeddatafragments.org/pods/00000000000000000933/profile/card#me> snvoc:likes _:g_0.
  _:g_0 (snvoc:hasPost|snvoc:hasComment) ?message.
  ?message snvoc:hasCreator ?creator.
  ?otherMessage snvoc:hasCreator ?creator;
    snvoc:content ?messageContent.
}
LIMIT 10`
const query_1 = `
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX snvoc: <https://solidbench.linkeddatafragments.org/www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>
SELECT ?messageId ?messageCreationDate ?messageContent WHERE {
  ?message snvoc:hasCreator <https://solidbench.linkeddatafragments.org/pods/00000000000000000933/profile/card#me>;
    rdf:type snvoc:Post;
    snvoc:content ?messageContent;
    snvoc:creationDate ?messageCreationDate;
    snvoc:id ?messageId.
}`
const query_short =`
PREFIX snvoc: <https://solidbench.linkeddatafragments.org/www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>
SELECT ?messageCreationDate ?messageContent WHERE {
  <https://solidbench.linkeddatafragments.org/pods/00000000000000000150/comments/Mexico#68719564521> snvoc:id ?messageId;
    snvoc:creationDate ?messageCreationDate;
    (snvoc:content|snvoc:imageFile) ?messageContent.
}`
console.log(btoa(query_short.trim()))
const results = benchmarkRunner.executeQuery('test', '0', query_short);
results.then(x => {
    console.log(x);
})