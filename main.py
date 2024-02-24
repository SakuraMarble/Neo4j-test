from neo4j import GraphDatabase
import pandas as pd
from tqdm import tqdm
import os.path as osp
from timeit import default_timer as timer
import datetime
import os
import getpass
import time

DATASETS = ["example"]
NEO4J_HOME = os.environ["NEO4J_HOME"]
IMPORT_DIR_PATH = osp.join(NEO4J_HOME, "import")
INIT_CONF_PATH = osp.join(NEO4J_HOME, "conf", "neo4j.conf.init")
CONF_PATH = osp.join(NEO4J_HOME, "conf", "neo4j.conf")
DATA_DIR_PATH = osp.join(NEO4J_HOME, "data", "databases")
TRANS_DIR_PATH = osp.join(NEO4J_HOME, "data", "transactions")


def get_graph_path(dataset):
    return "datasets/%s_graph.txt" % (dataset)


def get_graph_df(dataset):
    with open(get_graph_path(dataset), "r") as file:
        V_line = file.readline().strip()
        E_line = file.readline().strip()
        V = int(V_line.split("=")[1].strip())
        E = int(E_line.split("=")[1].strip())
    df = pd.read_csv(
        get_graph_path(dataset),
        sep=" ",
        skiprows=3,
        names=["Edge", "From", "To", "Weight"],
    )
    df.dropna(inplace=True)
    df = df.drop(columns=["Edge"])
    df["From"] = df["From"].astype(int)
    df["To"] = df["To"].astype(int)
    df["Weight"] = df["Weight"].astype(int)
    print(V, E)
    assert len(df) == 5000
    return V, E, df


if __name__ == "__main__":
    uri = "neo4j://localhost:7687"

    # 使用bash.sh前，需要将getpass.getpass("connecting to neo4j, password: ")替换为"YourPassword"
    auth = (
        "neo4j",
        getpass.getpass("connecting to neo4j, password: "),
    )

    for dataset in DATASETS:
        V, E, df = get_graph_df(dataset)
        print("dataset: {}, V={}, E={}".format(dataset, V, E))

        force = False
        exits = os.path.exists(osp.join(DATA_DIR_PATH, dataset)) and os.path.exists(
            osp.join(TRANS_DIR_PATH, dataset)
        )

        if not exits or force:
            if os.path.exists(osp.join(DATA_DIR_PATH, dataset)):
                os.system("rm -rf %s" % (osp.join(DATA_DIR_PATH, dataset)))
            if os.path.exists(osp.join(TRANS_DIR_PATH, dataset)):
                os.system("rm -rf %s" % (osp.join(TRANS_DIR_PATH, dataset)))
            nodes = pd.DataFrame({"ID:ID": range(V)})
            nodes[":LABEL"] = "Node"
            # nodes新增加一列，列名是id，值是0到V-1
            edges = df[["From", "To", "Weight"]]
            edges.columns = [":START_ID", ":END_ID", "weight:double"]
            edges[":TYPE"] = "Edge"
            nodes.to_csv(
                osp.join(IMPORT_DIR_PATH, dataset + "_nodes.csv"), index=False, sep=","
            )
            edges.to_csv(
                osp.join(IMPORT_DIR_PATH, dataset + "_edges.csv"), index=False, sep=","
            )
            os.system(
                "neo4j-admin database import full --nodes %s --id-type integer --relationships %s %s"
                % (
                    osp.join(IMPORT_DIR_PATH, dataset + "_nodes.csv"),
                    osp.join(IMPORT_DIR_PATH, dataset + "_edges.csv"),
                    dataset,
                )
            )

        os.system("cp %s %s" % (INIT_CONF_PATH, CONF_PATH))
        with open(CONF_PATH, "a") as f:
            f.write("dbms.default_database=%s\n" % (dataset))
        os.system("neo4j-admin server restart")
        print("Waiting for neo4j to ready...")
        while True:
            try:
                with GraphDatabase.driver(uri, auth=auth) as driver:
                    driver.verify_connectivity()
                break
            except Exception as e:
                if str(e) == "Unable to retrieve routing information":
                    time.sleep(3)
                else:
                    raise e

        with GraphDatabase.driver(uri, auth=auth, database=dataset) as driver:
            seed = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

            results = []
            with driver.session() as session:
                result = session.run("MATCH (n) RETURN count(n)")
                nodes = result.single()[0]
                result = session.run("MATCH ()-[r]->() RETURN count(r)")
                edges = result.single()[0]
                print("neo4j: V={}, E={}".format(nodes, edges))

                session.run(
                    r"""
                CALL gds.graph.project(
                    'graph_%s',
                    'Node',
                    'Edge',
                    {
                        relationshipProperties: 'weight'
                    }
                    )
                """
                    % (seed)
                )
                PageRank_time = []
                Dijkstra_time = []
                WCC_time = []
                LPA_time = []
                for i in range(1):
                    tic = timer()
                    result = session.run(
                        r"""
                        CALL gds.pageRank.stream('graph_%s')
                        YIELD nodeId, score
                        RETURN gds.util.asNode(nodeId).ID AS ID, score
                        ORDER BY score DESC, ID ASC
                        """
                        % (seed)
                    )
                    toc = timer()
                    PageRank_time.append(toc - tic)
                    df = pd.DataFrame([record for record in result])
                    df.to_csv("result/result_PageRank.csv", header=["ID","Score"], index=False)

                    tic = timer()
                    for node_id in range(0, V):
                        result = session.run(
                            r"""
                            MATCH (source:Node {ID: $source})
                            CALL gds.allShortestPaths.dijkstra.stream('graph_%s', {
                            sourceNode: source,
                            relationshipWeightProperty: 'weight'
                            })
                            YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
                            RETURN
                            index,
                            gds.util.asNode(sourceNode).ID AS sourceNodeid,
                            gds.util.asNode(targetNode).ID AS targetNodeid,
                            totalCost,
                            [nodeId IN nodeIds | gds.util.asNode(nodeId).ID] AS nodeIDs,
                            costs,
                            nodes(path) as path
                            ORDER BY index
                        """
                            % (seed),
                            source=node_id,
                        )
                    toc = timer()
                    Dijkstra_time.append(toc - tic)

                    tic = timer()
                    result = session.run(
                        r"""
                        CALL gds.wcc.stream('graph_%s')
                        YIELD nodeId, componentId
                        RETURN gds.util.asNode(nodeId).ID AS ID, componentId
                        ORDER BY componentId, ID
                        """
                        % (seed)
                    )
                    toc = timer()
                    WCC_time.append(toc - tic)
                    df = pd.DataFrame([record for record in result])
                    df.to_csv("result/result_WCC.csv",header=["ID","ComponentId"] ,index=False)

                    tic = timer()
                    result = session.run(
                        r"""
                        CALL gds.labelPropagation.stream('graph_%s')
                        YIELD nodeId, communityId AS Community
                        RETURN gds.util.asNode(nodeId).ID AS ID, Community
                        ORDER BY Community, ID
                        """
                        % (seed)
                    )
                    toc = timer()
                    LPA_time.append(toc - tic)
                    df = pd.DataFrame([record for record in result])
                    df.to_csv("result/result_LPA.csv",header=["ID","Community"], index=False)

                results.append(
                    {
                        "PageRank": sum(PageRank_time) / len(PageRank_time),
                        "Dijkstra": sum(Dijkstra_time) / len(Dijkstra_time) / V,
                        "WCC": sum(WCC_time) / len(WCC_time),
                        "LPA": sum(LPA_time) / len(LPA_time),
                    }
                )
                # 将results转换为dataframe
                results = pd.DataFrame(results)
                # 将results追加到results_time.csv文件中的末尾
                results.to_csv(
                    "result/results_time.csv", mode="a", header=False, index=False
                )
