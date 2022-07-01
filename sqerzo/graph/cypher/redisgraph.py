import redis
import urllib.parse as pr

from typing import Iterable, List, Type, Tuple

from redisgraph import Graph
from redisgraph.edge import Edge
from redisgraph.node import Node

from .lang import create_query
from ...exceptions import *
from ..model import GraphElement, GraphNode
from .transaction import CypherSQErzoTransaction
from .interfaces import ResultElement, CypherSQErzoGraphConnection
from ..interfaces import SQErzoQueryResponse, SQErzoGraphConnection

# TODO:
# 1. [X] Fix - Do not let the label string or relation be seperated into list of characters:
# ResultElement(id=1, alias='u', labels=['U', 's', 'e', 'r'], properties={'identity': '868497784832', 'name': 'DName-0'})]

class RedisGraphSQErzoQueryResponse(SQErzoQueryResponse):

    def __init__(self, graph: SQErzoGraphConnection, query: str, **kwargs):
        self.query = query
        self.graph = graph
        self.params = kwargs

    def __iter__(self):
        query_results = self.graph.connection.query(self.query, self.params)
        print(f'[redisgraph:RedisGraphSQErzoQueryResponse:__iter__] query_results.result_set: {query_results.result_set}')
        for res in query_results.result_set:
            result_list = []
            for i, element in enumerate(res):
                print(f'[redisgraph:RedisGraphSQErzoQueryResponse:__iter__] element - res[{i}]: {element}')
                if isinstance(element, Node):
                    result_list.append(
                        ResultElement(
                            id=element.id,
                            alias=query_results.header[i][1].decode(),
                            labels=element.labels,
                            properties=element.properties
                        )
                    )
                elif isinstance(element, Edge):
                    result_list.append(
                        ResultElement(
                            id=element.id,
                            alias=query_results.header[i][1].decode(),
                            labels=[element.relation],
                            properties=element.properties
                        )
                    )
                else:
                    raise ValueError('Result is not Node or Edge')
            yield result_list



class RedisSQErzoTransaction(CypherSQErzoTransaction):
    SUPPORTED_TYPES = ("str", "int", "float", "bool")

    def dump_data(self,
                  partial_query_nodes: dict,
                  partial_edged: dict):
        if partial_query_nodes:
            node_query_list = [create_query(node, partial=True) for node in partial_query_nodes.values()]
            node_query = f"CREATE {', '.join(node_query_list)}"
            self.graph.db_engine.query(node_query)

        #
        # Get all Edges
        #
        for edges in partial_edged.values():

            # Get first element for complete query
            edge = edges[0]

            #
            # Build bach information.
            #
            # We use indexed data batch, instead of dict, because not all
            # DB Engines support list of dict, but all supports indexed
            # data
            #
            batch = [
                [
                    b.source.make_identity(),
                    b.destination.make_identity(),
                    b.make_identity()
                ]
                for b in edges
            ]

            edge_query = f"""
            UNWIND $batch as row
            MATCH (from:{edge.source.labels()} {{ identity: row[0] }})
            MATCH (to:{edge.destination.labels()} {{ identity: row[1] }})
            CREATE (from)-[:{edge.labels()} {{ identity: row[2] }}]->(to)
            """
            print(f'[RedisSQErzoTransaction: dump_data] edge_query: \n{edge_query}')
            self.graph.db_engine.query(edge_query, batch=batch)


class RedisSQErzoGraphConnection(CypherSQErzoGraphConnection):

    def __init__(self, connection_string: str):
        self.connection = self._parse_connection_string(
            connection_string
        )

    def update_element(self, graph_element: GraphElement) \
            -> None or SQErzoElementExistException:
        """Redis doesn't support multiple labels -> we'll simulate then"""

        def update_fixed_label_nodes(n) -> Iterable[Tuple[bool, GraphElement]]:
            if len(n.__labels__) > 1:
                #
                # If multiple labels are not supported, then create
                # net node clone with only new labels
                #
                for lb in n.__labels__[1:]:
                    new_node = n.clone(exclude=["identity"])
                    new_node.__labels__ = [lb]

                    yield True, new_node

            else:
                yield False, n

        if not graph_element:
            raise SQErzoElementExistException("Node update needs a valid Node")

        for need_create, n in update_fixed_label_nodes(graph_element):

            # No multiple labels supported. Need to be created a new node
            if need_create:
                q = n.query_create()

            # Multiple labels supported -> update node
            else:
                q = n.query_update()

            self.query(q)

            n.__dirty_properties__.clear()

    def save_element(self, graph_element: GraphElement) \
            -> None or SQErzoElementExistException:

        node_id = graph_element.make_identity()

        if isinstance(graph_element, GraphNode):
            if self.get_node_by_id(node_id):
                raise SQErzoElementExistException(
                    f"Graph element with id '{node_id}' already exits"
                )

        super(RedisSQErzoGraphConnection, self).save_element(graph_element)

    @property
    def transaction_class(self) -> Type:
        return RedisSQErzoTransaction

    def query(self, query: str, **kwargs):
        self.connection.query(query, kwargs)

    def query_response(self, query: str, **kwargs) -> Iterable[ResultElement]:
        return RedisGraphSQErzoQueryResponse(self, query, **kwargs)

    def create_constraints_nodes(self, key: str, labels: List[str]):
        """Redis doesn't support constraints -> do nothing"""

    def create_constraints_edges(self, key: str, labels: List[str]):
        """Redis doesn't support constraints -> do nothing"""

    def create_indexes(self, attribute: str, labels: List[str]):
        #
        # Redis doesn't support multi-label nodes -> create two index. One
        # for each node
        #
        for label in labels:
            q = f"""
            CREATE INDEX ON :{label}({attribute})
            """

            self.query(q)


    def _parse_connection_string(self, cs: str) -> Graph:
        parsed = pr.urlparse(cs)

        if parsed.path:
            db = parsed.path[1:]

            if not db:
                db = 0
            else:
                db = int(db)
        else:
            db = 0

        if parsed.query:
            qp = parsed.query.split("&", maxsplit=1)[0]
            p_name, p_value = qp.split("=")
            if p_name != "graph":
                db_graph = "sqerzo"
            else:
                db_graph = p_value
        else:
            db_graph = "sqerzo"

        port = parsed.port
        if port is None:
            port = 6379

        r = redis.Redis(
            host=parsed.hostname,
            port=port,
            username=parsed.username,
            password=parsed.password,
            db=db
        )

        return Graph(db_graph, r)

__all__ = ("RedisSQErzoGraphConnection",)
