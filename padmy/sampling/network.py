from padmy.logs import logs

try:
    import networkx as nx
except ImportError:
    logs.error("Please install networkx to use this module")
    raise

from ..db import Database, Table


def add_nodes(graph: nx.Graph, table: Table, processed_nodes: set[str]):
    if table.full_name in processed_nodes:
        return

    graph.add_node(table.full_name.replace(".", "_"), count=table.count, label=table.full_name)

    processed_nodes.add(table.full_name)

    for _child_table in table.child_tables:
        graph.add_edge(
            table.full_name.replace(".", "_"),
            _child_table.full_name.replace(".", "_"),
        )
        add_nodes(graph, _child_table, processed_nodes)


def convert_db(db: Database) -> nx.DiGraph:
    g = nx.DiGraph()
    processed_nodes = set()

    for table in db.tables:
        add_nodes(g, table, processed_nodes)

    return g
