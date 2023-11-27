import typing
from typing import Literal

from padmy.db import Database
from .network import convert_db
from ..logs import logs

try:
    import dash_cytoscape as cyto
except ImportError:
    logs.error("Both dash and dash_cytoscape need to be installed")
    raise

from dash import Dash, Output, Input, html
import json
import networkx as nx

cyto.load_extra_layouts()


class NodeData(typing.TypedDict):
    id: str
    count: int
    label: str


class EdgeData(typing.TypedDict):
    id: str
    source: str
    target: str


class DirectedElement(typing.TypedDict):
    data: NodeData | EdgeData


def get_directed_edges(g: nx.DiGraph) -> list[DirectedElement]:
    directed_edges: list[DirectedElement] = [
        {"data": {"id": f"{src}{tgt}", "source": src, "target": tgt}} for src, tgt in g.edges
    ]
    node_elems: list[DirectedElement] = [{"data": {"id": k, **v}} for k, v in g.nodes.data()]  # type: ignore
    return node_elems + directed_edges


_CIRCULAR_NODE_STYLE = {
    "source-arrow-color": "red",
    "source-arrow-shape": "triangle",
    "line-color": "red",
}

_CIRCULAR_EDGE_STYLE = {
    "source-arrow-color": "red",
    "source-arrow-shape": "triangle",
    "line-color": "red",
}


def get_cycles_styles(g: nx.DiGraph):
    styles = []
    for cycles in nx.simple_cycles(g):
        # Self referencing
        if len(cycles) == 1:
            cycle = cycles[0]
            styles += [
                {
                    "selector": f"#{cycle}",
                    "style": {**_CIRCULAR_NODE_STYLE, "background-color": "red"},
                },
                {
                    "selector": f"#{cycle}{cycle}",
                    "style": {**_CIRCULAR_EDGE_STYLE, "background-color": "red"},
                },
            ]
        else:
            _src, _tgt = cycles
            styles += [
                {
                    "selector": f"#{_src}",
                    "style": {**_CIRCULAR_NODE_STYLE, "background-color": "red"},
                },
                {
                    "selector": f"#{_tgt}",
                    "style": {**_CIRCULAR_NODE_STYLE, "background-color": "red"},
                },
                {"selector": f"#{_src}{_tgt}", "style": _CIRCULAR_EDGE_STYLE},
                {"selector": f"#{_tgt}{_src}", "style": _CIRCULAR_EDGE_STYLE},
            ]
    return styles


# See https://dash.plotly.com/cytoscape/layout for a full list
Layout = Literal["cose", "breadthfirst", "circle", "preset", "random", "grid", "concentric"]
# needs cyto.load_extra_layouts() to be loaded
ExternalLayout = Literal["cose-bilkent", "cola", "euler", "spread", "dagre", "klay"]


def get_layout(
    g: nx.DiGraph,
    *,
    layout: Layout | ExternalLayout = "klay",
    # Eg: {'width': '100%', 'height': '400px'}
    style: dict | None = None,
) -> cyto.Cytoscape:
    elements = get_directed_edges(g)
    _style = style or {"width": "100%", "height": "800px"}
    stylesheet = [
        {"selector": "node", "style": {"label": "data(label)"}},
        {
            "selector": "edge",
            "style": {
                "curve-style": "bezier",
                "source-arrow-shape": "triangle",
            },
        },
        *get_cycles_styles(g),
    ]
    return cyto.Cytoscape(layout={"name": layout}, style=_style, elements=elements, stylesheet=stylesheet)


def run_simple_app(db: Database, port: int = 5555):
    g = convert_db(db)
    cyto_layout = get_layout(g)
    layout_id = "graph-layout"
    cyto_layout.id = layout_id  # type: ignore

    app = Dash(__name__)

    app.layout = html.Div(
        [
            cyto_layout,
            html.Pre(id="cytoscape-tapNodeData-output"),
            html.Pre(id="cytoscape-tapEdgeData-output"),
        ]
    )

    @app.callback(
        Output("cytoscape-tapNodeData-output", "children"),
        Input(layout_id, "tapNodeData"),
    )
    def _on_press_node(data):
        return json.dumps(data, indent=2)

    @app.callback(
        Output("cytoscape-tapEdgeData-output", "children"),
        Input(layout_id, "tapEdgeData"),
    )
    def _on_press_edge(data):
        return json.dumps(data, indent=2)

    app.run(port=port)  # type: ignore
