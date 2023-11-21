"""Visualizes the pipeline live using dash to plot the graph and update the status of each node over time."""

from typing import Any, Dict
from multiprocessing import Process, Queue
import asyncio

from dash import dcc, html  # type: ignore[import-untyped]
from dash.dependencies import Input, Output  # type: ignore[import-untyped]
import dash  # type: ignore[import-untyped]
import networkx as nx
import plotly.figure_factory as ff  # type: ignore[import-untyped]
import plotly.graph_objects as go  # type: ignore[import-untyped]

app = dash.Dash("pydatatask")


class TaskVisualizer:
    """The class that does the rendering of the task graph.

    Runs alongside the main pipeline and updates the graph interactively.
    """

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.status_colors = {
            "failed": "red",
            "success": "blue",
            "running": "green",
            "pending": "gray",
        }
        self.nodes = {}
        self.exit_codes = {}
        self.register_callbacks()

    def left_to_right_layout(self, G, ranksep=0.1):
        """Returns node positions for the graph laid out from laid to right.

        This layout is what ends up getting shown in the visualizer. This layout is fixed and the pixel positions
        returned are what is used to draw the lines in the diagram.
        """
        A = nx.nx_agraph.to_agraph(G)
        A.layout(prog="dot", args=f"-Grankdir=LR -Granksep={ranksep}")  # Left to Right layout
        pos = nx.nx_agraph.graphviz_layout(G, prog="dot", args=f"-Grankdir=LR -Granksep={ranksep}")
        return pos

    @staticmethod
    def generate_layout():
        """Sets up the graph view with the area and the update timeout."""
        graph_layout = dcc.Graph(
            id="network-graph", style={"width": "100%", "height": "90vh"}, config={"doubleClick": "reset+autosize"}
        )
        interval_layout = dcc.Interval(id="interval-component", interval=5 * 1000, n_intervals=0)  # in milliseconds
        style = {"width": "100%", "height": "90vh", "margin": "0", "padding": "0"}

        return html.Div([graph_layout, interval_layout], style=style)

    def create_rectangle_shapes(self, pos, width=60, height=30):
        """Renders the pipeline node rectangles."""
        shapes = []
        for _, (x, y) in pos.items():
            shape = {
                "type": "rect",
                "x0": x - width / 2,
                "y0": y - height / 2,
                "x1": x + width / 2,
                "y1": y + height / 2,
                "line": {
                    "color": "blue",
                    "width": 2,
                },
                "fillcolor": "blue",
            }
            shapes.append(shape)
        return shapes

    def create_quiver_plot(self, G, pos):
        """Currently unused AFAICT.

        Can probably be removed.
        """
        edge_x = []
        edge_y = []
        dx = []
        dy = []

        for edge in G.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            edge_x.append(x0)
            edge_y.append(y0)
            dx.append(x1 - x0)
            dy.append(y1 - y0)

        quiver = ff.create_quiver(
            edge_x, edge_y, dx, dy, scale=1, arrow_scale=0.1, line={"width": 0.8, "color": "#888"}
        )
        return quiver.data[0]

    async def get_task_info(self, node):
        """Retrieve the info about a given node from the repositories it is attached to and return it as a dict."""
        repo_entry_counts: Dict[Any, int] = {}
        exit_codes = self.exit_codes.get(node, {})
        for link in node.links:
            count = 0
            async for job in node.links[link].repo:
                if link == "done":
                    if job not in exit_codes[node]:
                        exit_code = (await node.done.info(job)).get("State", {}).get("ExitCode", None)
                        if exit_code is not None:
                            exit_codes[node][job] = exit_code
                    else:
                        exit_code = exit_codes[node][job]
                count += 1
            repo_entry_counts[link] = count

        return exit_codes, repo_entry_counts

    def run_async(self, queue, coroutine, *args):
        """Doesn't really need docs lol."""
        result = asyncio.run(coroutine(*args))
        queue.put(result)

    def sync_function(self, node):
        """Collects a bunch of stuff in a separate subprocess.

        This is PROBABLY done to avoid blocking the main thread? Only @Clasm knows for sure why this was necessary.
        """
        queue: Queue = Queue()

        process = Process(target=self.run_async, args=(queue, self.get_task_info, node))
        process.start()
        process.join()

        exit_codes, result = queue.get()

        self.nodes[node] = result
        self.exit_codes[node] = exit_codes
        return exit_codes, result

    def register_callbacks(self):
        """Registers the update callback for the graph."""

        @app.callback(Output("network-graph", "figure"), [Input("interval-component", "n_intervals")])
        def update_graph(_):
            """Updates the graph plot based on the current state of the pipeline.

            Gray nodes -> not scheduled Green nodes -> running blue nodes -> done nodes with red outline -> at least one
            task of this component failed
            """
            pl = self.pipeline
            new_graph = pl.task_graph
            pos = self.left_to_right_layout(new_graph)

            annotations = []
            for node, (x, y) in pos.items():
                exit_codes, results = self.sync_function(node)
                any_failed = any(x != 0 for x in exit_codes.values())
                if results is not None:
                    if results["live"] > 0:
                        node_color = self.status_colors["running"]
                    elif results["done"] > 0:
                        node_color = self.status_colors["success"]
                    else:
                        node_color = self.status_colors["pending"]
                else:
                    node_color = self.status_colors["pending"]

                border_color = "red" if any_failed else "black"
                annotations.append(
                    {
                        "x": x,
                        "y": y,
                        "xref": "x",
                        "yref": "y",
                        "text": node.name,
                        "showarrow": False,
                        "font": {"size": 14, "color": "white"},
                        "bgcolor": node_color,
                        "bordercolor": border_color,
                        "borderwidth": 2,
                        "borderpad": 4,
                        "hovertext": "<br>".join(f"{k}:{v}" for k, v in self.nodes[node].items()),
                    }
                )

            fig = go.Figure()
            for edge in new_graph.edges():
                x0, y0 = pos[edge[0]]
                x1, y1 = pos[edge[1]]

                if edge[0] in self.nodes and self.nodes[edge[0]]["live"] > 0:
                    line_color = self.status_colors["running"]
                elif edge[0] in self.nodes and self.nodes[edge[0]]["done"] > 0:
                    line_color = self.status_colors["success"]
                else:
                    line_color = self.status_colors["pending"]

                max_size = max(x for x in self.nodes[edge[0]].values() if isinstance(x, int))
                width = max(max_size.bit_length(), 1)

                fig.add_trace(
                    go.Scatter(
                        x=[x0, x1, None], y=[y0, y1, None], line={"width": width, "color": line_color}, mode="lines"
                    )
                )

            node_x = [pos[node][0] for node in new_graph.nodes()]
            node_y = [pos[node][1] for node in new_graph.nodes()]

            fig.add_trace(
                go.Scatter(
                    x=node_x,
                    y=node_y,
                    mode="markers",
                )
            )

            fig.update_layout(
                title="pydatatask visualizer",
                showlegend=False,
                uirevision="some-constant-value",
                margin={"b": 0, "l": 0, "r": 0, "t": 40},
                xaxis={"showgrid": False, "zeroline": False, "showticklabels": False},
                yaxis={"showgrid": False, "zeroline": False, "showticklabels": False},
                hoverlabel={"font_color": "white", "font_size": 16},
                annotations=annotations,
            )

            return fig


def run_viz(pipeline):
    """Entrypoint for "pd viz".

    Starts the visualizer and runs the dash server.
    """
    TaskVisualizer(pipeline)
    app.run_server(debug=True)


app.layout = TaskVisualizer.generate_layout()

if __name__ == "__main__":
    app.run_server(debug=True)
