import networkx as nx
import tempfile
import asyncio

#from queue import Queue
from typing import Tuple
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process, Queue

import dash
import pygraphviz as pgv
import plotly.graph_objects as go
import plotly.figure_factory as ff

from dash import dcc, html
from dash.dependencies import Input, Output



app = dash.Dash("pydatatask")

class TaskVisualizer:

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.status_colors = {
            "failed": "red",
            "success": "blue",
            "running": "green",
            "pending": "gray",
        }
        self.nodes = {}
        self.register_callbacks()
    
    def left_to_right_layout(self, G, ranksep=0.1):
        A = nx.nx_agraph.to_agraph(G)
        A.layout(prog='dot', args=f'-Grankdir=LR -Granksep={ranksep}')  # Left to Right layout
        pos = nx.nx_agraph.graphviz_layout(G, prog='dot', args=f'-Grankdir=LR -Granksep={ranksep}')
        return pos


    @staticmethod
    def generate_layout():
        graph_layout = dcc.Graph(id='network-graph',
                                 style={"width": "100%", "height": "90vh"},
                                 config={'doubleClick': 'reset+autosize'})
        interval_layout = dcc.Interval(id='interval-component',
                                       interval=5 * 1000,  # in milliseconds
                                       n_intervals=0)
        style = {'width': '100%', 'height': '90vh', 'margin': '0', 'padding': '0'}

        return html.Div([graph_layout, interval_layout], style=style)


    def create_rectangle_shapes(self, pos, width=60, height=30):
        shapes = []
        for _, (x, y) in pos.items():
            shape = {
                'type': 'rect',
                'x0': x - width/2,
                'y0': y - height/2,
                'x1': x + width/2,
                'y1': y + height/2,
                'line': {
                    'color': 'blue',
                    'width': 2,
                },
                'fillcolor': 'blue'
            }
            shapes.append(shape)
        return shapes


    def create_quiver_plot(self, G, pos):
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

        quiver = ff.create_quiver(edge_x, edge_y, dx, dy,
                                scale=1,
                                arrow_scale=0.1,
                                line=dict(width=0.8, color='#888'))
        return quiver.data[0]



    async def get_live_and_done(self, node):
        repo_count = {}
        for link in node.links:
            count = 0
            async for _ in node.links[link].repo:
                count += 1
            repo_count[link] = count

        return repo_count

    def run_async(self, queue, coroutine, *args):
        result = asyncio.run(coroutine(*args))
        queue.put(result)

    def sync_function(self, node):
        if node in self.nodes and 'done' in self.nodes[node] and self.nodes[node]['done'] > 0:
            return self.nodes[node]

        queue = Queue()

        process = Process(target=self.run_async, args=(queue, self.get_live_and_done, node))
        process.start()
        process.join()

        result = queue.get()

        self.nodes[node] = result
        return result
        
    def register_callbacks(self):
        @app.callback(
            Output('network-graph', 'figure'),
            [Input('interval-component', 'n_intervals')]
        )
        def update_graph(n):
            pl = self.pipeline
            new_graph = pl.task_graph
            pos = self.left_to_right_layout(new_graph)


            annotations = []
            for node, (x, y) in pos.items():
                results = self.sync_function(node)
                if 'done' not in results:
                    print(node, results)
                if results is not None:
                    if results['live'] > 0:
                        node_color = self.status_colors["running"]
                    elif results['done'] > 0:
                        node_color = self.status_colors["success"]
                    else:
                        node_color = self.status_colors["pending"]
                else:
                    node_color = self.status_colors["pending"]

                annotations.append(
                    dict(
                        x=x,
                        y=y,
                        xref="x",
                        yref="y",
                        text=node.name,
                        showarrow=False,
                        font=dict(size=10, color="white"),
                        bgcolor=node_color,
                        bordercolor="black",
                        borderwidth=2,
                        borderpad=4,
                        hovertext='<br>'.join(f"{k}:{v}" for k, v in self.nodes[node].items())
                    )
                )

            edge_x = []
            edge_y = []
            done_edge_x = []
            done_edge_y = []
            for edge in new_graph.edges():
                x0, y0 = pos[edge[0]]
                x1, y1 = pos[edge[1]]

                if edge[0] in self.nodes and self.nodes[edge[0]]['done'] > 0:
                    done_edge_x.extend([x0, x1, None])
                    done_edge_y.extend([y0, y1, None])
                else:
                    edge_x.extend([x0, x1, None])
                    edge_y.extend([y0, y1, None])


            fig = go.Figure()

            #fig.add_trace(create_quiver_plot(new_graph, pos))
            fig.add_trace(go.Scatter(
                x=edge_x, y=edge_y,
                line=dict(width=1, color='#888'),
                mode='lines'))

            fig.add_trace(go.Scatter(
                x=done_edge_x, y=done_edge_y,
                line=dict(width=1, color=self.status_colors["success"]),
                mode='lines'))


            node_x = [pos[node][0] for node in new_graph.nodes()]
            node_y = [pos[node][1] for node in new_graph.nodes()]

            fig.add_trace(go.Scatter(
                x=node_x, y=node_y,
                mode='markers',
            ))

            fig.update_layout(
                title='pydatatask visualizer',
                showlegend=False,
                #uirevision='some-constant-value',
                margin=dict(b=0, l=0, r=0, t=40),
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                hoverlabel=dict(font_color="white"),
                annotations=annotations
            )

            return fig

def run_viz(pipeline):
    tv = TaskVisualizer(pipeline)
    app.run_server(debug=True)

app.layout = TaskVisualizer.generate_layout()

if __name__ == '__main__':
    app.run_server(debug=True)
