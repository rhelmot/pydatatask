import networkx as nx
import plotly.graph_objects as go
import plotly.figure_factory as ff
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pygraphviz as pgv
import tempfile


external_stylesheets = [
    {
        "selector": "body",
        "rule": "margin: 0; padding: 0; overflow: hidden;"
    }
]

def left_to_right_layout(G, ranksep=0.1):
    A = nx.nx_agraph.to_agraph(G)
    A.layout(prog='dot', args=f'-Grankdir=LR -Granksep={ranksep}')  # Left to Right layout
    pos = nx.nx_agraph.graphviz_layout(G, prog='dot', args=f'-Grankdir=LR -Granksep={ranksep}')
    return pos

# Load the .dot file into a networkx graph
def load_dot_file(dot_file_path, is_string=False):
    if is_string:
        A = pgv.AGraph(string=dot_file_path, directed=True)
    else:
        A = pgv.AGraph(dot_file_path, directed=True)
    G = nx.DiGraph(A)
    return G

#pos = nx.spring_layout(G)

#app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Graph(id='network-graph', style={"width": "100%", "height": "90vh"}, config={'doubleClick': 'reset+autosize'}),
    dcc.Interval(
        id='interval-component',
        interval=10*1000,  # in milliseconds
        n_intervals=0
    )
],style={'width': '100%', 'height': '90vh', 'margin': '0', 'padding': '0'})

def create_rectangle_shapes(pos, width=60, height=30):
    shapes = []
    for node, (x, y) in pos.items():
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


def create_quiver_plot(G, pos):
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

COLORS = {
    "failed": "red",
    "success": "blue",
    "running": "green",
    "pending": "gray",
}

@app.callback(
    Output('network-graph', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def update_graph(n):
    global pl
    new_graph = pl.task_graph
    pos = left_to_right_layout(new_graph)

    edge_x = []
    edge_y = []
    for edge in new_graph.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    annotations = []
    for node, (x, y) in pos.items():
        print(node.live)
        annotations.append(
            dict(
                x=x,
                y=y,
                xref="x",
                yref="y",
                text=str(node),
                showarrow=False,
                font=dict(size=10, color="white"),
                bgcolor=COLORS["pending"],
                bordercolor="black",
                borderwidth=2,
                borderpad=4,
                hovertext='<br>'.join(f"{k}:{v.repo}" for k, v in node.links.items())
            )
        )


    fig = go.Figure()

    #fig.add_trace(create_quiver_plot(new_graph, pos))
    fig.add_trace(go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=1, color='#888'),
        mode='lines'))

    node_x = [pos[node][0] for node in new_graph.nodes()]
    node_y = [pos[node][1] for node in new_graph.nodes()]

    fig.add_trace(go.Scatter(
        x=node_x, y=node_y,
        mode='markers',
    ))

    fig.update_layout(
        title='Networkx Graph from .dot file with Plotly and Dash',
        showlegend=False,
        uirevision='some-constant-value',
        margin=dict(b=0, l=0, r=0, t=40),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        annotations=annotations
    )

    return fig

def run_viz(pipeline):
    global pl
    pl = pipeline
    app.run_server(debug=True)

if __name__ == '__main__':
    app.run_server(debug=True)

