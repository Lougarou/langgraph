from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO

app = Flask(__name__)
socketio = SocketIO(app)

nodes = [
    {"id": "human feedback"},
    {"id": "LLM Agent"},
    {"id": "decide next action"},
    {"id": "output suggestion"},
    {"id": "analytics subgraph"},
    {"id": "find metadata"},
    {"id": "compile analysis"},
    {"id": "diagnose stats file"},
    {"id": "get similar ticket from vector db"},
    {"id": "use changelog to find bugs from vector db"}
]

edges = [
    {"source": "human feedback", "target": "LLM Agent"},
    {"source": "human feedback", "target": "analytics subgraph"},
    {"source": "analytics subgraph", "target": "decide next action"},
    {"source": "LLM Agent", "target": "decide next action"},
    {"source": "decide next action", "target": "output suggestion"},

    # Edges from the deep dive subgraph
    {"source": "find metadata", "target": "diagnose stats file"},
    {"source": "find metadata", "target": "get similar ticket from vector db"},
    {"source": "find metadata", "target": "use changelog to find bugs from vector db"},
    {"source": "diagnose stats file", "target": "compile analysis"},
    {"source": "get similar ticket from vector db", "target": "compile analysis"},
    {"source": "use changelog to find bugs from vector db", "target": "compile analysis"}
]

@app.route('/')
def index():
    return render_template('index.html', nodes=nodes, edges=edges)


@app.route('/update', methods=['POST'])
def update():
    data = request.json
    nodes_to_highlight = data.get("nodes", [])
    message = data.get("message", "")

    socketio.emit('update_graph', {'nodes': nodes_to_highlight, 'message': message})
    return jsonify({"status": "success"})


if __name__ == '__main__':
    socketio.run(app, debug=True, allow_unsafe_werkzeug=True)
