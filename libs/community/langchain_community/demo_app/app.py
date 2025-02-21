from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO

app = Flask(__name__)
socketio = SocketIO(app)

# Sample graph data
nodes = [
    {"id": "A"}, {"id": "B"}, {"id": "C"}, {"id": "D"}
]
edges = [
    {"source": "A", "target": "B"},
    {"source": "A", "target": "C"},
    {"source": "B", "target": "D"}
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
