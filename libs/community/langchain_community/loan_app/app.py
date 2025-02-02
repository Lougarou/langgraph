from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit

app = Flask(__name__)
socketio = SocketIO(app)

# Serve the main chat page
@app.route('/')
def index():
    return render_template('index.html')

# Endpoint 1: Push a message for person 1
@app.route('/send_person1', methods=['POST'])
def send_person1():
    data = request.json
    message = data.get('message', '')
    socketio.emit('new_message', {'sender': 'Person 1', 'message': message})
    return jsonify({'status': 'Message sent from Person 1'})

# Endpoint 2: Push a message for person 2
@app.route('/send_person2', methods=['POST'])
def send_person2():
    data = request.json
    message = data.get('message', '')
    socketio.emit('new_message', {'sender': 'Person 2', 'message': message})
    return jsonify({'status': 'Message sent from Person 2'})

if __name__ == '__main__':
    socketio.run(app, debug=True)
