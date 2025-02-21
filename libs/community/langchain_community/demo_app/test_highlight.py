import requests

url = "http://localhost:5000/update"  # Change this if your Flask app is running on a different host/port

payload = {
    "nodes": ["A"],  # Replace with the actual node ID(s) you want to highlight
    "message": "Node1 is now highlighted"
}

response = requests.post(url, json=payload)

if response.status_code == 200:
    print("Successfully updated the graph!")
else:
    print(f"Failed to update graph. Status code: {response.status_code}, Response: {response.text}")
