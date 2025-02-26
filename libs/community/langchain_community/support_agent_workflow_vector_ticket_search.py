from bs4 import BeautifulSoup
import pickle
from tqdm import tqdm
def remove_html_tags(text):
  return BeautifulSoup(text, "html.parser").get_text()
def loadTickets():
  f = open('tickets.object', 'rb')
  tickets = pickle.load(f)
  f.close()
  ticket_map = {}
  for ticket in tickets:
      ticket_map[ticket['id']] = remove_html_tags(ticket['description'])
  return ticket_map
ticket_context_map = loadTickets()
def loadConversations():
  f = open('conversations.object', 'rb')
  conversations = pickle.load(f)
  f.close()
  return conversations

documents = {}
def search(query, top_k=1):
    conversations = loadConversations()

    tickets_map = {}

    for id in tqdm(conversations):
        if len(conversations[id]) < 1:
            continue
        ticket_id = conversations[id][0]['ticket_id']
        if ticket_id not in tickets_map:
            tickets_map[ticket_id] = []


        text = "CONTEXT: " + ticket_context_map[ticket_id] + " CONVERSATION: "
        for i in range(0, len(conversations[id])):
            conversation = conversations[id][i]
            from_email = conversation['from_email']
            # remove html
            body_html = conversation['body']
            body_text = remove_html_tags(body_html)
            tickets_map[ticket_id].append(
                {
                    "from_email": from_email,
                    "body_text": body_text
                }
            )

            if from_email is not None and ("eventstore" in from_email or "kurrent" in from_email):
                from_email = "SOLUTION PROVIDER"

            text = text + str(from_email) + " says " + body_text
        documents[ticket_id] = text

    import faiss
    import numpy as np
    from sentence_transformers import SentenceTransformer

    # Initialize the embedding model
    model = SentenceTransformer("all-MiniLM-L6-v2")

    # Convert documents to embeddings
    doc_texts = list(documents.values())
    doc_embeddings = model.encode(doc_texts, normalize_embeddings=True)
    dimension = doc_embeddings.shape[1]  # Get embedding dimension

    # Create FAISS index
    index = faiss.IndexFlatIP(dimension)  # Inner product similarity
    index.add(np.array(doc_embeddings, dtype=np.float32))

    # Store mapping of FAISS indices to document IDs
    id_map = list(documents.keys())


    """Search for the most relevant document for a given query."""
    query_embedding = model.encode([query], normalize_embeddings=True)
    distances, indices = index.search(np.array(query_embedding, dtype=np.float32), top_k)

    # Retrieve document IDs
    results = [(id_map[idx], distances[0][i]) for i, idx in enumerate(indices[0])]
    for doc_id, score in results:
        # print(f"Document ID: {doc_id}, Score: {score:.4f}, Text: {documents[doc_id]}")
        return doc_id, documents[doc_id], score
    return None


# query = "We have a commercial license, but have not received a license key."
# query = "We need to fix a faulted projection"
# results = search(query, top_k=2)

# Print results
