import requests
import re
import markdown
import faiss
import numpy as np
import pickle  # For saving the vectorizer
from bs4 import BeautifulSoup
from sklearn.feature_extraction.text import TfidfVectorizer

# File paths for storing FAISS index and vectorizer
INDEX_PATH = "faiss_index.bin"
VOCAB_PATH = "vectorizer.pkl"

# URL of the raw changelog file
CHANGELOG_URL = "https://raw.githubusercontent.com/EventStore/EventStore/master/CHANGELOG.md"


def fetch_changelog(url):
    """Fetches the markdown content of the changelog from GitHub."""
    response = requests.get(url)
    response.raise_for_status()
    return response.text


def extract_changelog_data(md_content):
    """
    Extracts versions and pull requests from the markdown content.
    """
    changelog_data = {
        "versions": [],
        "pull_requests": []
    }

    # Convert markdown to HTML
    html = markdown.markdown(md_content)

    # Use BeautifulSoup to parse the HTML
    soup = BeautifulSoup(html, "html.parser")

    # Extract versions from headings
    version_pattern = re.compile(r'\[v?(\d+\.\d+\.\d+[-\w]*)\]')

    for heading in soup.find_all(['h2', 'h3']):  # Changelogs typically use h2/h3 for versions
        match = re.search(version_pattern, heading.text)
        if match:
            changelog_data["versions"].append(match.group(1))

    # Extract pull requests from list items
    for li in soup.find_all("li"):  # Each change should be in a list item
        links = li.find_all("a", href=True)
        for link in links:
            url = link["href"]
            if "pull" in url:
                changelog_data["pull_requests"].append({"content": li.text.strip(), "link": url})

    return changelog_data


def index_in_faiss(data):
    """
    Indexes versions and pull requests using FAISS.
    """
    texts = []
    if data["versions"]:
        texts.extend(data["versions"])
    if data["pull_requests"]:
        texts.extend([pr["content"] for pr in data["pull_requests"]])

    if not texts:
        raise ValueError("No content found to index in FAISS.")

    # Use TF-IDF to convert the texts into vectors
    vectorizer = TfidfVectorizer()
    vectors = vectorizer.fit_transform(texts).toarray()

    # Create FAISS index
    dimension = vectors.shape[1]
    index = faiss.IndexFlatL2(dimension)
    index.add(np.array(vectors, dtype=np.float32))

    return index, vectorizer


def save_index(index, vectorizer, index_path=INDEX_PATH, vocab_path=VOCAB_PATH):
    """
    Saves FAISS index and vectorizer to disk.
    """
    faiss.write_index(index, index_path)
    with open(vocab_path, "wb") as f:
        pickle.dump(vectorizer, f)

    print(f"FAISS index saved to {index_path}")
    print(f"Vectorizer saved to {vocab_path}")


def load_index(index_path=INDEX_PATH, vocab_path=VOCAB_PATH):
    """
    Loads FAISS index and vectorizer from disk.
    """
    index = faiss.read_index(index_path)
    with open(vocab_path, "rb") as f:
        vectorizer = pickle.load(f)

    # print(f"FAISS index loaded from {index_path}")
    # print(f"Vectorizer loaded from {vocab_path}")
    return index, vectorizer


def search_faiss(query, k=5):
    """
    Searches the FAISS index with a given query and returns the top k results.
    """
    try:
        index, vectorizer = load_index()
        query_vector = vectorizer.transform([query]).toarray().astype(np.float32)
        D, I = index.search(query_vector, k)

        # Reload changelog data
        changelog_data = extract_changelog_data(fetch_changelog(CHANGELOG_URL))

        results = []
        for i in range(len(I[0])):
            idx = I[0][i]
            if idx < len(changelog_data["versions"]):
                results.append(f"Version: {changelog_data['versions'][idx]}")
            else:
                pr_idx = idx - len(changelog_data["versions"])
                results.append(
                    f"Pull Request: {changelog_data['pull_requests'][pr_idx]['content']} "
                    f"({changelog_data['pull_requests'][pr_idx]['link']})"
                )

        return results

    except ValueError as e:
        print("Error:", e)
        return []


# Run this once to index and save the data
if __name__ == "__main__":
    # markdown_content = fetch_changelog(CHANGELOG_URL)
    # changelog_data = extract_changelog_data(markdown_content)
    # index, vectorizer = index_in_faiss(changelog_data)
    # save_index(index, vectorizer)
    load_index()
    # Example search
    query_result = search_faiss("Fix memory leak")
    print("\nSearch Results:")
    for res in query_result:
        print(res)
