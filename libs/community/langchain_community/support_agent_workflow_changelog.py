import requests
import re
import markdown
import faiss
import numpy as np
import pickle  # For saving the vectorizer
from bs4 import BeautifulSoup
from sklearn.feature_extraction.text import TfidfVectorizer

# Step 1: Fetch the raw changelog file
url = "https://raw.githubusercontent.com/EventStore/EventStore/master/CHANGELOG.md"
response = requests.get(url)
markdown_content = response.text

# Step 2: Extract versions and pull requests
def extract_changelog_data(md_content):
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

    # Debugging output
    print("Extracted Data:")
    print("Versions:", changelog_data["versions"])
    print("Pull Requests:", changelog_data["pull_requests"])

    return changelog_data

# Extracted changelog data
changelog_data = extract_changelog_data(markdown_content)

# Step 3: Index data in FAISS
def index_in_faiss(data):
    # Ensure we have data to process
    texts = []
    if data["versions"]:
        texts.extend(data["versions"])
    if data["pull_requests"]:
        texts.extend([pr["content"] for pr in data["pull_requests"]])

    # Check if we have any content to vectorize
    if not texts:
        raise ValueError("No content found to index in FAISS.")

    # Use TF-IDF to convert the texts into vectors
    vectorizer = TfidfVectorizer()
    vectors = vectorizer.fit_transform(texts).toarray()

    # FAISS indexing
    dimension = vectors.shape[1]
    index = faiss.IndexFlatL2(dimension)
    index.add(np.array(vectors, dtype=np.float32))

    return index, vectorizer

# Index in FAISS
try:
    index, vectorizer = index_in_faiss(changelog_data)

    # Example: Query the FAISS index
    query = "Fix memory leak"
    query_vector = vectorizer.transform([query]).toarray().astype(np.float32)
    D, I = index.search(query_vector, k=5)

    # Display results
    print("\nQuery:", query)
    for i in range(len(I[0])):
        idx = I[0][i]
        if idx < len(changelog_data["versions"]):
            print(f"Version: {changelog_data['versions'][idx]}")
        else:
            pr_idx = idx - len(changelog_data["versions"])
            print(f"Pull Request: {changelog_data['pull_requests'][pr_idx]['content']} ({changelog_data['pull_requests'][pr_idx]['link']})")

except ValueError as e:
    print(e)
