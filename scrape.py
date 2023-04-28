import os
from multiprocessing import Pool, Manager
from functools import partial
from tqdm.auto import tqdm
from urllib.parse import urlparse
import os
from pyzotero import zotero
import backoff


# Set up Zotero connection
zot = zotero.Zotero("4886143", "group", "ujVlCfktLcGKvcPBQCj6eqfp")

# Create a folder named "papers" if it doesn't exist
os.makedirs("papers", exist_ok=True)

# Get all top-level items in the library
items = zot.everything(zot.top())

# Iterate through the items
for item in tqdm(items, desc="Downloading papers"):
    import json

    if "attachment" not in item["links"]:
        continue

    attachment = item["links"]["attachment"]

    if attachment["attachmentType"] == "application/pdf":
        key = attachment["href"].split("/")[-1]

        file_path = os.path.join("papers", f"{key}.pdf")

        if not os.path.exists(file_path):
            with open(file_path, "wb") as f:
                f.write(zot.file(key))

import pypdf


def parse_pdf(path, chunk_chars=2000, overlap=50):
    pdfFileObj = open(path, "rb")
    pdfReader = pypdf.PdfReader(pdfFileObj)
    splits = []
    split = ""
    pages = []
    metadatas = []
    for i, page in enumerate(pdfReader.pages):
        split += page.extract_text()
        pages.append(str(i + 1))
        while len(split) > chunk_chars:
            splits.append(split[:chunk_chars])
            pg = "-".join([pages[0], pages[-1]])
            split = split[chunk_chars - overlap :]
            pages = [str(i + 1)]

    if len(split) > overlap:
        splits.append(split[:chunk_chars])
        pg = "-".join([pages[0], pages[-1]])

    pdfFileObj.close()

    return splits, metadatas


import openai

openai.api_key = "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"


@backoff.on_exception(backoff.expo, openai.error.RateLimitError)
def summarize_chunk(chunk: str):
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that summarizes text."},
            {
                "role": "user",
                "content": f"Please provide a brief and succinct summary of the following text, focusing on key points, research questions, methods, results, and conclusions. Also, emphasize any relevance to renewable energy siting, public acceptance, opposition, conflict resolution, mediation, climate justice, energy justice, or a just energy transition. Text: {chunk}",
            },
        ],
    )

    return response["choices"][0]["message"]["content"]


@backoff.on_exception(backoff.expo, openai.error.RateLimitError)
def summarize_chunks(text):
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that summarizes text."},
            {
                "role": "user",
                "content": f"{text} \n\n Each paragraph above was a summary paragraph automatically generated from a section of the paper. Generate a one paragraph summary for the paper including the following information. Be as succinct as possible while still addressing all of the following points: A restatement of the central research question(s) of the paper. A reconstruction of the central argument of the paper, including what abstractions the author uses. What are their data, methods, and results? What conclusions do they draw? Focus the summary on these topics and themes: renewable energy siting, public acceptance, opposition, conflict resolution, mediation, climate justice, energy justice, and/or a just energy transition? Do not repeat this list anywhere in your response: “renewable energy siting, public acceptance, opposition, conflict resolution, mediation, climate justice, energy justice, and/or a just energy transition.” Keep the novel information density/entropy of your response high.",
            },
        ],
    )

    return response["choices"][0]["message"]["content"]


import os
from concurrent.futures import ThreadPoolExecutor
from tqdm.auto import tqdm


def process_item(item):
    if "attachment" not in item["links"]:
        return
    attachment = item["links"]["attachment"]
    if not attachment["attachmentType"] == "application/pdf":
        return

    key = attachment["href"].split("/")[-1]

    if os.path.exists(os.path.join("papers", f"{key}.txt")):
        return

    title = item["data"]["title"]

    authors = []
    for creator in item["data"]["creators"]:
        author = ""
        if "firstName" in creator:
            author += creator["firstName"] + " "
        if "lastName" in creator:
            author += creator["lastName"]
        authors.append(author)
    authors = ", ".join(authors)

    splits, _ = parse_pdf(os.path.join("papers", f"{key}.pdf"), round(4096 * 4 * 0.70), overlap=50)

    summaries = []

    for split in splits:
        summary = summarize_chunk(split)
        summaries.append(summary)

    chunks = "\n".join(summaries)
    summary = summarize_chunks("\n".join(summaries))

    full = f"Title:{title}\nAuthors:{authors}\nSummary: {summary}\n\n Chunks:{chunks}"

    with open(os.path.join("papers", f"{key}.txt"), "w") as f:
        f.write(full)


def main():
    with ThreadPoolExecutor(max_workers=10) as executor:
        list(tqdm(executor.map(process_item, items), total=len(items)))


if __name__ == "__main__":
    main()
