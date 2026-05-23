import pandas as pd
import os

# Read the two books
books = {
    "Feuillet-pauvre": "data/Feuillet-pauvre",
    "Le_Petit_Prince": "data/st_exupery_le_petit_prince"
}

corpus_data = []
doc_id = 1

for book_name, book_path in books.items():
    try:
        with open(book_path, 'r', encoding='utf-8', errors='ignore') as f:
            text = f.read()
            # Split by paragraphs or sentences
            paragraphs = text.split('\n\n')
            for para in paragraphs:
                if len(para.strip()) > 50:  # Only keep substantial paragraphs
                    corpus_data.append({
                        "doc_id": f"doc_{doc_id}_{book_name}",
                        "text": para.strip()
                    })
                    doc_id += 1
    except Exception as e:
        print(f"Error reading {book_path}: {e}")

# Create DataFrame and save to CSV
df = pd.DataFrame(corpus_data)
df.to_csv('data/corpus.csv', index=False)
print(f"Corpus created: {len(df)} documents")
print(df.head())
