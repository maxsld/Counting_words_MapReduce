import os
import re
from collections import Counter, defaultdict

# === MAP FUNCTION ===
def map_task(text):
    words = re.findall(r'\w+', text.lower())
    return Counter(words)

# === REDUCE FUNCTION ===
def reduce_task(grouped_pairs):
    reduced = {}
    for word, counts in grouped_pairs.items():
        reduced[word] = sum(counts)
    return reduced

# === SHUFFLE FUNCTION ===
def shuffle(mapped_results):
    grouped = defaultdict(list)
    for partial in mapped_results:
        for word, count in partial.items():
            grouped[word].append(count)
    return grouped

# === MAIN ===
if __name__ == "__main__":
    text_files = ['./texts/file1.txt']

    # Phase MAP
    mapped_results = []
    for file in text_files:
        with open(file, 'r', encoding='utf-8') as f:
            content = f.read()
            result = map_task(content)
            mapped_results.append(result)

    # Phase SHUFFLE
    grouped_pairs = shuffle(mapped_results)

    # Phase REDUCE
    final_result = reduce_task(grouped_pairs)

    # Résultat
    print("\n=== Résultat final ===")
    for word, count in sorted(final_result.items()):
        print(f"{word}: {count}")
