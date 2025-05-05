import re
from collections import Counter, defaultdict
import threading



# === MAP FUNCTION ===
def map_task(text, output_list, index):
    words = re.findall(r'\w+', text.lower())
    output_list[index] = Counter(words)

# === REDUCE FUNCTION ===
def reduce_task(word, counts, output_dict):
    output_dict[word] = sum(counts)

# === SHUFFLE FUNCTION ===
def shuffle(mapped_results):
    grouped = defaultdict(list)
    for partial in mapped_results:
        for word, count in partial.items():
            grouped[word].append(count)
    return grouped




if __name__ == "__main__":
    # Liste des fichiers texte à traiter
    text_files = ['./texts/file1.txt']

    # === PHASE MAP (en multithread)
    mapped_results = [None] * len(text_files)
    map_threads = []

    for i, file in enumerate(text_files):
        with open(file, 'r', encoding='utf-8') as f:
            content = f.read()
        t = threading.Thread(target=map_task, args=(content, mapped_results, i))
        map_threads.append(t)
        t.start()

    # On attend que toutes les tâches MAP finissent
    for t in map_threads:
        t.join()

    # === PHASE SHUFFLE
    grouped_pairs = shuffle(mapped_results)

    # === PHASE REDUCE (en multithread)
    final_result = {}
    reduce_threads = []

    for word, counts in grouped_pairs.items():
        t = threading.Thread(target=reduce_task, args=(word, counts, final_result))
        reduce_threads.append(t)
        t.start()

    # On attend que toutes les tâches REDUCE finissent
    for t in reduce_threads:
        t.join()

    # === Résultat final
    print("\n=== Résultat final ===")
    for word, count in sorted(final_result.items()):
        print(f"{word}: {count}")