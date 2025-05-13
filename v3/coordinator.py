### coordinator.py
import socket
import threading
import pickle

MAP_WORKERS = [('localhost', 8001), ('localhost', 8002)]
REDUCE_WORKERS = [('localhost', 9001), ('localhost', 9002)]
TEXT_FILES = ['../texts/file1.txt', '../texts/file2.txt']

# Attribution des mots aux reduce workers

def hash_word(word):
    return hash(word) % len(REDUCE_WORKERS)

def send_data(address, data):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(address)
        s.sendall(pickle.dumps(data))

def collect_maps():
    partial_results = []
    def listen_map():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('localhost', 7000))
            s.listen()
            for _ in range(len(MAP_WORKERS)):
                conn, _ = s.accept()
                with conn:
                    data = b""
                    while True:
                        packet = conn.recv(4096)
                        if not packet: break
                        data += packet
                    partial_results.append(pickle.loads(data))

    listener = threading.Thread(target=listen_map)
    listener.start()

    # Envoi des fichiers aux workers map
    for i, file in enumerate(TEXT_FILES):
        with open(file, 'r') as f:
            content = f.read()
        send_data(MAP_WORKERS[i], content)

    listener.join()
    return partial_results

def shuffle_and_send(partial_results):
    grouped = [{} for _ in REDUCE_WORKERS]

    for result in partial_results:
        for word, count in result.items():
            idx = hash_word(word)
            grouped[idx][word] = grouped[idx].get(word, 0) + count

    for i, data in enumerate(grouped):
        send_data(REDUCE_WORKERS[i], data)

def collect_reduces():
    final_result = {}
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', 7500))
        s.listen()
        for _ in range(len(REDUCE_WORKERS)):
            conn, _ = s.accept()
            with conn:
                data = b""
                while True:
                    packet = conn.recv(4096)
                    if not packet: break
                    data += packet
                result = pickle.loads(data)
                final_result.update(result)
    return final_result

if __name__ == '__main__':
    maps = collect_maps()
    shuffle_and_send(maps)
    final = collect_reduces()
    print("\n=== Résultat final ===")
    for word, count in sorted(final.items()):
        print(f"{word}: {count}")
