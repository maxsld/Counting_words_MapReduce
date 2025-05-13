import socket
import pickle
from collections import Counter
import re

# Port sur lequel ce map worker écoute (à adapter par machine)
MY_PORT = 8002

# Liste des Reduce Workers avec leurs IP et port
REDUCE_WORKERS = [
    ('localhost', 9001),  # exemple IP du reduce worker 1
    ('localhost', 9002)   # exemple IP du reduce worker 2
]

def hash_word(word):
    return hash(word) % len(REDUCE_WORKERS)

def map_task(text):
    words = re.findall(r'\w+', text.lower())
    return dict(Counter(words))

def shuffle_and_send(mapped_data):
    # Crée une partition des mots pour chaque reduce worker
    grouped = [{} for _ in REDUCE_WORKERS]

    for word, count in mapped_data.items():
        idx = hash_word(word)
        grouped[idx][word] = grouped[idx].get(word, 0) + count

    # Envoie chaque groupe au reduce worker correspondant
    for i, data in enumerate(grouped):
        ip, port = REDUCE_WORKERS[i]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((ip, port))
            client.sendall(pickle.dumps(data))

def start():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('0.0.0.0', MY_PORT))  # pour accepter depuis n'importe où
        s.listen()
        print(f"[MAP WORKER] En écoute sur le port {MY_PORT}...")

        while True:
            conn, _ = s.accept()
            with conn:
                print("[MAP WORKER] Reçu un fichier à mapper.")
                data = b""
                while True:
                    packet = conn.recv(4096)
                    if not packet:
                        break
                    data += packet
                text = pickle.loads(data)

            mapped_data = map_task(text)
            print("[MAP WORKER] Mapping terminé. Envoi aux reduce workers...")
            shuffle_and_send(mapped_data)
            print("[MAP WORKER] Envoi terminé.")

if __name__ == '__main__':
    start()
