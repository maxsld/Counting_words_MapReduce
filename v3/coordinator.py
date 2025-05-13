import socket
import threading
import pickle

# IP et ports des map et reduce workers
MAP_WORKERS = [('localhost', 8001), ('localhost', 8002)]
REDUCE_WORKERS = [('localhost', 9001), ('localhost', 9002)]

TEXT_FILES = ['../texts/file1.txt', '../texts/file2.txt']

def send_data(address, data):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(address)
        s.sendall(pickle.dumps(data))

def send_files_to_maps():
    print("[COORDINATOR] Envoi des fichiers aux map workers...")
    threads = []

    for i, (ip, port) in enumerate(MAP_WORKERS):
        with open(TEXT_FILES[i], 'r', encoding='utf-8') as f:
            content = f.read()

        t = threading.Thread(target=send_data, args=((ip, port), content))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    print("[COORDINATOR] Tous les fichiers ont été envoyés.")

def collect_reduces():
    final_result = {}
    print("[COORDINATOR] En attente des résultats des reduce workers...")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('0.0.0.0', 7500))
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
                print("[COORDINATOR] Résultat partiel reçu.")

    return final_result

if __name__ == '__main__':
    send_files_to_maps()
    final = collect_reduces()

    print("\n=== Résultat final ===")
    for word, count in sorted(final.items()):
        print(f"{word}: {count}")
