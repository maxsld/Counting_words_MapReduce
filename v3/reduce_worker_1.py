import socket
import pickle
from collections import defaultdict

# Ce port doit être unique par worker
MY_PORT = 9001

COORDINATOR_ADDRESS = ('localhost', 7500)

EXPECTED_MAPS = 2

accumulator = defaultdict(int)

def reduce_task(pairs):
    for word, count in pairs.items():
        accumulator[word] += count

def start():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('0.0.0.0', MY_PORT))  # écoute sur toutes les interfaces
        s.listen()
        print(f"[REDUCE WORKER] En écoute sur le port {MY_PORT}...")

        received = 0
        while received < EXPECTED_MAPS:
            conn, _ = s.accept()
            with conn:
                print(f"[REDUCE WORKER] Reçu un fragment de map worker ({received + 1}/{EXPECTED_MAPS})")
                data = b""
                while True:
                    packet = conn.recv(4096)
                    if not packet:
                        break
                    data += packet
                pairs = pickle.loads(data)
                reduce_task(pairs)
                received += 1

        # Une fois tout reçu → on envoie le résultat final au coordinateur
        print("[REDUCE WORKER] Envoi du résultat final au coordinateur...")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect(COORDINATOR_ADDRESS)
            client.sendall(pickle.dumps(dict(accumulator)))
        print("[REDUCE WORKER] Terminé.")

if __name__ == '__main__':
    start()
