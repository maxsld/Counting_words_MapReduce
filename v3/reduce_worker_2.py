### reduce_worker.py
import socket
import pickle

MY_PORT = 9002

def reduce_task(pairs):
    from collections import defaultdict
    reduced = defaultdict(int)
    for word, count in pairs.items():
        reduced[word] += count
    return dict(reduced)

def start():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', MY_PORT))
        s.listen()
        while True:
            conn, _ = s.accept()
            with conn:
                data = b""
                while True:
                    packet = conn.recv(4096)
                    if not packet: break
                    data += packet
                pairs = pickle.loads(data)
                result = reduce_task(pairs)

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.connect(('localhost', 7500))
                client.sendall(pickle.dumps(result))

if __name__ == '__main__':
    start()