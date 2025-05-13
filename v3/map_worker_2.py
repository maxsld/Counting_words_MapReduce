### map_worker.py
import socket
import pickle
from collections import Counter
import re

MY_PORT = 8002

def map_task(text):
    words = re.findall(r'\w+', text.lower())
    return dict(Counter(words))

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
                text = pickle.loads(data)
                result = map_task(text)

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.connect(('localhost', 7000))
                client.sendall(pickle.dumps(result))

if __name__ == '__main__':
    start()