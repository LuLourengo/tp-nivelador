import socket


def send_all(sock, data):
    total_sent = 0
    total_to_send = len(data)

    while total_sent < total_to_send:
        bytes_sent = sock.send(data[total_sent:])
        if bytes_sent == 0:
            raise BrokenPipeError("0 bytes sent: socket disconnected")
        total_sent += bytes_sent


def recv_all(sock, size):
    chunks = []
    bytes_recd = 0

    while bytes_recd < size:
        chunk = sock.recv(size - bytes_recd)
        if not chunk:
            return None
        chunks.append(chunk)
        bytes_recd += len(chunk)

    return b"".join(chunks)