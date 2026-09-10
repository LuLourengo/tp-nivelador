import socket

def send_all(connection_socket, data_to_send: bytes):
    bytes_already_sent = 0
    total_bytes_to_send = len(data_to_send)
    
    while bytes_already_sent < total_bytes_to_send:
        
        chunk_to_send = data_to_send[bytes_already_sent:]
        
        bytes_sent_in_this_call = connection_socket.send(chunk_to_send)
        
        if bytes_sent_in_this_call is None:
            raise ConnectionResetError("Socket disconnected")
            
        bytes_already_sent += bytes_sent_in_this_call

def recv_all(connection_socket, amount_to_read: int) -> bytes:
    data_buffer = bytearray()
    
    while len(data_buffer) < amount_to_read:
        current_buffer_length = len(data_buffer)
        bytes_remaining = amount_to_read - current_buffer_length
        
        incoming_packet = connection_socket.recv(bytes_remaining)
        if not incoming_packet:
            break
            
        data_buffer.extend(incoming_packet)

    final_buffer_length = len(data_buffer)
    if final_buffer_length < amount_to_read:
        if final_buffer_length == 0:
            return None
        else:
            return bytes(data_buffer)

    return bytes(data_buffer)
