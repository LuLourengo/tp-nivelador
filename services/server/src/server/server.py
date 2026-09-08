import socket
import struct
import logger
import safe_socket
from lottery.lottery import Lottery
from lottery.bet import Bet

MSG_BET = 1
MSG_END = 2
MSG_WINNERS = 3

STORAGE_PATH = "bets.csv"


class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port

        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.server_host, self.server_port))
        # Backlog suficiente para encolar los intentos de conexión secuenciales
        self._server_socket.listen(128)

        self.storage_path = STORAGE_PATH
        self.lottery = Lottery(self.storage_path)

    def run(self):
        logger.info("server-start", logger.LogResult.success)
        try:
            while True:
                logger.info("accept-connection", logger.LogResult.in_progress)
                client_socket, _ = self._server_socket.accept()
                logger.info("accept-connection", logger.LogResult.success)
                self._handle_client(client_socket)
        except Exception as e:
            logger.error("server-run", logger.LogResult.fail, "err", str(e))
        finally:
            self._server_socket.close()

    def _recv_msg(self, sock):
        header = safe_socket.recv_all(sock, 5)
        if not header or len(header) < 5:
            return None, None

        payload_len, msg_type = struct.unpack("!IB", header)
        if payload_len == 0:
            return msg_type, b""

        payload = safe_socket.recv_all(sock, payload_len)
        if payload is None or len(payload) < payload_len:
            return None, None

        return msg_type, payload

    def _send_msg(self, sock, msg_type, payload=b""):
        header = struct.pack("!IB", len(payload), msg_type)
        safe_socket.send_all(sock, header + payload)

    def _handle_client(self, client_socket):
        action = "handle-client"
        logger.info(action, logger.LogResult.in_progress)
        agency_id = None

        try:
            while True:
                msg_type, payload = self._recv_msg(client_socket)
                if msg_type is None:
                    break

                if msg_type == MSG_BET:
                    line = payload.decode("utf-8").strip()
                    if not line:
                        continue
                    parts = line.split(",")
                    if len(parts) >= 6:
                        agency_id = int(parts[0])
                        bet = Bet(
                            agency_id=agency_id,
                            first_name=parts[1],
                            last_name=parts[2],
                            document=int(parts[3]),
                            birthdate=parts[4],
                            number=int(parts[5]),
                        )
                        self.lottery.store_bets([bet])

                elif msg_type == MSG_END:
                    winners_lines = []
                    if agency_id is not None:
                        for b in self.lottery.load_bets():
                            if b.agency_id == agency_id and self.lottery.has_won(b):
                                winners_lines.append(
                                    f"{b.first_name},{b.last_name},{b.document},{b.birthdate},{b.number}"
                                )

                    winners_count = len(winners_lines)
                    body = ("\n".join(winners_lines) + "\n").encode("utf-8") if winners_lines else b""
                    self._send_msg(client_socket, MSG_WINNERS, body)

                    logger.info(
                        action,
                        logger.LogResult.success,
                        "agency-id",
                        agency_id,
                        "winners",
                        winners_count,
                    )
                    break

        except Exception as e:
            logger.error(action, logger.LogResult.fail, "err", str(e))
        finally:
            client_socket.close()