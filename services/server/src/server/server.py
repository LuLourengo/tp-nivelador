import os
import signal
import socket
import struct
import threading
import logger
import safe_socket
from lottery.lottery import Lottery
from lottery.bet import Bet

MSG_BET = 1
MSG_END = 2
MSG_WINNERS = 3
MSG_ACK = 4

STORAGE_PATH = "bets.csv"


class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port

        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.server_host, self.server_port))
        self._server_socket.listen(128)

        self.storage_path = STORAGE_PATH
        self.lottery = Lottery(self.storage_path)

        quorum_env = os.getenv("AGENCY_QUORUM_MIN", "5")
        try:
            self.quorum_min = int(quorum_env)
        except ValueError:
            self.quorum_min = 5

        self.storage_lock = threading.Lock()
        self.quorum_cond = threading.Condition()
        self.finished_agencies = set()

        self.is_running = True
        self.client_threads = []
        self.active_client_sockets = set()
        self.sockets_lock = threading.Lock()

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logger.info("signal-received", logger.LogResult.success, "signal", signum)
        self.shutdown()

    def shutdown(self):
        if not self.is_running:
            return
        self.is_running = False

        try:
            self._server_socket.close()
        except Exception:
            pass

        with self.quorum_cond:
            self.quorum_cond.notify_all()

        with self.sockets_lock:
            for sock in list(self.active_client_sockets):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    sock.close()
                except Exception:
                    pass
            self.active_client_sockets.clear()

    def run(self):
        logger.info("server-start", logger.LogResult.success)
        try:
            while self.is_running:
                try:
                    client_socket, _ = self._server_socket.accept()
                except OSError:
                    break

                if not self.is_running:
                    client_socket.close()
                    break

                with self.sockets_lock:
                    self.active_client_sockets.add(client_socket)

                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket,),
                )
                self.client_threads.append(client_thread)
                client_thread.start()

        except Exception as e:
            if self.is_running:
                logger.error("server-run", logger.LogResult.fail, "err", str(e))
        finally:
            self.shutdown()

            for thread in self.client_threads:
                thread.join(timeout=2.0)
            logger.info("server-shutdown", logger.LogResult.success)

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
            while self.is_running:
                msg_type, payload = self._recv_msg(client_socket)
                if msg_type is None:
                    break

                if msg_type == MSG_BET:
                    lines = payload.decode("utf-8").strip().split("\n")
                    bets_batch = []
                    for line in lines:
                        line = line.strip()
                        if not line:
                            continue
                        parts = line.split(",")
                        if len(parts) >= 6:
                            agency_id = int(parts[0])
                            bets_batch.append(
                                Bet(
                                    agency_id=agency_id,
                                    first_name=parts[1],
                                    last_name=parts[2],
                                    document=int(parts[3]),
                                    birthdate=parts[4],
                                    number=int(parts[5]),
                                )
                            )

                    if bets_batch:
                        with self.storage_lock:
                            self.lottery.store_bets(bets_batch)

                    self._send_msg(client_socket, MSG_ACK, b"")

                elif msg_type == MSG_END:
                    with self.quorum_cond:
                        if agency_id is not None:
                            self.finished_agencies.add(agency_id)

                        logger.info(
                            "agency-finished",
                            logger.LogResult.success,
                            "agency-id",
                            agency_id,
                            "ready-agencies",
                            len(self.finished_agencies),
                            "quorum-min",
                            self.quorum_min,
                        )

                        if len(self.finished_agencies) >= self.quorum_min:
                            self.quorum_cond.notify_all()
                        else:
                            while self.is_running and len(self.finished_agencies) < self.quorum_min:
                                self.quorum_cond.wait()

                    if not self.is_running and len(self.finished_agencies) < self.quorum_min:
                        break

                    winners_lines = []
                    if agency_id is not None:
                        with self.storage_lock:
                            all_bets = self.lottery.load_bets()

                        for b in all_bets:
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
            if self.is_running:
                logger.error(action, logger.LogResult.fail, "err", str(e))
        finally:
            with self.sockets_lock:
                self.active_client_sockets.discard(client_socket)
            try:
                client_socket.close()
            except Exception:
                pass