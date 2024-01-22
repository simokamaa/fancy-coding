import logging
import socket
import threading
import time
import random
from dataclasses import dataclass, field
from typing import Literal


@dataclass(unsafe_hash=True, frozen=True)
class NodeData:
    node_id: int
    address: str
    port: int


@dataclass
class Node:
    node_id: int
    address: str
    port: int
    nodes: dict[int, NodeData]

    state: Literal["free", "occupied", "requested"] = "free"
    time: int = 0
    rivals: set[int] = field(default_factory=set)
    grant_count: int = 0
    # mutex: threading.Lock = field(default_factory=threading.Lock)
    mutex = threading.Lock()

    @staticmethod
    def send_message(destination: NodeData, message: str) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((destination.address, destination.port))
            s.sendall(message.encode())

    def handle_connection(self, client_socket: socket.socket) -> None:
        data: str = client_socket.recv(1024).decode()
        if data:
            self.handle_message(data)
        client_socket.close()

    def handle_message(self, message: str) -> None:
        parts = message.split()
        action = parts[0]

        if action == "enter":
            self.handle_enter(parts)
        elif action == "grant":
            self.handle_grant(parts)

    def handle_enter(self, parts: list) -> None:
        requesting_node_id = int(parts[1])
        requesting_node_time = int(parts[2])

        logging.info(
            f"Enter: {self.node_id = }, {requesting_node_id = }, "
            f"{requesting_node_time = }, {(self.state, self.time, self.rivals) = }"
        )

        if self.state == "free" or (
            self.state == "requested"
            and (self.time, self.node_id) < (requesting_node_time, requesting_node_id)
        ):
            self.grant_count += 1
            self.send_message(self.nodes[requesting_node_id], f"grant {self.node_id}")
        else:
            self.rivals.add(requesting_node_id)

    def handle_grant(self, parts: list) -> None:
        granting_node_id = int(parts[1])
        logging.info(
            f"Grant: {self.node_id = }, {granting_node_id = }, "
            f"{(self.state, self.time, self.rivals) = }"
        )

        self.grant_count += 1

        if self.grant_count == len(self.nodes) - 1:
            self.enter_critical_section()

    def enter_critical_section(self) -> None:
        with self.mutex:
            print(f"Node {self.node_id} entering critical section")
            time.sleep(random.uniform(1, 5))
            self.state = "free"
            self.rivals.clear()
            self.grant_count = 0
            print(f"Node {self.node_id} leaving critical section")

    def request_critical_section(self) -> None:
        if self.state == "free":
            self.state = "requested"
            self.time = time.monotonic_ns()
            # self.time += 1
            # self.time = time.perf_counter_ns()
            for other_node in self.nodes.values():
                if other_node.node_id != self.node_id:
                    self.send_message(other_node, f"enter {self.node_id} {self.time}")

    def listen_for_connections(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.address, self.port))
            server_socket.listen()

            while True:
                client_socket, _ = server_socket.accept()
                threading.Thread(
                    target=self.handle_connection, args=(client_socket,)
                ).start()


def main():
    nodes_dict: dict[int, NodeData] = {
        1: NodeData(1, "localhost", 5001),
        2: NodeData(2, "localhost", 5002),
        3: NodeData(3, "localhost", 5003),
        4: NodeData(4, "localhost", 5004),
        5: NodeData(5, "localhost", 5005),
    }

    nodes = {
        id: Node(node_data.node_id, node_data.address, node_data.port, nodes_dict)
        for id, node_data in nodes_dict.items()
    }

    for node in nodes.values():
        threading.Thread(target=node.listen_for_connections, daemon=True).start()

    while True:
        requesting_node = random.choice(list(nodes.values()))
        requesting_node.request_critical_section()
        time.sleep(random.randint(1, 5))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(relativeCreated)d - %(message)s")
    try:
        main()
    except KeyboardInterrupt:
        pass
    finally:
        print("[[[Quit]]]")
