import threading
import time
import random
from queue import Queue

# Ricart-agrawala algrothim ------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------

class RicartAgrawalaNode:
    def __init__(self, node_id, total_nodes):
        self.node_id = node_id
        self.total_nodes = total_nodes
        self.is_alive = True
        self.in_cs = False
        self.mutex = threading.Lock()
        self.requesting_cs = False
        self.heartbeat_interval = 5
        self.failure_probability = 0.1  # A more closer realistic value
        self.message_loss_probability = 0.05  # This is a more realistic value
        self.request_queue = Queue()

    def send_message(self, recipient, message):
        # Simulate message loss
        if random.random() > self.message_loss_probability:
            recipient.receive_message(self, message)

    def receive_message(self, sender, message):
        with self.mutex:
            if self.is_alive:
                if isinstance(message, HeartbeatMessage):
                    self.handle_heartbeat(sender)
                elif isinstance(message, RequestMessage):
                    self.handle_request(sender)
                elif isinstance(message, ReleaseMessage):
                    self.handle_release(sender)
                elif isinstance(message, RejectMessage):
                    self.handle_reject(sender)

    def handle_heartbeat(self, sender):
        self.is_alive = True

    def handle_request(self, sender):
        with self.mutex:
            if not self.in_cs and not self.requesting_cs:
                # Grant access to the critical section
                self.in_cs = True
                print(f"Node {self.node_id} granted access to the critical section.")

                # Simulate using the resource for a few seconds
                time.sleep(random.uniform(2, 5))

                # Release the critical section
                self.in_cs = False
                print(f"Node {self.node_id} released the critical section.")

                # Send release messages to nodes in the request queue
                while not self.request_queue.empty():
                    next_requester = self.request_queue.get()
                    release_message = ReleaseMessage(self.node_id)
                    self.send_message(next_requester, release_message)
            else:
                # Enqueue the request
                self.request_queue.put(sender)
                # Send a reject message
                reject_message = RejectMessage(self.node_id)
                self.send_message(sender, reject_message)

    def handle_release(self, sender):
        with self.mutex:
            # Release the critical section
            self.in_cs = False
            print(f"Node {self.node_id} received release message from Node {sender.node_id}.")

    def handle_reject(self, sender):
        with self.mutex:
            # Remove sender from the request queue
            while not self.request_queue.empty():
                if self.request_queue.get() == sender:
                    break

    def start(self):
        # Node main loop
        while self.is_alive:
            # Simulate node failure
            if random.random() < self.failure_probability:
                self.is_alive = False
                print(f"Node {self.node_id} has failed. Going silent for a while.")
                time.sleep(random.uniform(60, 300))
                self.is_alive = True
                print(f"Node {self.node_id} is back online.")

            # Simulate sending heartbeats
            heartbeat_message = HeartbeatMessage(self.node_id)
            for i in range(self.total_nodes):
                if i != self.node_id:
                    self.send_message(nodes[i], heartbeat_message)

            # Simulate requesting access to the critical section
            if not self.requesting_cs and not self.in_cs:
                self.requesting_cs = True
                request_message = RequestMessage(self.node_id)
                for i in range(self.total_nodes):
                    if i != self.node_id:
                        self.send_message(nodes[i], request_message)

            # Simulate other node activities
            time.sleep(random.uniform(1, 10))

        print(f"Node {self.node_id} is shutting down.")

# Message classes (unchanged)
class HeartbeatMessage:
    def __init__(self, sender_id):
        self.sender_id = sender_id

class RequestMessage:
    def __init__(self, sender_id):
        self.sender_id = sender_id

class ReleaseMessage:
    def __init__(self, sender_id):
        self.sender_id = sender_id

class RejectMessage:
    def __init__(self, sender_id):
        self.sender_id = sender_id

# Create nodes
total_nodes = 5
nodes = [RicartAgrawalaNode(node_id=i, total_nodes=total_nodes) for i in range(total_nodes)]

# Start nodes
threads = [threading.Thread(target=node.start) for node in nodes]
for thread in threads:
    thread.start()

# Wait for threads to finish
for thread in threads:
    thread.join()

print("All nodes have completed.")
