import asyncio
from collections import deque
from concurrent.futures import Executor
from dataclasses import dataclass, field
import queue
import random
import sys
import threading
import time
from typing import Callable, Iterable, Literal, Self
import weakref
import grpc
from concurrent import futures

from grpc import logging

from grpc.aio import ServicerContext

import mutual_exclusion.proto_file_pb2 as pb2
import mutual_exclusion.proto_file_pb2_grpc as pb2_grpc

import logging



# https://en.wikipedia.org/wiki/Causal_consistency

FAIL_TIME_INTERVAL = [10, 30]
CS_TIME_INTERVAL = [2, 4]

NODE_FAILURE_PROBABILITY = 0.9
MESSAGE_LOSS_PROBABILITY = 0.05


# https://gitlab.com/heckad/asyncio_rlock
class RLock(asyncio.Lock):
    """
    https://gitlab.com/heckad/asyncio_rlock \n
    A reentrant lock for Python coroutines.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._task = None
        self._depth = 0

    async def acquire(self):
        if self._task is None or self._task != asyncio.current_task():
            await super().acquire()
            self._task = asyncio.current_task()
            assert self._depth == 0
        self._depth += 1

    def release(self):
        if self._depth > 0:
            self._depth -= 1
        if self._depth == 0:
            super().release()
            self._task = None


def lost_messaseg():
    return True
    return random.uniform(0, 1) > MESSAGE_LOSS_PROBABILITY


@dataclass
class CriticalSection:
    state: Literal["free", "occupied", "requested", "failed"] = "free"
    time: int = 0
    rivals: set[int] = field(default_factory=set[int])
    grant_count: int = 0
    # request_queue: deque[pb2.EnterRequest] = field(default_factory=deque)



class NodeInterface:
    def __init__(self, node_id: int, port: int, ip_address="localhost"):
        self.node_id, self.port, self.ip_address = node_id, port, ip_address
        self.channel = grpc.aio.insecure_channel(f"{ip_address}:{port}")
        self.stub = pb2_grpc.NodeStub(self.channel)



count_num = 0



class Node(pb2_grpc.NodeServicer):
    def __init__(self, node_id: int, recv_port: int, recv_ip_address="[::]"):
        self.node_id = node_id
        self.recv_ip_address = recv_ip_address
        self.recv_port = recv_port
        # self.server = grpc.aio.server(futures.ProcessPoolExecutor(max_workers=50))
        self.server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=50))
        
        
        self.server.add_insecure_port(f"{recv_ip_address}:{recv_port}")
        pb2_grpc.add_NodeServicer_to_server(self, self.server)

        self.interfaces: set[NodeInterface] = set()
        self.interfaces_dict: dict[int, NodeInterface] = dict()

        self.C = CriticalSection()
        self.sem = asyncio.Semaphore(1)
        # self.lock = asyncio.Lock()
        self.lock = RLock()
        # self.enter_requests = queue.Queue[pb2.EnterRequest](3)
        # self.grant_requests = asyncio.Queue[pb2.GrantRequest](2)
        # self.request_queue: deque[pb2.EnterRequest] = deque(maxlen=3)
        self.grant_requests: deque[pb2.GrantRequest] = deque(maxlen=3)



    async def run(self):
        try:
            await self.server.start()
            print(
                f"Node {self.node_id} server started on {self.recv_ip_address}:{self.recv_port}"
            )
            await self.server.wait_for_termination()
        finally:
            await self.stop()



    def add_NodeInterfaces(self, *node_interfaces: NodeInterface):
        for node_interface in node_interfaces:
            self.interfaces_dict[node_interface.node_id] = node_interface
        
        self.interfaces.update(node_interfaces)



    async def stop(self):
        await asyncio.gather(
            *[i.channel.close() for i in self.interfaces],
            # return_exceptions=True
        )
        await self.server.stop(None)





    async def simulate_failure(self):
        while True:
            await asyncio.sleep(random.uniform(*FAIL_TIME_INTERVAL))
            # fail-stop-return
            if random.uniform(0, 1) > NODE_FAILURE_PROBABILITY:  # 0.9
                # temp_state = self.C.state
                # self.C.state = "failed"
                # self.C.rivals.clear()
                # self.C.grant_count = 0

                print(f"Node {self.node_id} failed")

                # fail-stop
                if random.uniform(0, 1) > 0.8:
                    if True:
                        await self.stop()
                        return

                # await asyncio.sleep(random.uniform(*FAIL_TIME_INTERVAL))
                # self.C.state = temp_state



    async def request_critical_section(self):
        while True:
            await asyncio.sleep(random.uniform(*CS_TIME_INTERVAL))

            logging.info(f"PRE request_critical_section: {self.node_id = }, {self.C = }")
            
            if self.C.state == "failed" or self.C.state == "occupied":
                continue
            
            
            self.C.state = "requested"
            # self.C.time = time.process_time_ns()
            global count_num
            count_num += 1
            self.C.time = count_num

            # print(f"request_critical_section({self.node_id}, {self.C})")
            logging.info(f"POS request_critical_section: {self.node_id = }, {self.C = }")


            # await self.enter(self.C.time, *[cur.node_id for cur in self.interfaces if cur.node_id != self.node_id])
            
            # if self.node_id == 1:
            #     await asyncio.gather(*[
            #         self.send_Enter(self.node_id, 2, self.C.time),
            #         self.send_Enter(self.node_id, 3, self.C.time)
            #     ])


            # await asyncio.gather(
            #     *[
            #         self.send_Enter(self.node_id, cur.node_id, self.C.time)
            #         for cur in self.interfaces
            #         if cur.node_id != self.node_id
            #     ],
            #     # return_exceptions=True
            # )
            
            for cur in self.interfaces:
                if cur.node_id != self.node_id:
                    await self.send_Enter(self.node_id, cur.node_id, self.C.time)
                    # aa: pb2.EnterResponse = await self.Enter() # type: ignore
                    # aa
                    # if self.C.state == "free":
                    #     await self.send_Grant(self.node_id, cur.node_id)
                    # elif self.C.state == "occupied":
                    #     self.C.rivals.add(cur.node_id)
                    # elif self.C.state == "requested" and self.C.time < cur.time:
                    #     self.C.rivals.add(cur.node_id)
                    # elif self.C.state == "requested" and cur.time <= self.C.time:
                    #     await self.send_Grant(self.node_id, cur.node_id)
            
    async def Enter(self, request: pb2.EnterRequest, context: ServicerContext):
        logging.info(f"Enter: {self.node_id = }, {request.node_id = }, {self.C = }")

        # if self.C.state == "free" and not self.C.rivals:
        # # if self.C.state == "free" and not self.request_queue:
        #     await self.send_Grant(self.node_id, request.node_id)
        # else:
        #     self.request_queue.append(request)
        #     self.C.rivals.add(request.node_id)
        #     # self.enter_requests.put(request)
        
        # if self.request_queue:
        # if self.C.state == "free":
        #     await self.send_Grant(self.node_id, request.node_id)
        # elif self.C.state == "occupied":
        #     self.C.rivals.add(request.node_id)
        #     self.request_queue.append(request)
        #     # await self.enter_requests.put(request)
        # elif self.C.state == "requested" and self.C.time <= request.time:
        #     self.C.rivals.add(request.node_id)
        #     self.request_queue.append(request)
        #     # await self.enter_requests.put(request)
        # elif self.C.state == "requested" and request.time < self.C.time:
        #     await self.send_Grant(self.node_id, request.node_id)
        # else:
        #         self.C.rivals.add(request.node_id)
        #         self.request_queue.append(request)
        #         # await self.enter_requests.put(request)
        
        # async with self.lock:
        if self.C.state == "free":
            await self.send_Grant(self.node_id, request.node_id)
        elif self.C.state == "occupied":
            self.C.rivals.add(request.node_id)
        elif self.C.state == "requested" and self.C.time < request.time:
            self.C.rivals.add(request.node_id)
        elif self.C.state == "requested" and request.time <= self.C.time:
            await self.send_Grant(self.node_id, request.node_id)
        
        
    # logging.info(f"Enter: {len(self.request_queue) = }")
    
        return pb2.EnterResponse()



    async def Grant(self, request: pb2.GrantRequest, context: ServicerContext):
        
        
        async with self.lock:
        # if True:
            # self.grant_requests.append(request)
            
            logging.info(f"Grant: {self.node_id = }, {request.node_id = }, {self.C = }")
            
            self.C.grant_count += 1
            if self.C.grant_count >= len(self.interfaces) - 1:
                
                logging.info(f"???Enter-critical-section: {self.node_id = }, {request.node_id = }, {self.C = }")
                
                self.C.state = "occupied"
                await asyncio.sleep(random.uniform(2, 5))
                self.C.state = "free"
                
                logging.info(f"!!!Leaving-critical-section: {self.node_id = }, {request.node_id = }, {self.C = }")

                
                
                # await self.process_request_queue()
                
                # grant_requests: list[pb2.GrantRequest] = []
                # while not self.grant_requests.empty():
                #     grant_requests.append(await self.grant_requests.get())
                
                # grant_requests = await asyncio.gather(*[self.grant_requests.get() for _ in range(self.grant_requests.qsize())])
                
                # for grant_request in grant_requests:
                #     await self.send_Grant(self.node_id, grant_request.node_id)
                
                
                self.C.grant_count = 0
                
                # while rival := self.C.rivals.pop():
                for rival in self.C.rivals:
                    await self.send_Grant(self.node_id, rival)
                
                
                self.C.rivals.clear()
                # self.request_queue.clear()
                
                
                logging.info(f"###process_request_queue: {self.node_id = }, {request.node_id = }, {self.C = }")
                
                # await asyncio.wait_for(self.process_request_queue(), 2)


            return pb2.GrantResponse()



    # async def process_request_queue(self):
    #     while self.request_queue:
    #     # while self.enter_requests:
    #         request = self.request_queue.pop()
    #         # request = self.enter_requests.get()
    #         # self.C.rivals.discard(request.node_id)
    #         await self.send_Grant(self.node_id, request.node_id)




    # async def __Enter(self, request: pb2.EnterRequest, context: ServicerContext):
        
    #     logging.info(f"Enter: {self.C = }, {self.node_id = }, {request = }")
        
    #     # print(
    #     #     f"Node({request.node_id}) enter Node({self.node_id}) at {request.time}",
    #     #     end="",
    #     # )

    #     # await self.enter_requests.put(request)
        
        
    #     if self.C.state == "free":
    #         await self.send_Grant(self.node_id, request.node_id)
    #     elif self.C.state == "occupied":
    #         self.C.rivals.add(request.node_id)
    #     elif self.C.state == "requested" and self.C.time < request.time:
    #         self.C.rivals.add(request.node_id)
    #     elif self.C.state == "requested" and request.time < self.C.time:
    #         await self.send_Grant(self.node_id, request.node_id)
        
    #     # await context.abort(grpc.StatusCode.ABORTED)
        
    #     return pb2.EnterResponse()



    # async def __Grant(self, request: pb2.GrantRequest, context: ServicerContext):
    #     # print(context)
    #     logging.info(f"Grant: {self.C = }, {self.node_id = }, {request = }", stacklevel=2)

    #     async with self.lock:
    #         # print("Grant: ", request, self.C)
    #         logging.info(f"Critical Section: {self.C = }, {self.node_id = }, {request = }", stacklevel=2)
            
    #         self.C.grant_count += 1
    #         if self.C.grant_count >= len(self.interfaces) - 1:
                
    #             # print(f"!!!Node {self.node_id} entered critical section")

    #             self.C.state = "occupied"

    #             await asyncio.sleep(random.uniform(2, 5))

    #             self.C.state = "free"

    #             # for rival in self.C.rivals:
    #             #     await self.send_Grant(self.node_id, rival)

    #             await asyncio.gather(
    #                 *[self.send_Grant(self.node_id, rival) for rival in self.C.rivals],
    #                 return_exceptions=True
    #             )

    #             self.C.rivals.clear()
    #             self.C.grant_count = 0

    #         # print("Grant:\n", request, end="")
    #         # print("CS(", self.node_id, ") = ", self.C)
    #         logging.info(f"Critical Section: {self.C = }, {self.node_id = }, {request = }")
            
    #         # await context.abort(grpc.StatusCode.ABORTED)
            
    #         return pb2.GrantResponse()



    # async def enter(self, time: int, *node_ids: int):
    #     if lost_messaseg():
    #         for i in self.interfaces:
    #             if i.node_id in node_ids:
    #                 try:
    #                     await i.stub.Enter(pb2.EnterRequest(node_id=self.node_id, time=time))
    #                 except grpc.aio.AioRpcError as err:
    #                     print("send_Enter: ", self.node_id, err)
    #                     self.interfaces.remove(i)
    #                 finally:
    #                     return
    #     print(f"lost_messaseg(send_Enter) = ({node_id=}, {dest_node=}, {time=})")


    async def send_Enter(self, node_id: int, dest_node: int, time: int):
        if lost_messaseg():
            for i in self.interfaces:
                if i.node_id == dest_node:
                    try:
                        await i.stub.Enter(pb2.EnterRequest(node_id=node_id, time=time))
                    except grpc.aio.AioRpcError as err:
                        print("send_Enter: ", self.node_id, err)
                        self.interfaces.remove(i)
                    finally:
                        return
        print(f"lost_messaseg(send_Enter) = ({node_id=}, {dest_node=}, {time=})") 


    async def send_Grant(self, node_id: int, dest_node: int):
        # if random.uniform(0, 1) > 0.1:
        if lost_messaseg():
            for i in self.interfaces:
                if i.node_id == dest_node:
                    try:
                        await i.stub.Grant(pb2.GrantRequest(node_id=node_id))
                    except grpc.aio.AioRpcError as err:
                        print("send_Grant: ", self.node_id, err)
                        self.interfaces.remove(i)
                    finally:
                        return
        print(f"lost_messaseg(send_Grant) = ({node_id=}, {dest_node=})")


async def main():
    async with asyncio.TaskGroup() as tg:
        nodes: list[Node] = []
        node_interfaces: list[tuple[int, int, str]] = []
        ids = list(range(1, 3 + 1))



        for id in ids:
            port = 50050 + id
            node = Node(id, port, "[::]")
            nodes.append(node)
            node_interfaces.append((id, port, "localhost"))

        # node_interfaces.append((42, 50070, "localhost"))

        for node in nodes:
            for interface_tuple in node_interfaces:
                # node.interfaces.add()
                node.add_NodeInterfaces(NodeInterface(*interface_tuple))

        for node in nodes:
            tg.create_task(node.run())

        for node in nodes:
            tg.create_task(node.request_critical_section())

        # for node in nodes:
        #     tg.create_task(node.simulate_failure())


if __name__ == "__main__":
    try:
        logging.basicConfig(level=logging.INFO, format="%(relativeCreated)d - %(message)s")
        asyncio.run(main(), debug=False)

    except KeyboardInterrupt:
        pass
    finally:
        print("\n[[[Done]]]\n")
