import collections
from functools import wraps
import json
import logging
import re
from select import select
import socket
import struct
import time
import traceback

from django.conf import settings


tasks = dict()


class GearmanError(Exception):
    pass


class ClientType(type):

    def __new__(cls, name, bases, namespace):
        packet_type_doc = """
        #   Name                Args  Magic  Type
        1   CAN_DO              1     REQ    Worker
        2   CANT_DO             1     REQ    Worker
        3   RESET_ABILITIES     0     REQ    Worker
        4   PRE_SLEEP           0     REQ    Worker
        5   (unused)            -     -      -
        6   NOOP                0     RES    Worker
        7   SUBMIT_JOB          3     REQ    Client
        8   JOB_CREATED         1     RES    Client
        9   GRAB_JOB            0     REQ    Worker
        10  NO_JOB              0     RES    Worker
        11  JOB_ASSIGN          3     RES    Worker
        12  WORK_STATUS         3     REQ    Worker
                                      RES    Client
        13  WORK_COMPLETE       2     REQ    Worker
                                      RES    Client
        14  WORK_FAIL           1     REQ    Worker
                                      RES    Client
        15  GET_STATUS          1     REQ    Client
        16  ECHO_REQ            1     REQ    Client/Worker
        17  ECHO_RES            1     RES    Client/Worker
        18  SUBMIT_JOB_BG       3     REQ    Client
        19  ERROR               2     RES    Client/Worker
        20  STATUS_RES          5     RES    Client
        21  SUBMIT_JOB_HIGH     3     REQ    Client
        22  SET_CLIENT_ID       1     REQ    Worker
        23  CAN_DO_TIMEOUT      2     REQ    Worker
        24  ALL_YOURS           0     REQ    Worker
        25  WORK_EXCEPTION      2     REQ    Worker
                                      RES    Client
        26  OPTION_REQ          1     REQ    Client/Worker
        27  OPTION_RES          1     RES    Client/Worker
        28  WORK_DATA           2     REQ    Worker
                                      RES    Client
        29  WORK_WARNING        2     REQ    Worker
                                      RES    Client
        30  GRAB_JOB_UNIQ       0     REQ    Worker
        31  JOB_ASSIGN_UNIQ     4     RES    Worker
        32  SUBMIT_JOB_HIGH_BG  3     REQ    Client
        33  SUBMIT_JOB_LOW      3     REQ    Client
        34  SUBMIT_JOB_LOW_BG   3     REQ    Client
        35  SUBMIT_JOB_SCHED    8     REQ    Client
        36  SUBMIT_JOB_EPOCH    4     REQ    Client
        """
        packet_type_for_name = dict()
        packet_name_for_type = dict()
        num_args_for_packet_type = dict()

        for m in re.finditer(r'^ \s+ (?P<type> \d+) \s+ (?P<name> [A-Z_]+) \s+ (?P<num_args> \d+)', packet_type_doc, re.MULTILINE | re.DOTALL | re.VERBOSE):
            type_code = int(m.group('type'))
            name = m.group('name')
            num_args = int(m.group('num_args'))

            packet_name_for_type[type_code] = name
            packet_type_for_name[name] = type_code
            num_args_for_packet_type[type_code] = num_args

        namespace['packet_type_for_name'] = packet_type_for_name
        namespace['packet_name_for_type'] = packet_name_for_type
        namespace['num_args_for_packet_type'] = num_args_for_packet_type
        namespace.update(packet_type_for_name)

        return type.__new__(cls, name, bases, namespace)


class Client(object, metaclass=ClientType):

    def __init__(self, servers):
        self.servers = servers

    def write_request(self, sock, packet_type, *args):
        data = b'\x00'.join(args)
        header = struct.pack('!B3sII', 0, b'REQ', packet_type, len(data))
        sock.sendall(header)
        sock.sendall(data)

    def read_response(self, sock):
        header = b''
        while len(header) < 12:
            header += sock.recv(12 - len(header))
        response_head, response_type, len_args = struct.unpack('!4sII', header)

        if response_head != b'\x00RES':
            raise GearmanError('Received incorrect response header {!r} from server (expected {!r})'.format(response_head, b'\x00RES'))

        if len_args == 0:
            return response_type, ()

        try:
            num_args = self.num_args_for_packet_type[response_type]
        except KeyError:
            raise GearmanError('Received unknown packet type {} from server'.format(response_type))

        data = sock.recv(len_args)
        args = data.split(b'\x00', num_args)
        return response_type, args

    def connect(self):
        # TODO: pool/persist connections
        for server in self.servers:
            host, port = server
            try:
                return socket.create_connection(server)
            except socket.error:
                pass
        raise GearmanError('Could not connect to any servers')

    def request(self, packet_type, *args, expect=None):
        with self.connect() as conn:
            self.write_request(conn, packet_type, *args)

            if expect is None:
                return

            response_type, args = self.read_response(conn)

            if not isinstance(expect, collections.Container):
                expect = (expect,)
            if response_type not in expect:
                raise GearmanError('Received {} response to {} request (expected {})'.format(
                    self.packet_name_for_type[response_type], self.packet_name_for_type[packet_type],
                    ', '.join(self.packet_name_for_type[t] for t in expect)))

            return response_type, args

    def submit_job(self, name, data):
        # TODO: support other job orientations
        response_type, args = self.request(self.SUBMIT_JOB_BG, name, b'', data, expect=self.JOB_CREATED)
        (handle,) = args
        return handle

    def get_status(self, handle):
        response_type, args = self.request(self.GET_STATUS, handle, expect=self.STATUS_RES)
        handle, known, running, numerator, denominator = args
        known = known != b'\x00'
        running = running != b'\x00'
        return known, running, int(numerator), int(denominator)

    def work(self):
        log = logging.getLogger('anagrem.worker')
        while True:
            server_sockets = set()
            for server in self.servers:
                host, port = server
                try:
                    sock = socket.create_connection(server)
                except socket.error:
                    pass
                else:
                    sock.setblocking(0)
                    server_sockets.add(sock)

            log.debug("Connected to %d servers", len(server_sockets))
            if not server_sockets:
                raise GearmanError("Could not connect to any servers")

            for sock in server_sockets:
                for funcname in tasks.keys():
                    self.write_request(sock, self.CAN_DO, funcname.encode('ascii'))
                self.write_request(sock, self.PRE_SLEEP)

            while server_sockets:
                try:
                    read_ready, _, errored = select(server_sockets, (), server_sockets)
                except KeyboardInterrupt:
                    for sock in server_sockets:
                        sock.close()
                    return

                for sock in errored:
                    log.debug("Server socket reported error, closing bad connection")
                    sock.close()
                    server_sockets.remove(sock)

                for sock in read_ready:
                    packet_type, args = self.read_response(sock)

                    if packet_type == self.NOOP:
                        log.debug("Server woke me up, is there a job?")
                        self.write_request(sock, self.GRAB_JOB)
                        # We can check the other sockets too while waiting for the response.
                        continue

                    if packet_type == self.NO_JOB:
                        log.debug("Hmm, no job, ask the server to wait for one")
                        self.write_request(sock, self.PRE_SLEEP)
                        continue

                    if packet_type == self.JOB_ASSIGN:
                        handle, funcname, data = args
                        log.debug("Yay, got job %s!", handle)

                        try:
                            funcname = funcname.decode('ascii')
                            func = tasks[funcname]
                            ret_data = func.work(data)
                        except Exception as exc:
                            log.warning("Job %s ended in a %s exception", handle, type(exc).__name__)
                            if log.isEnabledFor(logging.DEBUG):
                                log.debug("%s", traceback.format_exc())
                            self.write_request(sock, self.WORK_EXCEPTION, handle, str(exc).encode('utf8'))
                            self.write_request(sock, self.WORK_FAIL, handle)
                        else:
                            log.debug("Job %s completed successfully", handle)
                            self.write_request(sock, self.WORK_COMPLETE, handle, ret_data)

                        # Ask for another to get us back on the NOOP/NO_JOB track.
                        self.write_request(sock, self.GRAB_JOB)
                        continue

                    # Not sure what the response was then. Either it was an error or we should treat it as one anyway.
                    if packet_type == self.ERROR:
                        code, text = args
                        log.warning("Error %s from server, closing bad connection: %s", code, text)
                    else:
                        log.warning("Unexpected response packet of type %d from server, closing bad connection", packet_type)
                    sock.close()
                    server_sockets.remove(sock)

            log.debug("Ran out of server sockets, let's start over from the top")


def task(fn):
    taskname = '.'.join((fn.__module__, fn.__name__))
    tasks[taskname] = fn

    def post(**kwargs):
        taskdata = json.dumps(kwargs).encode('utf8')
        cl = Client(settings.GEARMAN_SERVERS)
        return cl.submit_job(taskname.encode('ascii'), taskdata)
    fn.post = post

    def work(data):
        kwargs = json.loads(data.decode('utf8'))
        ret = fn(**kwargs)
        return json.dumps(ret).encode('utf8')
    fn.work = work

    return fn


@task
def moose():
    return {'moose': True}
