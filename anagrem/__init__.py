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


LOW_PRIORITY, NORMAL_PRIORITY, HIGH_PRIORITY = object(), object(), object()


# Prevent the super verbose socket debugging unless explicitly enabled.
logging.getLogger('anagrem.request').propagate = False


class GearmanError(Exception):
    pass


class ClientType(type):

    """Metaclass for the `Client` class.

    This metaclass automatically assigns several mappings of Gearman protocol
    information for use in the `Client` class.

    """

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

    """A Gearman client."""

    def __init__(self, servers):
        self.servers = servers
        self.tasks = dict()

    def write_request(self, sock, packet_type, *args):
        """Write a request with the given Gearman packet type and data
        arguments to the given server socket."""
        logging.getLogger('anagrem.request').debug('%r %r %r', sock, packet_type, args)
        data = b'\x00'.join(args)
        header = struct.pack('!B3sII', 0, b'REQ', packet_type, len(data))
        sock.sendall(header)
        sock.sendall(data)

    def read_response(self, sock):
        """Read a Gearman packet response from the given server socket.

        The given socket should be ready to read from. `read_response()` will
        block until a full Gearman response packet can be read.

        The response is returned as a 2-tuple of the packet type and the
        response arguments (as a list or empty tuple). If the response packet
        is malformed, a `GearmanError` is raised.

        """
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
        """Connects to the first available of the configured servers and
        returns its socket.

        If no connection to one of the configured servers can be established, a
        `GearmanError` is raised.

        """
        # TODO: pool/persist connections
        for server in self.servers:
            host, port = server
            try:
                return socket.create_connection(server)
            except socket.error:
                pass
        raise GearmanError('Could not connect to any servers')

    def request(self, packet_type, *args, expect=None):
        """Send the given Gearman packet to one of the client's configured
        servers.

        If `expect` is a packet type or sequence of packet types, a response to
        the sent packet is read from the server immediately. The response is
        returned as a 2-tuple of packet type and args (as a sequence). If the
        response is not of the given packet type or types, a `GearmanError` is
        raised.

        If `expect` is `None` (the default), no response is read and no value
        is returned.

        """
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

    def submit_job(self, name, data, level=NORMAL_PRIORITY):
        """Submit a job with the given funcname and job data (both `bytes`).

        If `level` is specified as `anagrem.LOW_PRIORITY` or
        `anagrem.HIGH_PRIORITY`, the job is submitted as a low or high priority
        job with ``SUBMIT_JOB_LOW_BG`` or ``SUBMIT_JOB_HIGH_BG`` respectively.
        If `level` is `anagrem.NORMAL_PRIORITY` (the default) the job is
        submitted normally (``SUBMIT_JOB_BG``).

        The job handle of the submitted job is returned.

        """
        if level is LOW_PRIORITY:
            packet_type = self.SUBMIT_JOB_LOW_BG
        elif level is NORMAL_PRIORITY:
            packet_type = self.SUBMIT_JOB_BG
        elif level is HIGH_PRIORITY:
            packet_type = self.SUBMIT_JOB_HIGH_BG
        else:
            raise ValueError("Unknown job level %r", level)

        response_type, args = self.request(packet_type, name, b'', data, expect=self.JOB_CREATED)
        (handle,) = args
        return handle

    def get_status(self, handle):
        """Get and return the status of the given job handle from the first
        available server (which may not be the one to which the job handle was
        posted).

        The job status -- whether the server knows the job, whether it's
        running, and the numerator and denominator of progress as reported by
        the worker -- are returned as a (`bool`, `bool`, `int`, `int`) tuple.

        """
        response_type, args = self.request(self.GET_STATUS, handle, expect=self.STATUS_RES)
        log = logging.getLogger('anagrem.client')
        log.debug("Got STATUS_RES response args %r", args)
        handle, known, running, numerator, denominator = args
        known = known != b'0'
        running = running != b'0'
        return known, running, int(numerator), int(denominator)

    def work(self, shirk=False):
        """Connect to all known servers and work known jobs from them.

        If `shirk` is ``True``, after working a job the worker will report to
        that server it can no longer do jobs of that type. In some cases this
        can prevent the worker from doing one type of job to the exclusion of
        others, starving out other queues of work. Once no more jobs are
        available from that server, the worker again reports all registered
        task types, so the worker can eventually do all available work.

        """
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
                for funcname in self.tasks.keys():
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
                        if shirk:
                            log.debug("Hmm, no job, reset our willingness to work and wait for one")
                            for funcname in self.tasks.keys():
                                self.write_request(sock, self.CAN_DO, funcname.encode('ascii'))
                        else:
                            log.debug("Hmm, no job, ask the server to wait for one")
                        self.write_request(sock, self.PRE_SLEEP)
                        continue

                    if packet_type == self.JOB_ASSIGN:
                        handle, funcname, data = args
                        log.debug("Yay, got job %s!", handle)

                        funcname_str = funcname.decode('ascii')
                        func = self.tasks[funcname_str]
                        try:
                            kwargs = json.loads(data.decode('utf8'))
                            ret = func(**kwargs)
                            ret_data = json.dumps(ret).encode('utf8')
                        except Exception as exc:
                            log.warning("Job %s ended in a %s exception", handle, type(exc).__name__)
                            if log.isEnabledFor(logging.DEBUG):
                                log.debug("%s", traceback.format_exc())
                            self.write_request(sock, self.WORK_EXCEPTION, handle, str(exc).encode('utf8'))
                            self.write_request(sock, self.WORK_FAIL, handle)
                        else:
                            log.debug("Job %s completed successfully", handle)
                            self.write_request(sock, self.WORK_COMPLETE, handle, ret_data)

                        # Get a different sort of job for a while.
                        if shirk:
                            self.write_request(sock, self.CANT_DO, funcname)
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

    def task(self, fn):
        """Decorate the given function as a job for this client instance.

        `task()` registers the function as a job by fully qualified package
        name. Two convenience methods are added to `fn` for posting jobs using
        it:

        * `post(**kwargs)`: post a job that calls `fn` with the given keyword
          arguments.
        * `submit_job(data, level=anagrem.NORMAL_PRIORITY)`: post a job that
          calls `fn` with values in a dictionary `data` as keyword arguments.
          The job is posted with the given priority level:
          `anagrem.LOW_PRIORITY`, `anagrem.HIGH_PRIORITY` or
          `anagrem.NORMAL_PRIORITY` (the default).

        """
        client = self
        taskname = '.'.join((fn.__module__, fn.__name__))
        self.tasks[taskname] = fn

        def submit_job(data, level=NORMAL_PRIORITY):
            data = json.dumps(data).encode('utf8')
            return client.submit_job(taskname.encode('ascii'), data, level=level)
        fn.submit_job = submit_job

        def post(**kwargs):
            return fn.submit_job(kwargs)
        fn.post = post

        return fn
