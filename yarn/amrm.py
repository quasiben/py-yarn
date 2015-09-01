import yarn.protobuf.applicationmaster_protocol_pb2 as application_master_protocol
import yarn.protobuf.containermanagement_protocol_pb2 as node_manager_protocol
import yarn.protobuf.yarn_service_protos_pb2 as yarn_service_protos
import yarn.protobuf.yarn_protos_pb2 as yarn_protos
from yarn.client import container_launch_context
from  yarn.protobuf import Security_pb2 as yarn_security
from yarn.ugi import create_ugi_from_app_report, create_effective_user_from_app_id, get_alias
from yarn.node import Node
import snakebite.glob as glob
from snakebite.errors import RequestError
from yarn.rpc.service import RpcService
from snakebite.errors import FileNotFoundException
from snakebite.errors import DirectoryException
from snakebite.errors import FileException
from snakebite.errors import InvalidInputException
from snakebite.errors import OutOfNNException
from snakebite.channel import DataXceiverChannel
from snakebite.config import HDFSConfig
from . import YARN_PROTOCOL_VERSION, dict_to_pb, pb_to_dict
import urlparse

import logging
import pickle
import time

log = logging.getLogger(__name__)

YARN_CONTAINER_MEM_DEFAULT = 128
YARN_CONTAINER_CPU_DEFAULT = 1
YARN_CONTAINER_LOCATION_DEFAULT = "*"
YARN_CONTAINER_PRIORITY_DEFAULT = 1
YARN_CONTAINER_NUM_DEFAULT = 1
YARN_NM_CONN_KEEPALIVE_SECS = 5*60

AM_RM_SERVICE = {"class" : "org.apache.hadoop.yarn.api.ApplicationMasterProtocolPB", 
    "stub" : application_master_protocol.ApplicationMasterProtocolService_Stub}


class YarnAppMaster(object):
    """
    A pure python Yarn Master
    """

    def __init__(self, host, port, hadoop_version=YARN_PROTOCOL_VERSION):
        '''
        :param host: Hostname or IP address of the ResourceManager
        :type host: string
        :param port: RPC Port of the ResourceManager Scheduler
        :type port: int
        :param hadoop_version: What hadoop protocol version should be used (default: 9)
        :type hadoop_version: int
        '''
        if hadoop_version < 9:
            raise Exception("Only protocol versions >= 9 supported")

        self.host = host
        self.port = port
        self.am_rm_service_stub_class = AM_RM_SERVICE["stub"]
        self.am_rm_service = RpcService(self.am_rm_service_stub_class, AM_RM_SERVICE["class"], self.port, self.host, hadoop_version)
        self.hadoop_version = hadoop_version
        self.scheduler_call_id = 1
        self.ask = []
        self.release = []
        self.blacklist = [] #TODO figure out proper blacklisting
        self.progress = None
        self.increase = None 
        self.nm_service = None
        self.alive = True #TODO: only set alive when registered?
        self.pending_requests = []
        self.nodes = [] #TODO dict?
        self.ugi = None

        log.debug("Created Master for %s:%s", host, port)


    def submit(self, client):
        clc = self._launchcontext()

        prio = dict(priority=YARN_CONTAINER_PRIORITY_DEFAULT)
        res = dict(memory=YARN_CONTAINER_MEM_DEFAULT, virtual_cores=YARN_CONTAINER_CPU_DEFAULT)

        newapp = client.get_new_application()
        newapp_dict = pb_to_dict(newapp)
        appid = newapp_dict['application_id']
        maxmem, maxcores = newapp_dict['maximumCapability']['memory'], newapp_dict['maximumCapability']['virtual_cores']

        appData = {'application_id': appid,
          'application_name': "YarnBZApp",
          'queue': 'default',
          'priority': 1,
          'unmanaged_am': True,
          'am_container_spec': clc,
          'resource': res,
          'maxAppAttempts': 2,
          'applicationType': "PYTHON",
          'keep_containers_across_application_attempts': False,
        }

        client.submit_application(**appData)
        app = client.get_application_report(appid['cluster_timestamp'], appid['id'])

        self.register(app)

    def _launchcontext(self, command="", environment={}, service_data={}):
        c = dict(
            command=command,
            environment=[yarn_protos.StringStringMapProto(key=k, value=v) for k, v in environment.iteritems()],
            service_data=[yarn_protos.StringBytesMapProto(key=k, value=v) for k, v in service_data.iteritems()],
            )
        return c

    def register(self, app):
        self.ugi = create_ugi_from_app_report(get_alias(self.am_rm_service), app)
        self.am_rm_service.channel.ugi = self.ugi
        app_report = app.application_report
        data = dict(host=self.host, rpc_port=self.port, tracking_url=app_report.trackingUrl)
        req = dict_to_pb(yarn_service_protos.RegisterApplicationMasterRequestProto, data)
        self.am_rm_service.registerApplicationMaster(req)

        #re-enable once multithreading is implemented
        #self.begin_heartbeat()
    
    def create_request(
        self,
        callback,
        num_containers=YARN_CONTAINER_NUM_DEFAULT,
        priority=YARN_CONTAINER_PRIORITY_DEFAULT,
        resource_name=YARN_CONTAINER_LOCATION_DEFAULT,
        memory=YARN_CONTAINER_MEM_DEFAULT,
        virtual_cores=YARN_CONTAINER_CPU_DEFAULT):

        request = Request(callback, num_containers, priority, resource_name, memory, virtual_cores)
        self.ask.append(request.to_protobuf())
        self.pending_requests.append(request)


    def _allocate(self, ask=[], release=[], blacklist=[], response_id=None, progress=None, increase=None):
        resource_request = yarn_service_protos.AllocateRequestProto()
        resource_request.ask.extend(ask)
        resource_request.release.extend(release)
        #resource_request.blacklist_request.extend(blacklist) TODO
        if response_id:
            resource_request.response_id = response_id
        if progress:
            resource_request.progress = progress
        if increase:
            resource_request.increase = increase
        return self.am_rm_service.allocate(resource_request)

    def begin_heartbeat(self, interval=1):
        log.debug("Starting AM heartbeat")
        #TODO Start in separate thread
        while self.alive:
            self._heartbeat()
            time.sleep(interval)

        import ipdb
        ipdb.set_trace()

        log.debug("AM no longer alive, stopping heartbeat")


    def _heartbeat(self):
        log.debug("AM heartbeat with ID " + str(self.scheduler_call_id))
        alloc_resp = self._allocate(self.ask, self.release, self.blacklist, self.scheduler_call_id, self.progress, self.increase)
        
        self.ask = []
        self.release = []
        self.blacklist = []
        self.scheduler_call_id += 1
        self.increase = None #TODO find out if appropriate

        if alloc_resp.nm_tokens is not None:
            self.add_nodes(alloc_resp.nm_tokens)
        #TODO updte ugi if nessecary

        self._delegate_resources(alloc_resp) #TODO set up system of callbacks

    def add_nodes(self, nm_tokens):
        for node in nm_tokens:
            if self.get_node(node.nodeId) is None:
                new_node = Node(node.nodeId, self, node_token=node.token)
                self.nodes.append(new_node)
                self.ugi.add_token(new_node.alias, new_node.node_token)

    #TODO better object than full alloc_resp
    def _delegate_resources(self, alloc_resp):
        #TODO order by priority. Or if possible, give via ID
        new_pending = []
        for request in self.pending_requests:
            if self.meets_requirements(alloc_resp, request):
                #TODO Remove container that was just started from available
                request.callback(self, alloc_resp)
            else:
                new_pending.append(request)

        self.pending_requests = new_pending


    def meets_requirements(self, alloc_resp, request):
        #TODO full checking
        return len(alloc_resp.allocated_containers) == request.num_containers

    def get_node(self, node_id=None):
        found = None
        to_find = Node(node_id, self).alias
        for node in self.nodes:
            if node.alias == to_find:
                found = node
        return found



class Request(object):
    def __init__(
        self,
        callback,
        num_containers=YARN_CONTAINER_NUM_DEFAULT,
        priority=YARN_CONTAINER_PRIORITY_DEFAULT,
        resource_name=YARN_CONTAINER_LOCATION_DEFAULT,
        memory=YARN_CONTAINER_MEM_DEFAULT,
        virtual_cores=YARN_CONTAINER_CPU_DEFAULT):

        self.callback = callback
        self.num_containers = num_containers
        self.priority = priority
        self.resource_name = resource_name
        self.memory = memory
        self.virtual_cores = virtual_cores

    def to_protobuf(self):
        resource = yarn_protos.ResourceRequestProto()
        resource.priority.priority = self.priority
        resource.num_containers = self.num_containers
        resource.resource_name = self.resource_name
        resource.capability.memory = self.memory
        resource.capability.virtual_cores = self.virtual_cores
        return resource