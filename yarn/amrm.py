import yarn.protobuf.applicationmaster_protocol_pb2 as application_master_protocol
import yarn.protobuf.containermanagement_protocol_pb2 as node_manager_protocol
import yarn.protobuf.yarn_service_protos_pb2 as yarn_service_protos
import yarn.protobuf.yarn_protos_pb2 as yarn_protos
from yarn.client import container_launch_context
from  yarn.protobuf import Security_pb2 as yarn_security
from yarn.ugi import create_ugi_from_app, create_effective_user_from_app_id, UserGroupInformation
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
AM_NM_SERVICE = {"class" : "org.apache.hadoop.yarn.api.ContainerManagementProtocolPB",
    "stub" : node_manager_protocol.ContainerManagementProtocolService_Stub}

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
        self.blacklist = []
        self.progress = None
        self.increase = None 
        self.nm_service = None
        self.alive = True #TODO: only set alive when registered?

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
          'unmanaged_am': True, # what does this mean?
          'am_container_spec': clc,
          'resource': res,
          'maxAppAttempts': 2,
          'applicationType': "PYTHON",
          'keep_containers_across_application_attempts': False,
        }

        client.submit_application(**appData)
        app = pb_to_dict(client.get_application_report(appid['cluster_timestamp'], appid['id']))

        import ipdb
        ipdb.set_trace()

        self.register(app)

    def _launchcontext(self, command="", environment={}, service_data={}):
        c = dict(
            command=command,
            environment=[yarn_protos.StringStringMapProto(key=k, value=v) for k, v in environment.iteritems()],
            service_data=[yarn_protos.StringBytesMapProto(key=k, value=v) for k, v in service_data.iteritems()],
            )
        return c

    def register(self, app):
        self.am_rm_service.channel.ugi = create_ugi_from_app(app)
        app_report = app['application_report']
        data = dict(host=self.host, rpc_port=self.port, tracking_url=app_report['trackingUrl'])
        req = dict_to_pb(yarn_service_protos.RegisterApplicationMasterRequestProto, data)
        self.am_rm_service.registerApplicationMaster(req)

        #Dummy resource request
        resource = yarn_protos.ResourceRequestProto()
        resource.priority.priority = YARN_CONTAINER_PRIORITY_DEFAULT
        resource.num_containers = YARN_CONTAINER_NUM_DEFAULT
        resource.resource_name = YARN_CONTAINER_LOCATION_DEFAULT
        resource.capability.memory = YARN_CONTAINER_MEM_DEFAULT
        resource.capability.virtual_cores = YARN_CONTAINER_CPU_DEFAULT

        self.ask.append(resource)

        self.begin_heartbeat()


    def allocate(self, ask=[], release=[], blacklist=[], response_id=None, progress=None, increase=None):
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
            self.heartbeat()
            time.sleep(interval)

        import ipdb
        ipdb.set_trace()

        log.debug("AM no longer alive, stopping heartbeat")


    def heartbeat(self):
        log.debug("AM heartbeat with ID " + str(self.scheduler_call_id))
        alloc_resp = self.allocate(self.ask, self.release, self.blacklist, self.scheduler_call_id, self.progress, self.increase)
        self.ask = []
        self.release = []
        self.blacklist = []
        self.scheduler_call_id += 1
        self.increase = None #TODO find out if appropriate
        self.handle_response(alloc_resp) #TODO set up system of callbacks

    #TODO generalized callbacks
    def handle_response(self, alloc_resp):
        if len(alloc_resp.allocated_containers) > 0:
            node = alloc_resp.nm_tokens[0]
            node_id = node.nodeId

            node_token = node.token

            container = alloc_resp.allocated_containers[0]
            container_token = container.container_token

            appid = container.id.app_attempt_id
            effective_user = create_effective_user_from_app_id(pb_to_dict(appid))

            node_ugi = UserGroupInformation(effective_user=effective_user)
            #TODO change once SASL auth is generalized
            node_ugi.add_token("am_rm_token", node.token.identifier, node.token.password)

            #Connect to node manager
            context_proto = "org.apache.hadoop.yarn.api.ContainerManagementProtocolPB"
            host = node_id.host
            port = node_id.port
            service_stub_class = node_manager_protocol.ContainerManagementProtocolService_Stub
            self.nm_service = RpcService(service_stub_class, context_proto, port, host, self.hadoop_version)

            self.nm_service.channel.ugi = node_ugi

            pb = container_launch_context(["yes"], {}, tokens=container_token.SerializeToString())
            self.nm_service.startContainers(pb)

            import ipdb
            ipdb.set_trace()

            log.info("Started container at node on " + str(node_id.host) + ":" + str(node_id.port))
            self.alive = False



