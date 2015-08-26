import yarn.protobuf.applicationmaster_protocol_pb2 as application_master_protocol
import yarn.protobuf.yarn_service_protos_pb2 as yarn_service_protos
import yarn.protobuf.yarn_protos_pb2 as yarn_protos
from  yarn.protobuf import Security_pb2 as yarn_security
from yarn.ugi import create_ugi_from_app
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

log = logging.getLogger(__name__)

YARN_CONTAINER_MEM_DEFAULT = 128
YARN_CONTAINER_CPU_DEFAULT = 1
YARN_CONTAINER_LOCATION_DEFAULT = "*"
YARN_CONTAINER_PRIORITY_DEFAULT = 1
YARN_NM_CONN_KEEPALIVE_SECS = 5*60

class YarnAppMaster(object):
    """
    A pure python Yarn Master
    """

    def __init__(self, host, port, hadoop_version=YARN_PROTOCOL_VERSION):
        '''
        :param host: Hostname or IP address of the ResourceManager
        :type host: string
        :param port: RPC Port of the ResourceManager
        :type port: int
        :param hadoop_version: What hadoop protocol version should be used (default: 9)
        :type hadoop_version: int
        '''
        if hadoop_version < 9:
            raise Exception("Only protocol versions >= 9 supported")

        context_proto = "org.apache.hadoop.yarn.api.ApplicationMasterProtocolPB"
        self.host = host
        self.port = port
        self.service_stub_class = application_master_protocol.ApplicationMasterProtocolService_Stub
        self.service = RpcService(self.service_stub_class, context_proto, self.port, self.host, hadoop_version)

        log.debug("Created Master for %s:%s", host, port)


    def submit(self, client):
        clc = self._launchcontext()

        prio = dict(priority=YARN_CONTAINER_PRIORITY_DEFAULT)
        res = dict(memory=YARN_CONTAINER_MEM_DEFAULT, virtual_cores=YARN_CONTAINER_CPU_DEFAULT)

        # appid, maxmem, maxcores = _new_app(client)
        newapp = client.get_new_application()
        newapp_dict = pb_to_dict(newapp)
        appid = newapp_dict['application_id']
        maxmem, maxcores = newapp_dict['maximumCapability']['memory'], newapp_dict['maximumCapability']['virtual_cores']

       # name='cluster_timestamp', full_name='hadoop.yarn.ApplicationIdProto.cluster_timestamp', index=1,

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
    #
    #     # register the unmanaged appmaster
    #     unmanagedappmaster.managed = false (AM is not on cluster)
    #     register(unmanagedappmaster)
        self.register(app)
    #     wait_for_attempt_state(app, Int32(1), YarnApplicationAttemptStateProto.APP_ATTEMPT_RUNNING) || throw(YarnException("Application attempt could not be launched"))
    #
    #     # initialize complete node list for appmaster
    #     nodes(client; nodelist=unmanagedappmaster.nodes)
    #
    #     # start the am_rm processing task once the app attempt starts running if it is an unmanaged application master
    #     @async(process_am_rm(unmanagedappmaster))
    #     app
    # end
    def _launchcontext(self, command="", environment={}, service_data={}):
        c = dict(
            command=command,
            environment=[yarn_protos.StringStringMapProto(key=k, value=v) for k, v in environment.iteritems()],
            service_data=[yarn_protos.StringBytesMapProto(key=k, value=v) for k, v in service_data.iteritems()],
            )
        return c

    def register(self, app):
        #app = pb_to_dict(client.get_application_report(appid['cluster_timestamp'], appid['id']))
        self.service.channel.ugi = create_ugi_from_app(app)
        app_report = app['application_report']
        data = dict(host=self.host, rpc_port=self.port, tracking_url=app_report['trackingUrl'])
        req = dict_to_pb(yarn_service_protos.RegisterApplicationMasterRequestProto, data)
        self.service.registerApplicationMaster(req)

        resource = yarn_protos.ResourceRequestProto()
        resource.priority.priority = 1
        resource.num_containers = 1
        resource.resource_name = "*" #Any node will do
        resource.capability.memory = 2045
        resource.capability.virtual_cores = 1 


        request = yarn_service_protos.AllocateRequestProto()
        request.ask.extend([resource]) 
        #req = dict_to_pb(yarn_service_protos.AllocateRequestProto, data)
        self.service.allocate(request)
        #logmsg("started processing am-rm messages")
    #     !isempty(alloc_pending) && set_field!(inp, :ask, alloc_pending)
    #     !isempty(release_pending) && set_field!(inp, :release, release_pending)
    #
    #     # send one-up response id
    #     if yam.response_id == typemax(Int32)
    #         yam.response_id = 1
    #     else
    #         yam.response_id += 1
    #     end
    #     set_field!(inp, :response_id, yam.response_id)
    #
    #     #logmsg(inp)
    #     resp = @locked(yam, allocate(yam.amrm_conn, inp))
    #     #logmsg(resp)
    #
    #     # store/update tokens
    #     channel = yam.amrm_conn.channel
    #     ugi = channel.ugi
    #     isfilled(resp, :am_rm_token) && add_token(ugi, token_alias(channel), resp.am_rm_token)
    #     if isfilled(resp, :nm_tokens)
    #         for nmtok in resp.nm_tokens
    #             add_token(ugi, token_alias(nmtok.nodeId), nmtok.token)
    #         end
    #     end
    #
    #     # update available headroom
    #     if isfilled(resp, :limit)
    #         yam.available_mem = resp.limit.memory
    #         yam.available_cores = resp.limit.virtual_cores
    #     end
    #
    #     # update node and container status
    #     update(yam.nodes, resp)
    #     update(yam.containers, resp)
    #     #logmsg("finished processing am-rm messages")
    #     #logmsg(yam)
    #     nothing
    # end


    # if isfilled(resp, :maximumCapability)
    #     yam.max_mem = resp.maximumCapability.memory
    #     yam.max_cores = resp.maximumCapability.virtual_cores
    # end
    # #logmsg("max capability: mem:$(yam.max_mem), cores:$(yam.max_cores)")
    # if isfilled(resp, :queue)
    #     yam.queue = resp.queue
    # end
    #
    # # start the am_rm processing task on registration if it is a managed application master
    # yam.managed && @async(process_am_rm(yam))
    # nothing

