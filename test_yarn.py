from yarn import pb_to_dict, dict_to_pb, yarn_protos, client, amrm
import logging
import os
from yarn.ugi import create_effective_user_from_app_id, UserGroupInformation
from yarn.client import container_launch_context
import yarn.protobuf.containermanagement_protocol_pb2 as node_manager_protocol
from yarn.rpc.service import RpcService
logging.basicConfig(level=logging.DEBUG)

log = logging.getLogger(__name__)

connection_type = "remote"
connection_info = {"local" : {"host" : "localhost", "client_port" : 8032, "master_port" : 8030}, \
				   "remote" : {"host" : "54.158.136.145", "client_port" : 9022, "master_port" : 9024}}

host = connection_info[connection_type]["host"]
#yarn.resourcemanager.address
client_port = connection_info[connection_type]["client_port"]

#yarn.resourcemanager.scheduler.address
master_port = connection_info[connection_type]["master_port"]

yarnclnt = client.Client(host, client_port)
yarnam = amrm.YarnAppMaster(host, master_port)
yarnam.client = yarnclnt
yarnam.submit(yarnclnt)

def dummy_callback(am, alloc_resp):
    node_id = alloc_resp.nm_tokens[0].nodeId
    #node_id = node.nodeId

    #node_token = node.token

    container = alloc_resp.allocated_containers[0]
    container_token = container.container_token

    #TODO change token handling?
    pb = container_launch_context(["yes"], {}, tokens=container_token.SerializeToString())
    am.get_node(node_id).service.startContainers(pb)
    log.info("Started container at node on " + str(node_id.host) + ":" + str(node_id.port))

    import ipdb
    ipdb.set_trace()

    #end test
    #TODO unregister AM
    am.alive = False

#Ask for the default amount of containers, trigger callback on allocation
yarnam.create_request(dummy_callback)

#TODO Remove once multithreading is enabled
yarnam.begin_heartbeat()