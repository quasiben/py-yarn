from yarn import pb_to_dict, dict_to_pb, yarn_protos, client, amrm
import yarn.protobuf.yarn_service_protos_pb2 as yarn_service_protos
import logging
import os
from yarn.ugi import create_effective_user_from_app_id, UserGroupInformation
from yarn.client import container_launch_context
import yarn.protobuf.containermanagement_protocol_pb2 as node_manager_protocol
import sys
from yarn.rpc.service import RpcService
logging.basicConfig(level=logging.DEBUG)

log = logging.getLogger(__name__)


def main(argv):
	connection_type = "local"
	if len(argv) > 0:
		connection_type = argv[0]

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

	    container = alloc_resp.allocated_containers[0]
	    container_token = container.container_token

	    context = yarn_protos.ContainerLaunchContextProto(command=["python -m SimpleHTTPServer"])
	    startReq = yarn_service_protos.StartContainerRequestProto(container_token=container_token, container_launch_context=context)
	    start_plural = yarn_service_protos.StartContainersRequestProto(start_container_request=[startReq])

	    am.get_node(alias=container_token.service).service.startContainers(start_plural)

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

if __name__ == "__main__":
	main(sys.argv[1:])
