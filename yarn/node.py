
import yarn.protobuf.containermanagement_protocol_pb2 as node_manager_protocol
from yarn.ugi import get_alias
from yarn.rpc.service import RpcService

AM_NM_SERVICE = {"class" : "org.apache.hadoop.yarn.api.ContainerManagementProtocolPB",
    "stub" : node_manager_protocol.ContainerManagementProtocolService_Stub}

class Node(object):

	def __init__(self, node_id, am, node_token=None, max_memory=None, max_cores=None):
		self.node_id = node_id
		self.am = am
		self.node_token = node_token
		self.max_memory = max_memory
		self.max_cores = max_cores
		self.service = RpcService(AM_NM_SERVICE["stub"], AM_NM_SERVICE["class"], self.port, self.host, am.hadoop_version)
		self.service.channel.ugi = am.ugi

	@property 
	def identifier(self):
		return self.node_token.identifier

	@property 
	def password(self):
		return self.node_token.password

	@property 
	def host(self):
		return self.node_id.host

	@property 
	def port(self):
		return self.node_id.port

	@property 
	def alias(self):
		return get_alias(self)