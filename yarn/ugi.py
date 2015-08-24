import pwd
import os

from snakebite.protobuf.IpcConnectionContext_pb2 import UserInformationProto
from snakebite.protobuf.Security_pb2 import TokenProto

class UserGroupInformation:
	def __init__(self, effective_user=None, real_user=None):
		self.tokens = {}
		self.user_info = UserInformationProto()
		
		if not effective_user:
			#default to local_user
			effective_user = pwd.getpwuid(os.getuid())[0]
		self.user_info.effectiveUser = effective_user
		
		if real_user:
			self.user_info.realUser = real_user
		
	def add_token(self, alias, token_identifier, token_password):
			self.tokens[alias] = {"identifier" : token_identifier, "password" : token_password}

	def get_all_tokens(self):
		return self.tokens

	def get_token(self, alias):
		return self.tokens[alias]

	def get_effective_user(self):
		return self.user_info.effectiveUser

def create_ugi_from_app(app):
	#TODO use proto instead of dict?
	appid = app["application_report"]["currentApplicationAttemptId"]

	effective_user = "appattempt_" + str(appid["application_id"]["cluster_timestamp"]) + \
		"_" + str(appid["application_id"]["id"]).zfill(4) + \
		"_" + str(appid["attemptId"]).zfill(6)
	
	ugi = UserGroupInformation(effective_user)

	token = app['application_report']['am_rm_token']
	ugi.add_token("am_rm_token", token["identifier"], token["password"])

	return ugi



