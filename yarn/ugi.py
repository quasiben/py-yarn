import pwd
import os

from snakebite.protobuf.IpcConnectionContext_pb2 import UserInformationProto
from snakebite.protobuf.Security_pb2 import TokenProto

class UserGroupInformation(object):
	def __init__(self, effective_user=None, user_info=None):
		self.user_info = UserInformationProto()
		if user_info:
			self.user_info = user_info
		elif effective_user:
			self.user_info.effectiveUser = effective_user
		else:
			self.user_info.effectiveUser = pwd.getpwuid(os.getuid())[0]
		self.tokens = {}

	@property 
	def effective_user(self):
		return self.user_info.effectiveUser

	@effective_user.setter
	def effective_user(self, value):
		self.user_info.effectiveUser = value

	def add_token(self, alias, token):
		if alias not in self.tokens.keys():
			self.tokens[alias] = [token]
		else:
			self.tokens[alias].append(token)

	def find_token(self, alias, kind=None):
		found = None
		if alias in self.tokens.keys():
			for token in self.tokens[alias]:
				if kind is None or token.kind == kind:
					found = token
		return found

def create_ugi_from_app_report(alias, app_report):
	appid = app_report.application_report.currentApplicationAttemptId
	effective_user = create_effective_user_from_app_id(appid)
	ugi = UserGroupInformation(effective_user)
	token = app_report.application_report.am_rm_token
	ugi.add_token(alias, token)
	return ugi

def create_effective_user_from_app_id(appid):
	effective_user = "appattempt_" + str(appid.application_id.cluster_timestamp) + \
		"_" + str(appid.application_id.id).zfill(4) + \
		"_" + str(appid.attemptId).zfill(6)
	return effective_user

def get_alias(channel):
	return channel.host + ":" + str(channel.port)


