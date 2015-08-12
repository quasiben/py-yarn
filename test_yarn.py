from yarn import pb_to_dict, dict_to_pb, yarn_protos, client, amrm
import logging
import os
logging.basicConfig(level=logging.DEBUG)

host = "54.158.136.145"
client_port = 9022
master_port = 9024

yarnclnt = client.Client(host, client_port)
yarnam = amrm.YarnAppMaster(host, master_port)
yarnam.submit(yarnclnt)
