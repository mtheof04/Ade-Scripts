import subprocess
import time
import sys
import json
from redfish import RedfishClient
from redfish.rest.v1 import ServerDownOrUnreachableError

#from get_resource_directory import get_resource_directory


def get_fast_power_meter_data(_redfishobj):
    power_metrics_uri = None

    resource_instances = _redfishobj.get_resource_directory()
    if DISABLE_RESOURCE_DIR or not resource_instances:
        #if we do not have a resource directory or want to force it's non use to find the
        #relevant URI
        chassis_uri = _redfishobj.root.obj['Chassis']['@odata.id']
        chassis_response = _redfishobj.get(chassis_uri)
        chassis_members_uri = next(iter(chassis_response.obj['Members']))['@odata.id']
        chassis_members_response = _redfishobj.get(chassis_members_uri)
        power_metrics_uri = chassis_members_response.obj.Oem.Hpe['Power']['@odata.id']
    else:
        for instance in resource_instances:
            #Use Resource directory to find the relevant URI
            if '#Power.' in instance['@odata.type']:
                power_metrics_uri = instance['@odata.id']
                break

    if power_metrics_uri:
        power_metrics_data = _redfishobj.get(power_metrics_uri).obj.Oem.Hpe
        fast_power_meter_uri = power_metrics_data['Links']['FastPowerMeter']['@odata.id']
        fast_power_meter_data = _redfishobj.get(fast_power_meter_uri).dict
        print("\n\nFastPowerMeter Data:\n\n")
        print(json.dumps(fast_power_meter_data, indent=4, sort_keys=True))


def usage(argv):
  print("%s <USERNAME> <PASSWORD> <HOSTNAME>" % argv[0])
  sys.exit()


if __name__ == "__main__":
    # When running on the server locally use the following commented values
    #SYSTEM_URL = None
    #LOGIN_ACCOUNT = None
    #LOGIN_PASSWORD = None

    # When running remotely connect using the secured (https://) address,
    # account name, and password to send https requests
    # SYSTEM_URL acceptable examples:
    # "https://10.0.0.100"
    # "https://ilo.hostname"
    
    if len(sys.argv) < 4:
        usage(sys.argv)

    LOGIN_ACCOUNT = sys.argv[1]
    LOGIN_PASSWORD = sys.argv[2]
    SYSTEM_URL = sys.argv[3]

    # flag to force disable resource directory. Resource directory and associated operations are
    # intended for HPE servers.
    DISABLE_RESOURCE_DIR = False

    try:
        # Create a Redfish client object
        REDFISHOBJ = RedfishClient(base_url=SYSTEM_URL, username=LOGIN_ACCOUNT, \
                                                                            password=LOGIN_PASSWORD)
        # Login with the Redfish client
        REDFISHOBJ.login()
    except ServerDownOrUnreachableError as excp:
        sys.stderr.write("ERROR: server not reachable or does not support RedFish.\n")
        sys.exit()

    get_fast_power_meter_data(REDFISHOBJ)
    REDFISHOBJ.logout()
