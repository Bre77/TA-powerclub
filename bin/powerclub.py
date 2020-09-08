from __future__ import print_function
from builtins import str
import os
import time
import sys
import xml.dom.minidom, xml.sax.saxutils
import logging
import json
import urllib3
import requests
from datetime import date, timedelta, datetime

#set up logging suitable for splunkd comsumption
logging.root
logging.root.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logging.root.addHandler(handler)

SCHEME = """<scheme>
    <title>Powerclub Usage Data</title>
    <description>Powerclub Usage Data</description>
    <use_external_validation>false</use_external_validation>
    <streaming_mode>xml</streaming_mode>

    <endpoint>
        <args>
            <arg name="email">
                <title>Email</title>
                <description>Email address</description>
                <data_type>string</data_type>
                <required_on_create>true</required_on_create>
            </arg>
            <arg name="password">
                <title>Password</title>
                <description>Your password</description>
                <data_type>string</data_type>
                <required_on_create>true</required_on_create>
            </arg>
        </args>
    </endpoint>
</scheme>
"""

def validate_conf(config, key):
    if key not in config:
        raise Exception("Invalid configuration received from Splunk: key '%s' is missing." % key)

# Routine to get the value of an input
def get_config():
    config = {}

    try:
        # read everything from stdin
        config_str = sys.stdin.read()

        # parse the config XML
        doc = xml.dom.minidom.parseString(config_str)
        root = doc.documentElement
        conf_node = root.getElementsByTagName("configuration")[0]
        if conf_node:
            logging.debug("XML: found configuration")
            stanza = conf_node.getElementsByTagName("stanza")[0]
            if stanza:
                stanza_name = stanza.getAttribute("name")
                if stanza_name:
                    logging.debug("XML: found stanza " + stanza_name)
                    config["name"] = stanza_name

                    params = stanza.getElementsByTagName("param")
                    for param in params:
                        param_name = param.getAttribute("name")
                        logging.debug("XML: found param '%s'" % param_name)
                        if param_name and param.firstChild and \
                           param.firstChild.nodeType == param.firstChild.TEXT_NODE:
                            data = param.firstChild.data
                            config[param_name] = data
                            logging.debug("XML: '%s' -> '%s'" % (param_name, data))

        checkpnt_node = root.getElementsByTagName("checkpoint_dir")[0]
        if checkpnt_node and checkpnt_node.firstChild and \
           checkpnt_node.firstChild.nodeType == checkpnt_node.firstChild.TEXT_NODE:
            config["checkpoint_dir"] = checkpnt_node.firstChild.data

        if not config:
            raise Exception("Invalid configuration received from Splunk.")

    except Exception as e:
        raise Exception("Error getting Splunk configuration via STDIN: %s" % str(e))

    return config

# Routine to index data
def run_script():
    config=get_config()
    auth = {
        "email":config.get('email'),
        "password":config.get('password'),
    }
    headers = {'Accept': "application/json"}

    login = requests.post("https://dest-pc-signup-sandbox.herokuapp.com/user/login",headers=headers,data=auth)
    if(login.status_code == 200):
        user = login.json()['data']
        headers.update({"Authorization": user['auth_token']})
        print("<stream>")
        for a in user['address']:
            checkpointfile = os.path.join(config["checkpoint_dir"], str(a['address_id']))
            try:
                day = datetime.strptime(open(checkpointfile, "r").read(),'%Y-%m-%d').date()
            except:
                day = (date.today() - timedelta(days=1))
            

            while day < date.today():
                logging.info(f"Pulling {day} at {a['street']}")
                resp = requests.get(f"https://dest-pc-signup-sandbox.herokuapp.com/usage/half-hourly/{a['address_id']}?start_date={day.strftime('%Y-%m-%d')}",headers=headers) 
                if(resp.status_code == 200):
                    data = resp.json()['data']
                else:
                    data = {}

                if data.get('usage_data') and len(data['usage_data']) == 48:
                    logging.info(f"Writing metrics of {day.strftime('%Y-%m-%d')} at {a['street']}")
                    for z in zip(data['usage_data'],data['spot_price_data']):
                        if(z[0]['date'] == z[1]['date']):
                            #Safe to merge them all
                            time = int(datetime.strptime(z[0]['date'], '%Y-%m-%dT%H:%M:%S').timestamp())
                            payload = json.dumps({
                                'metric_name:power':z[0]['amount'],
                                'metric_name:solar':z[0]['solar'],
                                'metric_name:spotprice':z[1]['amount'],
                                'metric_name:fixedprice':data['fixed_rate']
                            }, separators=(',',':'))
                            print(f"<event><time>{time}</time><source>{a['street']}</source><data>{payload}</data></event>")
                        else:
                            #Timestamp mismatch, seperate the events
                            time = int(datetime.strptime(z[0]['date'], '%Y-%m-%dT%H:%M:%S').timestamp())
                            payload = json.dumps({
                                'metric_name:power':z[0]['amount'],
                                'metric_name:solar':z[0]['solar']
                            }, separators=(',',':'))
                            print(f"<event><time>{time}</time><source>{a['street']}</source><data>{payload}</data></event>")

                            time = int(datetime.strptime(z[1]['date'], '%Y-%m-%dT%H:%M:%S').timestamp())
                            payload = json.dumps({
                                'metric_name:spotprice':z[1]['amount'],
                                'metric_name:fixedprice':data['fixed_rate']
                            }, separators=(',',':'))
                            print(f"<event><time>{time}</time><source>{a['street']}</source><data>{payload}</data></event>")
                    day = day+timedelta(days=1)
                    continue
                else:
                    logging.info(f"Incomplete data for {day.strftime('%Y-%m-%d')} at {a['street']}")
                    break
            open(checkpointfile, "w").write(day.strftime('%Y-%m-%d'))       
        print("</stream>")
        requests.delete("https://dest-pc-signup-sandbox.herokuapp.com/user/logout",headers=headers)

# Script must implement these args: scheme, validate-arguments
if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == "--scheme":
            print(SCHEME)
        elif sys.argv[1] == "--validate-arguments":
            pass
        else:
            pass
    else:
        run_script()

    sys.exit(0)