# installLibrary.py
#!/usr/bin/python3
import json
import requests
import sys
import getopt
import time
import os

def main():
    shard = ''
    token = ''
    clusterid = ''
    libspath = ''
    dbfspath = ''

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hstcld',
                                   ['shard=', 'token=', 'clusterid=', 'libs=', 'dbfspath='])
    except getopt.GetoptError:
        print(
            'installLibrary.py -s <shard> -t <token> -c <clusterid> -l <libs> -d <dbfspath>')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print(
                'installLibrary.py -s <shard> -t <token> -c <clusterid> -l <libs> -d <dbfspath>')
            sys.exit()
        elif opt in ('-s', '--shard'):
            shard = arg
        elif opt in ('-t', '--token'):
            token = arg
        elif opt in ('-c', '--clusterid'):
            clusterid = arg
        elif opt in ('-l', '--libs'):
            libspath=arg
        elif opt in ('-d', '--dbfspath'):
            dbfspath=arg

    print('-s is ' + shard)
    print('-t is ' + token)
    print('-c is ' + clusterid)
    print('-l is ' + libspath)
    print('-d is ' + dbfspath)

    # Uninstall library if exists on cluster
    i=0

    # Generate array from walking local path
    libslist = []
    for path, subdirs, files in os.walk(libspath):
        for name in files:

            name, file_extension = os.path.splitext(name)
            if file_extension.lower() in ['.jar']:
                libslist.append(name + file_extension.lower())

    for lib in libslist:
        dbfslib = dbfspath + '/' + lib
        print("dbfslib: " + dbfslib)
        print(dbfslib + ' before:' + getLibStatus(shard, token, clusterid, dbfslib))

        if (getLibStatus(shard, token, clusterid, dbfslib) != 'not found'):
            print(dbfslib + " exists. Uninstalling.")
            i = i + 1
            values = {'cluster_id': clusterid, 'libraries': [{'jar': dbfslib}]}

            resp = requests.post(shard + '/api/2.0/libraries/uninstall', data=json.dumps(values), auth=("token", token))
            runjson = resp.text
            d = json.loads(runjson)
            print(dbfslib + ' after:' + getLibStatus(shard, token, clusterid, dbfslib))

            # Restart if libraries uninstalled
            if i > 0:
                values = {'cluster_id': clusterid}
                print("Restarting cluster:" + clusterid)
                resp = requests.post(shard + '/api/2.0/clusters/restart', data=json.dumps(values), auth=("token", token))
                restartjson = resp.text
                print(restartjson)

                p = 0
                waiting = True
                while waiting:
                    time.sleep(30)
                    clusterresp = requests.get(shard + '/api/2.0/clusters/get?cluster_id=' + clusterid,
                                           auth=("token", token))
                    clusterjson = clusterresp.text
                    jsonout = json.loads(clusterjson)
                    current_state = jsonout['state']
                    print(clusterid + " state:" + current_state)
                    if current_state in ['TERMINATED', 'RUNNING','INTERNAL_ERROR', 'SKIPPED'] or p >= 10:
                        break
                    p = p + 1

        print("Installing " + dbfslib)
        values = {'cluster_id': clusterid, 'libraries': [{'jar': 'dbfs:' + dbfslib}]}

        resp = requests.post(shard + '/api/2.0/libraries/install', data=json.dumps(values), auth=("token", token))
        runjson = resp.text
        print("install result runjson: " + runjson)
        d = json.loads(runjson)
        #if not install error occurred
        if (d.get('error_code')):
            sys.exit(1)

        #print(dbfslib + ' after:' + getLibStatus(shard, token, clusterid, dbfslib))


def getLibStatus(shard, token, clusterid, dbfslib):

    resp = requests.get(shard + '/api/2.0/libraries/cluster-status?cluster_id='+ clusterid, auth=("token", token))
    libjson = resp.text
    print("libjson: " + libjson)
    d = json.loads(libjson)
    if (d.get('library_statuses')):
        statuses = d['library_statuses']

        for status in statuses:
            if (status['library'].get('jar')):
                print("status['library']['jar']: " + status['library']['jar'])
                if (status['library']['jar'] == 'dbfs:' + dbfslib):
                    return status['status']
                else:
                    return "not found"
        return "not found"
    else:
        # No libraries found
        return "not found"

if __name__ == '__main__':
    main()