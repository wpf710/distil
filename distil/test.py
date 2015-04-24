from datetime import timedelta, datetime
import interface
import json
import requests
import auth

from contextlib import contextmanager
import logging as log
import sys
import urlparse
import pdb


@contextmanager
def timed(desc):
    start = datetime.utcnow()
    yield
    end = datetime.utcnow()
    log.debug("%s: %s" % (desc, end - start))

window_leadin = timedelta(minutes=10)
# Most of the time we use date_format
date_format = "%Y-%m-%dT%H:%M:%S"
other_date_format = "%Y-%m-%dT%H:%M:%S.%f"

auth = auth.Keystone(
    username='admin',
    password='admin',
    tenant_name='admin',
    auth_url='http://10.104.0.76:5000/v2.0',
    insecure=True,
    region_name='RegionOne'
)


def sort_entries(data):
    """
    Setup timestamps as datetime objects,
    and sort.
    """
    for entry in data:
        try:
            entry['timestamp'] = datetime.strptime(
                entry['timestamp'], date_format)
        except ValueError:
            entry['timestamp'] = datetime.strptime(
                entry['timestamp'], other_date_format)
    return sorted(data, key=lambda x: x['timestamp'])


def add_dates(start, end):
    return [
        {
            "field": "timestamp",
            "op": "ge",
            "value": start
        },
        {
            "field": "timestamp",
            "op": "lt",
            "value": end
        }
    ]


def _clean_entry(entry):
    result = {
        'counter_volume': entry['resource_metadata'].get(
            'state', entry['resource_metadata'].get(
                'status', 'null'
            )
        ),
        'flavor': entry['resource_metadata'].get(
            'flavor.id', entry['resource_metadata'].get(
                'instance_flavor_id', 0
            )
        ),
        'timestamp': entry['timestamp']
    }
    return result


def usage(tenant_id, meter_name, start, end):
    """Queries ceilometer for all the entries in a given range,
        for a given meter, from this tenant."""
    fields = [{'field': 'project_id', 'op': 'eq', 'value': tenant_id}]
    fields.extend(add_dates(start, end))
    #pdb.set_trace()

    def sort_and_clip_end(usage):
        cleaned = (_clean_entry(s) for s in usage)
        clipped = [s for s in cleaned if s['timestamp'] < datetime.strptime(end,date_format)]
        return clipped

    with timed('fetch global usage for meter %s' % meter_name):
        endpoint = auth.get_ceilometer_endpoint()

        r = requests.Session().get(
            urlparse.urljoin(endpoint, '/v2/meters/%s' % meter_name),
            headers={
                "X-Auth-Token": auth.auth_token,
                "Content-Type": "application/json"
            },
            data=json.dumps({'q': fields}))

        if r.status_code == 200:
            return sort_and_clip_end(sort_entries(json.loads(r.text)))
        else:
            raise interface.InterfaceException('%d %s' % (r.status_code, r.text))

if __name__ == "__main__":
    result = usage('e9bbfa13e6534165a5411fe27af4df82', 'instance', sys.argv[1],sys.argv[2]) #'2015-02-09T07:50:00', '2015-02-09T09:00:00')
    print result
