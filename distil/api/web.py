# Copyright (C) 2014 Catalyst IT Ltd
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import flask
from flask import Flask, Blueprint
from distil import database, config
from distil.constants import iso_time, iso_date, dawn_of_time
from distil.transformers import active_transformers as transformers
from distil.rates import RatesFile
from distil.models import SalesOrder, _Last_Run
from distil.helpers import convert_to, reset_cache
from distil.interface import Interface, timed
from sqlalchemy import create_engine, func
from sqlalchemy.orm import scoped_session, create_session
from sqlalchemy.pool import NullPool
from sqlalchemy.exc import IntegrityError, OperationalError
from datetime import datetime, timedelta
from decimal import Decimal
import json
import logging as log
from keystoneclient.middleware.auth_token import AuthProtocol as KeystoneMiddleware

from .helpers import returns_json, json_must, validate_tenant_id, require_admin
from urlparse import urlparse


engine = None

Session = None

app = Blueprint("main", __name__)

DEFAULT_TIMEZONE = "Pacific/Auckland"


def get_app(conf):
    actual_app = Flask(__name__)
    actual_app.register_blueprint(app, url_prefix="/")

    config.setup_config(conf)

    global engine
    engine = create_engine(config.main["database_uri"], poolclass=NullPool)

    global Session
    Session = scoped_session(lambda: create_session(bind=engine))

    if config.main.get("timezone"):
        global DEFAULT_TIMEZONE
        DEFAULT_TIMEZONE = config.main["timezone"]

    log.basicConfig(filename=config.main["log_file"],
                    level=log.INFO,
                    format='%(asctime)s %(message)s')
    log.info("Billing API started.")

    # if configured to authenticate clients, then wrap the
    # wsgi app in the keystone middleware.
    if config.auth.get('authenticate_clients'):
        identity_url = urlparse(config.auth['identity_url'])
        conf = {
            'admin_user': config.auth['username'],
            'admin_password': config.auth['password'],
            'admin_tenant_name': config.auth['default_tenant'],
            'auth_host': identity_url.hostname,
            'auth_port': identity_url.port,
            'auth_protocol': identity_url.scheme
        }
        actual_app = KeystoneMiddleware(actual_app, conf)

    return actual_app


@app.route("last_collected", methods=["GET"])
@returns_json
@require_admin
def get_last_collected():
    """Simple call to get timestamp for the last collection run."""
    session = Session()
    session.begin()
    last_run = session.query(_Last_Run)
    if last_run.count() == 0:
        last_collected = dawn_of_time
    else:
        last_collected = last_run[0].last_run
    session.close()
    return 200, {'last_collected': str(last_collected)}


def generate_windows(start, end):
    """Generator for 1 hour windows in a given range."""
    window_size = timedelta(hours=1)
    while start + window_size <= end:
        window_end = start + window_size
        yield start, window_end
        start = window_end


def filter_and_group(usage, usage_by_resource):
    with timed("filter and group by resource"):
        trust_sources = set(config.main.get('trust_sources', []))
        for u in usage:
            # the user can make their own samples, including those
            # that would collide with what we care about for
            # billing.
            # if we have a list of trust sources configured, then
            # discard everything not matching.
            if trust_sources and u['source'] not in trust_sources:
                log.warning('ignoring untrusted usage sample ' +
                            'from source `%s`' % u['source'])
                continue

            resource_id = u['resource_id']
            entries = usage_by_resource.setdefault(resource_id, [])
            entries.append(u)


def transform_and_insert(tenant, usage_by_resource, transformer, service,
                         meter_info, window_start, window_end,
                         db, timestamp):
    with timed("apply transformer + insert"):
        for res, entries in usage_by_resource.items():
            # apply the transformer.
            transformed = transformer.transform_usage(
                service, entries, window_start, window_end)

            if transformed:
                res = meter_info.get('res_id_template', '%s') % res

                md_def = meter_info['metadata']

                db.insert_resource(tenant.id, res, meter_info['type'],
                                   timestamp, entries[-1], md_def)
                db.insert_usage(tenant.id, res, transformed,
                                meter_info['unit'], window_start,
                                window_end, timestamp)


def collect_usage(tenant, db, session, resp, end):
    """Collects usage for a given tenant from when they were last collected,
       up to the given end, and breaks the range into one hour windows."""
    run_once = False
    timestamp = datetime.utcnow()
    session.begin(subtransactions=True)

    log.info('collect_usage for %s %s' % (tenant.id, tenant.name))

    db_tenant = db.insert_tenant(tenant.id, tenant.name,
                                 tenant.description, timestamp)
    start = db_tenant.last_collected
    session.commit()

    max_windows = config.collection.get('max_windows_per_cycle', 0)
    windows = generate_windows(start, end)

    if max_windows:
        windows = list(windows)[:max_windows]

    for window_start, window_end in windows:
        try:
            with session.begin(subtransactions=True):
                log.info("%s %s slice %s %s" % (tenant.id, tenant.name,
                                                window_start, window_end))

                mappings = config.collection['meter_mappings']

                for meter_name, meter_info in mappings.items():
                    mn = config.collection['meter_pairs'].get(meter_name, meter_name)
                    usage = tenant.usage(mn, window_start, window_end)
                    usage_by_resource = {}

                    transformer = transformers[meter_info['transformer']]()

                    filter_and_group(usage, usage_by_resource)

                    if 'service' in meter_info:
                        service = meter_info['service']
                    else:
                        service = meter_name

                    transform_and_insert(tenant, usage_by_resource,
                                         transformer, service, meter_info,
                                         window_start, window_end, db,
                                         timestamp)

                db_tenant.last_collected = window_end
                session.add(db_tenant)

            resp["tenants"].append(
                {"id": tenant.id,
                 "updated": True,
                 "start": window_start.strftime(iso_time),
                 "end": window_end.strftime(iso_time)
                 }
            )
            run_once = True
        except (IntegrityError, OperationalError):
            # this is fine.
            session.rollback()
            resp["tenants"].append(
                {"id": tenant.id,
                 "updated": False,
                 "error": "Integrity error",
                 "start": window_start.strftime(iso_time),
                 "end": window_end.strftime(iso_time)
                 }
            )
            resp["errors"] += 1
            log.warning("IntegrityError for %s %s in window: %s - %s " %
                        (tenant.name, tenant.id,
                         window_start.strftime(iso_time),
                         window_end.strftime(iso_time)))
            return run_once
    return run_once


@app.route("collect_usage", methods=["POST"])
@require_admin
def run_usage_collection():
    """Run usage collection on all tenants present in Keystone."""
    try:
        log.info("Usage collection run started.")

        session = Session()

        interface = Interface()

        reset_cache()

        db = database.Database(session)

        end = datetime.utcnow().\
            replace(minute=0, second=0, microsecond=0)

        tenants = interface.tenants

        resp = {"tenants": [], "errors": 0}
        run_once = False

        for tenant in tenants:
            if collect_usage(tenant, db, session, resp, end):
                run_once = True

        if(run_once):
            session.begin()
            last_run = session.query(_Last_Run)
            if last_run.count() == 0:
                last_run = _Last_Run(last_run=end)
                session.add(last_run)
                session.commit()
            else:
                last_run[0].last_run = end
                session.commit()

        session.close()
        log.info("Usage collection run complete.")
        return json.dumps(resp)

    except Exception as e:
        import traceback
        trace = traceback.format_exc()
        log.critical('Exception escaped! %s \nTrace: \n%s' % (e, trace))

def make_serializable(obj):
    if isinstance(obj, list):
        return [make_serializable(x) for x in obj]
    if isinstance(obj, dict):
        return {make_serializable(k):make_serializable(v) for k,v in obj.items()}

    if isinstance(obj, Decimal):
        return str(obj)

    return obj

@app.route("get_usage", methods=["GET"])
@returns_json
@require_admin
def get_usage():
    """
    Get raw aggregated usage for a tenant, in a given timespan.
        - No rates are applied.
        - No conversion from collection unit to billing unit
        - No rounding
    """
    tenant_id = flask.request.args.get('tenant')
    start = flask.request.args.get('start')
    end = flask.request.args.get('end')

    log.info("get_usage for %s %s %s" % (tenant_id, start, end))

    try:
        start_dt = datetime.strptime(start, iso_time)
    except ValueError:
        return 400, {'error': 'Invalid start datetime'}

    try:
        end_dt = datetime.strptime(end, iso_time)
    except ValueError:
        return 400, {'error': 'Invalid end datetime'}

    if end_dt < start_dt:
        return 400, {'error': 'End must be after start'}

    session = Session()
    db = database.Database(session)

    valid_tenant = validate_tenant_id(tenant_id, session)
    if isinstance(valid_tenant, tuple):
        return valid_tenant

    log.info("parameter validation ok")

    # aggregate usage
    usage = db.usage(start, end, tenant_id)
    tenant_dict = build_tenant_dict(valid_tenant, usage, db)

    return 200, {'usage': make_serializable(tenant_dict)}


def build_tenant_dict(tenant, entries, db):
    """Builds a dict structure for a given tenant."""
    tenant_dict = {'name': tenant.name, 'tenant_id': tenant.id,
                   'resources': {}}

    for entry in entries:
        service = {'name': entry.service, 'volume': entry.volume,
                   'unit': entry.unit}

        if (entry.resource_id not in tenant_dict['resources']):
            resource = db.get_resource_metadata(entry.resource_id)

            resource['services'] = [service]

            tenant_dict['resources'][entry.resource_id] = resource

        else:
            resource = tenant_dict['resources'][entry.resource_id]
            resource['services'].append(service)

    return tenant_dict


def add_costs_for_tenant(tenant, RatesManager):
    """Adds cost values to services using the given rates manager."""
    tenant_total = 0
    for resource in tenant['resources'].values():
        resource_total = 0
        for service in resource['services']:
            try:
                rate = RatesManager.rate(service['name'])
            except KeyError:
                # no rate exists for this service
                service['cost'] = "0"
                service['volume'] = "unknown unit conversion"
                service['unit'] = "unknown"
                service['rate'] = "missing rate"
                continue

            volume = convert_to(service['volume'],
                                service['unit'],
                                rate['unit'])

            # round to 2dp so in dollars.
            cost = round(volume * rate['rate'], 2)

            service['cost'] = str(cost)
            service['volume'] = str(volume)
            service['unit'] = rate['unit']
            service['rate'] = str(rate['rate'])

            resource_total += cost
        resource['total_cost'] = str(resource_total)
        tenant_total += resource_total
    tenant['total_cost'] = str(tenant_total)

    return tenant


def generate_sales_order(draft, tenant_id, end):
    """Generates a sales order dict, and unless draft is true,
       creates a database entry for sales_order."""
    session = Session()
    db = database.Database(session)

    valid_tenant = validate_tenant_id(tenant_id, session)
    if isinstance(valid_tenant, tuple):
        return valid_tenant

    rates = RatesFile(config.rates_config)

    # Get the last sales order for this tenant, to establish
    # the proper ranging
    start = session.query(func.max(SalesOrder.end).label('end')).\
        filter(SalesOrder.tenant_id == tenant_id).first().end
    if not start:
        start = dawn_of_time

    # these coditionals need work, also some way to
    # ensure all given timedate values are in UTC?
    if end <= start:
        return 400, {"errors": ["end date must be greater than " +
                                "the end of the last sales order range."]}
    if end > datetime.utcnow():
        return 400, {"errors": ["end date cannot be a future date."]}

    usage = db.usage(start, end, tenant_id)

    session.begin()
    if not draft:
        order = SalesOrder(tenant_id=tenant_id, start=start, end=end)
        session.add(order)

    try:
        # Commit the record before we generate the bill, to mark this as a
        # billed region of data. Avoids race conditions by marking a tenant
        # BEFORE we start to generate the data for it.
        session.commit()

        # Transform the query result into a billable dict.
        tenant_dict = build_tenant_dict(valid_tenant, usage, db)
        tenant_dict = add_costs_for_tenant(tenant_dict, rates)

        # add sales order range:
        tenant_dict['start'] = str(start)
        tenant_dict['end'] = str(end)
        session.close()
        if not draft:
            log.info("Sales Order #%s Generated for %s in range: %s - %s" %
                     (order.id, tenant_id, start, end))
        return 200, tenant_dict
    except (IntegrityError, OperationalError):
        session.rollback()
        session.close()
        log.warning("IntegrityError creating sales-order for " +
                    "%s %s in range: %s - %s " %
                    (valid_tenant.name, valid_tenant.id, start, end))
        return 400, {"id": tenant_id,
                     "error": "IntegrityError, existing sales_order overlap."}


def regenerate_sales_order(tenant_id, target):
    """Finds a sales order entry nearest to the target,
       and returns a salesorder dict based on the entry."""
    session = Session()
    db = database.Database(session)
    rates = RatesFile(config.rates_config)

    valid_tenant = validate_tenant_id(tenant_id, session)
    if isinstance(valid_tenant, tuple):
        return valid_tenant

    try:
        sales_order = db.get_sales_orders(tenant_id, target, target)[0]
    except IndexError:
        return 400, {"errors": ["Given date not in existing sales orders."]}

    usage = db.usage(sales_order.start, sales_order.end, tenant_id)

    # Transform the query result into a billable dict.
    tenant_dict = build_tenant_dict(valid_tenant, usage, db)
    tenant_dict = add_costs_for_tenant(tenant_dict, rates)

    # add sales order range:
    tenant_dict['start'] = str(sales_order.start)
    tenant_dict['end'] = str(sales_order.end)

    return 200, tenant_dict


def regenerate_sales_order_range(tenant_id, start, end):
    """For all sales orders in a given range, generate sales order dicts,
       and return them."""
    session = Session()
    db = database.Database(session)
    rates = RatesFile(config.rates_config)

    valid_tenant = validate_tenant_id(tenant_id, session)
    if isinstance(valid_tenant, tuple):
        return valid_tenant

    sales_orders = db.get_sales_orders(tenant_id, start, end)

    tenants = []
    for sales_order in sales_orders:
        usage = db.usage(sales_order.start, sales_order.end, tenant_id)

        # Transform the query result into a billable dict.
        tenant_dict = build_tenant_dict(valid_tenant, usage, db)
        tenant_dict = add_costs_for_tenant(tenant_dict, rates)

        # add sales order range:
        tenant_dict['start'] = str(sales_order.start)
        tenant_dict['end'] = str(sales_order.end)

        tenants.append(tenant_dict)

    return 200, tenants


@app.route("sales_order", methods=["POST"])
@require_admin
@json_must()
@returns_json
def run_sales_order_generation():
    """Generates a sales order for the given tenant.
       -end: a given end date, or uses default"""
    tenant_id = flask.request.json.get("tenant", None)
    end = flask.request.json.get("end", None)
    if not end:
        # Today, the beginning of.
        end = datetime.utcnow().\
            replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        try:
            end = datetime.strptime(end, iso_date)
        except ValueError:
            return 400, {"errors": ["'end' date given needs to be in format:" +
                                    " y-m-d"]}

    return generate_sales_order(False, tenant_id, end)


@app.route("sales_draft", methods=["POST"])
@require_admin
@json_must()
@returns_json
def run_sales_draft_generation():
    """Generates a sales draft for the given tenant.
       -end: a given end datetime, or uses default"""
    tenant_id = flask.request.json.get("tenant", None)
    end = flask.request.json.get("end", None)

    if not end:
        end = datetime.utcnow()
    else:
        try:
            end = datetime.strptime(end, iso_date)
        except ValueError:
            try:
                end = datetime.strptime(end, iso_time)
            except ValueError:
                return 400, {
                    "errors": ["'end' date given needs to be in format: " +
                               "y-m-d, or y-m-dTH:M:S"]}

    return generate_sales_order(True, tenant_id, end)


@app.route("sales_historic", methods=["POST"])
@require_admin
@json_must()
@returns_json
def run_sales_historic_generation():
    """Returns the sales order that intersects with the given target date.
       -target: a given target date"""
    tenant_id = flask.request.json.get("tenant", None)
    target = flask.request.json.get("date", None)

    if target is not None:
        try:
            target = datetime.strptime(target, iso_date)
        except ValueError:
            return 400, {"errors": ["date given needs to be in format: " +
                                    "y-m-d"]}
    else:
        return 400, {"missing parameter": {"date": "target date in format: " +
                                           "y-m-d"}}

    return regenerate_sales_order(tenant_id, target)


@app.route("sales_range", methods=["POST"])
@require_admin
@json_must()
@returns_json
def run_sales_historic_range_generation():
    """Returns the sales orders that intersect with the given date range.
       -start: a given start for the range.
       -end: a given end for the range, defaults to now."""
    tenant_id = flask.request.json.get("tenant", None)
    start = flask.request.json.get("start", None)
    end = flask.request.json.get("end", None)

    try:
        if start is not None:
            start = datetime.strptime(start, iso_date)
        else:
            return 400, {"missing parameter": {"start": "start date" +
                                               " in format: y-m-d"}}
        if end is not None:
                end = datetime.strptime(end, iso_date)
        else:
            end = datetime.utcnow()
    except ValueError:
            return 400, {"errors": ["dates given need to be in format: " +
                                    "y-m-d"]}

    return regenerate_sales_order_range(tenant_id, start, end)


if __name__ == '__main__':
    pass
