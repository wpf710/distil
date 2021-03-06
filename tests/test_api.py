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

from webtest import TestApp
from . import test_interface, helpers, constants
from distil.api import web
from distil.api.web import get_app
from distil import models
from distil import interface
from distil.helpers import convert_to
from distil.constants import dawn_of_time
from datetime import datetime
from decimal import Decimal
import unittest
import json
import mock


class TestApi(test_interface.TestInterface):

    def setUp(self):
        super(TestApi, self).setUp()
        self.app = TestApp(get_app(constants.config))

    def tearDown(self):
        super(TestApi, self).tearDown()
        self.app = None

    @unittest.skip
    def test_usage_run_for_all(self):
        """Asserts a usage run generates data for all tenants"""

        usage = helpers.get_usage(self.start, self.end)

        with mock.patch('distil.interface.Interface') as Interface:

            tenants = []

            for tenant in constants.TENANTS:
                t = mock.Mock(spec=interface.Tenant)
                t.usage.return_value = usage
                t.conn = mock.Mock()
                t.tenant = tenant
                t.id = tenant['id']
                t.name = tenant['name']
                t.description = tenant['description']
                tenants.append(t)

            ceil_interface = mock.Mock(spec=interface.Interface)

            ceil_interface.tenants = tenants

            Interface.return_value = ceil_interface

            # patch to mock out the novaclient call
            with mock.patch('distil.helpers.flavor_name') as flavor_name:
                flavor_name.side_effect = lambda x: x

                resp = self.app.post("/collect_usage")
                self.assertEquals(resp.status_int, 200)

                tenants = self.session.query(models.Tenant)
                self.assertTrue(tenants.count() > 0)

                usages = self.session.query(models.UsageEntry)
                self.assertTrue(usages.count() > 0)
                resources = self.session.query(models.Resource)

                self.assertEquals(resources.count(), len(usage.values()))

    def test_sales_run_for_all(self):
        """Assertion that a sales run generates all tenant orders"""
        numTenants = 7
        numResources = 5

        now = datetime.utcnow().\
            replace(hour=0, minute=0, second=0, microsecond=0)

        helpers.fill_db(self.session, numTenants, numResources, now)

        for i in range(numTenants):
            resp = self.app.post("/sales_order",
                                 params=json.dumps({"tenant": "tenant_id_" +
                                                    str(i)}),
                                 content_type='application/json')
            resp_json = json.loads(resp.body)
            print resp_json

            query = self.session.query(models.SalesOrder)
            self.assertEquals(query.count(), i + 1)

            self.assertEquals(len(resp_json['resources']), numResources)

    def test_sales_run_single(self):
        """Assertion that a sales run generates one tenant only"""
        numTenants = 5
        numResources = 5

        now = datetime.utcnow().\
            replace(hour=0, minute=0, second=0, microsecond=0)
        helpers.fill_db(self.session, numTenants, numResources, now)
        resp = self.app.post("/sales_order",
                             params=json.dumps({"tenant": "tenant_id_0"}),
                             content_type="application/json")
        resp_json = json.loads(resp.body)

        query = self.session.query(models.SalesOrder)
        self.assertEquals(query.count(), 1)
        # todo: assert things in the response
        self.assertEquals(len(resp_json['resources']), numResources)

    def test_sales_raises_400(self):
        """Assertion that 400 is being thrown if content is not json."""
        resp = self.app.post("/sales_order", expect_errors=True)
        self.assertEquals(resp.status_int, 400)

    def test_sales_order_no_tenant_found(self):
        """Test that if a tenant is provided and not found,
        then we throw an error."""
        resp = self.app.post('/sales_order',
                             expect_errors=True,
                             params=json.dumps({'tenant': 'bogus tenant'}),
                             content_type='application/json')
        self.assertEquals(resp.status_int, 400)

    def test_tenant_dict(self):
        """Checking that the tenant dictionary is built correctly
           based on given entry data."""
        num_resources = 3
        num_services = 2
        volume = 5

        entries = helpers.create_usage_entries(num_resources,
                                               num_services, volume)

        tenant = mock.MagicMock()
        tenant.name = "tenant_1"
        tenant.id = "tenant_id_1"

        db = mock.MagicMock()
        db.get_resource_metadata.return_value = {}

        tenant_dict = web.build_tenant_dict(tenant, entries, db)

        self.assertEquals(len(tenant_dict['resources']), num_resources)
        self.assertEquals(tenant_dict['tenant_id'], "tenant_id_1")
        self.assertEquals(tenant_dict['name'], "tenant_1")

        for resource in tenant_dict['resources'].values():
            for service in resource['services']:
                self.assertEquals(service['volume'], volume)

    def test_tenant_dict_no_entries(self):
        """Test to ensure that the function handles an
           empty list of entries correctly."""
        entries = []

        tenant = mock.MagicMock()
        tenant.name = "tenant_1"
        tenant.id = "tenant_id_1"

        db = mock.MagicMock()
        db.get_resource_metadata.return_value = {}

        tenant_dict = web.build_tenant_dict(tenant, entries, db)

        self.assertEquals(len(tenant_dict['resources']), 0)
        self.assertEquals(tenant_dict['tenant_id'], "tenant_id_1")
        self.assertEquals(tenant_dict['name'], "tenant_1")

    def test_add_cost_to_tenant(self):
        """Checking that the rates are applied correctly,
           and that we get correct total values."""
        volume = 3600
        rate = {'rate': Decimal(0.25), 'unit': 'hour'}

        test_tenant = {
            'resources': {
                'resouce_ID_1': {
                    'services': [{'name': 'service_1',
                                  'volume': Decimal(volume),
                                  'unit': 'second'},
                                 {'name': 'service_2',
                                  'volume': Decimal(volume),
                                  'unit': 'second'}]
                },
                'resouce_ID_2': {
                    'services': [{'name': 'service_1',
                                  'volume': Decimal(volume),
                                  'unit': 'second'},
                                 {'name': 'service_2',
                                  'volume': Decimal(volume),
                                  'unit': 'second'}]
                }
            }
        }

        service_cost = round(
            convert_to(volume, 'second', rate['unit']) * rate['rate'], 2)
        total_cost = service_cost * 4

        ratesManager = mock.MagicMock()
        ratesManager.rate.return_value = rate

        tenant_dict = web.add_costs_for_tenant(test_tenant, ratesManager)

        self.assertEquals(tenant_dict['total_cost'], str(total_cost))
        for resource in tenant_dict['resources'].values():
            self.assertEquals(resource['total_cost'], str(service_cost * 2))
            for service in resource['services']:
                self.assertEquals(service['volume'],
                                  str(convert_to(volume, 'second',
                                                 rate['unit'])))
                self.assertEquals(service['unit'], rate['unit'])
                self.assertEquals(service['cost'], str(service_cost))

    def test_add_cost_to_empty_tenant(self):
        """An empty tenant should not be charged anything,
           nor cause errors."""

        empty_tenant = {'resources': {}}

        ratesManager = mock.MagicMock()

        tenant_dict = web.add_costs_for_tenant(empty_tenant, ratesManager)

        self.assertEquals(tenant_dict['total_cost'], str(0))

    def test_get_last_collected(self):
        """test to ensure last collected api call returns correctly"""
        now = datetime.utcnow()
        self.session.add(models._Last_Run(last_run=now))
        self.session.commit()
        resp = self.app.get("/last_collected")
        resp_json = json.loads(resp.body)
        self.assertEquals(resp_json['last_collected'], str(now))

    def test_get_last_collected_default(self):
        """test to ensure last collected returns correct default value"""
        resp = self.app.get("/last_collected")
        resp_json = json.loads(resp.body)
        self.assertEquals(resp_json['last_collected'], str(dawn_of_time))
