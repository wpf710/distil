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

import unittest
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, create_session
from sqlalchemy.pool import NullPool
from sqlalchemy.exc import IntegrityError, OperationalError
from distil.models import Resource, Tenant, UsageEntry, SalesOrder
import datetime

from . import PG_DATABASE_URI, MY_DATABASE_URI


pg_engine = None
mysql_engine = None


def setUp():
    global mysql_engine
    mysql_engine = create_engine(MY_DATABASE_URI, poolclass=NullPool)
    global pg_engine
    pg_engine = create_engine(PG_DATABASE_URI, poolclass=NullPool)


def tearDown():
    pg_engine.dispose()
    mysql_engine.dispose()


class db(unittest.TestCase):

    __test__ = False

    def setUp(self):
        self.db = self.session()

    def tearDown(self):
        try:
            self.db.rollback()
        except:
            pass
        self.db.begin()
        for obj in (SalesOrder, UsageEntry, Resource, Tenant, Resource):
            self.db.query(obj).delete(synchronize_session="fetch")
        self.db.commit()
        # self.db.close()
        self.db.close()
        # self.session.close_all()
        self.db = None

    def test_create_tenant(self):
        self.db.begin()
        t = Tenant(id="asfd", name="test", created=datetime.datetime.utcnow(),
                   last_collected=datetime.datetime.utcnow())
        self.db.add(t)
        self.db.commit()
        t2 = self.db.query(Tenant).get("asfd")
        self.assertTrue(t2.name == "test")
        # self.db.commit()

    def test_create_resource(self):
        self.test_create_tenant()
        self.db.begin()
        t = self.db.query(Tenant).get("asfd")
        r = Resource(id="1234", tenant=t, created=datetime.datetime.utcnow())
        self.db.add(r)
        self.db.commit()
        r2 = self.db.query(Resource).filter(Resource.id == "1234")[0]
        self.assertTrue(r2.tenant.id == t.id)

    def test_insert_usage_entry(self):
        self.test_create_resource()
        self.db.begin()
        r = self.db.query(Resource).filter(Resource.id == "1234")[0]
        u = UsageEntry(service="cheese",
                       volume=1.23,
                       resource=r,
                       tenant=r,
                       start=(datetime.datetime.utcnow() -
                              datetime.timedelta(minutes=5)),
                       end=datetime.datetime.utcnow(),
                       created=datetime.datetime.utcnow())
        self.db.add(u)
        try:
            self.db.commit()
        except Exception as e:
            self.fail("Exception: %s" % e)

    def test_overlapping_usage_entry_fails(self):
        self.test_insert_usage_entry()
        try:
            self.test_insert_usage_entry()
            # we fail here
            self.fail("Inserted overlapping row; failing")
        except (IntegrityError, OperationalError):
            self.db.rollback()
            self.assertEqual(self.db.query(UsageEntry).count(), 1)

    def test_insert_salesorder(self):
        self.test_insert_usage_entry()
        self.db.begin()
        usage = self.db.query(UsageEntry)[0]
        tenant = self.db.query(Tenant).get("asfd")
        so = SalesOrder(tenant=tenant,
                        start=usage.start,
                        end=usage.end)
        self.db.add(so)
        self.db.commit()
        so2 = self.db.query(SalesOrder)[0]
        self.assertTrue(so2.tenant.id == so.tenant.id)
        self.assertTrue(so2.start == so.start)
        self.assertTrue(so2.end == so.end)

    def test_overlap_sales_order_fails(self):
        self.test_insert_salesorder()
        try:
            self.test_insert_salesorder()
            self.fail("Inserted twice")
        except (IntegrityError, OperationalError):
            self.db.rollback()
            self.assertEqual(self.db.query(SalesOrder).count(), 1)


class TestDatabaseModelsPostgres(db):

    __test__ = True
    session = scoped_session(lambda: create_session(bind=pg_engine))


class TestDatabaseModelsMysql(db):

    __test__ = True
    session = scoped_session(lambda: create_session(bind=mysql_engine))
