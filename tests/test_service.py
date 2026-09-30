import unittest
from datetime import datetime, timezone

from digital_trade_foundation.clock import FixedClock
from digital_trade_foundation.errors import ConflictError, PermissionDenied
from digital_trade_foundation.service import DomainService
from digital_trade_foundation.storage import Database


class ServiceTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.service = DomainService(self.database, FixedClock(datetime(2026, 9, 25, tzinfo=timezone.utc)))
        self.service.register_organization(request_id="org", actor_id="bootstrap", organization_id="o1", name="合作机构一")
        self.service.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="a1",
                                    display_name="管理员", role="admin", organization_id="o1")
        self.service.register_actor(request_id="operator", actor_id="a1", new_actor_id="op1",
                                    display_name="操作员", role="operator", organization_id="o1")
        self.service.register_actor(request_id="auditor", actor_id="a1", new_actor_id="au1",
                                    display_name="审计员", role="auditor", organization_id="o1")
        self.service.register_site(request_id="site", actor_id="op1", site_id="s1",
                                   organization_id="o1", name="业务节点", timezone_name="Asia/Shanghai")

    def tearDown(self):
        self.database.close()

    def test_same_request_replays_original_receipt(self):
        first = self.service.record_domain_data(request_id="data", actor_id="op1", site_id="s1",
                                                category="partner_profile", external_key="k1", data={"value": 1})
        second = self.service.record_domain_data(request_id="data", actor_id="op1", site_id="s1",
                                                 category="partner_profile", external_key="k1", data={"value": 1})
        self.assertFalse(first.replayed)
        self.assertTrue(second.replayed)
        self.assertEqual(first.resource_id, second.resource_id)

    def test_request_id_rejects_changed_payload(self):
        self.service.record_domain_data(request_id="data", actor_id="op1", site_id="s1",
                                        category="partner_profile", external_key="k1", data={"value": 1})
        with self.assertRaises(ConflictError):
            self.service.record_domain_data(request_id="data", actor_id="op1", site_id="s1",
                                            category="partner_profile", external_key="k1", data={"value": 2})

    def test_auditor_cannot_write_domain_data(self):
        with self.assertRaises(PermissionDenied):
            self.service.record_domain_data(request_id="blocked", actor_id="au1", site_id="s1",
                                            category="partner_profile", external_key="k1", data={"value": 1})


if __name__ == "__main__":
    unittest.main()
