import unittest

from digital_trade_foundation.storage import Database


class StorageTest(unittest.TestCase):
    def test_transaction_rolls_back_on_error(self):
        database = Database()
        with self.assertRaises(RuntimeError):
            with database.transaction():
                database.connection.execute(
                    "INSERT INTO organizations(organization_id,name,created_at) VALUES('o1','合作机构','now')"
                )
                raise RuntimeError("停止事务")
        count = database.connection.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]
        self.assertEqual(0, count)
        database.close()

    def test_schema_enables_foreign_keys(self):
        database = Database()
        enabled = database.connection.execute("PRAGMA foreign_keys").fetchone()[0]
        self.assertEqual(1, enabled)
        database.close()


if __name__ == "__main__":
    unittest.main()
