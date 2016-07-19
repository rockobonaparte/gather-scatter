from agent_whitelist import AgentWhitelist
import unittest


class WhitelistTests(unittest.TestCase):
    def test_no_whitelist(self):
        whitelist = AgentWhitelist()
        self.assertTrue(whitelist.all_reported())

    def test_all_reported(self):
        whitelist = AgentWhitelist(["1", "2"])
        self.assertFalse(whitelist.all_reported())
        whitelist.add_reported("1")
        self.assertFalse(whitelist.all_reported())
        whitelist.add_reported("2")
        self.assertTrue(whitelist.all_reported())

    def test_extra_reported(self):
        whitelist = AgentWhitelist(["1"])
        self.assertFalse(whitelist.all_reported())
        whitelist.add_reported("1")
        self.assertTrue(whitelist.all_reported())
        whitelist.add_reported("2")
        self.assertTrue(whitelist.all_reported())


if __name__ == '__main__':
    unittest.main()
