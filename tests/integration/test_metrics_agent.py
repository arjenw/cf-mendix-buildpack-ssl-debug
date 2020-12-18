from tests.integration import basetest


class TestCaseTelegraf(basetest.BaseTest):
    def test_telegraf_running(self):
        self.stage_container(
            "BuildpackTestApp-mx-7-16.mda",
            env_vars={"APPMETRICS_TARGET": '{"url": "https://foo.bar/write"}'},
        )
        self.start_container()
        self.assert_app_running()
        self.assert_listening_on_port(8125, "telegraf")
