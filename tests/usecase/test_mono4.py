import basetest


class TestCaseMono4(basetest.BaseTest):

    def setUp(self):
        self.setUpCF('empty-model-7.0.2.mpk', env_vars={
            'DEPLOY_PASSWORD': self.mx_password,
        })
        self.startApp()

    def test_mono4(self):
        self.assert_app_running(self.app_name)
        self.assert_string_in_recent_logs(self.app_name, 'Selecting Mono Runtime: mono-4')
