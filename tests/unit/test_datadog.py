import os
import json

from buildpack import datadog


class TestCaseDatadogUtilFunctions:
    def test_get_service(self):

        tags_cases = [
            (["app:testapp", "service:testservice"], "testservice"),
            (
                [
                    "app:testapp",
                    "service:testservice2",
                    "service:testservice",
                    "app:testapp2",
                ],
                "testservice2",
            ),
            (["app:testapp"], "testapp"),
            (["service:testservice"], "testservice"),
            ([], "app"),
        ]

        for (tags, outcome) in tags_cases:
            os.environ["TAGS"] = json.dumps(tags)
            assert datadog._get_service() == outcome
