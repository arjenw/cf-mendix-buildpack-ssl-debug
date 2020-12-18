# This module adds the Telegraf metrics agent to an app container.
# This metrics agent collects StatsD events from Java agents injected into the runtime.
# Additionally, if enabled, PostgreSQL metrics are collected.
# Metrics will be forwarded to either the host defined in APPMETRICS_TARGET environment,
# or to other outputs such as Datadog.

import base64
import json
import logging
import os
import subprocess
from distutils.util import strtobool

from buildpack import datadog, util
from buildpack.runtime_components import database

VERSION = "1.16.3"
NAMESPACE = "telegraf"
INSTALL_PATH = os.path.join(os.path.abspath(".local"), NAMESPACE)
EXECUTABLE_PATH = os.path.join(
    INSTALL_PATH, "telegraf-{}".format(VERSION), "usr", "bin", "telegraf"
)
CONFIG_FILE_PATH = os.path.join(
    INSTALL_PATH,
    "telegraf-{}".format(VERSION),
    "etc",
    "telegraf",
    "telegraf.conf",
)
STATSD_PORT = 8125

# APPMETRICS_TARGET is a variable which includes JSON (single or array) with the following values:
# - url: complete url of the endpoint. Mandatory.
# - username: basic auth username. Optional.
# - password: basic auth password. Mandatory if username is specified.
# - kpionly: indicates that by default only metrics with KPI=true tag
#            are passed to custom end points
#
# Examples:
# {
#  "url": "https://customreceiver.mydomain.com",
#  "username": "user",
#  "password": "secret",
#  "kpionly": true
# }
# [{
#  "url": "https://customreceiver.mydomain.com",
#  "username": "user",
#   "password": "secret",
#   "kpionly": true
# }]
def _get_appmetrics_target():
    return os.getenv("APPMETRICS_TARGET")


def include_db_metrics():
    return strtobool(os.getenv("APPMETRICS_INCLUDE_DB", "true"))


def is_enabled():
    return _get_appmetrics_target() is not None or datadog.is_enabled()


def _is_installed():
    return os.path.exists(INSTALL_PATH)


def get_tags():
    # Tags are strings in a JSON array and must be in key:value format
    tags = []
    try:
        tags = json.loads(os.getenv("TAGS", "[]"))
    except ValueError:
        logging.warning(
            "Invalid TAGS set. Please check if it is a valid JSON array."
        )

    result = {}
    for kv in [t.split(":") for t in tags]:
        if len(kv) == 2:
            result[kv[0]] = kv[1]
        else:
            logging.warning(
                "Skipping tag [{}] from TAGS: not in key:value format".format(
                    kv[0]
                )
            )
    return result


def get_statsd_port():
    return STATSD_PORT


def _config_value_str(value):
    if type(value) is str:
        return '"%s"' % value
    elif type(value) is int:
        return value
    elif type(value) is bool:
        return str(value).lower()
    elif type(value) is list:
        return json.dumps(value)


def _create_config_file(agent_config):
    logging.debug("writing config file")
    with open(CONFIG_FILE_PATH, "w") as tc:
        print("[agent]", file=tc)
        for item in agent_config:
            value = agent_config[item]
            print("  {} = {}".format(item, _config_value_str(value)), file=tc)

        print("", file=tc)


def _write_config(section, config):
    logging.debug("writing section {}".format(section))
    with open(CONFIG_FILE_PATH, "a") as tc:
        _write_config_in_fd(section, config, tc)


def _write_config_in_fd(section, config, fd, indent=""):
    print("{}{}".format(indent, section), file=fd)
    # reverse sort to get '[section]' in last
    for item in sorted(config, reverse=True):
        value = config[item]
        if type(value) is dict:
            _write_config_in_fd(item, value, fd, "{}  ".format(indent))
        else:
            print(
                "{}  {} = {}".format(indent, item, _config_value_str(value)),
                file=fd,
            )

    print("", file=fd)


def _write_http_output_config(http_config):
    logging.debug("writing http output config")
    if "url" not in http_config:
        logging.error(
            "APPMETRICS_TARGET.url value is not defined in {}".format(
                _get_appmetrics_target()
            )
        )
        return

    http_output = {
        "url": http_config["url"],
        "method": "POST",
        "data_format": "influx",
        "timeout": "30s",
    }

    username = http_config.get("username")
    password = http_config.get("password")
    if username:
        # Workaround for https://github.com/influxdata/telegraf/issues/4544
        # http_output['username'] = username
        # http_output['password'] = password
        credentials = base64.b64encode(
            ("{}:{}".format(username, password)).encode()
        ).decode("ascii")
        http_output["[outputs.http.headers]"] = {
            "Authorization": "Basic {}".format(credentials)
        }

    kpionly = http_config["kpionly"] if "kpionly" in http_config else True
    if kpionly:
        http_output["[outputs.http.tagpass]"] = {"KPI": ["true"]}

    _write_config("[[outputs.http]]", http_output)


def update_config(m2ee, app_name):
    if not is_enabled() or not _is_installed():
        return

    # Telegraf config, taking over defaults from telegraf.conf from the distro
    logging.debug("Creating Telegraf config...")
    _create_config_file(
        {
            "interval": "10s",
            "round_interval": True,
            "metric_batch_size": 1000,
            "metric_buffer_limit": 10000,
            "collection_jitter": "0s",
            "flush_interval": "10s",
            "flush_jitter": "5s",
            "precision": "",
            "debug": False,
            "logfile": "",
            "hostname": util.get_hostname(),
            "omit_hostname": False,
        }
    )

    _write_config("[global_tags]", get_tags())
    _write_config(
        "[[inputs.statsd]]",
        {
            "protocol": "udp",
            "max_tcp_connections": 250,
            "tcp_keep_alive": False,
            "service_address": ":{}".format(get_statsd_port()),
            "delete_gauges": True,
            "delete_counters": True,
            "delete_sets": True,
            "delete_timings": True,
            "percentiles": [90],
            "metric_separator": ".",
            "datadog_extensions": True,
            "allowed_pending_messages": 10000,
            "percentile_limit": 1000,
        },
    )

    # Configure PostgreSQL input plugin
    if include_db_metrics() or datadog.is_enabled():
        db_config = database.get_config()
        if db_config and db_config["DatabaseType"] == "PostgreSQL":
            # TODO: check if we need to change the metric name for Datadog compatibility
            return _write_config(
                "[[inputs.postgresql]]",
                {
                    "address": "postgres://{}:{}@{}/{}".format(
                        db_config["DatabaseUserName"],
                        db_config["DatabasePassword"],
                        db_config["DatabaseHost"],
                        db_config["DatabaseName"],
                    )
                },
            )

    # Forward metrics also to Datadog when enabled
    if datadog.is_enabled():
        # TODO: write to correct API endpoint and / or write to DogStatsD agent
        logging.info("Enabling metrics forwarding to Datadog...")
        _write_config(
            "[[outputs.datadog]]",
            {
                "apikey": datadog.get_api_key(),
                "url": "{}series/".format(datadog.get_api_url()),
            },
        )

    # Write HTTP_outputs (one or array)
    if _get_appmetrics_target():
        try:
            http_configs = json.loads(_get_appmetrics_target())
        except ValueError:
            logging.error(
                "Invalid APPMETRICS_TARGET set. Please check if it contains valid JSON. Telegraf will not forward metrics."
            )
        if type(http_configs) is list:
            for http_config in http_configs:
                _write_http_output_config(http_config)
        else:
            _write_http_output_config(http_configs)


def stage(build_path, cache_dir):
    if not is_enabled():
        return

    logging.debug("Staging the Telegraf metrics agent...")
    util.download_and_unpack(
        util.get_blobstore_url(
            "/mx-buildpack/telegraf/telegraf-{}_linux_amd64.tar.gz".format(
                VERSION
            )
        ),
        os.path.join(build_path, NAMESPACE),
        cache_dir=cache_dir,
    )


def run():
    if not is_enabled():
        return

    if not _is_installed():
        logging.warning(
            "Telegraf isn't installed yet. "
            "Please redeploy your application to "
            "complete Telegraf installation."
        )
        return

    logging.info("Starting the Telegraf metrics agent...")
    e = dict(os.environ)
    subprocess.Popen(
        (EXECUTABLE_PATH, "--config", CONFIG_FILE_PATH), env=e,
    )
