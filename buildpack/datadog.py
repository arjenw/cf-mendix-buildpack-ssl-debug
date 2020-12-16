import logging
import os
import shutil
import socket
import subprocess
from distutils.util import strtobool

import backoff
import yaml

from buildpack import util
from buildpack.databroker import is_enabled as is_databroker_enabled
from buildpack.databroker import is_producer_app as is_databroker_producer_app
from buildpack.databroker.config_generator.scripts.generators import (
    jmx as jmx_cfg_generator,
)
from buildpack.databroker.config_generator.scripts.utils import write_file
from buildpack.databroker.config_generator.templates.jmx import consumer
from buildpack.runtime_components import database

NAMESPACE = "datadog"

SIDECAR_VERSION = "v0.22.2"
SIDECAR_ARTIFACT_NAME = "cf-datadog-sidecar-{}.tar.gz".format(SIDECAR_VERSION)
SIDECAR_URL_ROOT = "/mx-buildpack/{}".format(NAMESPACE)
JAVA_AGENT_VERSION = "0.68.0"
JAVA_AGENT_ARTIFACT_NAME = "dd-java-agent-{}.jar".format(JAVA_AGENT_VERSION)
JAVA_AGENT_URL_ROOT = "/mx-buildpack/{}".format(NAMESPACE)

ROOT_DIR = os.path.abspath(".local")
SIDECAR_ROOT_DIR = os.path.join(ROOT_DIR, NAMESPACE)
AGENT_DIR = os.path.join(SIDECAR_ROOT_DIR, "datadog")
AGENT_USER_CHECKS_DIR = os.path.abspath("/home/vcap/app/datadog_integrations")

LOGS_PORT = 9032

# Returns the directory additional agent checks can be put into
# Can be used by other modules to set up e.g. JMX checks
def get_user_checks_dir():
    os.makedirs(AGENT_USER_CHECKS_DIR, exist_ok=True)
    return AGENT_USER_CHECKS_DIR


# Returns the Datadog API key in use
def get_api_key():
    return os.getenv("DD_API_KEY")


# Returns whether Datadog is enabled
def is_enabled():
    return get_api_key() is not None


def _is_tracing_enabled():
    return strtobool(os.environ.get("DD_TRACE_ENABLED", "false"))


def _is_logs_redaction_enabled():
    return strtobool(os.environ.get("DATADOG_LOGS_REDACTION", "true"))


def _is_installed():
    return os.path.exists(AGENT_DIR)


def _get_service():
    dd_service_name = os.environ.get("DD_SERVICE_NAME")
    if dd_service_name:
        return dd_service_name
    else:
        service_from_tags = _get_service_from_tags()
        if service_from_tags:
            return service_from_tags
    return util.get_app_from_domain()


def _get_service_from_tags():
    service_tags = sorted(
        list(filter(lambda x: "service:" or "app:" in x, util.get_tags())),
        reverse=True,
    )
    if service_tags:
        return service_tags[0].split(":")[1]
    return None


# Appends user tags with mandatory tags if required
def _get_datadog_tags():
    tags = util.get_tags()

    if not _get_service_from_tags():
        # app and / or service tag not set
        tags.append("service:{}".format(util.get_app_from_domain()))

    return ",".join(tags)


def _get_statsd_port(is_telegraf_enabled):
    if is_telegraf_enabled:
        # Telegraf is already enabled
        return 18125
    else:
        return 8125


def _enable_dd_java_agent(m2ee):
    if _is_tracing_enabled():
        jar = os.path.join(SIDECAR_ROOT_DIR, JAVA_AGENT_ARTIFACT_NAME)

        # Check if already configured
        if 0 in [
            v.find("-javaagent:{}".format(jar))
            for v in m2ee.config._conf["m2ee"]["javaopts"]
        ]:
            return

        # Extend with tracing options
        m2ee.config._conf["m2ee"]["javaopts"].extend(
            [
                "-javaagent:{}".format(jar),
                "-D{}={}".format("dd.service", _get_service()),
                "-D{}={}".format("dd.logs.injection", "true"),
                "-D{}={}".format("dd.jmxfetch.enabled", "false"),
            ]
        )

        # Extend with database service mapping
        dbconfig = database.get_config()
        if dbconfig:
            m2ee.config._conf["m2ee"]["javaopts"].extend(
                [
                    "-D{}={}".format(
                        "dd.service.mapping",
                        "{}:{}.db".format(
                            dbconfig["DatabaseType"].lower(), _get_service()
                        ),
                    ),
                ]
            )


def _is_database_diskstorage_enabled():
    return strtobool(
        os.environ.get("DD_ENABLE_DATABASE_DISKSTORAGE_CHECK", "true")
    )


def _set_up_database_diskstorage():
    # Enables the Mendix database diskstorage check
    # This check is a very dirty workaround
    # and makes an environment variable into a gauge with a fixed value.
    if _is_database_diskstorage_enabled():
        with open(
            os.path.join(get_user_checks_dir(), "mx_database_diskstorage.yml"),
            "w",
        ) as fh:
            config = {
                "init_config": {},
                "instances": [{"min_collection_interval": 15}],
            }
            fh.write(yaml.safe_dump(config))


def _set_up_jmx(statsd_port, extra_jmx_instance_config=None):
    jmx_check_conf_dir = os.path.join(get_user_checks_dir(), "jmx.d")
    jmx_check_conf_file = os.path.join(jmx_check_conf_dir, "conf.yaml")

    # JMX beans and values can be inspected with jmxterm
    # Download the jmxterm jar into the container
    # and run app/.local/bin/java -jar ~/jmxterm.jar
    #
    # The extra attributes are only available from Mendix 7.15.0+
    config = {
        "init_config": {"collect_default_metrics": True, "is_jmx": True},
        "instances": [
            {
                "host": "localhost",
                "port": 7845,
                "java_bin_path": str(os.path.abspath(".local/bin/java")),
                "java_options": "-Xmx50m -Xms5m",
                "reporter": "statsd:localhost:{}".format(statsd_port),
                "refresh_beans": 120,  # runtime takes time to initialize the beans
                "conf": [
                    {
                        "include": {
                            "bean": "com.mendix:type=SessionInformation",
                            # NamedUsers = 1;
                            # NamedUserSessions = 0;
                            # AnonymousSessions = 0;
                            "attribute": {
                                "NamedUsers": {"metrics_type": "gauge"},
                                "NamedUserSessions": {"metrics_type": "gauge"},
                                "AnonymousSessions": {"metrics_type": "gauge"},
                            },
                        }
                    },
                    {
                        "include": {
                            "bean": "com.mendix:type=Statistics,name=DataStorage",
                            # Selects = 1153;
                            # Inserts = 1;
                            # Updates = 24;
                            # Deletes = 0;
                            # Transactions = 25;
                            "attribute": {
                                "Selects": {"metrics_type": "counter"},
                                "Updates": {"metrics_type": "counter"},
                                "Inserts": {"metrics_type": "counter"},
                                "Deletes": {"metrics_type": "counter"},
                                "Transactions": {"metrics_type": "counter"},
                            },
                        }
                    },
                    {
                        "include": {
                            "bean": "com.mendix:type=General",
                            # Languages = en_US;
                            # Entities = 24;
                            "attribute": {
                                "Entities": {"metrics_type": "gauge"}
                            },
                        }
                    },
                    {
                        "include": {
                            "bean": "com.mendix:type=JettyThreadPool",
                            # Threads = 8
                            # IdleThreads = 3;
                            # IdleTimeout = 60000;
                            # MaxThreads = 254;
                            # StopTimeout = 30000;
                            # MinThreads = 8;
                            # ThreadsPriority = 5;
                            # QueueSize = 0;
                            "attribute": {
                                "Threads": {"metrics_type": "gauge"},
                                "MaxThreads": {"metrics_type": "gauge"},
                                "IdleThreads": {"metrics_type": "gauge"},
                                "QueueSize": {"metrics_type": "gauge"},
                            },
                        }
                    },
                ],
                #  }, {
                #    'include': {
                #        'bean': 'com.mendix:type=Jetty',
                #        # ConnectedEndPoints = 0;
                #        # IdleTimeout = 30000;
                #        # RequestsActiveMax = 0;
                #        'attribute': {
                #        }
                #    },
            }
        ],
    }

    # This section should be moved to the Databroker module itself
    if is_databroker_enabled():
        if is_databroker_producer_app():
            jmx_check_conf_dir = os.path.join(get_user_checks_dir(), "jmx_1.d")

            # kafka connect cfg
            os.makedirs(
                os.path.join(get_user_checks_dir(), "jmx_2.d"), exist_ok=True
            )
            kafka_connect_cfg = (
                jmx_cfg_generator.generate_kafka_connect_jmx_config()
            )
            write_file(
                os.path.join(get_user_checks_dir(), "jmx_2.d", "conf.yaml"),
                kafka_connect_cfg,
            )

            # kafka streams cfg
            os.makedirs(
                os.path.join(get_user_checks_dir(), "jmx_3.d"), exist_ok=True
            )
            kafka_streams_cfg = (
                jmx_cfg_generator.generate_kafka_streams_jmx_config()
            )
            write_file(
                os.path.join(get_user_checks_dir(), "jmx_3.d", "conf.yaml"),
                kafka_streams_cfg,
            )
        else:
            extra_jmx_instance_config = consumer.jmx_metrics

    # Merge extra instance configuration
    if extra_jmx_instance_config:
        config["instances"][0]["conf"].extend(extra_jmx_instance_config)

    # Write JMX configuration file
    os.makedirs(jmx_check_conf_dir, exist_ok=True)
    with open(jmx_check_conf_file, "w") as fh:
        fh.write(yaml.safe_dump(config))


def _set_up_postgres(is_telegraf_enabled):
    if is_telegraf_enabled:
        return
    # TODO: set up a way to disable this, on shared database (mxapps.io) we
    # don't want to allow this.
    if not util.i_am_primary_instance():
        return
    dbconfig = database.get_config()
    if dbconfig:
        for k in (
            "DatabaseType",
            "DatabaseUserName",
            "DatabasePassword",
            "DatabaseHost",
        ):
            if k not in dbconfig:
                logging.warning(
                    "Skipping database configuration for Datadog because "
                    "configuration is not found. See database_config.py "
                    "for details"
                )
                return
        if dbconfig["DatabaseType"] != "PostgreSQL":
            return

        postgres_check_conf_dir = os.path.join(
            get_user_checks_dir, "postgres.d"
        )
        os.makedirs(postgres_check_conf_dir, exist_ok=True)
        with open(
            os.path.join(postgres_check_conf_dir, "conf.yaml"), "w"
        ) as fh:
            config = {
                "init_config": {},
                "instances": [
                    {
                        "host": dbconfig["DatabaseHost"].split(":")[0],
                        "port": int(dbconfig["DatabaseHost"].split(":")[1]),
                        "username": dbconfig["DatabaseUserName"],
                        "password": dbconfig["DatabasePassword"],
                        "dbname": dbconfig["DatabaseName"],
                    }
                ],
            }
            fh.write(yaml.safe_dump(config))


def _set_up_environment(statsd_port):

    # Trace variables need to be set in the global environment
    # since the Datadog Java Trace Agent does not live inside the Datadog Agent process

    e = dict(os.environ.copy())

    # Everything in datadog.yaml can be configured with environment variables
    # This is the "official way" of working with the DD buildpack, so let's do this to ensure forward compatibility
    e["DD_API_KEY"] = get_api_key()
    e["DD_HOSTNAME"] = util.get_hostname()

    # Explicitly turn off tracing to ensure backward compatibility
    if not _is_tracing_enabled():
        e["DD_TRACE_ENABLED"] = "false"
    e["DD_LOGS_ENABLED"] = "true"
    e["DD_LOG_FILE"] = "/dev/null"
    e["DD_PROCESS_CONFIG_LOG_FILE"] = "/dev/null"
    e["DD_DOGSTATSD_PORT"] = str(statsd_port)

    # Transform and append tags
    e["DD_TAGS"] = _get_datadog_tags()
    if "TAGS" in e:
        del e["TAGS"]

    # Set Mendix Datadog sidecar specific environment variables
    e["DD_ENABLE_USER_CHECKS"] = "true"
    e["DATADOG_DIR"] = str(AGENT_DIR)

    return e


def update_config(
    m2ee, is_telegraf_enabled=False, extra_jmx_instance_config=None
):
    if (
        not is_enabled()
        or not _is_installed()
        or m2ee.config.get_runtime_version() < 7.14
    ):
        return

    # Set up JVM JMX
    m2ee.config._conf["m2ee"]["javaopts"].extend(
        [
            "-Dcom.sun.management.jmxremote",
            "-Dcom.sun.management.jmxremote.port=7845",
            "-Dcom.sun.management.jmxremote.local.only=true",
            "-Dcom.sun.management.jmxremote.authenticate=false",
            "-Dcom.sun.management.jmxremote.ssl=false",
            "-Djava.rmi.server.hostname=127.0.0.1",
        ]
    )

    # Set up runtime logging
    if m2ee.config.get_runtime_version() >= 7.15:
        m2ee.config._conf["logging"].append(
            {
                "type": "tcpjsonlines",
                "name": "DatadogSubscriber",
                "autosubscribe": "INFO",
                "host": "localhost",
                # For MX8 integer is supported again, this change needs to be
                # made when MX8 is GA
                "port": str(LOGS_PORT),
            }
        )

    # Experimental: enable Datadog Java Trace Agent
    # if tracing is explicitly enabled
    _enable_dd_java_agent(m2ee)

    # Set up Mendix checks (database diskstorage and logging)
    _set_up_database_diskstorage()
    logs_check_conf_dir = os.path.join(get_user_checks_dir(), "mendix.d")
    os.makedirs(logs_check_conf_dir, exist_ok=True)
    with open(os.path.join(logs_check_conf_dir, "conf.yaml"), "w") as fh:
        config = {
            "logs": [
                {
                    "type": "tcp",
                    "port": str(LOGS_PORT),
                    "source": "mendix",
                    "service": _get_service(),
                }
            ]
        }

        if _is_logs_redaction_enabled():
            logging.info(
                "Datadog logs redaction enabled, all email addresses will be redacted"
            )
            log_processing_rules = {
                "log_processing_rules": [
                    {
                        "type": "mask_sequences",
                        "name": "RFC_5322_email",
                        "pattern": r"""(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])""",
                        "replace_placeholder": "[EMAIL REDACTED]",
                    }
                ]
            }
            config["logs"][0] = {**config["logs"][0], **log_processing_rules}

        fh.write(yaml.safe_dump(config))

    # Set up embedded checks
    _set_up_jmx(
        statsd_port=_get_statsd_port(is_telegraf_enabled),
        extra_jmx_instance_config=extra_jmx_instance_config,
    )
    _set_up_postgres(is_telegraf_enabled)


def _download(build_path, cache_dir):
    util.download_and_unpack(
        util.get_blobstore_url(
            "{}/{}".format(SIDECAR_URL_ROOT, SIDECAR_ARTIFACT_NAME)
        ),
        os.path.join(build_path, NAMESPACE),
        cache_dir=cache_dir,
    )
    util.download_and_unpack(
        util.get_blobstore_url(
            "{}/{}".format(JAVA_AGENT_URL_ROOT, JAVA_AGENT_ARTIFACT_NAME)
        ),
        os.path.join(build_path, NAMESPACE),
        cache_dir=cache_dir,
        unpack=False,
    )


def _copy_files(buildpack_path, build_path):
    file_name = "mx_database_diskstorage.py"
    shutil.copyfile(
        os.path.join(buildpack_path, "etc", NAMESPACE, "checks.d", file_name),
        os.path.join(
            build_path,
            NAMESPACE,
            "datadog",
            "etc",
            "datadog-agent",
            "checks.d",
            file_name,
        ),
    )


def stage(buildpack_path, build_path, cache_path):
    if not is_enabled():
        return

    _download(build_path, cache_path)
    _copy_files(buildpack_path, build_path)


def run(runtime_version, is_telegraf_enabled=False):
    if not is_enabled():
        return

    if runtime_version < 7.14:
        logging.warning(
            "Datadog integration requires Mendix 7.14 or newer. "
            "The Datadog Agent is not enabled."
        )
        return

    if not _is_installed():
        logging.warning(
            "Datadog Agent isn't installed yet but DD_API_KEY is set."
            "Please push or restage your app to complete Datadog installation."
        )
        return

    # Start the run script "borrowed" from the official DD buildpack
    # and include settings as environment variables
    agent_environment = _set_up_environment(
        _get_statsd_port(is_telegraf_enabled)
    )
    logging.debug(
        "Datadog Agent environment variables: [{}]".format(agent_environment)
    )

    logging.info("Starting Datadog Agent...")
    subprocess.Popen(
        os.path.join(AGENT_DIR, "run-datadog.sh"), env=agent_environment
    )

    # The runtime does not handle a non-open logs endpoint socket
    # gracefully, so wait until it's up
    @backoff.on_predicate(backoff.expo, lambda x: x > 0, max_time=10)
    def _await_logging_endpoint():
        return socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect_ex(
            ("localhost", LOGS_PORT)
        )

    logging.info("Awaiting Datadog Agent log subscriber...")
    if _await_logging_endpoint() == 0:
        logging.info("Datadog Agent log subscriber is ready")
    else:
        logging.error(
            "Datadog Agent log subscriber was not initialized correctly."
            "Application logs will not be shipped to Datadog."
        )
