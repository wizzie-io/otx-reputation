#!/usr/bin/env bash
envsubst < /opt/reputation-otx/config/config_env.json > /opt/reputation-otx/config/config.json
/opt/reputation-otx/bin/otx-service-start.sh /opt/reputation-otx/config/config.json