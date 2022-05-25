#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

DEFAULT_CONFIG_DIR="$HOME/.config/spicerack"
DEFAULT_COOKBOOK_CONFIG_PATH="$DEFAULT_CONFIG_DIR/cookbook.yaml"
DEFAULT_LOGS_DIR="/tmp/spicerack_logs"
DEFAULT_COOKBOOKS_DIR="$PWD"
DEFAULT_CUMIN_CONFIG_PATH="$DEFAULT_CONFIG_DIR/cumin.yaml"

help() {
    cat <<EOH
    Script to setup your cookbook/spicerack and cumin configuration to run from your laptop.
    Focused on WMCS cookbooks (but probably working for others too).

    Usage $0 [-h|--use-defaults]

    Options:
        -h
            Show this help

        --use-defaults
            If specified, it will not ask the user for configuration values and use the defaults.
            Defaults:
                DEFAULT_CONFIG_DIR=$DEFAULT_CONFIG_DIR
                DEFAULT_COOKBOOK_CONFIG_PATH=$DEFAULT_COOKBOOK_CONFIG_PATH
                DEFAULT_LOGS_DIR=$DEFAULT_LOGS_DIR
                DEFAULT_COOKBOOKS_DIR=$DEFAULT_COOKBOOKS_DIR
                DEFAULT_CUMIN_CONFIG_PATH=$DEFAULT_CUMIN_CONFIG_PATH

EOH
}


main() {
    local answer \
        config_dir \
        cookbook_config_path \
        logs_dir \
        cookbooks_dir \
        cumin_config_path \
        rewrite="no" \
        use_defaults="no"

    if [[ $# -eq 1 ]]; then
        if [[ "$1" == "--use-defaults" ]]; then
            use_defaults="yes"
        elif [[ "$1" == "-h" ]]; then
            help
            exit 0
        else
            echo "Unknown option/argument: $1"
            help
            exit 2
        fi
    elif [[ $# -gt 1 ]]; then
        echo "Only one argument is allowed."
        help
        exit 2
    fi

    if [[ "$use_defaults" == "no" ]]; then
        echo "What will be the directory for spicerack's config? [default: $DEFAULT_CONFIG_DIR]"
        read -r config_dir
        if [[ $config_dir = "" ]]; then
            config_dir="$DEFAULT_CONFIG_DIR"
        fi

        echo "What will be the path for cookbook.yaml config? [default: $DEFAULT_COOKBOOK_CONFIG_PATH]"
        read -r cookbook_config_path
        if [[ $cookbook_config_path = "" ]]; then
            cookbook_config_path=$DEFAULT_COOKBOOK_CONFIG_PATH
        fi

        echo "What will be the directory for sipcerack logs? [default: $DEFAULT_LOGS_DIR]"
        read -r logs_dir
        if [[ $logs_dir = "" ]]; then
            logs_dir="$DEFAULT_LOGS_DIR"
        fi

        echo "What will be the path for the cumin config file? [default: $DEFAULT_CUMIN_CONFIG_PATH]"
        read -r cumin_config_path
        if [[ $cumin_config_path = "" ]]; then
            cumin_config_path="$DEFAULT_CUMIN_CONFIG_PATH"
        fi

        echo "What will be the path where the cookbooks will be? [default: $DEFAULT_COOKBOOKS_DIR]"
        read -r cookbooks_dir
        if [[ $cookbooks_dir = "" ]]; then
            cookbooks_dir="$DEFAULT_COOKBOOKS_DIR"
        fi
    else
        config_dir="$DEFAULT_CONFIG_DIR"
        cookbook_config_path=$DEFAULT_COOKBOOK_CONFIG_PATH
        logs_dir="$DEFAULT_LOGS_DIR"
        cumin_config_path="$DEFAULT_CUMIN_CONFIG_PATH"
        cookbooks_dir="$DEFAULT_COOKBOOKS_DIR"
    fi

    [[ -e $config_dir ]] || mkdir -p "$config_dir"
    [[ -e $logs_dir ]] || mkdir -p "$logs_dir"

    rewrite="yes"
    if [[ -f $cookbook_config_path ]]; then
        echo "The configuration file $cookbook_config_path already exists, overwrite?[Ny] (Ctr+C to abort)"
        read -r answer
        if ! [[ $answer =~ [yY].* ]]; then
            rewrite="no"
            echo "Skipping $cookbook_config_path"
        fi
    fi
    if [[ $rewrite == "yes" ]]; then
        cat > "$cookbook_config_path" <<EOC
# Base path of the cookbooks. It's usually a checkout of a different repository that has all the cookbooks.
cookbooks_base_dir: $cookbooks_dir
# Base directory for cookbook's logs.
logs_base_dir:  $logs_dir
# [optional] Hostname and port to use for the special IRC logging using tcpircbot.
#tcpircbot_host: tcpircbot.example.com
#tcpircbot_port: 1234

# [optional] Key-value hash of additional parameters to pass to the Spicerack constructor. All keys are optional.
instance_params:
  cumin_config: $cumin_config_path  # Cumin's YAML configuration file.
#  conftool_config: /etc/conftool/config.yaml  # Conftool's YAML configuration file.
#  conftool_schema: /etc/conftool/schema.yaml  # Conftool's YAML schema file.
#  debmonitor_config: /etc/debmonitor.conf  # Debmonitor's INI configuration file.
  spicerack_config_dir: $config_dir
#  http_proxy: http://proxy.example.com:8080  # HTTP/HTTPS proxy scheme://url:port to use for external calls.
#

# jenkins_api_token = sometoken
EOC
    fi

    rewrite="yes"
    if [[ -f $cumin_config_path ]]; then
        echo "The configuration file $cumin_config_path already exists, overwrite?[Ny] (Ctr+C to abort)"
        read -r answer
        if ! [[ $answer =~ [yY].* ]]; then
            rewrite="no"
            echo "Skipping $cumin_config_path"
        fi
    fi
    if [[ $rewrite == "yes" ]]; then
            cat > "$cumin_config_path" <<EOC
transport: clustershell
log_file: cumin.log
#default_backend: puppetdb
default_backend: direct

# environment: {}
#    SSH_AUTH_SOCK: /run/keyholder/proxy.sock

#puppetdb:
#    host: puppetdb1002.eqiad.wmnet
#    port: 443
#    api_version: 4
#    urllib3_disable_warnings:
#      - SubjectAltNameWarning  # Temporary fix for T158757
#
#knownhosts:
#    files:
#        - cumin_ssh_known_hosts

clustershell:
    ssh_options:
        # needed for vms that repeat a name
        - |
          -o StrictHostKeyChecking=no
          -o "UserKnownHostsFile=/dev/null"
          -o "LogLevel=ERROR"
EOC
    fi

    echo "Do you want to add a link from /etc/spicerack/config.yaml? (the default place, needs sudo), otherwise " \
        "you'll have to pass the config path every time (cookbooks -c $cookbook_config_path ""...) [Ny]"
    read -r answer
    if [[ $answer =~ [yY].* ]]; then
        sudo mkdir -p "/etc/spicerack"
        sudo ln -s "$cookbook_config_path" /etc/spicerack/config.yaml
    fi
}


main "$@"
