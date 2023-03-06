"""Kubernetes cluster operations"""
from typing import Union

# Prometheus matchers to properly downtime a k8s cluster.
# If we downtime only the hosts we may end up in alerts firing when
# we upgrade, for example due to Calico etc..
PROMETHEUS_MATCHERS: dict[str, list[dict[str, Union[str, int, float, bool]]]] = {
    "staging-codfw": [
        {
            "name": "site",
            "value": "codfw",
            "isRegex": False
        },
        {
            "name": "prometheus",
            "value": "k8s-staging",
            "isRegex": False
        }
    ],
    "staging-eqiad": [
        {
            "name": "site",
            "value": "eqiad",
            "isRegex": False
        },
        {
            "name": "prometheus",
            "value": "k8s-staging",
            "isRegex": False
        }
    ],
    "wikikube-codfw": [
        {
            "name": "site",
            "value": "codfw",
            "isRegex": False
        },
        {
            "name": "prometheus",
            "value": "k8s",
            "isRegex": False
        }
    ],
    "wikikube-eqiad": [
        {
            "name": "site",
            "value": "eqiad",
            "isRegex": False
        },
        {
            "name": "prometheus",
            "value": "k8s",
            "isRegex": False
        }
    ],
    "ml-serve-eqiad": [
        {
            "name": "site",
            "value": "eqiad",
            "isRegex": False
        },
        {
            "name": "prometheus",
            "value": "k8s-mlserve",
            "isRegex": False
        }
    ],
    "ml-serve-codfw": [
        {
            "name": "site",
            "value": "codfw",
            "isRegex": False
        },
        {
            "name": "prometheus",
            "value": "k8s-mlserve",
            "isRegex": False
        }
    ],
    "ml-staging-codfw": [
        {
            "name": "site",
            "value": "codfw",
            "isRegex": False
        },
        {
            "name": "prometheus",
            "value": "k8s-mlstaging",
            "isRegex": False
        }
    ],
    "dse-eqiad": [
        {
            "name": "site",
            "value": "eqiad",
            "isRegex": False
        },
        {
            "name": "prometheus",
            "value": "k8s-dse",
            "isRegex": False
        }
    ],
    "aux-eqiad": [
        {
            "name": "site",
            "value": "eqiad",
            "isRegex": False
        },
        {
            "name": "prometheus",
            "value": "k8s-aux",
            "isRegex": False
        }
    ]
}

ALLOWED_CUMIN_ALIASES = {
    "staging-codfw": {
        "etcd": "wikikube-staging-etcd-codfw",
        "control-plane": "wikikube-staging-master-codfw",
        "workers": "wikikube-staging-worker-codfw"
    },
    "staging-eqiad": {
        "etcd": "wikikube-staging-etcd-eqiad",
        "control-plane": "wikikube-staging-master-eqiad",
        "workers": "wikikube-staging-worker-eqiad"
    },
    "wikikube-codfw": {
        "etcd": "wikikube-etcd-codfw",
        "control-plane": "wikikube-master-codfw",
        "workers": "wikikube-worker-codfw"
    },
    "wikikube-eqiad": {
        "etcd": "wikikube-etcd-eqiad",
        "control-plane": "wikikube-master-eqiad",
        "workers": "wikikube-worker-eqiad"
    },
    "ml-serve-eqiad": {
        "etcd": "ml-serve-etcd-eqiad",
        "control-plane": "ml-serve-master-eqiad",
        "workers": "ml-serve-worker-eqiad"
    },
    "ml-serve-codfw": {
        "etcd": "ml-serve-etcd-codfw",
        "control-plane": "ml-serve-master-codfw",
        "workers": "ml-serve-worker-codfw"
    },
    "ml-staging-codfw": {
        "etcd": "ml-staging-etcd",
        "control-plane": "ml-staging-master",
        "workers": "ml-staging-worker"
    },
    "dse-eqiad": {
        "etcd": "dse-k8s-etcd",
        "control-plane": "dse-k8s-master",
        "workers": "dse-k8s-worker"
    },
    "aux-eqiad": {
        "etcd": "aux-etcd",
        "control-plane": "aux-master",
        "workers": "aux-worker"
    },
}
