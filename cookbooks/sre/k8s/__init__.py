"""Kubernetes cluster operations"""

# Prometheus matchers to properly downtime a k8s cluster.
# If we downtime only the hosts we may end up in alerts firing when
# we upgrade, for example due to Calico etc..
PROMETHEUS_MATCHERS = {
    "staging-codfw": [
        {
            "name": "site",
            "value": "codfw"
        },
        {
            "name": "prometheus",
            "value": "k8s-staging"
        }
    ],
    "staging-eqiad": [
        {
            "name": "site",
            "value": "eqiad"
        },
        {
            "name": "prometheus",
            "value": "k8s-staging"
        }
    ],
    "wikikube-codfw": [
        {
            "name": "site",
            "value": "codfw"
        },
        {
            "name": "prometheus",
            "value": "k8s"
        }
    ],
    "wikikube-eqiad": [
        {
            "name": "site",
            "value": "eqiad"
        },
        {
            "name": "prometheus",
            "value": "k8s"
        }
    ],
    "ml-serve-eqiad": [
        {
            "name": "site",
            "value": "eqiad"
        },
        {
            "name": "prometheus",
            "value": "k8s-mlserve"
        }
    ],
    "ml-serve-codfw": [
        {
            "name": "site",
            "value": "codfw"
        },
        {
            "name": "prometheus",
            "value": "k8s-mlserve"
        }
    ],
    "ml-staging-codfw": [
        {
            "name": "site",
            "value": "codfw"
        },
        {
            "name": "prometheus",
            "value": "k8s-mlstaging"
        }
    ],
    "dse-eqiad": [
        {
            "name": "site",
            "value": "eqiad"
        },
        {
            "name": "prometheus",
            "value": "k8s-dse"
        }
    ],
    "aux-eqiad": [
        {
            "name": "site",
            "value": "eqiad"
        },
        {
            "name": "prometheus",
            "value": "k8s-aux"
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
