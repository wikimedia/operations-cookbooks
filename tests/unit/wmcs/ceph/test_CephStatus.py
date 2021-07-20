from cookbooks.wmcs import (
    CephClusterStatus,
    CephClusterUnhealthy,
    CephOSDFlag,
    CephTestUtils,
)
import pytest
from typing import Dict, Any, Optional, List


# the examples are trimmed down from the output of `ceph status -f json-pretty`
@pytest.mark.parametrize(
    **CephTestUtils.to_parametrize(
        test_cases={
            "passes_if_HEALTH_OK": {
                "status_dict": CephTestUtils.get_status_dict(
                    {"health": {"status": "HEALTH_OK"}}
                )
            },
            "passes_if_HEALTH_WARN_and_AUTH_INSECURE_GLOBAL_ID_RECLAIM": {
                "status_dict": CephTestUtils.get_status_dict(
                    {
                        "health": {
                            "status": "HEALTH_WARN",
                            "checks": {"AUTH_INSECURE_GLOBAL_ID_RECLAIM": "some value"},
                        }
                    }
                )
            },
            "passes_if_HEALTH_WARN_and_AUTH_INSECURE_GLOBAL_ID_RECLAIM_ALLOWED": {
                "status_dict": CephTestUtils.get_status_dict(
                    {
                        "health": {
                            "status": "HEALTH_WARN",
                            "checks": {
                                "AUTH_INSECURE_GLOBAL_ID_RECLAIM_ALLOWED": "some value"
                            },
                        }
                    }
                )
            },
            "passes_if_maintenance_status_set_and_maintenance_considered_healthy": {
                "status_dict": CephTestUtils.get_status_dict(
                    {
                        "health": {
                            "status": "HEALTH_WARN",
                            "checks": {
                                "OSDMAP_FLAGS": {
                                    "summary": {
                                        "message": "noout,norebalance flag(s) set",
                                    },
                                }
                            },
                        },
                    }
                ),
                "consider_maintenance_healthy": True,
            },
        }
    )
)
def test_check_healthy_happy_path(
    status_dict: Dict[str, Any], consider_maintenance_healthy: Optional[bool]
):
    my_status = CephClusterStatus(status_dict=status_dict)
    if consider_maintenance_healthy is not None:
        my_status.check_healthy(
            consider_maintenance_healthy=consider_maintenance_healthy
        )
    else:
        my_status.check_healthy()


@pytest.mark.parametrize(
    **CephTestUtils.to_parametrize(
        test_cases={
            "raises_if_HEALTH_CRITICAL": {
                "status_dict": CephTestUtils.get_status_dict(
                    {"health": {"status": "HEALTH_CRITICAL"}}
                )
            },
            "raises_if_maintenance_status_set_but_maintenance_considered_healthy_not_set": {
                "status_dict": CephTestUtils.get_status_dict(
                    {
                        "health": {
                            "status": "HEALTH_WARN",
                            "checks": {
                                "OSDMAP_FLAGS": {
                                    "summary": {
                                        "message": "noout,norebalance flag(s) set",
                                    },
                                }
                            },
                        },
                    }
                )
            },
            "raises_if_maintenance_status_set_but_not_maintenance_considered_healthy": {
                "status_dict": CephTestUtils.get_status_dict(
                    {
                        "health": {
                            "status": "HEALTH_WARN",
                            "checks": {
                                "OSDMAP_FLAGS": {
                                    "summary": {
                                        "message": "noout,norebalance flag(s) set",
                                    },
                                }
                            },
                        },
                    }
                ),
                "consider_maintenance_healthy": False,
            },
            "raises_if_maintenance_status_set_and_other_osdmap_flags_with_consider_maintenance_healthy": {
                "status_dict": CephTestUtils.get_status_dict(
                    {
                        "health": {
                            "status": "HEALTH_WARN",
                            "checks": {
                                "OSDMAP_FLAGS": {
                                    "summary": {
                                        "message": "noout,norebalance,pause flag(s) set",
                                    },
                                }
                            },
                        },
                    }
                ),
                "consider_maintenance_healthy": True,
            },
        }
    )
)
def test_check_healthy_unhappy_path(
    status_dict: Dict[str, Any], consider_maintenance_healthy: Optional[bool]
):
    my_status = CephClusterStatus(status_dict=status_dict)

    with pytest.raises(CephClusterUnhealthy):
        if consider_maintenance_healthy is not None:
            my_status.check_healthy(
                consider_maintenance_healthy=consider_maintenance_healthy
            )
        else:
            my_status.check_healthy()


@pytest.mark.parametrize(
    **CephTestUtils.to_parametrize(
        test_cases={
            "returns_no_flags_if_none_there": {
                "status_dict": CephTestUtils.get_status_dict(
                    {
                        "health": {
                            "checks": {
                                "OSDMAP_FLAGS": {"summary": {"message": "no flags set"}}
                            }
                        }
                    },
                ),
                "expected_flags": [],
            },
            "returns_no_flags_if_no_OSDMAP_FLAGS_check": {
                "status_dict": CephTestUtils.get_status_dict(),
                "expected_flags": [],
            },
            "returns_": {
                "status_dict": CephTestUtils.get_status_dict(
                    {
                        "health": {
                            "checks": {
                                "OSDMAP_FLAGS": {
                                    "summary": {
                                        "message": "noout,norebalance,pause flag(s) set",
                                    },
                                }
                            },
                        },
                    }
                ),
                "expected_flags": [
                    CephOSDFlag("noout"),
                    CephOSDFlag("norebalance"),
                    CephOSDFlag("pause"),
                ],
            },
        }
    )
)
def test_get_osndmap_set_flags_happy_path(
    status_dict: Dict[str, Any], expected_flags: List[CephOSDFlag]
):
    my_status = CephClusterStatus(status_dict=status_dict)

    gotten_flags = my_status.get_osdmap_set_flags()

    def _order_key(flag):
        return flag.name

    assert sorted(gotten_flags, key=_order_key) == sorted(
        expected_flags, key=_order_key
    )


@pytest.mark.parametrize(
    **CephTestUtils.to_parametrize(
        test_cases={
            "returns_empty_dict_if_in_progress_not_defined": {
                "status_dict": CephTestUtils.get_status_dict(),
                "expected_in_progress": {},
            },
            "returns_empty_dict_if_in_progress_empty": {
                "status_dict": CephTestUtils.get_status_dict({"progress_events": {}}),
                "expected_in_progress": {},
            },
            "returns_progress_events_if_theres_any": {
                "status_dict": CephTestUtils.get_status_dict(
                    {"progress_events": {"event1": {}, "event2": {}}}
                ),
                "expected_in_progress": {"event1": {}, "event2": {}},
            },
        }
    )
)
def test_in_progress_happy_path(
    status_dict: Dict[str, Any], expected_in_progress: Dict[str, Any]
):
    my_status = CephClusterStatus(status_dict=status_dict)

    gotten_in_progress = my_status.get_in_progress()

    assert gotten_in_progress == expected_in_progress


@pytest.mark.parametrize(
    **CephTestUtils.to_parametrize(
        test_cases={
            "returns_true_if_only_noout_and_norebalance_set": {
                "status_dict": CephTestUtils.get_status_dict(
                    {
                        "health": {
                            "status": "HEALTH_WARN",
                            "checks": {
                                "OSDMAP_FLAGS": {
                                    "summary": {
                                        "message": "noout,norebalance flag(s) set",
                                    },
                                }
                            },
                        },
                    }
                ),
                "expected_return": True,
            },
            "returns_false_if_HEALTH_OK": {
                "status_dict": CephTestUtils.get_status_dict(
                    {
                        "health": {
                            "status": "HEALTH_OK",
                            "checks": {
                                "OSDMAP_FLAGS": {
                                    "summary": {
                                        "message": "noout,norebalance flag(s) set",
                                    },
                                }
                            },
                        },
                    }
                ),
                "expected_return": False,
            },
            "returns_false_if_theres_more_flags": {
                "status_dict": CephTestUtils.get_status_dict(
                    {
                        "health": {
                            "status": "HEALTH_WARNING",
                            "checks": {
                                "OSDMAP_FLAGS": {
                                    "summary": {
                                        "message": "noout,norebalance,pause flag(s) set",
                                    },
                                }
                            },
                        },
                    }
                ),
                "expected_return": False,
            },
            "returns_false_if_theres_more_checks": {
                "status_dict": CephTestUtils.get_status_dict(
                    {
                        "health": {
                            "status": "HEALTH_WARNING",
                            "checks": {
                                "OSDMAP_FLAGS": {
                                    "summary": {
                                        "message": "noout,norebalance flag(s) set",
                                    },
                                },
                                "AUTH_INSECURE_GLOBAL_ID_RECLAIM": {
                                    "summary": {
                                        "message": "some error",
                                    },
                                },
                            },
                        },
                    }
                ),
                "expected_return": False,
            },
            "returns_false_if_theres_no_OSDMAP_FLAGS_check": {
                "status_dict": CephTestUtils.get_status_dict(
                    {
                        "health": {
                            "status": "HEALTH_WARNING",
                            "checks": {
                                "AUTH_INSECURE_GLOBAL_ID_RECLAIM": {
                                    "summary": {
                                        "message": "some error",
                                    },
                                },
                            },
                        },
                    }
                ),
                "expected_return": False,
            },
        }
    )
)
def test_in_is_cluster_status_just_maintenance_happy_path(
    status_dict: Dict[str, Any], expected_return: bool
):
    my_status = CephClusterStatus(status_dict=status_dict)

    gotten_return = my_status.is_cluster_status_just_maintenance()

    assert gotten_return == expected_return
