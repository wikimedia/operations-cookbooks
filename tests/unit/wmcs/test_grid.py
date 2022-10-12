from __future__ import annotations

import pytest

from cookbooks.wmcs.libs.common import TestUtils
from cookbooks.wmcs.libs.grid import GridQueueInfo


@pytest.mark.parametrize(
    **TestUtils.to_parametrize(
        test_cases={
            "simple case": {
                "messages": [
                    (
                        "queue webgrid-lighttpd marked QERROR as result of job 9836072's failure at host "
                        "tools-sgeweblight-10-20.tools.eqiad1.wikimedia.cloud"
                    ),
                ],
                "expected_job_ids": [9836072],
            },
        }
    )
)
def test_GridQueueInfo_gets_job_id_from_message_ok(messages: list[str], expected_job_ids: int):
    my_queue_info = GridQueueInfo(name="dummy-grid-queue", messages=messages)

    gotten_job_ids = my_queue_info.get_failed_jobs_from_message()

    assert gotten_job_ids == expected_job_ids
