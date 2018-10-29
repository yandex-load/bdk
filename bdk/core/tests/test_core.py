import re
import pytest
from mock import MagicMock

from bdk.core.core import LPQJob, LPQClient


def test_lpqjob():
    mock_client = LPQClient('')
    # mock_client.get_send_status = lambda jid: MagicMock()
    mock_client.get_send_stdout = lambda jid, session: MagicMock()
    test_job = LPQJob(mock_client, {'task_id': 'abc123',
                                    'config': ''})
    with test_job.stdout_sender() as sender:
        sender('foo')
        sender('bar')
        sender('baz')
    lines = test_job.send_stdout.call_args[0][0].split('\n')
    assert len(lines) == 3
    for line, msg in zip(lines, ['foo', 'bar', 'baz']):
        ts, log = re.match(r'^(\d+)\.?\d*:: (.+)$', line).groups()
        assert len(ts) == 13 and str(int(ts)) == ts
        assert msg == log


def test_lpqjob_buffer():
    mock_client = LPQClient('')
    # mock_client.get_send_status = lambda jid: MagicMock()
    mock_client.get_send_stdout = lambda jid, session: MagicMock()
    test_job = LPQJob(mock_client, {'task_id': 'abc123',
                                    'config': ''})
    with test_job.stdout_sender(buffer_size=10) as sender:
        sender('foo')
        sender('bar')
        sender('baz')
    assert test_job.send_stdout.call_count == 3
