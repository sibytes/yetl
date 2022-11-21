from yetl.flow.audit._audit import Audit, AuditLevel
from unittest import TestCase

audit_log = {
    AuditLevel.DATAFLOW.value: {AuditLevel.DATASETS.value: {}},
    AuditLevel.WARNING.value: {Audit._COUNT: 0},
    AuditLevel.ERROR.value: {Audit._COUNT: 0},
}


def test_audit():

    audit = Audit()

    assert audit.audit_log == audit_log


def test_audit_exception():

    exception = Exception("TestException")
    expected = {"count": 1, "exception": "Exception", "message": str(exception)}
    audit = Audit()
    audit.error(exception)

    TestCase().assertDictEqual(audit.audit_log["error"], expected)


def test_audit_exceptions():

    exception1 = Exception("TestException1")
    exception2 = Exception("TestException2")
    expected = {"count": 2, "exception": "Exception", "message": str(exception2)}
    audit = Audit()
    audit.error(exception1)
    audit.error(exception2)

    TestCase().assertDictEqual(audit.audit_log["error"], expected)
