import pytest

from storey.dtypes import EmitEveryEvent, EmitAfterPeriod, EmitAfterWindow, _dict_to_emit_policy, EmitAfterDelay, EmitAfterMaxEvent


@pytest.mark.parametrize('emit_policy', [EmitEveryEvent, EmitAfterPeriod, EmitAfterWindow])
def test_emit_policy_basic(emit_policy):
    policy_dict = {'mode': emit_policy.name()}
    policy = _dict_to_emit_policy(policy_dict)
    assert type(policy) == emit_policy


@pytest.mark.parametrize('emit_policy', [EmitAfterDelay, EmitAfterMaxEvent])
def test_emit_policy_bad_parameters(emit_policy):
    policy_dict = {'mode': emit_policy.name()}
    try:
        _dict_to_emit_policy(policy_dict)
        assert False
    except ValueError:
        pass


def test_emit_policy_wrong_type():
    policy_dict = {'mode': 'd-o-g-g'}
    try:
        _dict_to_emit_policy(policy_dict)
        assert False
    except TypeError:
        pass


def test_emit_policy_wrong_args():
    policy_dict = {'mode': EmitAfterWindow.name(), 'daily': 8}
    try:
        _dict_to_emit_policy(policy_dict)
        assert False
    except ValueError:
        pass


def test_emit_policy_delay():
    policy_dict = {'mode': EmitAfterDelay.name(), 'delay': 8}
    policy = _dict_to_emit_policy(policy_dict)
    assert type(policy) == EmitAfterDelay
    assert policy.delay_in_seconds == 8


def test_emit_policy_max_events():
    policy_dict = {'mode': EmitAfterMaxEvent.name(), 'maxEvents': 8}
    policy = _dict_to_emit_policy(policy_dict)
    assert type(policy) == EmitAfterMaxEvent
    assert policy.max_events == 8


def test_emit_policy_window():
    policy_dict = {'mode': EmitAfterWindow.name(), 'delay': 8}
    policy = _dict_to_emit_policy(policy_dict)
    assert type(policy) == EmitAfterWindow
    assert policy.delay_in_seconds == 8


def test_emit_policy_period():
    policy_dict = {'mode': EmitAfterPeriod.name(), 'delay': 8}
    policy = _dict_to_emit_policy(policy_dict)
    assert type(policy) == EmitAfterPeriod
    assert policy.delay_in_seconds == 8
