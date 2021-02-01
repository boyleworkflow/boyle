from boyleworkflow.util import FrozenJSON, JSONData, freeze, unfreeze
from boyleworkflow.frozendict import FrozenDict


def get_data() -> JSONData:
    return {
        "key": ["to", "list", {"of": "things"}],
        "another key": 123,
        "and": [True, False, None, 3.14],
    }


def get_frozen_data() -> FrozenJSON:
    return FrozenDict(
        {
            "key": ("to", "list", FrozenDict({"of": "things"})),
            "another key": 123,
            "and": (True, False, None, 3.14),
        }
    )


def test_freezes_correctly():
    data = get_data()
    frozen_data = get_frozen_data()
    assert freeze(data) == frozen_data


def test_unfreezes_correctly():
    data = get_data()
    frozen_data = get_frozen_data()
    assert unfreeze(frozen_data) == data


def test_freeze_is_idempotent():
    data = get_data()
    assert freeze(freeze(data)) == freeze(data)


def test_unfreeze_is_idempotent():
    frozen_data = get_frozen_data()
    assert unfreeze(unfreeze(frozen_data)) == unfreeze(frozen_data)
