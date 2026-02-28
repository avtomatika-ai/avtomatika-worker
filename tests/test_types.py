# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

from avtomatika_worker import types


def test_types_constants():
    """Tests that the constants in the types module are defined correctly."""
    assert types.TRANSIENT_ERROR == "TRANSIENT_ERROR"
    assert types.PERMANENT_ERROR == "PERMANENT_ERROR"
    assert types.INVALID_INPUT_ERROR == "INVALID_INPUT_ERROR"
