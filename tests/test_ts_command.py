from multiprocessing.util import info

import pytest

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestTimeSeriesCommand(ValkeyTimeSeriesTestCaseBase):
    """Verifies the ``COMMAND INFO`` metadata (arity, flags and legacy key range) that the
    module registers for every user-facing ``TS.*`` command.

    The expectations below are derived directly from the
    ``#[valkey_module_macros::command(...)]`` annotations on each command handler in
    ``src/commands/*`` and were confirmed against a running server. They intentionally mirror
    the observed code rather than any prior assumption about the metadata.
    """

    # command -> (arity, first_key, last_key, step, flags)
    #
    # Selector-based commands (MGET, MRANGE, QUERYINDEX, CARD, the LABEL* family, ...) match
    # series by FILTER expressions rather than positional keys, so they expose no key range
    # (0, 0, 0). MADD takes a key every third argument (1, -1, 3); JOIN/CREATERULE/DELETERULE
    # take two adjacent keys (1, 2, 1); the remaining keyed commands take a single key at
    # position 1 (1, 1, 1). NRANGE/NREVRANGE take a `numkeys key [key ...]` prefix, which the
    # legacy triple cannot express: they report no key range and carry `movablekeys` instead,
    # so clients must ask COMMAND GETKEYS (see test_commands.py).
    COMMAND_INFO = {
        "TS.CREATE":      (-2, 1,  1, 1, [b"write", b"denyoom", b"module"]),
        "TS.ALTER":       (-2, 1,  1, 1, [b"write", b"denyoom", b"module"]),
        "TS.ADD":         (-4, 1,  1, 1, [b"write", b"denyoom", b"module"]),
        "TS.ADDBULK":     (-3, 1,  1, 1, [b"write", b"denyoom", b"module"]),
        "TS.GET":         (-2, 1,  1, 1, [b"readonly", b"module", b"fast"]),
        "TS.MGET":        (-2, 0,  0, 0, [b"readonly", b"module", b"fast"]),
        "TS.MADD":        (-4, 1, -1, 3, [b"write", b"denyoom", b"module"]),
        "TS.DEL":         (-3, 1,  1, 1, [b"write", b"denyoom", b"module"]),
        "TS.DECRBY":      (-3, 1,  1, 1, [b"write", b"denyoom", b"module"]),
        "TS.INCRBY":      (-3, 1,  1, 1, [b"write", b"denyoom", b"module"]),
        "TS.JOIN":        (-4, 1,  2, 1, [b"readonly", b"module"]),
        "TS.MDEL":        (-2, 0,  0, 0, [b"write", b"denyoom", b"module"]),
        "TS.MRANGE":      (-4, 0,  0, 0, [b"readonly", b"module"]),
        "TS.MREVRANGE":   (-4, 0,  0, 0, [b"readonly", b"module"]),
        "TS.NRANGE":      (-5, 0,  0, 0, [b"readonly", b"module", b"movablekeys"]),
        "TS.NREVRANGE":   (-5, 0,  0, 0, [b"readonly", b"module", b"movablekeys"]),
        "TS.RANGE":       (-4, 1,  1, 1, [b"readonly", b"module"]),
        "TS.READ":        (-3, 1,  1, 1, [b"readonly", b"module"]),
        "TS.REVRANGE":    (-4, 1,  1, 1, [b"readonly", b"module"]),
        "TS.INFO":        (-2, 1,  1, 1, [b"readonly", b"module"]),
        "TS.QUERYINDEX":  (-2, 0,  0, 0, [b"readonly", b"module"]),
        "TS.CARD":        (-1, 0,  0, 0, [b"readonly", b"module"]),
        "TS.LABELNAMES":  (-1, 0,  0, 0, [b"readonly", b"module"]),
        "TS.LABELVALUES": (-2, 0,  0, 0, [b"readonly", b"module"]),
        "TS.METRICNAMES": (-1, 0,  0, 0, [b"readonly", b"module"]),
        "TS.LABELSTATS":  (-1, 0,  0, 0, [b"readonly", b"module"]),
        "TS.CREATERULE":  (-6, 1,  2, 1, [b"write", b"denyoom", b"module"]),
        "TS.DELETERULE":   (3, 1,  2, 1, [b"write", b"denyoom", b"module"]),
        "TS.OUTLIERS":    (-6, 1,  1, 1, [b"readonly", b"denyoom", b"module"]),
        # Analysis/forecasting commands. Those accepting a STORE clause declare a second,
        # keyword-based key spec for the destination series, which the server reports as
        # `movablekeys`; their legacy key range still covers only the source key at
        # position 1. TS.SANITIZE writes the sanitized samples back to the source series,
        # so its source key spec is RW rather than RO.
        "TS.XCORR":          (-6, 1, 2, 1, [b"readonly", b"denyoom", b"module"]),
        "TS.BACKTEST":       (-8, 1, 1, 1, [b"readonly", b"denyoom", b"module"]),
        "TS.DECOMPOSE":      (-4, 1, 1, 1, [b"readonly", b"denyoom", b"module"]),
        "TS.PERIODS":        (-4, 1, 1, 1, [b"readonly", b"denyoom", b"module"]),
        "TS.AUTOCORRELATION":(-5, 1, 1, 1, [b"readonly", b"denyoom", b"module"]),
        "TS.STATS":          (-2, 1, 1, 1, [b"readonly", b"denyoom", b"module"]),
        "TS.FEATURES":       (-4, 1, 1, 1, [b"readonly", b"denyoom", b"module"]),
        "TS.STATIONARITY":   (-4, 1, 1, 1, [b"readonly", b"denyoom", b"module"]),
        "TS.TREND":          (-4, 1, 1, 1, [b"write", b"denyoom", b"module", b"movablekeys"]),
        "TS.FORECAST":       (-8, 1, 1, 1, [b"write", b"denyoom", b"module", b"movablekeys"]),
        "TS.AUTOFORECAST":   (-5, 1, 1, 1, [b"write", b"denyoom", b"module", b"movablekeys"]),
        "TS.FILLGAPS":       (-4, 1, 1, 1, [b"write", b"denyoom", b"module", b"movablekeys"]),
        "TS.SANITIZE":       (-4, 1, 1, 1, [b"write", b"denyoom", b"module", b"movablekeys"]),
    }

    # Commands whose optional STORE clause names a destination series, and the argument
    # prefix needed to reach that clause. COMMAND GETKEYS must report the destination in
    # addition to the source key so the server can route and ACL-check it.
    STORE_COMMANDS = {
        "TS.FILLGAPS": ["src", "0", "100"],
        "TS.SANITIZE": ["src", "0", "100"],
        "TS.TREND": ["src", "0", "100"],
        "TS.FORECAST": ["src", "0", "100", "MODELS", "AutoARIMA", "HORIZON", "5"],
        "TS.AUTOFORECAST": ["src", "0", "100", "HORIZON", "5"],
    }

    def command_info(self, command):
        # Use the single-string form so the raw reply is returned unmodified. The multi-arg
        # form ('COMMAND', 'INFO', command) triggers the client's COMMAND response callback,
        # which reshapes the reply.
        info = self.client.execute_command(f"COMMAND INFO {command}")
        assert info and info[0] is not None, f"Command {command} is not registered"
        return info[0]

    def getkeys(self, command, *args):
        # As with command_info, the single-string form avoids the client's COMMAND
        # response callback, which would otherwise try to reshape the key list.
        argv = " ".join(str(a) for a in args)
        keys = self.client.execute_command(f"COMMAND GETKEYS {command} {argv}")
        return [k.decode() if isinstance(k, bytes) else k for k in keys]

    def test_command_arity(self):
        for command, expected in self.COMMAND_INFO.items():
            info = self.command_info(command)
            assert info[1] == expected[0], (
                f"Arity mismatch for '{command}': expected {expected[0]}, got {info[1]}"
            )

    def test_command_flags(self):
        for command, expected in self.COMMAND_INFO.items():
            info = self.command_info(command)
            assert sorted(info[2]) == sorted(expected[4]), (
                f"Flags mismatch for '{command}': expected {expected[4]}, got {info[2]}"
            )

    def test_command_key_range(self):
        for command, expected in self.COMMAND_INFO.items():
            info = self.command_info(command)
            first, last, step = info[3], info[4], info[5]
            assert (first, last, step) == (expected[1], expected[2], expected[3]), (
                f"Key range mismatch for '{command}': expected "
                f"{(expected[1], expected[2], expected[3])}, got {(first, last, step)}"
            )

    # COMMAND INFO reply layout (Redis/Valkey 7+): name, arity, flags, first_key, last_key,
    # step, acl_categories, tips, key_specs, subcommands.
    TIPS_INDEX = 7
    KEY_SPECS_INDEX = 8

    def test_ts_read_declares_the_dont_cache_tip(self):
        # TS.READ is the only command in this module that carries a command tip, and it is
        # deliberately matched to the 8.10 reference rather than registered as a metadata
        # divergence: the macro's `tips` field can express it. The tip is load-bearing —
        # TS.READ's reply depends on when it is served, so a caching proxy must not reuse one.
        info = self.command_info("TS.READ")
        tips = [t.decode() if isinstance(t, bytes) else t for t in info[self.TIPS_INDEX]]
        assert tips == ["dont_cache"], (
            f"TS.READ should declare exactly the dont_cache tip, got {tips}"
        )

    def test_only_ts_read_declares_a_tip(self):
        # Guards the claim above: if another command grows a tip, this test should be updated
        # deliberately rather than the assertion above silently becoming unrepresentative.
        for command in self.COMMAND_INFO:
            if command == "TS.READ":
                continue
            info = self.command_info(command)
            assert not info[self.TIPS_INDEX], (
                f"{command} unexpectedly declares tips {info[self.TIPS_INDEX]}"
            )

    def test_ts_read_key_spec_declares_access(self):
        # A deliberate, recorded difference from the reference, which declares `RO` only for
        # TS.READ. This module's read commands declare RO+ACCESS throughout (see ts_get.rs);
        # ACCESS is the semantically correct flag for a command that returns key data to the
        # caller, and dropping it would weaken ACL key-permission checking. Asserted here so
        # the difference stays intentional instead of drifting.
        info = self.command_info("TS.READ")
        specs = info[self.KEY_SPECS_INDEX]
        assert len(specs) == 1, f"TS.READ should declare one key spec, got {specs}"
        spec = {specs[0][i].decode(): specs[0][i + 1] for i in range(0, len(specs[0]), 2)}
        flags = {f.decode() if isinstance(f, bytes) else f for f in spec["flags"]}
        assert "RO" in flags, f"TS.READ key spec should be read-only, got {flags}"
        assert "access" in {f.lower() for f in flags}, (
            f"TS.READ key spec should declare ACCESS (module convention), got {flags}"
        )

    def test_getkeys_reports_store_destination(self):
        """The STORE destination must be discoverable via COMMAND GETKEYS.

        The keyword-based key spec is what lets the server route and ACL-check the
        destination series; without it only the source key would be reported.
        """
        for command, prefix in self.STORE_COMMANDS.items():
            keys = self.getkeys(command, *prefix, "STORE", "dest")
            assert keys == ["src", "dest"], (
                f"GETKEYS mismatch for '{command}' with STORE: expected "
                f"['src', 'dest'], got {keys}"
            )

    def test_getkeys_without_store(self):
        """Without a STORE clause only the source key is reported."""
        for command, prefix in self.STORE_COMMANDS.items():
            keys = self.getkeys(command, *prefix)
            assert keys == ["src"], (
                f"GETKEYS mismatch for '{command}' without STORE: expected "
                f"['src'], got {keys}"
            )
