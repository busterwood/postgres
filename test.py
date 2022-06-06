from __future__ import annotations # allow __enter__ to return Test
from enum import Enum
from types import TracebackType


class FailNow(Exception):
    pass

class SkipNow(Exception):
    pass

class Test:
    class State(Enum):
        FAILED = 1,
        SKIPPED = 2

    name: str
    __state: State|None
    __messages: list[str]

    def __init__(self, test_name:str):
        self.name = test_name
        self.__messages = []
        self.__state = None

    def log(self, message:str) -> None:
        """records information that is shown only if the test fails"""
        self.__messages.append(message)

    def fail(self, message:str|None = None) -> None:
        """marks the function as having failed but continues execution"""
        if message is not None:
            self.log(message)
        self.__state = Test.State.FAILED

    def fail_now(self, message:str|None = None) -> None:
        """marks the function as having failed and stops the test executing"""
        if message is not None:
            self.fail(message)
        raise FailNow()

    def skip_now(self, message:str|None = None) -> None:
        """skip_now() marks the test as having been skipped and stops its execution"""
        if message is not None:
            self.log(message)
        self.__state = Test.State.SKIPPED
        raise SkipNow()

    @property
    def failed(self) -> bool:
        """reports whether the test was failed"""
        return self.__state == Test.State.FAILED

    @property 
    def skipped(self) -> bool:
        """reports whether the test was skipped"""
        return self.__state == Test.State.SKIPPED

    def report(self):
        if self.skipped:
            print(f"\nSkipped: {self.name}")
        elif self.failed:
            mess = [f"\nFAILED: {self.name}"]
            mess.extend(self.__messages)
            print("\n  ".join(mess))
        else:
            print(".", end="", flush=True)

    def __enter__(self) -> Test:
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None) -> bool:
        if exc_value is None or isinstance(exc_value, FailNow) or isinstance(exc_value, SkipNow):
            pass
        else:
            # we caught a real exception
            self.log(str(exc_value))
            self.log(str(traceback))
            self.fail()

        self.report()
        return True # swallow exception

with Test("can do stuff") as t:
    t.log("going ok")
    # t.fail("bad, but continue")
    t.log("maybe?")


with Test("can do stuff") as t:
    pass
with Test("can do stuff") as t:
    pass


